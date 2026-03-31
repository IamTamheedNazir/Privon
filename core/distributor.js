const { decryptPayload, encryptPayload, isEncryptionEnabled } = require("./cryptoTransport");
const { createOpaqueId } = require("./createOpaqueId");
const { logPipelineStage, validateFragment, validateNodeResponse } = require("./pipelineValidation");

const PIPELINE_STAGE_ORDER = ["fetch", "filter", "rank", "verify"];

function buildStageResult(stage, details = {}) {
  return {
    stage,
    durationMs: Math.max(0, Number(details.durationMs || 0)),
    success: details.success !== false,
    nodes: Array.isArray(details.nodes) ? [...new Set(details.nodes.filter(Boolean))] : [],
    summary: details.summary || "",
  };
}

function aggregateResponsePipelineDiagnostics(responses) {
  const groupedStages = new Map();

  for (const response of responses) {
    for (const stageEntry of response.response?.pipelineDiagnostics || []) {
      const current = groupedStages.get(stageEntry.stage) || {
        stage: stageEntry.stage,
        durationMs: 0,
        samples: 0,
        success: true,
        nodes: new Set(),
      };

      current.durationMs += Number(stageEntry.durationMs || 0);
      current.samples += 1;
      current.success = current.success && stageEntry.success !== false;
      for (const nodeUrl of stageEntry.nodes || [response.nodeUrl]) {
        if (nodeUrl) {
          current.nodes.add(nodeUrl);
        }
      }
      groupedStages.set(stageEntry.stage, current);
    }
  }

  return PIPELINE_STAGE_ORDER
    .filter((stage) => groupedStages.has(stage))
    .map((stage) => {
      const entry = groupedStages.get(stage);
      return buildStageResult(stage, {
        durationMs: entry.samples === 0 ? 0 : Math.round(entry.durationMs / entry.samples),
        success: entry.success,
        nodes: [...entry.nodes],
      });
    });
}
function createNodeError(node, message) {
  const error = new Error(message);
  error.nodeUrl = node?.url;
  error.node = node;
  return error;
}

function clampRatio(value) {
  return Math.max(0, Math.min(1, Number(value ?? 0)));
}

function normalizeNodes(nodes) {
  return nodes.map((node) => ({
    url: node.url.trim().replace(/\/$/, ""),
    capacity: Math.max(1, Number(node.capacity || 1)),
    status: node.status,
    score: Number(node.score ?? 100),
    totalTasks: Number(node.totalTasks ?? 0),
    successfulTasks: Number(node.successfulTasks ?? 0),
    failedTasks: Number(node.failedTasks ?? 0),
    shardId: String(node.shardId || "default-shard"),
    replicaGroup: String(node.replicaGroup || "default-replica"),
  }));
}

function validateNodes(nodes, options = {}) {
  const isNodeSelectable = options.isNodeSelectable || ((node) => node.status === "active");
  const isProbationNode = options.isProbationNode || ((node) => node.status === "probation");

  if (!Array.isArray(nodes) || nodes.length === 0) {
    throw new Error("At least one active node is required.");
  }

  const validNodes = nodes.filter((node) => node && typeof node.url === "string");
  const activeNodes = validNodes.filter((node) => isNodeSelectable(node));
  const probationNodes = validNodes.filter((node) => isProbationNode(node));

  if (activeNodes.length === 0) {
    throw new Error("No active nodes are currently available.");
  }

  return {
    activeNodes: normalizeNodes(activeNodes),
    probationNodes: normalizeNodes(probationNodes),
  };
}

function groupNodesByReplicaGroup(nodes) {
  const groups = new Map();

  for (const node of nodes) {
    const groupKey = node.replicaGroup || "default-replica";
    const nextGroup = groups.get(groupKey) || [];
    nextGroup.push(node);
    groups.set(groupKey, nextGroup);
  }

  return groups;
}

function chooseCandidateNodes(nodes, assignmentCounts, fragmentIndex) {
  const tieBreakerBase = fragmentIndex % nodes.length;

  return [...nodes].sort((left, right) => {
    const scoreDelta = right.score - left.score;

    if (scoreDelta !== 0) {
      return scoreDelta;
    }

    const leftLoad = (assignmentCounts.get(left.url) || 0) / left.capacity;
    const rightLoad = (assignmentCounts.get(right.url) || 0) / right.capacity;

    if (leftLoad !== rightLoad) {
      return leftLoad - rightLoad;
    }

    const leftTieBreak =
      (nodes.indexOf(left) - tieBreakerBase + nodes.length) % nodes.length;
    const rightTieBreak =
      (nodes.indexOf(right) - tieBreakerBase + nodes.length) % nodes.length;

    return leftTieBreak - rightTieBreak;
  });
}

function normalizeMatches(matches) {
  return [...(matches || [])]
    .map((match) => ({
      documentId: match.documentId,
      title: match.title,
      snippet: match.snippet,
      score: match.score,
      matchedFragments: [...(match.matchedFragments || [])].sort(),
    }))
    .sort((left, right) => {
      const idDelta = String(left.documentId).localeCompare(String(right.documentId));

      if (idDelta !== 0) {
        return idDelta;
      }

      const titleDelta = String(left.title).localeCompare(String(right.title));

      if (titleDelta !== 0) {
        return titleDelta;
      }

      const scoreDelta = Number(left.score || 0) - Number(right.score || 0);

      if (scoreDelta !== 0) {
        return scoreDelta;
      }

      return String(left.snippet).localeCompare(String(right.snippet));
    });
}

function normalizeResponseForComparison(response) {
  return JSON.stringify({
    fragmentIndex: Number(response?.fragmentIndex ?? -1),
    matches: normalizeMatches(response?.matches),
  });
}

function responsesMatch(responses) {
  if (!Array.isArray(responses) || responses.length === 0) {
    return false;
  }

  const baseline = normalizeResponseForComparison(responses[0].response);

  return responses.every(
    (response) => normalizeResponseForComparison(response.response) === baseline,
  );
}

async function executeOnNode(node, fragment, fragmentIndex, options = {}) {
  validateFragment(fragment, "verify");
  const limit = options.limit ?? 5;
  const requestTimeoutMs = options.requestTimeoutMs ?? 3000;
  const assignmentCounts = options.assignmentCounts || new Map();
  const onTaskAssigned = options.onTaskAssigned || (() => {});
  const taskId = createOpaqueId("task");
  const encryptionEnabled = isEncryptionEnabled(options);
  const requestPayload = {
    taskId,
    fragment,
    fragmentIndex,
    limit,
  };

  assignmentCounts.set(node.url, (assignmentCounts.get(node.url) || 0) + 1);
  onTaskAssigned(node);

  try {
    const response = await fetch(`${node.url}/compute`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      signal: AbortSignal.timeout(requestTimeoutMs),
      body: JSON.stringify(encryptPayload(requestPayload, options)),
    });

    if (!response.ok) {
      let errorBody = null;

      try {
        errorBody = await response.json();
      } catch (_error) {
        errorBody = null;
      }

      const stageSuffix = errorBody?.stage ? ` stage=${errorBody.stage}` : "";
      const messageSuffix = errorBody?.error ? ` ${errorBody.error}` : "";
      throw createNodeError(node, `Node ${node.url} returned ${response.status}${stageSuffix}${messageSuffix}`.trim());
    }

    const responseBody = await response.json();
    const decryptedResponse = decryptPayload(responseBody, options);
    validateNodeResponse(decryptedResponse, fragment);
    logPipelineStage(options.log, "verify", {
      fragmentIndex,
      nodeUrl: node.url,
      response: decryptedResponse,
    });

    return {
      node,
      nodeUrl: node.url,
      response: decryptedResponse,
      encrypted: encryptionEnabled,
    };
  } catch (error) {
    if (error.nodeUrl) {
      throw error;
    }

    throw createNodeError(
      node,
      error.name === "TimeoutError"
        ? `Node ${node.url} timed out`
        : `Node ${node.url} failed: ${error.message}`,
    );
  }
}

async function collectVerifiedResponses(fragment, fragmentIndex, candidateNodes, options = {}) {
  const redundancyFactor = options.redundancyFactor ?? 2;
  const maxRetries = options.maxRetries ?? 3;
  const log = options.log || (() => {});
  const onTaskFailure = options.onTaskFailure || (() => {});
  const attemptedNodeUrls = [];
  const failures = [];
  const successes = [];
  const maxAttempts = Math.min(candidateNodes.length, redundancyFactor + maxRetries);
  let cursor = 0;

  while (successes.length < redundancyFactor && cursor < maxAttempts) {
    const neededResponses = redundancyFactor - successes.length;
    const batchNodes = candidateNodes.slice(cursor, cursor + neededResponses);
    cursor += batchNodes.length;

    if (batchNodes.length === 0) {
      break;
    }

    for (const node of batchNodes) {
      attemptedNodeUrls.push(node.url);
    }

    log(
      `[distributor] fragment ${fragmentIndex} executing on ${batchNodes.map((node) => node.url).join(", ")}`,
    );

    const settledResponses = await Promise.allSettled(
      batchNodes.map((node) =>
        executeOnNode(node, fragment, fragmentIndex, {
          ...options,
        }),
      ),
    );

    for (const settledResponse of settledResponses) {
      if (settledResponse.status === "fulfilled") {
        successes.push(settledResponse.value);
        log(
          `[distributor] fragment ${fragmentIndex} completed on ${settledResponse.value.nodeUrl}`,
        );
        continue;
      }

      failures.push({
        nodeUrl: settledResponse.reason.nodeUrl,
        message: settledResponse.reason.message,
      });
      onTaskFailure(settledResponse.reason.node, settledResponse.reason.message);
      log(
        `[distributor] fragment ${fragmentIndex} failed on ${settledResponse.reason.nodeUrl}: ${settledResponse.reason.message}`,
      );
    }

    if (successes.length < redundancyFactor && cursor < maxAttempts) {
      log(`[distributor] fragment ${fragmentIndex} retrying failed copy on another node`);
    }
  }

  return {
    responses: successes.slice(0, redundancyFactor),
    failures,
    attemptedNodeUrls,
  };
}

async function runProbationCheck(
  fragment,
  fragmentIndex,
  acceptedResponse,
  probationNodes,
  options = {},
) {
  const probationTrafficRatio = clampRatio(options.probationTrafficRatio ?? 0.1);
  const random = options.random || Math.random;
  const assignmentCounts = options.assignmentCounts || new Map();
  const log = options.log || (() => {});
  const onTaskFailure = options.onTaskFailure || (() => {});
  const onProbationSuccess = options.onProbationSuccess || (() => {});

  if (probationNodes.length === 0 || probationTrafficRatio === 0 || random() >= probationTrafficRatio) {
    return {
      attempted: false,
    };
  }

  const candidateNode = chooseCandidateNodes(probationNodes, assignmentCounts, fragmentIndex)[0];

  if (!candidateNode) {
    return {
      attempted: false,
    };
  }

  log(
    `[distributor] fragment ${fragmentIndex} sending shadow execution to probation node ${candidateNode.url}`,
  );

  try {
    const probationResponse = await executeOnNode(candidateNode, fragment, fragmentIndex, options);

    if (
      normalizeResponseForComparison(probationResponse.response)
      === normalizeResponseForComparison(acceptedResponse)
    ) {
      onProbationSuccess(probationResponse.node);
      log(`[distributor] fragment ${fragmentIndex} probation verification succeeded on ${candidateNode.url}`);

      return {
        attempted: true,
        matched: true,
        nodeUrl: candidateNode.url,
      };
    }

    onTaskFailure(probationResponse.node, "Probation result mismatch");
    log(`[distributor] fragment ${fragmentIndex} probation mismatch on ${candidateNode.url}`);

    return {
      attempted: true,
      matched: false,
      nodeUrl: candidateNode.url,
      message: "Probation result mismatch",
    };
  } catch (error) {
    onTaskFailure(error.node, error.message);
    log(`[distributor] fragment ${fragmentIndex} probation node failed on ${candidateNode.url}: ${error.message}`);

    return {
      attempted: true,
      matched: false,
      nodeUrl: candidateNode.url,
      message: error.message,
    };
  }
}

async function executeReplicaGroup(fragment, fragmentIndex, replicaGroup, activeNodes, probationNodes, options = {}) {
  const redundancyFactor = options.redundancyFactor ?? 2;
  const verificationRetryRounds = options.verificationRetryRounds ?? 2;
  const assignmentCounts = options.assignmentCounts || new Map();
  const isNodeSelectable = options.isNodeSelectable || (() => true);
  const onTaskSuccess = options.onTaskSuccess || (() => {});
  const onTaskFailure = options.onTaskFailure || (() => {});
  const log = options.log || (() => {});
  const usedNodeUrls = new Set();
  const failures = [];
  const totalRounds = verificationRetryRounds + 1;
  const shardIds = [...new Set(activeNodes.map((node) => node.shardId))];
  const verifyStartedAt = Date.now();

  for (let roundIndex = 0; roundIndex < totalRounds; roundIndex += 1) {
    const availableNodes = chooseCandidateNodes(
      activeNodes.filter(
        (node) => !usedNodeUrls.has(node.url) && isNodeSelectable(node),
      ),
      assignmentCounts,
      fragmentIndex,
    );

    if (availableNodes.length < redundancyFactor) {
      failures.push({
        round: roundIndex + 1,
        message: `Replica group ${replicaGroup} has only ${availableNodes.length} active node(s); ${redundancyFactor} required`,
      });

      return {
        success: false,
        fragmentIndex,
        replicaGroup,
        shardIds,
        failures,
        pipeline: [
          buildStageResult("verify", {
            durationMs: Date.now() - verifyStartedAt,
            success: false,
            nodes: availableNodes.map((node) => node.url),
            summary: "Insufficient active nodes for verification",
          }),
        ],
      };
    }

    log(`[distributor] fragment ${fragmentIndex} starting verification round ${roundIndex + 1}/${totalRounds} for replica group ${replicaGroup}`);
    logPipelineStage(log, "verify", {
      fragmentIndex,
      replicaGroup,
      round: roundIndex + 1,
      expectedResponses: redundancyFactor,
      candidateNodes: availableNodes,
    });

    const roundResult = await collectVerifiedResponses(fragment, fragmentIndex, availableNodes, {
      ...options,
      assignmentCounts,
    });

    for (const nodeUrl of roundResult.attemptedNodeUrls) {
      usedNodeUrls.add(nodeUrl);
    }

    failures.push(
      ...roundResult.failures.map((failure) => ({
        round: roundIndex + 1,
        nodeUrl: failure.nodeUrl,
        message: failure.message,
      })),
    );

    if (roundResult.responses.length < redundancyFactor) {
      failures.push({
        round: roundIndex + 1,
        message: `Unable to collect ${redundancyFactor} successful responses for replica group ${replicaGroup}`,
      });

      return {
        success: false,
        fragmentIndex,
        replicaGroup,
        shardIds,
        failures,
        pipeline: [
          buildStageResult("verify", {
            durationMs: Date.now() - verifyStartedAt,
            success: false,
            nodes: availableNodes.map((node) => node.url),
            summary: "Insufficient active nodes for verification",
          }),
        ],
      };
    }

    log(
      `[distributor] fragment ${fragmentIndex} comparing responses from ${roundResult.responses.map((response) => response.nodeUrl).join(", ")} in replica group ${replicaGroup}`,
    );

    if (responsesMatch(roundResult.responses)) {
      for (const response of roundResult.responses) {
        onTaskSuccess(response.node);
      }

      log(`[distributor] fragment ${fragmentIndex} verification succeeded for replica group ${replicaGroup}`);
      logPipelineStage(log, "verify", {
        fragmentIndex,
        replicaGroup,
        verified: true,
        nodeUrls: roundResult.responses.map((response) => response.nodeUrl),
      });

      const probationResult = await runProbationCheck(
        fragment,
        fragmentIndex,
        roundResult.responses[0].response,
        probationNodes,
        {
          ...options,
          assignmentCounts,
        },
      );

      return {
        success: true,
        fragmentIndex,
        replicaGroup,
        shardIds,
        nodeUrls: roundResult.responses.map((response) => response.nodeUrl),
        response: roundResult.responses[0].response,
        probation: probationResult,
        pipeline: [
          ...aggregateResponsePipelineDiagnostics(roundResult.responses),
          buildStageResult("verify", {
            durationMs: Date.now() - verifyStartedAt,
            success: true,
            nodes: roundResult.responses.map((response) => response.nodeUrl),
            summary: "Redundant responses matched",
          }),
        ],
      };
    }

    for (const response of roundResult.responses) {
      onTaskFailure(response.node, `Result mismatch across redundant executions in replica group ${replicaGroup}`);
    }

    failures.push({
      round: roundIndex + 1,
      message: `Result mismatch across redundant executions in replica group ${replicaGroup}`,
      nodeUrls: roundResult.responses.map((response) => response.nodeUrl),
    });
    log(`[distributor] fragment ${fragmentIndex} mismatch detected in replica group ${replicaGroup}`);
    logPipelineStage(log, "verify", {
      fragmentIndex,
      replicaGroup,
      verified: false,
      nodeUrls: roundResult.responses.map((response) => response.nodeUrl),
      failures: roundResult.failures,
    });

    if (roundIndex < totalRounds - 1) {
      log(`[distributor] fragment ${fragmentIndex} retrying verification with new nodes in replica group ${replicaGroup}`);
    }
  }

  return {
    success: false,
    fragmentIndex,
    replicaGroup,
    shardIds,
    failures,
    pipeline: [
      buildStageResult("verify", {
        durationMs: Date.now() - verifyStartedAt,
        success: false,
        nodes: [...usedNodeUrls],
        summary: "Verification rounds exhausted",
      }),
    ],
  };
}

async function distributeFragments(fragments, nodes, options = {}) {
  const redundancyFactor = options.redundancyFactor ?? 2;
  const isNodeSelectable = options.isNodeSelectable || ((node) => node.status === "active");
  const isProbationNode = options.isProbationNode || ((node) => node.status === "probation");
  const { activeNodes, probationNodes } = validateNodes(nodes, {
    isNodeSelectable,
    isProbationNode,
  });
  const activeGroups = groupNodesByReplicaGroup(activeNodes);
  const probationGroups = groupNodesByReplicaGroup(probationNodes);
  const assignmentCounts = new Map(
    [...activeNodes, ...probationNodes].map((node) => [node.url, 0]),
  );
  const fragmentResults = [];

  if (!Array.isArray(fragments) || fragments.length === 0) {
    throw new Error("A non-empty fragments array is required.");
  }

  logPipelineStage(options.log, "verify", {
    fragments,
    activeNodes,
    probationNodes,
  });

  for (const [fragmentIndex, fragment] of fragments.entries()) {
    validateFragment(fragment, "verify");

    for (const [replicaGroup, activeGroupNodes] of activeGroups.entries()) {
      const probationGroupNodes = probationGroups.get(replicaGroup) || [];
      const fragmentResult = await executeReplicaGroup(
        fragment,
        fragmentIndex,
        replicaGroup,
        activeGroupNodes,
        probationGroupNodes,
        {
          ...options,
          redundancyFactor,
          assignmentCounts,
          isNodeSelectable,
        },
      );

      fragmentResults.push(fragmentResult);
    }
  }

  return {
    fragmentResults,
    partialResponses: fragmentResults
      .filter((result) => result.success)
      .map((result) => result.response),
    failures: fragmentResults
      .filter((result) => !result.success)
      .map((result) => ({
        fragmentIndex: result.fragmentIndex,
        replicaGroup: result.replicaGroup,
        attempts: result.failures,
      })),
  };
}

module.exports = {
  distributeFragments,
};
