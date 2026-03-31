const DEFAULT_SCORE = 100;
const MIN_SCORE = 0;
const MAX_SCORE = 200;
const DEFAULT_PROBATION_THRESHOLD = 20;
const DEFAULT_REINTEGRATION_THRESHOLD = 40;
const DEFAULT_SHARD_ID = "default-shard";
const DEFAULT_REPLICA_GROUP = "default-replica";

function sanitizeUrl(url) {
  if (typeof url !== "string") {
    return "";
  }

  return url.trim().replace(/\/$/, "");
}

function sanitizeLabel(value, fallback) {
  if (typeof value !== "string") {
    return fallback;
  }

  const normalized = value.trim();
  return normalized || fallback;
}

function sortNodes(nodes) {
  return [...nodes].sort((left, right) => left.url.localeCompare(right.url));
}

function clampScore(score) {
  return Math.max(MIN_SCORE, Math.min(MAX_SCORE, score));
}

function deriveStatus(score, previousStatus, options = {}) {
  const probationThreshold = options.probationThreshold ?? DEFAULT_PROBATION_THRESHOLD;
  const reintegrationThreshold =
    options.reintegrationThreshold ?? DEFAULT_REINTEGRATION_THRESHOLD;
  const safeScore = clampScore(score);

  if (safeScore <= MIN_SCORE) {
    return "inactive";
  }

  if (previousStatus === "probation") {
    return safeScore > reintegrationThreshold ? "active" : "probation";
  }

  if (safeScore < probationThreshold) {
    return "probation";
  }

  return "active";
}

function createNodeRegistry(options = {}) {
  const nodes = new Map();
  const now = options.now || Date.now;
  const inactivityThresholdMs = options.inactivityThresholdMs ?? 30000;
  const scoreSuccessIncrement = options.scoreSuccessIncrement ?? 2;
  const scoreFailureDecrement = options.scoreFailureDecrement ?? 5;
  const probationSuccessBoost = options.probationSuccessBoost ?? 5;
  const probationThreshold = options.probationThreshold ?? DEFAULT_PROBATION_THRESHOLD;
  const reintegrationThreshold =
    options.reintegrationThreshold ?? DEFAULT_REINTEGRATION_THRESHOLD;
  const onInactive = options.onInactive || (() => {});
  const onReconnect = options.onReconnect || (() => {});
  const onProbation = options.onProbation || (() => {});
  const onRecovery = options.onRecovery || (() => {});
  const onReintegration = options.onReintegration || (() => {});
  const onScoreChange = options.onScoreChange || (() => {});

  function buildNodeRecord(existingNode, nextValues) {
    const score = clampScore(nextValues.score ?? existingNode?.score ?? DEFAULT_SCORE);
    const previousStatus = nextValues.previousStatus ?? existingNode?.status ?? "active";
    const shardId = sanitizeLabel(nextValues.shardId ?? existingNode?.shardId, DEFAULT_SHARD_ID);
    const replicaGroup = sanitizeLabel(
      nextValues.replicaGroup ?? existingNode?.replicaGroup,
      DEFAULT_REPLICA_GROUP,
    );

    return {
      url: nextValues.url,
      capacity: nextValues.capacity,
      lastSeen: nextValues.lastSeen,
      status: nextValues.status || deriveStatus(score, previousStatus, {
        probationThreshold,
        reintegrationThreshold,
      }),
      score,
      totalTasks: nextValues.totalTasks ?? existingNode?.totalTasks ?? 0,
      successfulTasks: nextValues.successfulTasks ?? existingNode?.successfulTasks ?? 0,
      failedTasks: nextValues.failedTasks ?? existingNode?.failedTasks ?? 0,
      shardId,
      replicaGroup,
    };
  }

  function saveNode(node) {
    nodes.set(node.url, node);
    return node;
  }

  function emitLifecycleEvents(previousNode, updatedNode, reason = "update") {
    if (!previousNode) {
      return;
    }

    if (updatedNode.score !== previousNode.score) {
      onScoreChange(updatedNode, previousNode.score);
    }

    if (previousNode.status !== "inactive" && updatedNode.status === "inactive") {
      onInactive(updatedNode, reason);
    }

    if (previousNode.status === "inactive" && updatedNode.status !== "inactive") {
      onReconnect(updatedNode);
    }

    if (previousNode.status !== "probation" && updatedNode.status === "probation") {
      onProbation(updatedNode, reason);
    }

    if (
      previousNode.status === "probation"
      && updatedNode.status === "probation"
      && updatedNode.score > previousNode.score
    ) {
      onRecovery(updatedNode, previousNode.score);
    }

    if (previousNode.status === "probation" && updatedNode.status === "active") {
      onReintegration(updatedNode);
    }
  }

  function registerNode(node) {
    const sanitizedUrl = sanitizeUrl(node?.url);
    const capacity = Number(node?.capacity || 1);

    if (!sanitizedUrl) {
      throw new Error("A non-empty node url is required.");
    }

    if (!Number.isFinite(capacity) || capacity <= 0) {
      throw new Error("Node capacity must be a positive number.");
    }

    const currentTime = now();
    const existingNode = nodes.get(sanitizedUrl);
    const registeredNode = buildNodeRecord(existingNode, {
      url: sanitizedUrl,
      capacity,
      lastSeen: currentTime,
      shardId: node?.shardId,
      replicaGroup: node?.replicaGroup,
      previousStatus: existingNode?.status,
    });

    saveNode(registeredNode);
    emitLifecycleEvents(existingNode, registeredNode, "registration");

    return {
      node: registeredNode,
      duplicate: Boolean(existingNode),
    };
  }

  function heartbeat(node) {
    const sanitizedUrl = sanitizeUrl(node?.url);

    if (!sanitizedUrl) {
      throw new Error("A non-empty node url is required.");
    }

    const existingNode = nodes.get(sanitizedUrl);

    if (!existingNode) {
      return {
        found: false,
      };
    }

    const updatedNode = buildNodeRecord(existingNode, {
      url: existingNode.url,
      capacity: existingNode.capacity,
      lastSeen: now(),
      previousStatus: existingNode.status,
    });

    saveNode(updatedNode);
    emitLifecycleEvents(existingNode, updatedNode, "heartbeat");

    return {
      found: true,
      node: updatedNode,
    };
  }

  function cleanupInactiveNodes() {
    const currentTime = now();
    const inactivatedNodes = [];

    for (const [url, node] of nodes.entries()) {
      if (node.status === "inactive") {
        continue;
      }

      if (currentTime - node.lastSeen > inactivityThresholdMs) {
        const inactiveNode = buildNodeRecord(node, {
          url,
          capacity: node.capacity,
          lastSeen: node.lastSeen,
          status: "inactive",
          previousStatus: node.status,
        });

        saveNode(inactiveNode);
        inactivatedNodes.push(inactiveNode);
        emitLifecycleEvents(node, inactiveNode, "heartbeat timeout");
      }
    }

    return inactivatedNodes;
  }

  function updateNode(url, updater, reason = "update") {
    const sanitizedUrl = sanitizeUrl(url);
    const existingNode = nodes.get(sanitizedUrl);

    if (!existingNode) {
      return null;
    }

    const updatedNode = buildNodeRecord(existingNode, {
      ...updater(existingNode),
      previousStatus: existingNode.status,
    });

    saveNode(updatedNode);
    emitLifecycleEvents(existingNode, updatedNode, reason);
    return updatedNode;
  }

  function recordAssignment(url) {
    return updateNode(url, (existingNode) => ({
      url: existingNode.url,
      capacity: existingNode.capacity,
      lastSeen: existingNode.lastSeen,
      status: existingNode.status,
      totalTasks: existingNode.totalTasks + 1,
    }), "task assignment");
  }

  function recordSuccess(url) {
    return updateNode(url, (existingNode) => ({
      url: existingNode.url,
      capacity: existingNode.capacity,
      lastSeen: now(),
      score: existingNode.score + scoreSuccessIncrement,
      successfulTasks: existingNode.successfulTasks + 1,
    }), "verified success");
  }

  function recordProbationSuccess(url) {
    return updateNode(url, (existingNode) => ({
      url: existingNode.url,
      capacity: existingNode.capacity,
      lastSeen: now(),
      score: existingNode.score + probationSuccessBoost,
      successfulTasks: existingNode.successfulTasks + 1,
    }), "probation success");
  }

  function recordFailure(url, reason = "failure") {
    return updateNode(url, (existingNode) => ({
      url: existingNode.url,
      capacity: existingNode.capacity,
      lastSeen: existingNode.lastSeen,
      score: existingNode.score - scoreFailureDecrement,
      failedTasks: existingNode.failedTasks + 1,
    }), reason);
  }

  function getNode(url) {
    return nodes.get(sanitizeUrl(url)) || null;
  }

  function isNodeSelectable(url) {
    const node = getNode(url);
    return Boolean(node && node.status === "active");
  }

  function isNodeProbationEligible(url) {
    const node = getNode(url);
    return Boolean(node && node.status === "probation" && node.score > MIN_SCORE);
  }

  function isNodeRoutable(url) {
    const node = getNode(url);
    return Boolean(node && (node.status === "active" || node.status === "probation"));
  }

  function listNodes() {
    return sortNodes(nodes.values());
  }

  function listActiveNodes() {
    return sortNodes(
      [...nodes.values()].filter((node) => node.status === "active"),
    );
  }

  function listProbationNodes() {
    return sortNodes(
      [...nodes.values()].filter((node) => node.status === "probation"),
    );
  }

  function listRoutableNodes() {
    return sortNodes(
      [...nodes.values()].filter((node) => node.status === "active" || node.status === "probation"),
    );
  }

  function getActiveNodeUrls() {
    return listActiveNodes().map((node) => node.url);
  }

  return {
    registerNode,
    heartbeat,
    cleanupInactiveNodes,
    recordAssignment,
    recordSuccess,
    recordProbationSuccess,
    recordFailure,
    getNode,
    isNodeSelectable,
    isNodeProbationEligible,
    isNodeRoutable,
    listNodes,
    listActiveNodes,
    listProbationNodes,
    listRoutableNodes,
    getActiveNodeUrls,
  };
}

module.exports = {
  createNodeRegistry,
};
