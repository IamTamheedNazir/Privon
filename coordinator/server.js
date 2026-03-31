const path = require("path");
const express = require("express");

const { isEncryptionEnabled } = require("../core/cryptoTransport");
const { createOpaqueId } = require("../core/createOpaqueId");
const { dispatchTasks } = require("./dispatchTasks");
const { createApiKeyAuth } = require("./auth");
const { createDashboardState } = require("./dashboardState");
const { getHealthyNodeUrls } = require("./getHealthyNodeUrls");
const { mergeResults } = require("./mergeResults");
const { createNodeRegistry } = require("./nodeRegistry");
const { createNodeRegistryStore } = require("./nodeRegistryStore");

const app = express();
const port = Number(process.env.PORT || 4000);
const aggregatorUrl = process.env.AGGREGATOR_URL || "";
const healthCheckTimeoutMs = Number(process.env.NODE_HEALTH_TIMEOUT_MS || 800);
const nodeRequestTimeoutMs = Number(process.env.NODE_REQUEST_TIMEOUT_MS || 3000);
const aggregatorRequestTimeoutMs = Number(
  process.env.AGGREGATOR_REQUEST_TIMEOUT_MS || 1500,
);
const maxNodeRetries = Number(process.env.NODE_MAX_RETRIES || 3);
const redundancyFactor = Number(process.env.REDUNDANCY_FACTOR || 2);
const verificationRetryRounds = Number(process.env.VERIFICATION_RETRY_ROUNDS || 2);
const cleanupIntervalMs = Number(process.env.NODE_CLEANUP_INTERVAL_MS || 10000);
const inactivityThresholdMs = Number(process.env.NODE_INACTIVE_AFTER_MS || 30000);
const scoreSuccessIncrement = Number(process.env.SCORE_SUCCESS_INC || 2);
const scoreFailureDecrement = Number(process.env.SCORE_FAILURE_DEC || 5);
const probationTrafficRatio = Number(process.env.PROBATION_TRAFFIC_RATIO || 0.1);
const probationSuccessBoost = Number(process.env.PROBATION_SUCCESS_BOOST || 5);
const apiKey = process.env.API_KEY || "privon-demo-key";
const encryptionEnabled = isEncryptionEnabled();
const apiKeyAuth = createApiKeyAuth({ apiKey });
const dashboardState = createDashboardState();
const nodeRegistryStore = createNodeRegistryStore({
  filePath: process.env.NODE_REGISTRY_FILE,
});

function emitCoordinatorLog(message, options = {}) {
  const level = options.level || "info";
  const type = options.type || "system";
  const metadata = options.metadata || {};

  dashboardState.recordLog({
    level,
    type,
    message,
    metadata,
  });

  const method = level === "error" ? console.error : console.log;
  method(message);
}

const nodeRegistry = createNodeRegistry({
  initialNodes: nodeRegistryStore.loadNodes(),
  inactivityThresholdMs,
  scoreSuccessIncrement,
  scoreFailureDecrement,
  probationSuccessBoost,
  onChange(nodes) {
    nodeRegistryStore.saveNodes(nodes);
    dashboardState.setNodes(nodes);
  },
  onInactive(node, reason) {
    emitCoordinatorLog(
      `[coordinator] node became inactive: ${node.url} reason=${reason}`,
      {
        level: "warn",
        type: "node.inactive",
        metadata: { node },
      },
    );
  },
  onReconnect(node) {
    emitCoordinatorLog(
      `[coordinator] node reconnected: ${node.url} status=${node.status}`,
      {
        type: "node.reconnect",
        metadata: { node },
      },
    );
  },
  onProbation(node, reason) {
    emitCoordinatorLog(
      `[coordinator] node entered probation: ${node.url} (score ${node.score}) reason=${reason}`,
      {
        level: "warn",
        type: "node.probation",
        metadata: { node, reason },
      },
    );
  },
  onRecovery(node, previousScore) {
    emitCoordinatorLog(
      `[coordinator] node recovery progress: ${node.url} ${previousScore} -> ${node.score}`,
      {
        type: "node.recovery",
        metadata: { node, previousScore },
      },
    );
  },
  onReintegration(node) {
    emitCoordinatorLog(
      `[coordinator] node reintegrated: ${node.url} (score ${node.score})`,
      {
        type: "node.reintegration",
        metadata: { node },
      },
    );
  },
  onScoreChange(node, previousScore) {
    if (node.score !== previousScore) {
      emitCoordinatorLog(
        `[coordinator] node score changed: ${node.url} ${previousScore} -> ${node.score}`,
        {
          type: "node.score",
          metadata: { node, previousScore },
        },
      );
      dashboardState.recordScoreChange(node, previousScore);
    }
  },
});

dashboardState.setNodes(nodeRegistry.listNodes(), { event: "snapshot" });
nodeRegistry.cleanupInactiveNodes();

app.use(express.json({ limit: "16kb" }));
app.use("/dashboard-app", express.static(path.join(__dirname, "..", "dashboard")));

app.get(["/dashboard", "/dashboard/"], (_request, response) => {
  response.sendFile(path.resolve(__dirname, "..", "dashboard", "index.html"));
});

app.use("/dashboard", (request, response, next) => {
  if (request.path === "/session") {
    if (!apiKeyAuth.ensureAuthorized(request, response)) {
      return;
    }

    next();
    return;
  }

  if (!apiKeyAuth.ensureAuthorized(request, response, { allowSession: true })) {
    return;
  }

  next();
});

app.post("/register-node", (request, response) => {
  if (!apiKeyAuth.ensureAuthorized(request, response)) {
    return;
  }

  try {
    const { node, duplicate } = nodeRegistry.registerNode({
      url: request.body?.url,
      capacity: request.body?.capacity,
      shardId: request.body?.shardId,
      replicaGroup: request.body?.replicaGroup,
    });

    return response.json({
      success: true,
      duplicate,
      node,
      totalNodes: nodeRegistry.listActiveNodes().length,
    });
  } catch (error) {
    return response.status(400).json({
      error: error.message,
    });
  }
});

app.post("/heartbeat", (request, response) => {
  try {
    const result = nodeRegistry.heartbeat({
      url: request.body?.url,
    });

    if (!result.found) {
      return response.json({
        success: true,
        ignored: true,
      });
    }

    return response.json({
      success: true,
      ignored: false,
      node: result.node,
    });
  } catch (error) {
    return response.status(400).json({
      error: error.message,
    });
  }
});

app.post("/dashboard/session", (_request, response) => {
  apiKeyAuth.setSessionCookie(response);

  return response.json({
    success: true,
  });
});

app.get("/nodes", (_request, response) => {
  const activeNodes = nodeRegistry.listActiveNodes();

  response.json({
    nodes: activeNodes,
    totalNodes: activeNodes.length,
  });
});

app.get("/dashboard/nodes", (_request, response) => {
  const nodes = nodeRegistry.listNodes().map((node) => ({
    ...node,
    tasksHandled: node.totalTasks,
  }));

  response.json({
    nodes,
    totalNodes: nodes.length,
    lastUpdatedAt: Date.now(),
  });
});

app.get("/dashboard/stats", (_request, response) => {
  response.json(dashboardState.getStats(nodeRegistry.listNodes()));
});

app.get("/dashboard/logs", (request, response) => {
  const limit = Number(request.query?.limit || 100);

  response.json({
    logs: dashboardState.getLogs(limit),
    totalLogs: dashboardState.getLogs().length,
    lastUpdatedAt: Date.now(),
  });
});

app.get("/dashboard/stream", (request, response) => {
  response.setHeader("Content-Type", "text/event-stream");
  response.setHeader("Cache-Control", "no-cache, no-transform");
  response.setHeader("Connection", "keep-alive");
  response.setHeader("X-Accel-Buffering", "no");
  response.flushHeaders?.();
  response.write("retry: 2000\n\n");

  const sendEvent = (event, payload) => {
    response.write(`event: ${event}\n`);
    response.write(`data: ${JSON.stringify(payload)}\n\n`);
  };

  sendEvent("snapshot", dashboardState.getSnapshot());

  const unsubscribe = dashboardState.subscribe(({ event, payload }) => {
    sendEvent(event, payload);
  });
  const keepAlive = setInterval(() => {
    response.write(": keepalive\n\n");
  }, 15000);

  request.on("close", () => {
    clearInterval(keepAlive);
    unsubscribe();
    response.end();
  });
});

app.get("/health", async (_request, response) => {
  const activeNodeUrls = nodeRegistry.getActiveNodeUrls();
  const healthyNodeUrls = await getHealthyNodeUrls(activeNodeUrls, {
    timeoutMs: healthCheckTimeoutMs,
  });

  response.json({
    status: "ok",
    nodesTracked: nodeRegistry.listNodes().length,
    nodesActive: activeNodeUrls.length,
    nodesProbation: nodeRegistry.listProbationNodes().length,
    nodesHealthy: healthyNodeUrls.length,
    aggregatorConfigured: Boolean(aggregatorUrl),
    encryptionEnabled,
  });
});

app.post("/search", async (request, response) => {
  const fragments = request.body?.fragments;
  const limit = Number(request.body?.limit || 5);
  const minimumMatchedFragments = Number(
    request.body?.minimumMatchedFragments || 1,
  );

  if (!Array.isArray(fragments) || fragments.length === 0) {
    return response.status(400).json({
      error: "A non-empty fragments array is required.",
    });
  }

  const sanitizedFragments = fragments.filter(
    (fragment) => typeof fragment === "string" && fragment.trim().length > 0,
  );

  if (sanitizedFragments.length === 0) {
    return response.status(400).json({
      error: "No valid fragments were provided.",
    });
  }

  const activeNodes = nodeRegistry.listActiveNodes();

  if (activeNodes.length === 0) {
    return response.status(503).json({
      error: "No active nodes are currently available.",
    });
  }

  const jobId = createOpaqueId("job");

  emitCoordinatorLog(
    `[coordinator] search job ${jobId} started with ${sanitizedFragments.length} fragment(s)`,
    {
      type: "task.start",
      metadata: {
        jobId,
        fragmentsProcessed: sanitizedFragments.length,
      },
    },
  );

  try {
    const { partialResponses, failures, fragmentResults } = await dispatchTasks(
      sanitizedFragments,
      nodeRegistry.listRoutableNodes(),
      {
        limit,
        maxRetries: maxNodeRetries,
        redundancyFactor,
        verificationRetryRounds,
        probationTrafficRatio,
        probationSuccessBoost,
        requestTimeoutMs: nodeRequestTimeoutMs,
        enabled: encryptionEnabled,
        isNodeSelectable(node) {
          return nodeRegistry.isNodeSelectable(node.url);
        },
        isProbationNode(node) {
          return nodeRegistry.isNodeProbationEligible(node.url);
        },
        onTaskAssigned(node) {
          nodeRegistry.recordAssignment(node.url);
        },
        onTaskSuccess(node) {
          nodeRegistry.recordSuccess(node.url);
        },
        onProbationSuccess(node) {
          nodeRegistry.recordProbationSuccess(node.url);
        },
        onTaskFailure(node, reason) {
          nodeRegistry.recordFailure(node.url, reason);
        },
        log(message) {
          emitCoordinatorLog(message, {
            type: "task.log",
          });
        },
      },
    );

    const results = await mergeResults(partialResponses, {
      aggregatorUrl,
      limit,
      minimumMatchedFragments,
      requestTimeoutMs: aggregatorRequestTimeoutMs,
    });

    dashboardState.recordSearchExecution({
      jobId,
      fragments: sanitizedFragments,
      fragmentResults,
      failures,
    });

    emitCoordinatorLog(
      `[coordinator] search job ${jobId} completed with ${results.length} merged result(s)`,
      {
        type: "task.complete",
        metadata: {
          jobId,
          failures,
          resultsReturned: results.length,
        },
      },
    );

    return response.json({
      jobId,
      fragmentsProcessed: sanitizedFragments.length,
      minimumMatchedFragments,
      redundancyFactor,
      probationTrafficRatio,
      encryptionEnabled,
      nodesTracked: nodeRegistry.listNodes().length,
      nodesUsed: [...new Set(fragmentResults
        .filter((result) => result.success)
        .flatMap((result) => {
          const probationNodeUrls = result.probation?.attempted && result.probation.nodeUrl
            ? [result.probation.nodeUrl]
            : [];
          return [...(result.nodeUrls || []), ...probationNodeUrls];
        }))].length,
      failures,
      results,
    });
  } catch (error) {
    emitCoordinatorLog(
      `[coordinator] search job ${jobId} failed: ${error.message}`,
      {
        level: "error",
        type: "task.error",
        metadata: {
          jobId,
          error: error.message,
        },
      },
    );

    return response.status(502).json({
      error: error.message,
    });
  }
});

const cleanupTimer = setInterval(() => {
  nodeRegistry.cleanupInactiveNodes();
}, cleanupIntervalMs);

cleanupTimer.unref();

app.listen(port, () => {
  emitCoordinatorLog(`[coordinator] listening on port ${port}`);
});

