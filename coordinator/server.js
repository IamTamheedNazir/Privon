const crypto = require("crypto");
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
const {
  ADMIN_ROLES,
  DASHBOARD_ROLES,
  NODE_ROUTE_ROLES,
  SUPER_ADMIN_ROLE,
  VALID_API_KEY_ROLES,
} = require("./roles");
const { createCoordinatorStore } = require("./sqliteStore");

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
const dashboardSessionTtlMs = Number(process.env.DASHBOARD_SESSION_TTL_MS || 1000 * 60 * 30);
const bootstrapKeyTtlMs = Number(process.env.BOOTSTRAP_KEY_TTL_MS || 1000 * 60 * 60 * 24 * 365);
const createdApiKeyTtlMs = Number(process.env.CREATED_API_KEY_TTL_MS || 1000 * 60 * 60 * 24 * 30);
const encryptionEnabled = isEncryptionEnabled();
const dashboardState = createDashboardState();
const coordinatorStore = createCoordinatorStore({
  filePath: process.env.COORDINATOR_DB_FILE,
});

const bootstrapKeyDeadline = Date.now() + bootstrapKeyTtlMs;
const bootstrapApiKeys = [
  {
    key: process.env.API_KEY || "privon-demo-key",
    role: process.env.API_KEY_ROLE || SUPER_ADMIN_ROLE,
    expiresAt: Number(process.env.API_KEY_EXPIRES_AT || bootstrapKeyDeadline),
  },
  process.env.NODE_API_KEY
    ? {
      key: process.env.NODE_API_KEY,
      role: "node",
      expiresAt: Number(process.env.NODE_API_KEY_EXPIRES_AT || bootstrapKeyDeadline),
    }
    : null,
].filter(Boolean);

for (const bootstrapApiKey of bootstrapApiKeys) {
  coordinatorStore.ensureApiKey({
    ...bootstrapApiKey,
    status: "active",
  });
}

const apiKeyAuth = createApiKeyAuth({
  keyStore: coordinatorStore,
  sessionTtlMs: dashboardSessionTtlMs,
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

function maskApiKeyValue(value) {
  const apiKey = String(value || "").trim();

  if (!apiKey) {
    return "****";
  }

  return `****${apiKey.slice(-4)}`;
}

const nodeRegistry = createNodeRegistry({
  initialNodes: coordinatorStore.loadNodes(),
  inactivityThresholdMs,
  scoreSuccessIncrement,
  scoreFailureDecrement,
  probationSuccessBoost,
  onChange(nodes) {
    coordinatorStore.saveNodes(nodes);
    dashboardState.setNodes(nodes);
  },
  onInactive(node, reason) {
    emitCoordinatorLog(
      `[coordinator] node became inactive: ${node.url} reason=${reason}`,
      {
        level: "warn",
        type: "node.inactive",
        metadata: { node, shardId: node.shardId, replicaGroup: node.replicaGroup },
      },
    );
  },
  onReconnect(node) {
    emitCoordinatorLog(
      `[coordinator] node reconnected: ${node.url} status=${node.status}`,
      {
        type: "node.reconnect",
        metadata: { node, shardId: node.shardId, replicaGroup: node.replicaGroup },
      },
    );
  },
  onProbation(node, reason) {
    emitCoordinatorLog(
      `[coordinator] node entered probation: ${node.url} (score ${node.score}) reason=${reason}`,
      {
        level: "warn",
        type: "node.probation",
        metadata: { node, reason, shardId: node.shardId, replicaGroup: node.replicaGroup },
      },
    );
  },
  onRecovery(node, previousScore) {
    emitCoordinatorLog(
      `[coordinator] node recovery progress: ${node.url} ${previousScore} -> ${node.score}`,
      {
        type: "node.recovery",
        metadata: { node, previousScore, shardId: node.shardId, replicaGroup: node.replicaGroup },
      },
    );
  },
  onReintegration(node) {
    emitCoordinatorLog(
      `[coordinator] node reintegrated: ${node.url} (score ${node.score})`,
      {
        type: "node.reintegration",
        metadata: { node, shardId: node.shardId, replicaGroup: node.replicaGroup },
      },
    );
  },
  onScoreChange(node, previousScore) {
    if (node.score !== previousScore) {
      emitCoordinatorLog(
        `[coordinator] node score changed: ${node.url} ${previousScore} -> ${node.score}`,
        {
          type: "node.score",
          metadata: { node, previousScore, shardId: node.shardId, replicaGroup: node.replicaGroup },
        },
      );
      dashboardState.recordScoreChange(node, previousScore);
    }
  },
});

function parseCsvFilter(value) {
  if (!value) {
    return [];
  }

  return String(value)
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function parseDashboardFilters(query = {}) {
  return {
    shardId: String(query.shardId || "").trim(),
    replicaGroup: String(query.replicaGroup || "").trim(),
    logLevels: parseCsvFilter(query.levels),
    logTypes: parseCsvFilter(query.types),
    statuses: parseCsvFilter(query.statuses),
    events: parseCsvFilter(query.events),
  };
}

function buildSessionPayload(request) {
  return {
    role: request.auth?.role || "unknown",
    keyExpiresAt: Number(request.auth?.expiresAt || 0),
    expiresAt: Number(request.session?.expiresAt || request.auth?.expiresAt || 0),
  };
}

function buildDashboardEventPayload(event, filters, payload) {
  const filtersOnly = {
    shardId: filters.shardId,
    replicaGroup: filters.replicaGroup,
    logLevels: filters.logLevels,
    logTypes: filters.logTypes,
    statuses: filters.statuses,
  };

  if (event === "snapshot") {
    return dashboardState.getSnapshot(filtersOnly);
  }

  if (event === "node.update") {
    return {
      nodes: dashboardState.filterNodes(nodeRegistry.listNodes(), filtersOnly),
      stats: dashboardState.getStats(nodeRegistry.listNodes(), filtersOnly),
      filters: dashboardState.getFilterOptions(),
    };
  }

  if (event === "task.execution") {
    return {
      recentExecutions: dashboardState.getExecutions(24, filtersOnly),
      stats: dashboardState.getStats(nodeRegistry.listNodes(), filtersOnly),
    };
  }

  if (event === "log.append") {
    return {
      logs: dashboardState.getLogs(40, filtersOnly),
      totalLogs: dashboardState.getLogs(250, filtersOnly).length,
      lastUpdatedAt: Date.now(),
    };
  }

  if (event === "score.change") {
    const matchingNodes = dashboardState.filterNodes([payload.node], filtersOnly);

    if (matchingNodes.length === 0) {
      return null;
    }

    return {
      node: payload.node,
      previousScore: payload.previousScore,
      stats: dashboardState.getStats(nodeRegistry.listNodes(), filtersOnly),
    };
  }

  return payload;
}

function createApiKeyValue() {
  return `pvn_${crypto.randomBytes(18).toString("hex")}`;
}

dashboardState.setNodes(nodeRegistry.listNodes(), { event: "snapshot" });
nodeRegistry.cleanupInactiveNodes();

app.use(express.json({ limit: "16kb" }));
app.use("/dashboard-app", express.static(path.join(__dirname, "..", "dashboard")));

app.get(["/dashboard", "/dashboard/"], (_request, response) => {
  response.sendFile(path.resolve(__dirname, "..", "dashboard", "index.html"));
});

app.use("/dashboard", (request, response, next) => {
  if (request.path === "/session") {
    if (!apiKeyAuth.ensureAuthorized(request, response, { allowedRoles: DASHBOARD_ROLES })) {
      return;
    }

    next();
    return;
  }

  if (!apiKeyAuth.ensureAuthorized(request, response, {
    allowSession: true,
    allowedRoles: DASHBOARD_ROLES,
  })) {
    return;
  }

  next();
});

app.use("/admin", (request, response, next) => {
  if (!apiKeyAuth.ensureAuthorized(request, response, {
    allowSession: true,
    allowedRoles: ADMIN_ROLES,
  })) {
    return;
  }

  next();
});

app.post("/register-node", (request, response) => {
  if (!apiKeyAuth.ensureAuthorized(request, response, { allowedRoles: NODE_ROUTE_ROLES })) {
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
  if (!apiKeyAuth.ensureAuthorized(request, response, { allowedRoles: NODE_ROUTE_ROLES })) {
    return;
  }

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

app.post("/dashboard/session", (request, response) => {
  const session = apiKeyAuth.createSessionFromApiKey(request, response, DASHBOARD_ROLES);

  if (!session) {
    return;
  }

  emitCoordinatorLog(
    `[coordinator] dashboard session opened: role=${session.role}`,
    {
      type: "auth.session.create",
      metadata: {
        role: session.role,
        keyExpiresAt: session.keyExpiresAt,
        sessionExpiresAt: session.expiresAt,
      },
    },
  );

  return response.json({
    success: true,
    session,
  });
});

app.post("/dashboard/session/renew", (request, response) => {
  const session = apiKeyAuth.renewSession(request, response, DASHBOARD_ROLES);

  if (!session) {
    return;
  }

  emitCoordinatorLog(
    `[coordinator] dashboard session renewed: role=${session.role}`,
    {
      type: "auth.session.renew",
      metadata: {
        role: session.role,
        keyExpiresAt: session.keyExpiresAt,
        sessionExpiresAt: session.expiresAt,
      },
    },
  );

  return response.json({
    success: true,
    session,
  });
});

app.post("/dashboard/logout", (request, response) => {
  const sessionPrincipal = apiKeyAuth.getSessionPrincipal(request, DASHBOARD_ROLES);
  apiKeyAuth.clearSessionCookie(response, request);

  emitCoordinatorLog(
    `[coordinator] dashboard session closed: role=${sessionPrincipal.ok ? sessionPrincipal.principal.role : "unknown"}`,
    {
      type: "auth.session.logout",
      metadata: {
        role: sessionPrincipal.ok ? sessionPrincipal.principal.role : "unknown",
      },
    },
  );

  return response.json({
    success: true,
  });
});

app.get("/dashboard/meta", (request, response) => {
  response.json({
    filters: dashboardState.getFilterOptions(),
    session: buildSessionPayload(request),
  });
});

app.get("/nodes", (_request, response) => {
  const activeNodes = nodeRegistry.listActiveNodes();

  response.json({
    nodes: activeNodes,
    totalNodes: activeNodes.length,
  });
});

app.get("/dashboard/nodes", (request, response) => {
  const filters = parseDashboardFilters(request.query);
  const nodes = dashboardState.filterNodes(nodeRegistry.listNodes(), filters).map((node) => ({
    ...node,
    tasksHandled: node.totalTasks,
  }));

  response.json({
    nodes,
    totalNodes: nodes.length,
    lastUpdatedAt: Date.now(),
  });
});

app.get("/dashboard/stats", (request, response) => {
  const filters = parseDashboardFilters(request.query);
  response.json(dashboardState.getStats(nodeRegistry.listNodes(), filters));
});

app.get("/dashboard/logs", (request, response) => {
  const limit = Number(request.query?.limit || 100);
  const filters = parseDashboardFilters(request.query);

  response.json({
    logs: dashboardState.getLogs(limit, filters),
    totalLogs: dashboardState.getLogs(250, filters).length,
    lastUpdatedAt: Date.now(),
  });
});

app.get("/dashboard/stream", (request, response) => {
  const filters = parseDashboardFilters(request.query);

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

  sendEvent("snapshot", buildDashboardEventPayload("snapshot", filters, {}));

  const unsubscribe = dashboardState.subscribe(({ event, payload }) => {
    if (filters.events.length > 0 && !filters.events.includes(event)) {
      return;
    }

    const eventPayload = buildDashboardEventPayload(event, filters, payload);

    if (!eventPayload) {
      return;
    }

    sendEvent(event, eventPayload);
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

app.post("/admin/api-keys/create", (request, response) => {
  const role = String(request.body?.role || "").trim().toLowerCase();
  const requestedKey = String(request.body?.key || "").trim();
  const expiresInMs = Number(request.body?.expiresInMs || createdApiKeyTtlMs);
  const explicitExpiresAt = Number(request.body?.expiresAt || 0);
  const expiresAt = explicitExpiresAt > 0 ? explicitExpiresAt : Date.now() + expiresInMs;

  if (!VALID_API_KEY_ROLES.includes(role)) {
    return response.status(400).json({
      error: `role must be one of: ${VALID_API_KEY_ROLES.join(", ")}`,
    });
  }

  if (!Number.isFinite(expiresAt) || expiresAt <= Date.now()) {
    return response.status(400).json({
      error: "expiresAt must be in the future.",
    });
  }

  try {
    const apiKey = coordinatorStore.createApiKey({
      key: requestedKey || createApiKeyValue(),
      role,
      createdAt: Date.now(),
      expiresAt,
      status: "active",
    });

    emitCoordinatorLog(
      `[coordinator] api key created: role=${apiKey.role} key=${maskApiKeyValue(apiKey.key)}`,
      {
        type: "auth.api_key.create",
        metadata: {
          actorRole: request.auth?.role || "unknown",
          key: maskApiKeyValue(apiKey.key),
          role: apiKey.role,
          status: apiKey.status,
          expiresAt: apiKey.expiresAt,
        },
      },
    );

    return response.json({
      success: true,
      apiKey,
    });
  } catch (error) {
    return response.status(400).json({
      error: error.message,
    });
  }
});

app.get("/admin/api-keys", (request, response) => {
  response.json({
    apiKeys: coordinatorStore.listApiKeys({
      role: request.query?.role,
      status: request.query?.status,
    }),
  });
});

app.post("/admin/api-keys/revoke", (request, response) => {
  const revokedApiKey = coordinatorStore.revokeApiKey(request.body?.key);

  if (!revokedApiKey) {
    return response.status(404).json({
      error: "API key not found.",
    });
  }

  emitCoordinatorLog(
    `[coordinator] api key revoked: role=${revokedApiKey.role} key=${maskApiKeyValue(revokedApiKey.key)}`,
    {
      type: "auth.api_key.revoke",
      metadata: {
        actorRole: request.auth?.role || "unknown",
        key: maskApiKeyValue(revokedApiKey.key),
        role: revokedApiKey.role,
        status: revokedApiKey.status,
      },
    },
  );

  return response.json({
    success: true,
    apiKey: revokedApiKey,
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
    databasePath: coordinatorStore.filePath,
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

