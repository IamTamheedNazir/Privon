const AUDIT_EVENT_TYPE_MAP = {
  "auth.api_key.create": "key.create",
  "auth.api_key.revoke": "key.revoke",
  "auth.session.create": "session.login",
  "auth.session.renew": "session.renew",
  "auth.session.logout": "session.logout",
};

function createBoundedStore(limit) {
  const items = [];

  function push(entry) {
    items.unshift(entry);

    if (items.length > limit) {
      items.length = limit;
    }
  }

  function list(max = limit) {
    return items.slice(0, max);
  }

  return {
    push,
    list,
  };
}

function normalizeListFilter(values) {
  if (!Array.isArray(values)) {
    return [];
  }

  return values
    .map((value) => String(value || "").trim())
    .filter(Boolean);
}

function filterNodes(nodes, filters = {}) {
  const shardId = String(filters.shardId || "").trim();
  const replicaGroup = String(filters.replicaGroup || "").trim();
  const statusFilters = normalizeListFilter(filters.statuses);

  return (Array.isArray(nodes) ? nodes : []).filter((node) => {
    if (shardId && node.shardId !== shardId) {
      return false;
    }

    if (replicaGroup && node.replicaGroup !== replicaGroup) {
      return false;
    }

    if (statusFilters.length > 0 && !statusFilters.includes(node.status)) {
      return false;
    }

    return true;
  });
}

function filterExecutions(executions, filters = {}) {
  const shardId = String(filters.shardId || "").trim();
  const replicaGroup = String(filters.replicaGroup || "").trim();

  return (Array.isArray(executions) ? executions : []).filter((execution) => {
    if (shardId && !(execution.shardIds || []).includes(shardId)) {
      return false;
    }

    if (replicaGroup && execution.replicaGroup !== replicaGroup) {
      return false;
    }

    return true;
  });
}

function logMatchesFilters(entry, filters = {}) {
  const shardId = String(filters.shardId || "").trim();
  const replicaGroup = String(filters.replicaGroup || "").trim();
  const typeFilters = normalizeListFilter(filters.logTypes);
  const levelFilters = normalizeListFilter(filters.logLevels);

  if (typeFilters.length > 0 && !typeFilters.includes(entry.type)) {
    return false;
  }

  if (levelFilters.length > 0 && !levelFilters.includes(entry.level)) {
    return false;
  }

  if (!shardId && !replicaGroup) {
    return true;
  }

  const candidates = [
    entry.metadata?.node,
    entry.metadata?.failedNode,
    entry.metadata?.matchedNode,
  ].filter(Boolean);

  const hasShardMatch = !shardId || candidates.some((candidate) => candidate.shardId === shardId)
    || entry.metadata?.shardId === shardId;
  const hasReplicaMatch = !replicaGroup || candidates.some((candidate) => candidate.replicaGroup === replicaGroup)
    || entry.metadata?.replicaGroup === replicaGroup;

  return hasShardMatch && hasReplicaMatch;
}

function groupNodeSummaries(nodes, keyName) {
  const groups = new Map();

  for (const node of nodes) {
    const key = node[keyName] || "unknown";
    const current = groups.get(key) || {
      key,
      totalNodes: 0,
      activeNodes: 0,
      probationNodes: 0,
      inactiveNodes: 0,
      tasksHandled: 0,
      averageScore: 0,
      totalScore: 0,
      members: [],
    };

    current.totalNodes += 1;
    current.tasksHandled += Number(node.totalTasks || 0);
    current.totalScore += Number(node.score || 0);
    current.members.push({
      url: node.url,
      status: node.status,
      score: node.score,
      shardId: node.shardId,
      replicaGroup: node.replicaGroup,
    });

    if (node.status === "active") {
      current.activeNodes += 1;
    } else if (node.status === "probation") {
      current.probationNodes += 1;
    } else {
      current.inactiveNodes += 1;
    }

    groups.set(key, current);
  }

  return [...groups.values()]
    .map((group) => ({
      ...group,
      averageScore: group.totalNodes === 0 ? 0 : Math.round(group.totalScore / group.totalNodes),
    }))
    .sort((left, right) => left.key.localeCompare(right.key));
}

function sanitizePipelineStages(stages) {
  return (Array.isArray(stages) ? stages : []).map((stage) => ({
    stage: String(stage.stage || "unknown"),
    durationMs: Math.max(0, Number(stage.durationMs || 0)),
    success: stage.success !== false,
    nodes: Array.isArray(stage.nodes) ? [...new Set(stage.nodes.filter(Boolean))] : [],
    summary: String(stage.summary || "").trim(),
  }));
}
function normalizeAuditFilters(filters = {}) {
  const since = Number(filters.since || 0);

  return {
    eventTypes: normalizeListFilter(filters.eventTypes),
    since: Number.isFinite(since) && since > 0 ? since : 0,
  };
}

function isAuditLogEntry(entry) {
  return Boolean(AUDIT_EVENT_TYPE_MAP[entry.type]);
}

function buildAuditActor(metadata = {}) {
  const actorRole = String(metadata.actorRole || metadata.role || "system").trim();
  const actorKey = String(metadata.actorKey || "").trim();

  if (actorKey) {
    return `${actorRole} ${actorKey}`;
  }

  return actorRole || "system";
}

function sanitizeAuditDetails(metadata = {}) {
  const detailKeys = [
    "actorKey",
    "actorRole",
    "key",
    "role",
    "status",
    "expiresAt",
    "keyExpiresAt",
    "sessionExpiresAt",
  ];

  return detailKeys.reduce((details, key) => {
    if (metadata[key] !== undefined && metadata[key] !== null && metadata[key] !== "") {
      details[key] = metadata[key];
    }

    return details;
  }, {});
}

function buildAuditSummary(eventType, details = {}) {
  switch (eventType) {
    case "key.create":
      return `Created ${details.role || "managed"} key ${details.key || ""}`.trim();
    case "key.revoke":
      return `Revoked ${details.role || "managed"} key ${details.key || ""}`.trim();
    case "session.login":
      return `Opened ${details.role || "unknown"} dashboard session`;
    case "session.logout":
      return `Closed ${details.role || "unknown"} dashboard session`;
    case "session.renew":
      return `Renewed ${details.role || "unknown"} dashboard session`;
    default:
      return "Recorded audit event";
  }
}

function buildAuditSeverity(eventType) {
  if (eventType === "key.revoke") {
    return "warn";
  }

  return "info";
}

function mapAuditLog(entry) {
  const eventType = AUDIT_EVENT_TYPE_MAP[entry.type];
  const details = sanitizeAuditDetails(entry.metadata || {});

  return {
    id: entry.id,
    eventType,
    timestamp: entry.timestamp,
    actor: buildAuditActor(entry.metadata || {}),
    summary: buildAuditSummary(eventType, details),
    details,
    severity: buildAuditSeverity(eventType),
  };
}

function createDashboardState(options = {}) {
  const logStore = createBoundedStore(options.maxLogs ?? 250);
  const executionStore = createBoundedStore(options.maxExecutions ?? 120);
  const subscribers = new Set();
  let nodesSnapshot = [];
  let logSequence = 0;

  function publish(event, payload) {
    const frame = { event, payload };

    for (const subscriber of subscribers) {
      subscriber(frame);
    }
  }

  function recordLog({
    level = "info",
    type = "system",
    message,
    metadata = {},
    timestamp = Date.now(),
  }) {
    logSequence += 1;
    logStore.push({
      id: `${timestamp}:${type}:${logSequence}`,
      timestamp,
      level,
      type,
      message,
      metadata,
    });

    publish("log.append", {
      logs: logStore.list(40),
    });
  }

  function setNodes(nodes, options = {}) {
    nodesSnapshot = Array.isArray(nodes) ? [...nodes] : [];
    publish(options.event || "node.update", {
      nodes: nodesSnapshot,
      stats: getStats(nodesSnapshot),
      ...(options.payload || {}),
    });
  }

  function recordScoreChange(node, previousScore) {
    publish("score.change", {
      node,
      previousScore,
      stats: getStats(nodesSnapshot),
    });
  }

  function recordSearchExecution({ jobId, fragments, fragmentResults, failures }) {
    const timestamp = Date.now();

    for (const fragmentResult of fragmentResults || []) {
      executionStore.push({
        id: `${jobId}:${fragmentResult.fragmentIndex}:${fragmentResult.replicaGroup}`,
        timestamp,
        jobId,
        fragmentIndex: fragmentResult.fragmentIndex,
        fragment: fragments?.[fragmentResult.fragmentIndex] || null,
        replicaGroup: fragmentResult.replicaGroup || "default-replica",
        shardIds: fragmentResult.shardIds || [],
        status: fragmentResult.success ? "verified" : "failed",
        nodes: fragmentResult.nodeUrls || [],
        probation: fragmentResult.probation || { attempted: false },
        failureCount: Array.isArray(fragmentResult.failures) ? fragmentResult.failures.length : 0,
        pipeline: sanitizePipelineStages(fragmentResult.pipeline),
      });
    }

    for (const failure of failures || []) {
      recordLog({
        level: "warn",
        type: "task.failure",
        message: `Fragment ${failure.fragmentIndex} failed for replica group ${failure.replicaGroup}`,
        metadata: failure,
        timestamp,
      });
    }

    publish("task.execution", {
      recentExecutions: executionStore.list(24),
      stats: getStats(nodesSnapshot),
    });
  }

  function getLogs(limit = 100, filters = {}) {
    return logStore.list().filter((entry) => logMatchesFilters(entry, filters)).slice(0, limit);
  }

  function getExecutions(limit = 24, filters = {}) {
    return filterExecutions(executionStore.list(), filters).slice(0, limit);
  }

  function getAuditLogs(limit = 100, filters = {}) {
    const normalizedFilters = normalizeAuditFilters(filters);

    return logStore.list()
      .filter(isAuditLogEntry)
      .map(mapAuditLog)
      .filter((entry) => {
        if (normalizedFilters.eventTypes.length > 0 && !normalizedFilters.eventTypes.includes(entry.eventType)) {
          return false;
        }

        if (normalizedFilters.since > 0 && entry.timestamp < normalizedFilters.since) {
          return false;
        }

        return true;
      })
      .slice(0, limit);
  }

  function getFilterOptions() {
    const nodes = nodesSnapshot;
    const logs = logStore.list();

    return {
      shardIds: [...new Set(nodes.map((node) => node.shardId).filter(Boolean))].sort(),
      replicaGroups: [...new Set(nodes.map((node) => node.replicaGroup).filter(Boolean))].sort(),
      logTypes: [...new Set(logs.map((entry) => entry.type).filter(Boolean))].sort(),
      logLevels: [...new Set(logs.map((entry) => entry.level).filter(Boolean))].sort(),
      eventTypes: ["snapshot", "node.update", "score.change", "task.execution", "log.append"],
      auditEventTypes: [...new Set(
        logs
          .filter(isAuditLogEntry)
          .map((entry) => AUDIT_EVENT_TYPE_MAP[entry.type])
          .filter(Boolean),
      )].sort(),
    };
  }

  function getStats(nodes = nodesSnapshot, filters = {}) {
    const filteredNodes = filterNodes(nodes, filters);
    const replicaGroups = groupNodeSummaries(filteredNodes, "replicaGroup");
    const shardSummaries = groupNodeSummaries(filteredNodes, "shardId");
    const filteredExecutions = getExecutions(24, filters);
    const filteredLogs = getLogs(logStore.list().length, filters);
    const summary = {
      totalNodes: filteredNodes.length,
      activeNodes: filteredNodes.filter((node) => node.status === "active").length,
      probationNodes: filteredNodes.filter((node) => node.status === "probation").length,
      inactiveNodes: filteredNodes.filter((node) => node.status === "inactive").length,
      totalTasksHandled: filteredNodes.reduce((sum, node) => sum + Number(node.totalTasks || 0), 0),
      averageScore:
        filteredNodes.length === 0
          ? 0
          : Math.round(
            filteredNodes.reduce((sum, node) => sum + Number(node.score || 0), 0) / filteredNodes.length,
          ),
      recentFailures: filteredLogs.filter((entry) => entry.level === "warn" || entry.level === "error").length,
    };

    return {
      summary,
      replicaGroups,
      shardSummaries,
      recentExecutions: filteredExecutions,
      recentPipelines: filteredExecutions.filter((execution) => Array.isArray(execution.pipeline) && execution.pipeline.length > 0),
      logCount: filteredLogs.length,
      lastUpdatedAt: Date.now(),
      appliedFilters: {
        shardId: String(filters.shardId || ""),
        replicaGroup: String(filters.replicaGroup || ""),
      },
    };
  }

  function getSnapshot(filters = {}) {
    return {
      nodes: filterNodes(nodesSnapshot, filters),
      stats: getStats(nodesSnapshot, filters),
      logs: getLogs(40, filters),
      filters: getFilterOptions(),
    };
  }

  function subscribe(listener) {
    subscribers.add(listener);
    return () => {
      subscribers.delete(listener);
    };
  }

  return {
    filterNodes,
    getAuditLogs,
    getExecutions,
    getFilterOptions,
    getLogs,
    getSnapshot,
    getStats,
    recordLog,
    recordScoreChange,
    recordSearchExecution,
    setNodes,
    subscribe,
  };
}

module.exports = {
  createDashboardState,
};
