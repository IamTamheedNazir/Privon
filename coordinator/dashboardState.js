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

function createDashboardState(options = {}) {
  const logStore = createBoundedStore(options.maxLogs ?? 250);
  const executionStore = createBoundedStore(options.maxExecutions ?? 120);

  function recordLog({
    level = "info",
    type = "system",
    message,
    metadata = {},
    timestamp = Date.now(),
  }) {
    logStore.push({
      id: `${timestamp}:${type}:${logStore.list(1).length}`,
      timestamp,
      level,
      type,
      message,
      metadata,
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
  }

  function getStats(nodes) {
    const allNodes = Array.isArray(nodes) ? nodes : [];
    const summary = {
      totalNodes: allNodes.length,
      activeNodes: allNodes.filter((node) => node.status === "active").length,
      probationNodes: allNodes.filter((node) => node.status === "probation").length,
      inactiveNodes: allNodes.filter((node) => node.status === "inactive").length,
      totalTasksHandled: allNodes.reduce((sum, node) => sum + Number(node.totalTasks || 0), 0),
      averageScore:
        allNodes.length === 0
          ? 0
          : Math.round(
            allNodes.reduce((sum, node) => sum + Number(node.score || 0), 0) / allNodes.length,
          ),
      recentFailures: logStore.list(100).filter((entry) => entry.level === "warn" || entry.level === "error").length,
    };

    return {
      summary,
      recentExecutions: executionStore.list(24),
      logCount: logStore.list().length,
      lastUpdatedAt: Date.now(),
    };
  }

  return {
    recordLog,
    recordSearchExecution,
    getLogs(limit) {
      return logStore.list(limit);
    },
    getExecutions(limit) {
      return executionStore.list(limit);
    },
    getStats,
  };
}

module.exports = {
  createDashboardState,
};
