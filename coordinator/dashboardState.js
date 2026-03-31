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

  function getStats(nodes = nodesSnapshot) {
    const allNodes = Array.isArray(nodes) ? nodes : [];
    const replicaGroups = groupNodeSummaries(allNodes, "replicaGroup");
    const shardSummaries = groupNodeSummaries(allNodes, "shardId");
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
      replicaGroups,
      shardSummaries,
      recentExecutions: executionStore.list(24),
      logCount: logStore.list().length,
      lastUpdatedAt: Date.now(),
    };
  }

  function getSnapshot() {
    return {
      nodes: nodesSnapshot,
      stats: getStats(nodesSnapshot),
      logs: logStore.list(40),
    };
  }

  function subscribe(listener) {
    subscribers.add(listener);
    return () => {
      subscribers.delete(listener);
    };
  }

  return {
    recordLog,
    recordScoreChange,
    recordSearchExecution,
    setNodes,
    subscribe,
    getSnapshot,
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
