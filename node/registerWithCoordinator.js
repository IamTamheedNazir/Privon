async function registerWithCoordinator(options = {}) {
  const coordinatorUrl = options.coordinatorUrl || process.env.COORDINATOR_URL;

  if (!coordinatorUrl) {
    return {
      attempted: false,
    };
  }

  const nodeUrl = options.nodeUrl || process.env.NODE_PUBLIC_URL;
  const capacity = Number(options.capacity || process.env.NODE_CAPACITY || 10);
  const shardId = options.shardId || process.env.NODE_SHARD_ID || "default-shard";
  const replicaGroup = options.replicaGroup || process.env.NODE_REPLICA_GROUP || "default-replica";
  const apiKey = String(
    options.apiKey
      || process.env.COORDINATOR_API_KEY
      || process.env.API_KEY
      || "privon-demo-key",
  ).trim();

  if (!nodeUrl) {
    throw new Error("NODE_PUBLIC_URL is required to register a node.");
  }

  const headers = {
    "content-type": "application/json",
  };

  if (apiKey) {
    headers.authorization = `Bearer ${apiKey}`;
  }

  const response = await fetch(`${coordinatorUrl}/register-node`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      url: nodeUrl,
      capacity,
      shardId,
      replicaGroup,
    }),
  });

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Coordinator registration failed: ${response.status} ${errorBody}`);
  }

  return {
    attempted: true,
    ...await response.json(),
  };
}

async function sendHeartbeat(options = {}) {
  const coordinatorUrl = options.coordinatorUrl || process.env.COORDINATOR_URL;

  if (!coordinatorUrl) {
    return {
      attempted: false,
    };
  }

  const nodeUrl = options.nodeUrl || process.env.NODE_PUBLIC_URL;
  const apiKey = String(
    options.apiKey
      || process.env.COORDINATOR_API_KEY
      || process.env.API_KEY
      || "privon-demo-key",
  ).trim();

  if (!nodeUrl) {
    throw new Error("NODE_PUBLIC_URL is required to send a heartbeat.");
  }

  const headers = {
    "content-type": "application/json",
  };

  if (apiKey) {
    headers.authorization = `Bearer ${apiKey}`;
  }

  const response = await fetch(`${coordinatorUrl}/heartbeat`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      url: nodeUrl,
    }),
  });

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Coordinator heartbeat failed: ${response.status} ${errorBody}`);
  }

  return {
    attempted: true,
    ...await response.json(),
  };
}

module.exports = {
  registerWithCoordinator,
  sendHeartbeat,
};
