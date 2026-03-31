const express = require("express");

const { decryptPayload, encryptPayload, isEncryptionEnabled } = require("../core/cryptoTransport");
const { loadDataset } = require("./loadDataset");
const { registerWithCoordinator, sendHeartbeat } = require("./registerWithCoordinator");
const { searchShard } = require("./searchShard");

const app = express();
const port = Number(process.env.PORT || 4001);
const nodeId = process.env.NODE_ID || `node-${port}`;
const datasetName = process.env.NODE_DATASET || "shard-a";
const shardId = process.env.NODE_SHARD_ID || datasetName;
const replicaGroup = process.env.NODE_REPLICA_GROUP || process.env.NODE_SHARD_ID || datasetName;
const heartbeatIntervalMs = Number(process.env.NODE_HEARTBEAT_INTERVAL_MS || 10000);
const nodeUrl = process.env.NODE_PUBLIC_URL || `http://localhost:${port}`;
const encryptionEnabled = isEncryptionEnabled();
const documents = loadDataset(datasetName);

app.use(express.json());

app.get("/health", (_request, response) => {
  response.json({
    status: "ok",
    nodeId,
    shardId,
    replicaGroup,
    documentsLoaded: documents.length,
    encryptionEnabled,
  });
});

function handleCompute(request, response) {
  let decryptedRequest;

  try {
    decryptedRequest = decryptPayload(request.body, {
      enabled: encryptionEnabled,
    });
  } catch (error) {
    console.error(`[${nodeId}] compute request decryption failed: ${error.message}`);
    return response.status(400).json({
      error: "Invalid encrypted compute payload.",
    });
  }

  const fragment = decryptedRequest?.fragment;
  const taskId = decryptedRequest?.taskId;
  const fragmentIndex = Number(decryptedRequest?.fragmentIndex ?? -1);
  const limit = Number(decryptedRequest?.limit || 5);

  if (!fragment || typeof fragment !== "string") {
    return response.status(400).json({
      error: "A non-empty fragment is required.",
    });
  }

  const matches = searchShard(documents, fragment, { limit });
  const resultPayload = {
    nodeId,
    shardId,
    replicaGroup,
    taskId,
    fragmentIndex,
    matches,
  };

  return response.json(
    encryptPayload(resultPayload, {
      enabled: encryptionEnabled,
    }),
  );
}

app.post("/compute", handleCompute);
app.post("/tasks/search", handleCompute);

app.listen(port, async () => {
  console.log(`[${nodeId}] listening on port ${port} with dataset ${datasetName}`);

  try {
    const registration = await registerWithCoordinator({
      nodeUrl,
      shardId,
      replicaGroup,
    });

    if (registration.attempted) {
      console.log(
        `[${nodeId}] registered with coordinator at ${registration.node.url} shard=${shardId} replica=${replicaGroup}`,
      );
    }
  } catch (error) {
    console.error(`[${nodeId}] registration failed: ${error.message}`);
  }

  const heartbeatTimer = setInterval(async () => {
    try {
      const heartbeat = await sendHeartbeat({
        nodeUrl,
      });

      if (heartbeat.attempted && !heartbeat.ignored) {
        console.log(`[${nodeId}] heartbeat acknowledged`);
      }
    } catch (error) {
      console.error(`[${nodeId}] heartbeat failed: ${error.message}`);
    }
  }, heartbeatIntervalMs);

  heartbeatTimer.unref();
});
