const express = require("express");

const { loadDataset } = require("./loadDataset");
const { searchShard } = require("./searchShard");

const app = express();
const port = Number(process.env.PORT || 4001);
const nodeId = process.env.NODE_ID || `node-${port}`;
const datasetName = process.env.NODE_DATASET || "shard-a";
const documents = loadDataset(datasetName);

app.use(express.json());

app.get("/health", (_request, response) => {
  response.json({
    status: "ok",
    nodeId,
    documentsLoaded: documents.length,
  });
});

app.post("/tasks/search", (request, response) => {
  const fragment = request.body?.fragment;
  const taskId = request.body?.taskId;
  const limit = Number(request.body?.limit || 5);

  if (!fragment || typeof fragment !== "string") {
    return response.status(400).json({
      error: "A non-empty fragment is required.",
    });
  }

  const matches = searchShard(documents, fragment, { limit });

  return response.json({
    nodeId,
    taskId,
    matches,
  });
});

app.listen(port, () => {
  console.log(`[${nodeId}] listening on port ${port} with dataset ${datasetName}`);
});
