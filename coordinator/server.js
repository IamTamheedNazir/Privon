const express = require("express");

const { createOpaqueId } = require("../core/createOpaqueId");
const { dispatchTasks } = require("./dispatchTasks");
const { mergeResults } = require("./mergeResults");

const app = express();
const port = Number(process.env.PORT || 4000);
const aggregatorUrl = process.env.AGGREGATOR_URL || "";
const nodeUrls = (process.env.NODE_URLS ||
  "http://localhost:4001,http://localhost:4002")
  .split(",")
  .map((url) => url.trim())
  .filter(Boolean);

app.use(express.json({ limit: "16kb" }));

app.get("/health", (_request, response) => {
  response.json({
    status: "ok",
    nodesConfigured: nodeUrls.length,
    aggregatorConfigured: Boolean(aggregatorUrl),
  });
});

app.post("/search", async (request, response) => {
  const fragments = request.body?.fragments;
  const limit = Number(request.body?.limit || 5);

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

  try {
    const { partialResponses, failures } = await dispatchTasks(
      sanitizedFragments,
      nodeUrls,
      { limit },
    );

    const results = await mergeResults(partialResponses, {
      aggregatorUrl,
      limit,
    });

    return response.json({
      jobId: createOpaqueId("job"),
      fragmentsProcessed: sanitizedFragments.length,
      nodesUsed: nodeUrls.length,
      failures,
      results,
    });
  } catch (error) {
    return response.status(502).json({
      error: error.message,
    });
  }
});

app.listen(port, () => {
  console.log(`[coordinator] listening on port ${port} with ${nodeUrls.length} nodes`);
});
