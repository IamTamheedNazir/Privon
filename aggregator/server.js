const express = require("express");

const { mergeSearchResults } = require("./resultMerger");

const app = express();
const port = Number(process.env.PORT || 4003);

app.use(express.json({ limit: "64kb" }));

app.get("/health", (_request, response) => {
  response.json({
    status: "ok",
  });
});

app.post("/merge", (request, response) => {
  const partialResponses = request.body?.partialResponses;
  const limit = Number(request.body?.limit || 10);
  const minimumMatchedFragments = Number(
    request.body?.minimumMatchedFragments || 1,
  );

  if (!Array.isArray(partialResponses)) {
    return response.status(400).json({
      error: "A partialResponses array is required.",
    });
  }

  const results = mergeSearchResults(partialResponses, {
    limit,
    minimumMatchedFragments,
  });

  return response.json({
    partialResponsesMerged: partialResponses.length,
    resultsReturned: results.length,
    results,
  });
});

app.listen(port, () => {
  console.log(`[aggregator] listening on port ${port}`);
});
