const { mergeSearchResults } = require("../aggregator/resultMerger");

async function mergeResults(partialResponses, options = {}) {
  const aggregatorUrl = options.aggregatorUrl || process.env.AGGREGATOR_URL;
  const limit = options.limit ?? 10;
  const minimumMatchedFragments = options.minimumMatchedFragments ?? 1;

  if (!aggregatorUrl) {
    return mergeSearchResults(partialResponses, {
      limit,
      minimumMatchedFragments,
    });
  }

  const response = await fetch(`${aggregatorUrl}/merge`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    signal: AbortSignal.timeout(options.requestTimeoutMs ?? 1500),
    body: JSON.stringify({
      partialResponses,
      limit,
      minimumMatchedFragments,
    }),
  });

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(
      `Aggregator request failed: ${response.status} ${errorBody}`,
    );
  }

  const body = await response.json();
  return body.results;
}

module.exports = {
  mergeResults,
};
