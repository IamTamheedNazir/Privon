function mergeSearchResults(partialResponses, options = {}) {
  const minimumMatchedFragments = options.minimumMatchedFragments ?? 1;
  const limit = options.limit ?? 10;
  const mergedResults = new Map();

  for (const response of partialResponses) {
    if (!response || !Array.isArray(response.matches)) {
      continue;
    }

    for (const match of response.matches) {
      const existing = mergedResults.get(match.documentId) ?? {
        documentId: match.documentId,
        title: match.title,
        snippet: match.snippet,
        score: 0,
        matchedFragments: new Set(),
        sourceNodes: new Set(),
      };

      existing.score += match.score ?? 0;
      existing.title = existing.title || match.title;
      existing.snippet = existing.snippet || match.snippet;
      existing.sourceNodes.add(response.nodeId || "unknown-node");

      for (const fragment of match.matchedFragments || []) {
        existing.matchedFragments.add(fragment);
      }

      mergedResults.set(match.documentId, existing);
    }
  }

  return [...mergedResults.values()]
    .filter(
      (result) => result.matchedFragments.size >= minimumMatchedFragments,
    )
    .map((result) => ({
      documentId: result.documentId,
      title: result.title,
      snippet: result.snippet,
      score: result.score,
      matchedFragments: [...result.matchedFragments].sort(),
      sourceNodes: [...result.sourceNodes].sort(),
    }))
    .sort((left, right) => {
      const fragmentDelta =
        right.matchedFragments.length - left.matchedFragments.length;

      if (fragmentDelta !== 0) {
        return fragmentDelta;
      }

      const scoreDelta = right.score - left.score;

      if (scoreDelta !== 0) {
        return scoreDelta;
      }

      return left.title.localeCompare(right.title);
    })
    .slice(0, limit);
}

module.exports = {
  mergeSearchResults,
};
