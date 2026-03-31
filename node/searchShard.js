function countOccurrences(text, fragment) {
  if (!text || !fragment) {
    return 0;
  }

  const loweredText = text.toLowerCase();
  const loweredFragment = fragment.toLowerCase();
  let index = 0;
  let count = 0;

  while (index < loweredText.length) {
    const foundAt = loweredText.indexOf(loweredFragment, index);

    if (foundAt === -1) {
      break;
    }

    count += 1;
    index = foundAt + loweredFragment.length;
  }

  return count;
}

function buildSnippet(content, fragment) {
  const loweredContent = content.toLowerCase();
  const loweredFragment = fragment.toLowerCase();
  const foundAt = loweredContent.indexOf(loweredFragment);

  if (foundAt === -1) {
    return content.slice(0, 120);
  }

  const start = Math.max(0, foundAt - 30);
  const end = Math.min(content.length, foundAt + loweredFragment.length + 60);

  return content.slice(start, end).trim();
}

function searchShard(documents, fragment, options = {}) {
  const limit = options.limit ?? 5;

  if (!fragment || typeof fragment !== "string") {
    return [];
  }

  return documents
    .map((document) => {
      const titleHits = countOccurrences(document.title, fragment);
      const contentHits = countOccurrences(document.content, fragment);
      const score = titleHits * 2 + contentHits;

      if (score === 0) {
        return null;
      }

      return {
        documentId: document.id,
        title: document.title,
        snippet: buildSnippet(document.content, fragment),
        score,
        matchedFragments: [fragment],
      };
    })
    .filter(Boolean)
    .sort((left, right) => right.score - left.score)
    .slice(0, limit);
}

module.exports = {
  searchShard,
};
