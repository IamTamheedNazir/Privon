const DEFAULT_MIN_FRAGMENT_LENGTH = 2;
const DEFAULT_MAX_FRAGMENTS = 8;

function normalizeQuery(query) {
  if (typeof query !== "string") {
    return "";
  }

  return query
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function splitSearchTask(query, options = {}) {
  const minFragmentLength = options.minFragmentLength ?? DEFAULT_MIN_FRAGMENT_LENGTH;
  const maxFragments = options.maxFragments ?? DEFAULT_MAX_FRAGMENTS;
  const normalizedQuery = normalizeQuery(query);

  if (!normalizedQuery) {
    return [];
  }

  const phraseFragments = normalizedQuery
    .split(/\s*(?:[\n,;|]+|\s+-\s+)\s*/)
    .map((fragment) => fragment.trim())
    .filter((fragment) => fragment.length >= minFragmentLength);

  if (phraseFragments.length === 0) {
    return [];
  }

  return [...new Set(phraseFragments)].slice(0, maxFragments);
}

module.exports = {
  normalizeQuery,
  splitSearchTask,
};
