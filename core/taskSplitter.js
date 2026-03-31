const DEFAULT_MIN_FRAGMENT_LENGTH = 2;
const DEFAULT_MAX_FRAGMENTS = 8;

function normalizeQuery(query) {
  if (typeof query !== "string") {
    return "";
  }

  return query.trim().toLowerCase();
}

function splitSearchTask(query, options = {}) {
  const minFragmentLength =
    options.minFragmentLength ?? DEFAULT_MIN_FRAGMENT_LENGTH;
  const maxFragments = options.maxFragments ?? DEFAULT_MAX_FRAGMENTS;

  const normalizedQuery = normalizeQuery(query);

  if (!normalizedQuery) {
    return [];
  }

  const uniqueFragments = new Set();
  const tokens = normalizedQuery.split(/[^a-z0-9]+/i);

  for (const token of tokens) {
    if (token.length < minFragmentLength) {
      continue;
    }

    uniqueFragments.add(token);

    if (uniqueFragments.size >= maxFragments) {
      break;
    }
  }

  return [...uniqueFragments];
}

module.exports = {
  normalizeQuery,
  splitSearchTask,
};
