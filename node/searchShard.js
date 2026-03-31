const {
  createPipelineError,
  logPipelineStage,
  validateDocuments,
  validateFragment,
  validateRankedMatches,
} = require("../core/pipelineValidation");

function countOccurrences(text, term) {
  if (!text || !term) {
    return 0;
  }

  const loweredText = text.toLowerCase();
  const loweredTerm = term.toLowerCase();
  let index = 0;
  let count = 0;

  while (index < loweredText.length) {
    const foundAt = loweredText.indexOf(loweredTerm, index);

    if (foundAt === -1) {
      break;
    }

    count += 1;
    index = foundAt + loweredTerm.length;
  }

  return count;
}

function buildSnippet(content, term) {
  const loweredContent = content.toLowerCase();
  const loweredTerm = term.toLowerCase();
  const foundAt = loweredContent.indexOf(loweredTerm);

  if (foundAt === -1) {
    return content.slice(0, 120);
  }

  const start = Math.max(0, foundAt - 30);
  const end = Math.min(content.length, foundAt + loweredTerm.length + 60);

  return content.slice(start, end).trim();
}

function extractTerms(fragment) {
  return [...new Set(
    String(fragment || "")
      .toLowerCase()
      .split(/[^a-z0-9]+/i)
      .map((term) => term.trim())
      .filter((term) => term.length >= 2),
  )];
}

function buildStageDiagnostics(stage, startedAt, details = {}) {
  return {
    stage,
    durationMs: Math.max(0, Date.now() - startedAt),
    success: details.success ?? true,
    ...details,
  };
}

function fetchDocuments(documents, fragment, logger) {
  const startedAt = Date.now();
  validateFragment(fragment, "fetch");
  validateDocuments(documents, "fetch");

  const fetchedDocuments = documents.map((document) => ({
    id: document.id,
    title: document.title,
    content: document.content,
  }));

  logPipelineStage(logger, "fetch", {
    documentCount: fetchedDocuments.length,
  });

  return {
    documents: fetchedDocuments,
    diagnostics: buildStageDiagnostics("fetch", startedAt, {
      documentCount: fetchedDocuments.length,
    }),
  };
}

function filterDocuments(documents, fragment, logger) {
  const startedAt = Date.now();
  validateDocuments(documents, "filter");
  validateFragment(fragment, "filter");

  const terms = extractTerms(fragment);

  if (terms.length === 0) {
    throw createPipelineError(
      "filter",
      "Pipeline filter stage requires at least one searchable term.",
    );
  }

  const filteredDocuments = documents
    .map((document) => {
      const titleHits = terms.reduce((total, term) => total + countOccurrences(document.title, term), 0);
      const contentHits = terms.reduce((total, term) => total + countOccurrences(document.content, term), 0);
      const score = titleHits * 2 + contentHits;

      if (score === 0) {
        return null;
      }

      return {
        document,
        score,
        primaryTerm: terms.find(
          (term) => countOccurrences(document.title, term) > 0 || countOccurrences(document.content, term) > 0,
        ) || terms[0],
      };
    })
    .filter(Boolean);

  logPipelineStage(logger, "filter", {
    termCount: terms.length,
    matchedDocuments: filteredDocuments.length,
  });

  return {
    documents: filteredDocuments,
    diagnostics: buildStageDiagnostics("filter", startedAt, {
      termCount: terms.length,
      documentCount: filteredDocuments.length,
    }),
  };
}

function rankDocuments(filteredDocuments, fragment, options = {}, logger) {
  const startedAt = Date.now();
  validateFragment(fragment, "rank");

  if (!Array.isArray(filteredDocuments)) {
    throw createPipelineError(
      "rank",
      "Pipeline rank stage requires an array of filtered documents.",
    );
  }

  const limit = options.limit ?? 5;
  const rankedMatches = filteredDocuments
    .map((entry) => ({
      documentId: entry.document.id,
      title: entry.document.title,
      snippet: buildSnippet(entry.document.content, entry.primaryTerm),
      score: entry.score,
      matchedFragments: [fragment],
    }))
    .sort((left, right) => right.score - left.score)
    .slice(0, limit);

  validateRankedMatches(rankedMatches, fragment, "rank");
  logPipelineStage(logger, "rank", {
    limit,
    resultCount: rankedMatches.length,
  });

  return {
    matches: rankedMatches,
    diagnostics: buildStageDiagnostics("rank", startedAt, {
      resultCount: rankedMatches.length,
      limit,
    }),
  };
}

function searchShard(documents, fragment, options = {}) {
  const logger = options.logger;
  const includeDiagnostics = options.includeDiagnostics === true;
  const fetchStage = fetchDocuments(documents, fragment, logger);
  const filterStage = filterDocuments(fetchStage.documents, fragment, logger);
  const rankStage = rankDocuments(filterStage.documents, fragment, options, logger);

  if (!includeDiagnostics) {
    return rankStage.matches;
  }

  return {
    matches: rankStage.matches,
    pipelineDiagnostics: [
      fetchStage.diagnostics,
      filterStage.diagnostics,
      rankStage.diagnostics,
    ],
  };
}

module.exports = {
  searchShard,
};
