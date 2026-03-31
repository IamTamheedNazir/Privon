function createPipelineError(stage, message, details = {}) {
  const error = new Error(message);
  error.stage = stage;
  error.details = details;
  return error;
}

function summarizeValueShape(value) {
  if (Array.isArray(value)) {
    return {
      type: "array",
      length: value.length,
      itemShape: value.length > 0 ? summarizeValueShape(value[0]) : "empty",
    };
  }

  if (value && typeof value === "object") {
    const summary = {};

    for (const [key, nextValue] of Object.entries(value)) {
      if (typeof nextValue === "string") {
        summary[key] = {
          type: "string",
          length: nextValue.length,
        };
        continue;
      }

      if (typeof nextValue === "number") {
        summary[key] = {
          type: "number",
        };
        continue;
      }

      if (typeof nextValue === "boolean") {
        summary[key] = {
          type: "boolean",
        };
        continue;
      }

      summary[key] = summarizeValueShape(nextValue);
    }

    return {
      type: "object",
      keys: Object.keys(value),
      summary,
    };
  }

  return typeof value;
}

function logPipelineStage(logger, stage, details = {}) {
  if (typeof logger !== "function") {
    return;
  }

  logger(
    `[pipeline] stage=${stage} shape=${JSON.stringify(summarizeValueShape(details))}`,
  );
}

function assertPipelineStage(condition, stage, message, details = {}) {
  if (!condition) {
    throw createPipelineError(stage, message, details);
  }
}

function validateFragment(fragment, stage) {
  assertPipelineStage(
    typeof fragment === "string" && fragment.trim().length > 0,
    stage,
    "Pipeline fragment must be a non-empty string.",
    { fragment },
  );
}

function validateDocuments(documents, stage) {
  assertPipelineStage(Array.isArray(documents), stage, "Pipeline documents must be an array.", {
    documents,
  });

  documents.forEach((document, index) => {
    assertPipelineStage(
      document && typeof document === "object",
      stage,
      `Document at index ${index} must be an object.`,
      { index, document },
    );
    assertPipelineStage(
      typeof document.id === "string" && document.id.trim().length > 0,
      stage,
      `Document at index ${index} is missing a valid id.`,
      { index },
    );
    assertPipelineStage(
      typeof document.title === "string",
      stage,
      `Document at index ${index} is missing a valid title.`,
      { index },
    );
    assertPipelineStage(
      typeof document.content === "string",
      stage,
      `Document at index ${index} is missing valid content.`,
      { index },
    );
  });
}

function validateRankedMatches(matches, fragment, stage) {
  assertPipelineStage(Array.isArray(matches), stage, "Ranked matches must be an array.", {
    matches,
  });

  matches.forEach((match, index) => {
    assertPipelineStage(
      match && typeof match === "object",
      stage,
      `Ranked match at index ${index} must be an object.`,
      { index, match },
    );
    assertPipelineStage(
      typeof match.documentId === "string" && match.documentId.trim().length > 0,
      stage,
      `Ranked match at index ${index} is missing documentId.`,
      { index },
    );
    assertPipelineStage(
      typeof match.title === "string",
      stage,
      `Ranked match at index ${index} is missing title.`,
      { index },
    );
    assertPipelineStage(
      typeof match.snippet === "string",
      stage,
      `Ranked match at index ${index} is missing snippet.`,
      { index },
    );
    assertPipelineStage(
      Number.isFinite(Number(match.score)),
      stage,
      `Ranked match at index ${index} is missing a numeric score.`,
      { index },
    );
    assertPipelineStage(
      Array.isArray(match.matchedFragments) && match.matchedFragments.includes(fragment),
      stage,
      `Ranked match at index ${index} must include the originating fragment.`,
      { index },
    );
  });
}

function validateStageDiagnostics(stageDiagnostics, stage = "verify") {
  if (stageDiagnostics === undefined) {
    return;
  }

  assertPipelineStage(
    Array.isArray(stageDiagnostics),
    stage,
    "Pipeline stage diagnostics must be an array.",
    { stageDiagnostics },
  );

  stageDiagnostics.forEach((entry, index) => {
    assertPipelineStage(
      entry && typeof entry === "object",
      stage,
      `Pipeline stage diagnostic at index ${index} must be an object.`,
      { index, entry },
    );
    assertPipelineStage(
      typeof entry.stage === "string" && entry.stage.trim().length > 0,
      stage,
      `Pipeline stage diagnostic at index ${index} must include stage.`,
      { index },
    );
    assertPipelineStage(
      Number.isFinite(Number(entry.durationMs)) && Number(entry.durationMs) >= 0,
      stage,
      `Pipeline stage diagnostic at index ${index} must include durationMs.`,
      { index },
    );
    assertPipelineStage(
      typeof entry.success === "boolean",
      stage,
      `Pipeline stage diagnostic at index ${index} must include success.`,
      { index },
    );
    assertPipelineStage(
      !entry.nodes || Array.isArray(entry.nodes),
      stage,
      `Pipeline stage diagnostic at index ${index} nodes must be an array when present.`,
      { index },
    );
  });
}
function validateNodeResponse(response, fragment) {
  const stage = "verify";
  assertPipelineStage(response && typeof response === "object", stage, "Node response must be an object.", {
    response,
  });
  assertPipelineStage(
    Number.isInteger(Number(response.fragmentIndex)),
    stage,
    "Node response must include a numeric fragmentIndex.",
    { response },
  );
  validateRankedMatches(response.matches || [], fragment, stage);
  validateStageDiagnostics(response.pipelineDiagnostics, stage);
}

module.exports = {
  assertPipelineStage,
  createPipelineError,
  logPipelineStage,
  summarizeValueShape,
  validateDocuments,
  validateFragment,
  validateNodeResponse,
  validateRankedMatches,
  validateStageDiagnostics,
};
