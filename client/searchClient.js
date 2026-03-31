const { splitSearchTask } = require("../core/taskSplitter");

function buildSearchPayload(query, options = {}) {
  const limit = options.limit ?? 5;
  const fragments = splitSearchTask(query, options);

  if (fragments.length === 0) {
    throw new Error("Search query must include at least one searchable fragment.");
  }

  return {
    fragments,
    limit,
  };
}

async function search(query, options = {}) {
  const coordinatorUrl =
    options.coordinatorUrl || process.env.COORDINATOR_URL || "http://localhost:4000";
  const payload = buildSearchPayload(query, options);

  const response = await fetch(`${coordinatorUrl}/search`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Coordinator request failed: ${response.status} ${errorBody}`);
  }

  return {
    ...await response.json(),
    fragments: payload.fragments,
  };
}

async function runFromCommandLine() {
  const query = process.argv.slice(2).join(" ");

  if (!query) {
    console.error("Usage: npm run start:client:cli -- \"search terms\"");
    process.exitCode = 1;
    return;
  }

  try {
    const result = await search(query);
    console.log(JSON.stringify(result, null, 2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

if (require.main === module) {
  runFromCommandLine();
}

module.exports = {
  buildSearchPayload,
  search,
};
