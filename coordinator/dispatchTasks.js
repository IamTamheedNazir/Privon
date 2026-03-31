const { createOpaqueId } = require("../core/createOpaqueId");

async function dispatchTasks(fragments, nodeUrls, options = {}) {
  const limit = options.limit ?? 5;

  if (!Array.isArray(nodeUrls) || nodeUrls.length === 0) {
    throw new Error("At least one node URL is required.");
  }

  const requests = [];

  for (const fragment of fragments) {
    for (const nodeUrl of nodeUrls) {
      const taskId = createOpaqueId("task");

      requests.push(
        fetch(`${nodeUrl}/tasks/search`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify({
            taskId,
            fragment,
            limit,
          }),
        }).then(async (response) => {
          if (!response.ok) {
            throw new Error(`Node ${nodeUrl} returned ${response.status}`);
          }

          return response.json();
        }),
      );
    }
  }

  const settledResponses = await Promise.allSettled(requests);
  const partialResponses = [];
  const failures = [];

  for (const settledResponse of settledResponses) {
    if (settledResponse.status === "fulfilled") {
      partialResponses.push(settledResponse.value);
      continue;
    }

    failures.push({
      message: settledResponse.reason.message,
    });
  }

  return {
    partialResponses,
    failures,
  };
}

module.exports = {
  dispatchTasks,
};
