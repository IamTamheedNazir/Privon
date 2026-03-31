const assert = require("node:assert/strict");

const { mergeSearchResults } = require("../aggregator/resultMerger");
const { buildSearchPayload } = require("../client/searchClient");
const { dispatchTasks } = require("../coordinator/dispatchTasks");
const { createOpaqueId } = require("../core/createOpaqueId");
const { normalizeQuery, splitSearchTask } = require("../core/taskSplitter");
const { loadDataset } = require("../node/loadDataset");
const { searchShard } = require("../node/searchShard");

async function runTest(name, callback) {
  try {
    await callback();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error.stack);
    process.exitCode = 1;
  }
}

async function main() {
  await runTest("normalizeQuery lowercases and trims input", () => {
    assert.equal(normalizeQuery("  Private Compute  "), "private compute");
  });

  await runTest("splitSearchTask returns unique searchable fragments", () => {
    assert.deepEqual(splitSearchTask("Private compute compute search"), [
      "private",
      "compute",
      "search",
    ]);
  });

  await runTest("splitSearchTask drops short fragments", () => {
    assert.deepEqual(splitSearchTask("a pc distributed", { minFragmentLength: 3 }), [
      "distributed",
    ]);
  });

  await runTest("createOpaqueId creates opaque prefixed identifiers", () => {
    const id = createOpaqueId("task");
    assert.equal(id.startsWith("task-"), true);
    assert.equal(id.includes("private"), false);
  });

  await runTest("buildSearchPayload converts a query into coordinator-safe fragments", () => {
    assert.deepEqual(buildSearchPayload("Private compute search", { limit: 7 }), {
      fragments: ["private", "compute", "search"],
      limit: 7,
    });
  });

  await runTest("dispatchTasks uses opaque task identifiers", async () => {
    const originalFetch = global.fetch;
    const seenTaskIds = [];

    global.fetch = async (_url, options) => {
      const payload = JSON.parse(options.body);
      seenTaskIds.push(payload.taskId);

      return {
        ok: true,
        async json() {
          return {
            nodeId: "node-a",
            taskId: payload.taskId,
            matches: [],
          };
        },
      };
    };

    try {
      const result = await dispatchTasks(["private"], ["http://node-a:4001"], {
        limit: 5,
      });

      assert.equal(result.failures.length, 0);
      assert.equal(result.partialResponses.length, 1);
      assert.equal(seenTaskIds.length, 1);
      assert.equal(seenTaskIds[0].startsWith("task-"), true);
      assert.equal(seenTaskIds[0].includes("private"), false);
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("mergeSearchResults combines matches from multiple nodes", () => {
    const merged = mergeSearchResults([
      {
        nodeId: "node-a",
        matches: [
          {
            documentId: "doc-1",
            title: "Private compute overview",
            snippet: "Private compute systems split sensitive work.",
            score: 3,
            matchedFragments: ["private"],
          },
        ],
      },
      {
        nodeId: "node-b",
        matches: [
          {
            documentId: "doc-1",
            title: "Private compute overview",
            snippet: "Private compute systems split sensitive work.",
            score: 2,
            matchedFragments: ["compute"],
          },
        ],
      },
    ]);

    assert.equal(merged.length, 1);
    assert.equal(merged[0].score, 5);
    assert.deepEqual(merged[0].matchedFragments, ["compute", "private"]);
    assert.deepEqual(merged[0].sourceNodes, ["node-a", "node-b"]);
  });

  await runTest("mergeSearchResults enforces fragment threshold", () => {
    const merged = mergeSearchResults(
      [
        {
          nodeId: "node-a",
          matches: [
            {
              documentId: "doc-1",
              title: "Only one fragment",
              snippet: "single match",
              score: 1,
              matchedFragments: ["private"],
            },
          ],
        },
      ],
      { minimumMatchedFragments: 2 },
    );

    assert.deepEqual(merged, []);
  });

  await runTest("loadDataset loads a configured shard", () => {
    const dataset = loadDataset("shard-a");
    assert.equal(Array.isArray(dataset), true);
    assert.equal(dataset.length > 0, true);
  });

  await runTest("searchShard ranks matching documents", () => {
    const dataset = loadDataset("shard-a");
    const matches = searchShard(dataset, "compute");

    assert.equal(matches.length > 0, true);
    assert.equal(matches[0].documentId, "doc-a1");
  });

  if (!process.exitCode) {
    console.log("All checks passed.");
  }
}

main();
