const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const { mergeSearchResults } = require("../aggregator/resultMerger");
const { buildSearchPayload } = require("../client/searchClient");
const { createApiKeyAuth } = require("../coordinator/auth");
const { dispatchTasks } = require("../coordinator/dispatchTasks");
const { createDashboardState } = require("../coordinator/dashboardState");
const { getHealthyNodeUrls } = require("../coordinator/getHealthyNodeUrls");
const { createNodeRegistry } = require("../coordinator/nodeRegistry");
const { createCoordinatorStore } = require("../coordinator/sqliteStore");
const { decryptPayload, encryptPayload, isEncryptedPayload } = require("../core/cryptoTransport");
const { createOpaqueId } = require("../core/createOpaqueId");
const { distributeFragments } = require("../core/distributor");
const { loadDataset } = require("../node/loadDataset");
const { registerWithCoordinator, sendHeartbeat } = require("../node/registerWithCoordinator");
const { searchShard } = require("../node/searchShard");
const { normalizeQuery, splitSearchTask } = require("../core/taskSplitter");

const encryptionOptions = {
  enabled: true,
  secret: "pcn-test-secret",
};

function createTempStore(options = {}) {
  const tempDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "privon-store-"));
  const storePath = path.join(tempDirectory, "privon.sqlite");
  const store = createCoordinatorStore({
    filePath: storePath,
    now: options.now,
  });

  return {
    tempDirectory,
    storePath,
    store,
    cleanup() {
      store.close();
      fs.rmSync(tempDirectory, { recursive: true, force: true });
    },
  };
}

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

  await runTest("node registry registers and deduplicates nodes", () => {
    let currentTime = 1000;
    const registry = createNodeRegistry({ now: () => currentTime });

    const firstRegistration = registry.registerNode({
      url: "http://localhost:4001/",
      capacity: 10,
    });

    currentTime = 2000;

    const duplicateRegistration = registry.registerNode({
      url: "http://localhost:4001",
      capacity: 12,
    });

    assert.equal(firstRegistration.duplicate, false);
    assert.equal(duplicateRegistration.duplicate, true);
    assert.equal(registry.listNodes().length, 1);
    assert.deepEqual(registry.listNodes()[0], {
      url: "http://localhost:4001",
      capacity: 12,
      lastSeen: 2000,
      status: "active",
      score: 100,
      totalTasks: 0,
      successfulTasks: 0,
      failedTasks: 0,
      shardId: "default-shard",
      replicaGroup: "default-replica",
    });
  });

  await runTest("node registry marks nodes inactive and heartbeat reactivates them", () => {
    let currentTime = 1000;
    const events = [];
    const registry = createNodeRegistry({
      now: () => currentTime,
      inactivityThresholdMs: 30000,
      onInactive(node) {
        events.push(`inactive:${node.url}`);
      },
      onReconnect(node) {
        events.push(`reconnect:${node.url}`);
      },
    });

    registry.registerNode({
      url: "http://localhost:4001",
      capacity: 10,
    });

    currentTime = 32001;
    registry.cleanupInactiveNodes();

    assert.equal(registry.listActiveNodes().length, 0);
    assert.deepEqual(events, ["inactive:http://localhost:4001"]);

    currentTime = 33000;
    const heartbeat = registry.heartbeat({ url: "http://localhost:4001" });

    assert.equal(heartbeat.found, true);
    assert.equal(registry.listActiveNodes().length, 1);
    assert.deepEqual(events, [
      "inactive:http://localhost:4001",
      "reconnect:http://localhost:4001",
    ]);
  });

  await runTest("node registry ignores heartbeat for unknown nodes", () => {
    const registry = createNodeRegistry();
    const result = registry.heartbeat({ url: "http://localhost:4999" });

    assert.deepEqual(result, { found: false });
  });

  await runTest("node registry updates task metrics and clamps node scores", () => {
    const scoreChanges = [];
    const registry = createNodeRegistry({
      scoreSuccessIncrement: 150,
      scoreFailureDecrement: 250,
      onScoreChange(node, previousScore) {
        scoreChanges.push(`${node.url}:${previousScore}->${node.score}`);
      },
    });

    registry.registerNode({
      url: "http://localhost:4101",
      capacity: 2,
    });

    registry.recordAssignment("http://localhost:4101");
    registry.recordSuccess("http://localhost:4101");
    registry.recordFailure("http://localhost:4101", "timeout");

    assert.deepEqual(registry.getNode("http://localhost:4101"), {
      url: "http://localhost:4101",
      capacity: 2,
      lastSeen: registry.getNode("http://localhost:4101").lastSeen,
      status: "inactive",
      score: 0,
      totalTasks: 1,
      successfulTasks: 1,
      failedTasks: 1,
      shardId: "default-shard",
      replicaGroup: "default-replica",
    });
    assert.deepEqual(scoreChanges, [
      "http://localhost:4101:100->200",
      "http://localhost:4101:200->0",
    ]);
  });

  await runTest("node registry moves low-score nodes into probation and reintegrates them", () => {
    const events = [];
    const registry = createNodeRegistry({
      scoreFailureDecrement: 81,
      probationSuccessBoost: 5,
      onProbation(node, reason) {
        events.push(`probation:${node.url}:${node.score}:${reason}`);
      },
      onRecovery(node, previousScore) {
        events.push(`recovery:${node.url}:${previousScore}->${node.score}`);
      },
      onReintegration(node) {
        events.push(`reintegration:${node.url}:${node.score}`);
      },
    });

    registry.registerNode({
      url: "http://localhost:4102",
      capacity: 1,
    });

    registry.recordFailure("http://localhost:4102", "mismatch");

    assert.equal(registry.isNodeSelectable("http://localhost:4102"), false);
    assert.equal(registry.isNodeProbationEligible("http://localhost:4102"), true);
    assert.equal(registry.listActiveNodes().length, 0);
    assert.equal(registry.listProbationNodes().length, 1);

    registry.recordProbationSuccess("http://localhost:4102");
    registry.recordProbationSuccess("http://localhost:4102");
    registry.recordProbationSuccess("http://localhost:4102");
    registry.recordProbationSuccess("http://localhost:4102");
    registry.recordProbationSuccess("http://localhost:4102");

    assert.equal(registry.isNodeSelectable("http://localhost:4102"), true);
    assert.equal(registry.isNodeProbationEligible("http://localhost:4102"), false);
    assert.equal(registry.listActiveNodes().length, 1);
    assert.equal(registry.listProbationNodes().length, 0);
    assert.deepEqual(events, [
      "probation:http://localhost:4102:19:mismatch",
      "recovery:http://localhost:4102:19->24",
      "recovery:http://localhost:4102:24->29",
      "recovery:http://localhost:4102:29->34",
      "recovery:http://localhost:4102:34->39",
      "reintegration:http://localhost:4102:44",
    ]);
  });

  await runTest("createOpaqueId creates opaque prefixed identifiers", () => {
    const id = createOpaqueId("task");
    assert.equal(id.startsWith("task-"), true);
    assert.equal(id.includes("private"), false);
  });

  await runTest("encryptPayload and decryptPayload round-trip data", () => {
    const encrypted = encryptPayload({ fragment: "private" }, encryptionOptions);

    assert.equal(isEncryptedPayload(encrypted), true);
    assert.deepEqual(decryptPayload(encrypted, encryptionOptions), {
      fragment: "private",
    });
  });

  await runTest("api key auth enforces roles, expiration, and session renewal", () => {
    let currentTime = 50_000;
    const { store, cleanup } = createTempStore({ now: () => currentTime });

    try {
      store.ensureApiKey({
        key: "admin-key",
        role: "super_admin",
        createdAt: currentTime - 1_000,
        expiresAt: currentTime + 120_000,
        status: "active",
      });
      store.ensureApiKey({
        key: "node-key",
        role: "node",
        createdAt: currentTime - 1_000,
        expiresAt: currentTime + 120_000,
        status: "active",
      });
      store.ensureApiKey({
        key: "expired-key",
        role: "viewer",
        createdAt: currentTime - 10_000,
        expiresAt: currentTime - 1,
        status: "active",
      });

      const auth = createApiKeyAuth({
        keyStore: store,
        sessionTtlMs: 60_000,
        now: () => currentTime,
      });

      const createResponse = () => ({
        statusCode: 200,
        payload: null,
        headers: {},
        setHeader(name, value) {
          this.headers[name] = value;
        },
        status(code) {
          this.statusCode = code;
          return this;
        },
        json(payload) {
          this.payload = payload;
          return this;
        },
      });
      const createRequest = (headers = {}) => ({
        headers,
        get(name) {
          return this.headers[String(name).toLowerCase()] || this.headers[name] || "";
        },
      });

      const adminRequest = createRequest({ authorization: "Bearer admin-key" });
      const adminResponse = createResponse();
      assert.equal(auth.ensureAuthorized(adminRequest, adminResponse, { allowedRoles: ["super_admin"] }), true);

      const nodeRequest = createRequest({ authorization: "Bearer node-key" });
      const nodeResponse = createResponse();
      assert.equal(auth.ensureAuthorized(nodeRequest, nodeResponse, { allowedRoles: ["super_admin"] }), false);
      assert.equal(nodeResponse.statusCode, 403);

      const expiredRequest = createRequest({ authorization: "Bearer expired-key" });
      const expiredResponse = createResponse();
      assert.equal(auth.ensureAuthorized(expiredRequest, expiredResponse, { allowedRoles: ["viewer"] }), false);
      assert.equal(expiredResponse.statusCode, 401);
      assert.equal(store.getApiKey("expired-key").status, "expired");

      const sessionResponse = createResponse();
      const sessionInfo = auth.createSessionFromApiKey(adminRequest, sessionResponse, ["super_admin"]);
      const sessionCookie = String(sessionResponse.headers["Set-Cookie"]).split(";")[0];
      const sessionRequest = createRequest({ cookie: sessionCookie });
      const sessionAuthResponse = createResponse();
      assert.equal(auth.ensureAuthorized(sessionRequest, sessionAuthResponse, { allowSession: true, allowedRoles: ["super_admin"] }), true);
      assert.equal(sessionInfo.role, "super_admin");

      currentTime += 20_000;
      const renewResponse = createResponse();
      const renewed = auth.renewSession(sessionRequest, renewResponse, ["super_admin"]);
      assert.equal(renewed.role, "super_admin");
      assert.equal(renewed.expiresAt > sessionInfo.expiresAt, true);
    } finally {
      cleanup();
    }
  });

  await runTest("sqlite store persists nodes and manageable api keys", () => {
    const { store, storePath, cleanup } = createTempStore();

    try {
      const nodes = [
        {
          url: "http://localhost:4001",
          capacity: 5,
          lastSeen: 1000,
          status: "active",
          score: 120,
          totalTasks: 8,
          successfulTasks: 7,
          failedTasks: 1,
          shardId: "shard-a",
          replicaGroup: "replica-a",
        },
      ];

      store.saveNodes(nodes);
      const createdApiKey = store.createApiKey({
        key: "viewer-key",
        role: "viewer",
        createdAt: 1000,
        expiresAt: 2000,
        status: "active",
      });
      const revokedApiKey = store.revokeApiKey("viewer-key");

      assert.deepEqual(store.loadNodes(), nodes);
      assert.equal(fs.existsSync(storePath), true);
      assert.equal(createdApiKey.role, "viewer");
      assert.equal(revokedApiKey.status, "revoked");
      assert.equal(store.listApiKeys().length >= 1, true);
    } finally {
      cleanup();
    }
  });

  await runTest("buildSearchPayload converts a query into coordinator-safe fragments", () => {
    assert.deepEqual(
      buildSearchPayload("Private compute search", {
        limit: 7,
        minimumMatchedFragments: 2,
      }),
      {
        fragments: ["private", "compute", "search"],
        limit: 7,
        minimumMatchedFragments: 2,
      },
    );
  });

  await runTest("getHealthyNodeUrls keeps only healthy nodes", async () => {
    const originalFetch = global.fetch;

    global.fetch = async (url) => ({ ok: !String(url).includes("node-b") });

    try {
      const healthyNodeUrls = await getHealthyNodeUrls([
        "http://node-a:4001",
        "http://node-b:4002",
      ]);

      assert.deepEqual(healthyNodeUrls, ["http://node-a:4001"]);
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("dashboard state captures logs, executions, and shard summaries", () => {
    const dashboardState = createDashboardState({ maxLogs: 10, maxExecutions: 10 });
    const publishedEvents = [];

    const unsubscribe = dashboardState.subscribe((frame) => {
      publishedEvents.push(frame.event);
    });

    dashboardState.recordLog({
      type: "node.score",
      message: "node score changed",
      metadata: { nodeUrl: "http://node-a:4001" },
      timestamp: 1000,
    });
    dashboardState.recordSearchExecution({
      jobId: "job-1",
      fragments: ["alpha"],
      fragmentResults: [
        {
          success: true,
          fragmentIndex: 0,
          replicaGroup: "replica-a",
          shardIds: ["shard-a"],
          nodeUrls: ["http://node-a:4001", "http://node-a:4002"],
          probation: { attempted: false },
        },
      ],
      failures: [],
    });
    dashboardState.setNodes([
      {
        url: "http://node-a:4001",
        status: "active",
        score: 100,
        totalTasks: 4,
        shardId: "shard-a",
        replicaGroup: "replica-a",
      },
      {
        url: "http://node-a:4002",
        status: "probation",
        score: 25,
        totalTasks: 1,
        shardId: "shard-a",
        replicaGroup: "replica-a",
      },
      {
        url: "http://node-b:4003",
        status: "inactive",
        score: 90,
        totalTasks: 0,
        shardId: "shard-b",
        replicaGroup: "replica-b",
      },
    ]);

    const stats = dashboardState.getStats();

    assert.equal(stats.summary.totalNodes, 3);
    assert.equal(stats.summary.activeNodes, 1);
    assert.equal(stats.summary.probationNodes, 1);
    assert.equal(stats.summary.inactiveNodes, 1);
    assert.equal(stats.replicaGroups.length, 2);
    assert.equal(stats.shardSummaries.length, 2);
    assert.equal(stats.recentExecutions.length, 1);
    assert.equal(dashboardState.getLogs(5).length, 1);
    assert.deepEqual(publishedEvents, ["log.append", "task.execution", "node.update"]);
    unsubscribe();
  });

  await runTest("distributeFragments verifies matching redundant results", async () => {
    const originalFetch = global.fetch;
    const attemptedUrls = [];

    global.fetch = async (url, options) => {
      attemptedUrls.push(String(url));
      const payload = JSON.parse(options.body);

      return {
        ok: true,
        async json() {
          return {
            nodeId: String(url).includes("node-a") ? "node-a" : "node-b",
            taskId: payload.taskId,
            fragmentIndex: payload.fragmentIndex,
            matches: [
              {
                documentId: "doc-1",
                title: "Same",
                snippet: "Same result",
                score: 2,
                matchedFragments: [payload.fragment],
              },
            ],
          };
        },
      };
    };

    try {
      const result = await distributeFragments(
        ["alpha"],
        [
          { url: "http://node-a:4001", capacity: 1, status: "active" },
          { url: "http://node-b:4002", capacity: 1, status: "active" },
        ],
        { redundancyFactor: 2, verificationRetryRounds: 2, requestTimeoutMs: 1000 },
      );

      assert.equal(result.partialResponses.length, 1);
      assert.equal(result.failures.length, 0);
      assert.deepEqual(attemptedUrls, [
        "http://node-a:4001/compute",
        "http://node-b:4002/compute",
      ]);
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("distributeFragments verifies replica groups independently", async () => {
    const originalFetch = global.fetch;
    const attemptedUrls = [];

    global.fetch = async (url, options) => {
      attemptedUrls.push(String(url));
      const payload = JSON.parse(options.body);
      const group = String(url).includes("group-b") ? "b" : "a";

      return {
        ok: true,
        async json() {
          return {
            nodeId: String(url),
            shardId: "shard-" + group,
            replicaGroup: "replica-" + group,
            taskId: payload.taskId,
            fragmentIndex: payload.fragmentIndex,
            matches: [
              {
                documentId: "doc-" + group,
                title: "Group " + group.toUpperCase(),
                snippet: "Group " + group.toUpperCase() + " result",
                score: 2,
                matchedFragments: [payload.fragment],
              },
            ],
          };
        },
      };
    };

    try {
      const result = await distributeFragments(
        ["alpha"],
        [
          { url: "http://group-a-node-1:4001", capacity: 1, status: "active", shardId: "shard-a", replicaGroup: "replica-a" },
          { url: "http://group-a-node-2:4002", capacity: 1, status: "active", shardId: "shard-a", replicaGroup: "replica-a" },
          { url: "http://group-b-node-1:4003", capacity: 1, status: "active", shardId: "shard-b", replicaGroup: "replica-b" },
          { url: "http://group-b-node-2:4004", capacity: 1, status: "active", shardId: "shard-b", replicaGroup: "replica-b" },
        ],
        { redundancyFactor: 2, verificationRetryRounds: 0, requestTimeoutMs: 1000 },
      );

      assert.equal(result.partialResponses.length, 2);
      assert.equal(result.failures.length, 0);
      assert.deepEqual(result.fragmentResults.map((entry) => entry.replicaGroup), ["replica-a", "replica-b"]);
      assert.deepEqual(attemptedUrls, [
        "http://group-a-node-1:4001/compute",
        "http://group-a-node-2:4002/compute",
        "http://group-b-node-1:4003/compute",
        "http://group-b-node-2:4004/compute",
      ]);
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("distributeFragments prefers higher-score nodes and lower load", async () => {
    const originalFetch = global.fetch;
    const attemptedUrls = [];

    global.fetch = async (url, options) => {
      attemptedUrls.push(String(url));
      const payload = JSON.parse(options.body);

      return {
        ok: true,
        async json() {
          return {
            nodeId: String(url),
            taskId: payload.taskId,
            fragmentIndex: payload.fragmentIndex,
            matches: [
              {
                documentId: "doc-" + payload.fragmentIndex,
                title: "Stable",
                snippet: "Stable result",
                score: 1,
                matchedFragments: [payload.fragment],
              },
            ],
          };
        },
      };
    };

    try {
      const result = await distributeFragments(
        ["alpha", "beta"],
        [
          { url: "http://node-a:4001", capacity: 1, status: "active", score: 150 },
          { url: "http://node-b:4002", capacity: 2, status: "active", score: 150 },
          { url: "http://node-c:4003", capacity: 1, status: "active", score: 100 },
        ],
        { redundancyFactor: 2, verificationRetryRounds: 0, requestTimeoutMs: 1000 },
      );

      assert.equal(result.partialResponses.length, 2);
      assert.deepEqual(attemptedUrls, [
        "http://node-a:4001/compute",
        "http://node-b:4002/compute",
        "http://node-b:4002/compute",
        "http://node-a:4001/compute",
      ]);
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("distributeFragments keeps probation nodes out of verification pairs and uses shadow traffic", async () => {
    const originalFetch = global.fetch;
    const attemptedUrls = [];
    const probationSuccesses = [];

    global.fetch = async (url, options) => {
      attemptedUrls.push(String(url));
      const payload = JSON.parse(options.body);

      return {
        ok: true,
        async json() {
          return {
            nodeId: String(url),
            taskId: payload.taskId,
            fragmentIndex: payload.fragmentIndex,
            matches: [
              {
                documentId: "doc-1",
                title: "Shadow Safe",
                snippet: "Shadow Safe",
                score: 1,
                matchedFragments: [payload.fragment],
              },
            ],
          };
        },
      };
    };

    try {
      const result = await distributeFragments(
        ["alpha"],
        [
          { url: "http://node-a:4001", capacity: 1, status: "active", score: 150 },
          { url: "http://node-b:4002", capacity: 1, status: "active", score: 140 },
          { url: "http://node-p:4003", capacity: 1, status: "probation", score: 15 },
        ],
        {
          redundancyFactor: 2,
          verificationRetryRounds: 0,
          requestTimeoutMs: 1000,
          probationTrafficRatio: 1,
          random: () => 0,
          onProbationSuccess(node) {
            probationSuccesses.push(node.url);
          },
        },
      );

      assert.equal(result.partialResponses.length, 1);
      assert.deepEqual(attemptedUrls, [
        "http://node-a:4001/compute",
        "http://node-b:4002/compute",
        "http://node-p:4003/compute",
      ]);
      assert.deepEqual(probationSuccesses, ["http://node-p:4003"]);
      assert.deepEqual(result.fragmentResults[0].nodeUrls, [
        "http://node-a:4001",
        "http://node-b:4002",
      ]);
      assert.deepEqual(result.fragmentResults[0].probation, {
        attempted: true,
        matched: true,
        nodeUrl: "http://node-p:4003",
      });
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("distributeFragments encrypts payloads and decrypts responses when enabled", async () => {
    const originalFetch = global.fetch;

    global.fetch = async (_url, options) => {
      const encryptedRequest = JSON.parse(options.body);
      assert.equal(isEncryptedPayload(encryptedRequest), true);
      const decryptedRequest = decryptPayload(encryptedRequest, encryptionOptions);

      return {
        ok: true,
        async json() {
          return encryptPayload(
            {
              nodeId: "node-a",
              taskId: decryptedRequest.taskId,
              fragmentIndex: decryptedRequest.fragmentIndex,
              matches: [
                {
                  documentId: "doc-1",
                  title: "Encrypted",
                  snippet: "Encrypted result",
                  score: 1,
                  matchedFragments: [decryptedRequest.fragment],
                },
              ],
            },
            encryptionOptions,
          );
        },
      };
    };

    try {
      const result = await distributeFragments(
        ["alpha"],
        [
          { url: "http://node-a:4001", capacity: 1, status: "active" },
          { url: "http://node-b:4002", capacity: 1, status: "active" },
        ],
        {
          ...encryptionOptions,
          redundancyFactor: 2,
          verificationRetryRounds: 0,
          requestTimeoutMs: 1000,
        },
      );

      assert.equal(result.partialResponses.length, 1);
      assert.equal(result.partialResponses[0].matches[0].title, "Encrypted");
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("distributeFragments treats decryption failure as a node failure", async () => {
    const originalFetch = global.fetch;

    global.fetch = async (_url, options) => {
      const encryptedRequest = JSON.parse(options.body);
      const decryptedRequest = decryptPayload(encryptedRequest, encryptionOptions);

      return {
        ok: true,
        async json() {
          return {
            iv: "broken",
            content: decryptedRequest.taskId,
            tag: "broken",
          };
        },
      };
    };

    try {
      const result = await distributeFragments(
        ["alpha"],
        [
          { url: "http://node-a:4001", capacity: 1, status: "active" },
          { url: "http://node-b:4002", capacity: 1, status: "active" },
        ],
        {
          ...encryptionOptions,
          redundancyFactor: 2,
          verificationRetryRounds: 0,
          requestTimeoutMs: 1000,
        },
      );

      assert.equal(result.partialResponses.length, 0);
      assert.equal(result.failures.length, 1);
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("distributeFragments retries failed redundant copy on another node", async () => {
    const originalFetch = global.fetch;
    const attemptedUrls = [];

    global.fetch = async (url, options) => {
      attemptedUrls.push(String(url));
      const payload = JSON.parse(options.body);

      if (String(url).includes("node-b")) {
        throw new Error("socket hang up");
      }

      return {
        ok: true,
        async json() {
          return {
            nodeId: String(url).includes("node-a") ? "node-a" : "node-c",
            taskId: payload.taskId,
            fragmentIndex: payload.fragmentIndex,
            matches: [
              {
                documentId: "doc-1",
                title: "Same",
                snippet: "Same result",
                score: 2,
                matchedFragments: [payload.fragment],
              },
            ],
          };
        },
      };
    };

    try {
      const result = await distributeFragments(
        ["alpha"],
        [
          { url: "http://node-a:4001", capacity: 1, status: "active" },
          { url: "http://node-b:4002", capacity: 1, status: "active" },
          { url: "http://node-c:4003", capacity: 1, status: "active" },
        ],
        { redundancyFactor: 2, verificationRetryRounds: 2, maxRetries: 3, requestTimeoutMs: 1000 },
      );

      assert.equal(result.partialResponses.length, 1);
      assert.equal(result.failures.length, 0);
      assert.deepEqual(attemptedUrls, [
        "http://node-a:4001/compute",
        "http://node-b:4002/compute",
        "http://node-c:4003/compute",
      ]);
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("distributeFragments rejects mismatched redundant results after retry rounds", async () => {
    const originalFetch = global.fetch;

    global.fetch = async (url, options) => {
      const payload = JSON.parse(options.body);
      const documentId = String(url).includes("node-a") || String(url).includes("node-c")
        ? "doc-a"
        : "doc-b";

      return {
        ok: true,
        async json() {
          return {
            nodeId: String(url),
            taskId: payload.taskId,
            fragmentIndex: payload.fragmentIndex,
            matches: [
              {
                documentId,
                title: documentId,
                snippet: documentId,
                score: 1,
                matchedFragments: [payload.fragment],
              },
            ],
          };
        },
      };
    };

    try {
      const result = await distributeFragments(
        ["alpha"],
        [
          { url: "http://node-a:4001", capacity: 1, status: "active" },
          { url: "http://node-b:4002", capacity: 1, status: "active" },
          { url: "http://node-c:4003", capacity: 1, status: "active" },
          { url: "http://node-d:4004", capacity: 1, status: "active" },
        ],
        { redundancyFactor: 2, verificationRetryRounds: 1, maxRetries: 3, requestTimeoutMs: 1000 },
      );

      assert.equal(result.partialResponses.length, 0);
      assert.equal(result.failures.length, 1);
      assert.equal(result.failures[0].fragmentIndex, 0);
      assert.equal(
        result.failures[0].attempts.some((attempt) => attempt.message.includes("Result mismatch across redundant executions")),
        true,
      );
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("distributeFragments never reuses the same node for one fragment", async () => {
    const originalFetch = global.fetch;
    const attemptedUrls = [];

    global.fetch = async (url, options) => {
      attemptedUrls.push(String(url));
      const payload = JSON.parse(options.body);

      if (String(url).includes("node-a")) {
        throw new Error("timeout");
      }

      return {
        ok: true,
        async json() {
          return {
            nodeId: String(url),
            taskId: payload.taskId,
            fragmentIndex: payload.fragmentIndex,
            matches: [
              {
                documentId: "doc-1",
                title: "Same",
                snippet: "Same result",
                score: 1,
                matchedFragments: [payload.fragment],
              },
            ],
          };
        },
      };
    };

    try {
      await distributeFragments(
        ["alpha"],
        [
          { url: "http://node-a:4001", capacity: 1, status: "active" },
          { url: "http://node-b:4002", capacity: 1, status: "active" },
          { url: "http://node-c:4003", capacity: 1, status: "active" },
        ],
        { redundancyFactor: 2, verificationRetryRounds: 1, maxRetries: 3, requestTimeoutMs: 1000 },
      );

      assert.equal(new Set(attemptedUrls).size, attemptedUrls.length);
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("dispatchTasks delegates to the verified distributor", async () => {
    const originalFetch = global.fetch;

    global.fetch = async (_url, options) => {
      const payload = JSON.parse(options.body);

      return {
        ok: true,
        async json() {
          return {
            nodeId: payload.taskId,
            taskId: payload.taskId,
            fragmentIndex: payload.fragmentIndex,
            matches: [],
          };
        },
      };
    };

    try {
      const result = await dispatchTasks(
        ["private"],
        [
          { url: "http://node-a:4001", capacity: 1, status: "active" },
          { url: "http://node-b:4002", capacity: 1, status: "active" },
        ],
        { limit: 5, redundancyFactor: 2 },
      );

      assert.equal(result.failures.length, 0);
      assert.equal(result.partialResponses.length, 1);
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("registerWithCoordinator sends node registration payload", async () => {
    const originalFetch = global.fetch;
    let requestBody;
    let requestHeaders;

    global.fetch = async (_url, options) => {
      requestBody = JSON.parse(options.body);
      requestHeaders = options.headers;

      return {
        ok: true,
        async json() {
          return {
            success: true,
            duplicate: false,
            node: requestBody,
          };
        },
      };
    };

    try {
      const result = await registerWithCoordinator({
        coordinatorUrl: "http://localhost:4000",
        nodeUrl: "http://localhost:4001",
        capacity: 10,
        apiKey: "secret-key",
      });

      assert.equal(result.success, true);
      assert.deepEqual(requestBody, {
        url: "http://localhost:4001",
        capacity: 10,
        shardId: "default-shard",
        replicaGroup: "default-replica",
      });
      assert.equal(requestHeaders.authorization, "Bearer secret-key");
    } finally {
      global.fetch = originalFetch;
    }
  });

  await runTest("sendHeartbeat sends heartbeat payload", async () => {
    const originalFetch = global.fetch;
    let requestBody;
    let requestHeaders;

    global.fetch = async (_url, options) => {
      requestBody = JSON.parse(options.body);
      requestHeaders = options.headers;

      return {
        ok: true,
        async json() {
          return {
            success: true,
            ignored: false,
          };
        },
      };
    };

    try {
      const result = await sendHeartbeat({
        coordinatorUrl: "http://localhost:4000",
        nodeUrl: "http://localhost:4001",
        apiKey: "secret-key",
      });

      assert.equal(result.success, true);
      assert.deepEqual(requestBody, {
        url: "http://localhost:4001",
      });
      assert.equal(requestHeaders.authorization, "Bearer secret-key");
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







