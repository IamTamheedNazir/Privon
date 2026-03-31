const fs = require("fs");
const path = require("path");
const { DatabaseSync } = require("node:sqlite");

const { VALID_API_KEY_ROLES, normalizeRole } = require("./roles");

const VALID_API_KEY_STATUSES = ["active", "revoked", "expired"];

function normalizeApiKeyStatus(status, expiresAt, currentTime = Date.now()) {
  if (status === "revoked") {
    return "revoked";
  }

  if (Number(expiresAt || 0) <= currentTime) {
    return "expired";
  }

  return "active";
}

function mapNodeRow(row) {
  return {
    url: row.url,
    shardId: row.shardId,
    replicaGroup: row.replicaGroup,
    status: row.status,
    score: Number(row.score),
    totalTasks: Number(row.totalTasks),
    successfulTasks: Number(row.successfulTasks),
    failedTasks: Number(row.failedTasks),
    lastSeen: Number(row.lastSeen),
    capacity: Number(row.capacity || 1),
  };
}

function createCoordinatorStore(options = {}) {
  const filePath = options.filePath || path.join(__dirname, "..", "data", "privon.sqlite");
  const now = options.now || Date.now;

  fs.mkdirSync(path.dirname(filePath), { recursive: true });

  const database = new DatabaseSync(filePath);
  database.exec("PRAGMA journal_mode = WAL;");
  database.exec("PRAGMA synchronous = NORMAL;");
  database.exec(`
    CREATE TABLE IF NOT EXISTS nodes (
      url TEXT PRIMARY KEY,
      shardId TEXT NOT NULL,
      replicaGroup TEXT NOT NULL,
      status TEXT NOT NULL,
      score INTEGER NOT NULL,
      totalTasks INTEGER NOT NULL,
      successfulTasks INTEGER NOT NULL,
      failedTasks INTEGER NOT NULL,
      lastSeen INTEGER NOT NULL,
      capacity INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS api_keys (
      key TEXT PRIMARY KEY,
      role TEXT NOT NULL,
      createdAt INTEGER NOT NULL,
      expiresAt INTEGER NOT NULL,
      status TEXT NOT NULL
    );
  `);

  const selectNodesStatement = database.prepare(`
    SELECT url, shardId, replicaGroup, status, score, totalTasks, successfulTasks, failedTasks, lastSeen, capacity
    FROM nodes
    ORDER BY url ASC
  `);
  const selectNodeUrlsStatement = database.prepare("SELECT url FROM nodes ORDER BY url ASC");
  const upsertNodeStatement = database.prepare(`
    INSERT INTO nodes (url, shardId, replicaGroup, status, score, totalTasks, successfulTasks, failedTasks, lastSeen, capacity)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(url) DO UPDATE SET
      shardId = excluded.shardId,
      replicaGroup = excluded.replicaGroup,
      status = excluded.status,
      score = excluded.score,
      totalTasks = excluded.totalTasks,
      successfulTasks = excluded.successfulTasks,
      failedTasks = excluded.failedTasks,
      lastSeen = excluded.lastSeen,
      capacity = excluded.capacity
  `);
  const deleteNodeStatement = database.prepare("DELETE FROM nodes WHERE url = ?");
  const deleteAllNodesStatement = database.prepare("DELETE FROM nodes");

  const selectApiKeyStatement = database.prepare(`
    SELECT key, role, createdAt, expiresAt, status
    FROM api_keys
    WHERE key = ?
  `);
  const selectApiKeysStatement = database.prepare(`
    SELECT key, role, createdAt, expiresAt, status
    FROM api_keys
    ORDER BY createdAt DESC, key ASC
  `);
  const upsertApiKeyStatement = database.prepare(`
    INSERT INTO api_keys (key, role, createdAt, expiresAt, status)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET
      role = excluded.role,
      createdAt = excluded.createdAt,
      expiresAt = excluded.expiresAt,
      status = excluded.status
  `);
  const updateApiKeyStatusStatement = database.prepare(`
    UPDATE api_keys
    SET status = ?
    WHERE key = ?
  `);

  function materializeApiKey(row) {
    if (!row) {
      return null;
    }

    const currentTime = now();
    const normalizedRole = normalizeRole(row.role);
    const normalizedStatus = normalizeApiKeyStatus(row.status, row.expiresAt, currentTime);

    if (normalizedStatus !== row.status) {
      updateApiKeyStatusStatement.run(normalizedStatus, row.key);
    }

    return {
      key: row.key,
      role: normalizedRole,
      createdAt: Number(row.createdAt),
      expiresAt: Number(row.expiresAt),
      status: normalizedStatus,
    };
  }

  function loadNodes() {
    return selectNodesStatement.all().map(mapNodeRow);
  }

  function saveNodes(nodes) {
    const nextNodes = Array.isArray(nodes) ? nodes : [];
    const seenUrls = new Set();

    database.exec("BEGIN IMMEDIATE TRANSACTION");

    try {
      if (nextNodes.length === 0) {
        deleteAllNodesStatement.run();
      } else {
        for (const node of nextNodes) {
          seenUrls.add(node.url);
          upsertNodeStatement.run(
            node.url,
            node.shardId || "default-shard",
            node.replicaGroup || "default-replica",
            node.status || "inactive",
            Number(node.score || 0),
            Number(node.totalTasks || 0),
            Number(node.successfulTasks || 0),
            Number(node.failedTasks || 0),
            Number(node.lastSeen || now()),
            Math.max(1, Number(node.capacity || 1)),
          );
        }

        for (const existingNode of selectNodeUrlsStatement.all()) {
          if (!seenUrls.has(existingNode.url)) {
            deleteNodeStatement.run(existingNode.url);
          }
        }
      }

      database.exec("COMMIT");
    } catch (error) {
      database.exec("ROLLBACK");
      throw error;
    }
  }

  function getApiKey(key) {
    const normalizedKey = String(key || "").trim();

    if (!normalizedKey) {
      return null;
    }

    return materializeApiKey(selectApiKeyStatement.get(normalizedKey));
  }

  function createApiKey(record = {}) {
    const key = String(record.key || "").trim();
    const role = normalizeRole(record.role);
    const createdAt = Number(record.createdAt || now());
    const expiresAt = Number(record.expiresAt || 0);
    const status = String(record.status || "active").trim().toLowerCase();

    if (!key) {
      throw new Error("API key value is required.");
    }

    if (!VALID_API_KEY_ROLES.includes(role)) {
      throw new Error(`API key role must be one of: ${VALID_API_KEY_ROLES.join(", ")}.`);
    }

    if (!Number.isFinite(expiresAt) || expiresAt <= createdAt) {
      throw new Error("API key expiresAt must be greater than createdAt.");
    }

    if (!VALID_API_KEY_STATUSES.includes(status)) {
      throw new Error(`API key status must be one of: ${VALID_API_KEY_STATUSES.join(", ")}.`);
    }

    upsertApiKeyStatement.run(key, role, createdAt, expiresAt, status);
    return getApiKey(key);
  }

  function ensureApiKey(record = {}) {
    if (!record.key) {
      return null;
    }

    const existingRecord = getApiKey(record.key);
    return createApiKey({
      key: record.key,
      role: record.role,
      createdAt: existingRecord?.createdAt || record.createdAt || now(),
      expiresAt: record.expiresAt,
      status: record.status || "active",
    });
  }

  function revokeApiKey(key) {
    const existingRecord = getApiKey(key);

    if (!existingRecord) {
      return null;
    }

    updateApiKeyStatusStatement.run("revoked", existingRecord.key);
    return getApiKey(existingRecord.key);
  }

  function listApiKeys(filters = {}) {
    const statusFilter = filters.status ? String(filters.status).trim().toLowerCase() : "";
    const roleFilter = filters.role ? normalizeRole(filters.role) : "";

    return selectApiKeysStatement.all()
      .map(materializeApiKey)
      .filter(Boolean)
      .filter((record) => {
        if (statusFilter && record.status !== statusFilter) {
          return false;
        }

        if (roleFilter && record.role !== roleFilter) {
          return false;
        }

        return true;
      });
  }

  return {
    database,
    filePath,
    loadNodes,
    saveNodes,
    createApiKey,
    ensureApiKey,
    getApiKey,
    listApiKeys,
    revokeApiKey,
    close() {
      database.close();
    },
  };
}

module.exports = {
  createCoordinatorStore,
};



