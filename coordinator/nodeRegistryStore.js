const fs = require("fs");
const path = require("path");

function createNodeRegistryStore(options = {}) {
  const filePath = options.filePath || path.join(__dirname, "..", "data", "node-registry.json");

  function ensureDirectory() {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
  }

  function loadNodes() {
    try {
      if (!fs.existsSync(filePath)) {
        return [];
      }

      const raw = fs.readFileSync(filePath, "utf8");
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed.nodes) ? parsed.nodes : [];
    } catch (error) {
      console.error(`[coordinator] failed to load node registry store: ${error.message}`);
      return [];
    }
  }

  function saveNodes(nodes) {
    ensureDirectory();
    const tempPath = `${filePath}.tmp`;
    const payload = {
      savedAt: Date.now(),
      nodes,
    };

    fs.writeFileSync(tempPath, JSON.stringify(payload, null, 2));
    fs.renameSync(tempPath, filePath);
  }

  return {
    filePath,
    loadNodes,
    saveNodes,
  };
}

module.exports = {
  createNodeRegistryStore,
};
