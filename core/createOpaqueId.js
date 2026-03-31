const { randomUUID } = require("node:crypto");

function createOpaqueId(prefix = "id") {
  return `${prefix}-${randomUUID()}`;
}

module.exports = {
  createOpaqueId,
};
