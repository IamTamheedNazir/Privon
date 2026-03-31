const { distributeFragments } = require("../core/distributor");

async function dispatchTasks(fragments, nodes, options = {}) {
  return distributeFragments(fragments, nodes, options);
}

module.exports = {
  dispatchTasks,
};
