const fs = require("fs");
const path = require("path");

function loadDataset(datasetName = "shard-a") {
  const datasetPath = path.join(__dirname, "data", `${datasetName}.json`);
  const raw = fs.readFileSync(datasetPath, "utf8");
  const documents = JSON.parse(raw);

  if (!Array.isArray(documents)) {
    throw new Error(`Dataset "${datasetName}" must be an array of documents.`);
  }

  return documents;
}

module.exports = {
  loadDataset,
};
