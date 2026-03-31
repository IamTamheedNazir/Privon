async function getHealthyNodeUrls(nodeUrls, options = {}) {
  const timeoutMs = options.timeoutMs ?? 800;
  const checks = nodeUrls.map(async (nodeUrl) => {
    try {
      const response = await fetch(`${nodeUrl}/health`, {
        signal: AbortSignal.timeout(timeoutMs),
      });

      if (!response.ok) {
        return null;
      }

      return nodeUrl;
    } catch {
      return null;
    }
  });

  const results = await Promise.all(checks);
  return results.filter(Boolean);
}

module.exports = {
  getHealthyNodeUrls,
};
