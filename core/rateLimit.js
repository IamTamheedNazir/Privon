function normalizeWindowMs(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeMaxRequests(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

function getClientIp(request) {
  const forwardedFor = request.headers?.["x-forwarded-for"] || request.get?.("x-forwarded-for");

  if (typeof forwardedFor === "string" && forwardedFor.trim()) {
    return forwardedFor.split(",")[0].trim();
  }

  const forwarded = request.headers?.["x-real-ip"] || request.get?.("x-real-ip");

  if (typeof forwarded === "string" && forwarded.trim()) {
    return forwarded.trim();
  }

  if (typeof request.ip === "string" && request.ip.trim()) {
    return request.ip.trim();
  }

  if (typeof request.socket?.remoteAddress === "string" && request.socket.remoteAddress.trim()) {
    return request.socket.remoteAddress.trim();
  }

  return "unknown";
}

function createRateLimitMiddleware(options = {}) {
  const keyPrefix = String(options.keyPrefix || "default");
  const windowMs = normalizeWindowMs(options.windowMs, 60_000);
  const maxRequests = normalizeMaxRequests(options.maxRequests, 100);
  const store = options.store instanceof Map ? options.store : new Map();
  const logger = typeof options.log === "function" ? options.log : () => {};
  const message = options.message || "Too many requests. Please try again later.";

  return function rateLimitMiddleware(request, response, next) {
    if (maxRequests === 0) {
      return next();
    }

    const now = typeof options.now === "function" ? options.now() : Date.now();
    const ip = getClientIp(request);
    const key = `${keyPrefix}:${ip}`;
    const record = store.get(key) || [];
    const activeWindow = record.filter((timestamp) => now - timestamp < windowMs);

    if (activeWindow.length >= maxRequests) {
      const oldestTimestamp = activeWindow[0] || now;
      const retryAfterMs = Math.max(0, windowMs - (now - oldestTimestamp));
      const retryAfterSeconds = Math.max(1, Math.ceil(retryAfterMs / 1000));

      logger({
        ip,
        route: request.originalUrl || request.url || keyPrefix,
        keyPrefix,
        maxRequests,
        windowMs,
      });

      response.setHeader("Retry-After", String(retryAfterSeconds));
      return response.status(429).json({
        error: message,
      });
    }

    activeWindow.push(now);
    store.set(key, activeWindow);
    return next();
  };
}

module.exports = {
  createRateLimitMiddleware,
  getClientIp,
};
