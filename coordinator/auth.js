const crypto = require("crypto");

function secureEqual(left, right) {
  const leftBuffer = Buffer.from(String(left || ""));
  const rightBuffer = Buffer.from(String(right || ""));

  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }

  return crypto.timingSafeEqual(leftBuffer, rightBuffer);
}

function parseCookies(cookieHeader) {
  if (!cookieHeader || typeof cookieHeader !== "string") {
    return {};
  }

  return cookieHeader.split(";").reduce((cookies, segment) => {
    const [name, ...rest] = segment.trim().split("=");

    if (!name) {
      return cookies;
    }

    cookies[name] = decodeURIComponent(rest.join("="));
    return cookies;
  }, {});
}

function extractBearerToken(request) {
  const authorization = request.get("authorization") || "";
  const [scheme, token] = authorization.split(" ");

  if (!scheme || scheme.toLowerCase() !== "bearer" || !token) {
    return "";
  }

  return token.trim();
}

function createApiKeyAuth(options = {}) {
  const apiKey = String(options.apiKey || "").trim();
  const sessionCookieName = options.sessionCookieName || "privon_dashboard_session";
  const sessionTtlMs = options.sessionTtlMs ?? 1000 * 60 * 60 * 12;
  const sessions = new Map();

  function isConfigured() {
    return apiKey.length > 0;
  }

  function isValidApiKey(candidate) {
    if (!isConfigured()) {
      return true;
    }

    return secureEqual(candidate, apiKey);
  }

  function createSession() {
    const token = crypto.randomBytes(24).toString("hex");
    const expiresAt = Date.now() + sessionTtlMs;
    sessions.set(token, expiresAt);
    return { token, expiresAt };
  }

  function cleanupExpiredSessions() {
    const currentTime = Date.now();

    for (const [token, expiresAt] of sessions.entries()) {
      if (expiresAt <= currentTime) {
        sessions.delete(token);
      }
    }
  }

  function hasValidSession(request) {
    cleanupExpiredSessions();
    const cookies = parseCookies(request.headers.cookie);
    const token = cookies[sessionCookieName];

    if (!token) {
      return false;
    }

    const expiresAt = sessions.get(token);

    if (!expiresAt || expiresAt <= Date.now()) {
      sessions.delete(token);
      return false;
    }

    return true;
  }

  function setSessionCookie(response) {
    const { token } = createSession();
    response.setHeader(
      "Set-Cookie",
      `${sessionCookieName}=${encodeURIComponent(token)}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${Math.floor(sessionTtlMs / 1000)}`,
    );
  }

  function clearSessionCookie(response) {
    response.setHeader(
      "Set-Cookie",
      `${sessionCookieName}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0`,
    );
  }

  function ensureAuthorized(request, response, options = {}) {
    const allowSession = options.allowSession ?? false;
    const token = extractBearerToken(request);

    if (isValidApiKey(token)) {
      return true;
    }

    if (allowSession && hasValidSession(request)) {
      return true;
    }

    response.status(401).json({
      error: "Unauthorized",
    });
    return false;
  }

  return {
    isConfigured,
    isValidApiKey,
    extractBearerToken,
    ensureAuthorized,
    setSessionCookie,
    clearSessionCookie,
    hasValidSession,
  };
}

module.exports = {
  createApiKeyAuth,
};
