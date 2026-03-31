const crypto = require("crypto");

const { isRoleAllowed } = require("./roles");

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
  const keyStore = options.keyStore;
  const now = options.now || Date.now;
  const sessionCookieName = options.sessionCookieName || "privon_dashboard_session";
  const sessionTtlMs = Number(options.sessionTtlMs || 1000 * 60 * 30);
  const sessions = new Map();

  if (!keyStore) {
    throw new Error("createApiKeyAuth requires a keyStore instance.");
  }

  function cleanupExpiredSessions() {
    const currentTime = now();

    for (const [token, session] of sessions.entries()) {
      if (session.expiresAt <= currentTime) {
        sessions.delete(token);
      }
    }
  }

  function formatCookie(token, expiresAt) {
    const maxAgeSeconds = Math.max(0, Math.floor((expiresAt - now()) / 1000));
    return `${sessionCookieName}=${encodeURIComponent(token)}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${maxAgeSeconds}`;
  }

  function buildErrorRecord(message, status, code) {
    return {
      ok: false,
      message,
      status,
      code,
    };
  }

  function validatePrincipal(principal, allowedRoles = []) {
    if (!principal) {
      return buildErrorRecord("Unauthorized", 401, "invalid_api_key");
    }

    if (principal.status === "expired") {
      return buildErrorRecord("API key expired.", 401, "expired_api_key");
    }

    if (principal.status !== "active") {
      return buildErrorRecord("API key inactive.", 401, "inactive_api_key");
    }

    if (allowedRoles.length > 0 && !isRoleAllowed(principal.role, allowedRoles)) {
      return buildErrorRecord("Forbidden", 403, "insufficient_role");
    }

    return {
      ok: true,
      principal,
    };
  }

  function authenticateApiKey(candidate, allowedRoles = []) {
    const token = String(candidate || "").trim();

    if (!token) {
      return buildErrorRecord("Unauthorized", 401, "missing_api_key");
    }

    return validatePrincipal(keyStore.getApiKey(token), allowedRoles);
  }

  function getSessionToken(request) {
    const cookies = parseCookies(request.headers.cookie);
    return cookies[sessionCookieName] || "";
  }

  function getSessionPrincipal(request, allowedRoles = []) {
    cleanupExpiredSessions();

    const sessionToken = getSessionToken(request);

    if (!sessionToken) {
      return buildErrorRecord("Unauthorized", 401, "missing_session");
    }

    const session = sessions.get(sessionToken);

    if (!session || session.expiresAt <= now()) {
      sessions.delete(sessionToken);
      return buildErrorRecord("Dashboard session expired.", 401, "expired_session");
    }

    const validatedPrincipal = validatePrincipal(keyStore.getApiKey(session.key), allowedRoles);

    if (!validatedPrincipal.ok) {
      sessions.delete(sessionToken);
      return validatedPrincipal;
    }

    return {
      ok: true,
      sessionToken,
      principal: {
        ...validatedPrincipal.principal,
        sessionExpiresAt: session.expiresAt,
      },
    };
  }

  function createSession(principal, previousToken = "") {
    if (previousToken) {
      sessions.delete(previousToken);
    }

    const expiresAt = Math.min(now() + sessionTtlMs, Number(principal.expiresAt));
    const token = crypto.randomBytes(24).toString("hex");
    sessions.set(token, {
      key: principal.key,
      role: principal.role,
      expiresAt,
    });

    return {
      token,
      expiresAt,
      role: principal.role,
    };
  }

  function setSessionCookie(response, principal, previousToken = "") {
    const session = createSession(principal, previousToken);
    response.setHeader("Set-Cookie", formatCookie(session.token, session.expiresAt));

    return {
      role: session.role,
      expiresAt: session.expiresAt,
      keyExpiresAt: principal.expiresAt,
    };
  }

  function clearSessionCookie(response, request) {
    const sessionToken = request ? getSessionToken(request) : "";

    if (sessionToken) {
      sessions.delete(sessionToken);
    }

    response.setHeader(
      "Set-Cookie",
      `${sessionCookieName}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0`,
    );
  }

  function ensureAuthorized(request, response, options = {}) {
    const allowSession = options.allowSession ?? false;
    const allowedRoles = options.allowedRoles || [];
    const bearerToken = extractBearerToken(request);
    let failedResult = null;

    if (bearerToken) {
      const bearerResult = authenticateApiKey(bearerToken, allowedRoles);

      if (bearerResult.ok) {
        request.auth = bearerResult.principal;
        request.authSource = "api_key";
        return true;
      }

      failedResult = bearerResult;
    }

    if (allowSession) {
      const sessionResult = getSessionPrincipal(request, allowedRoles);

      if (sessionResult.ok) {
        request.auth = sessionResult.principal;
        request.authSource = "session";
        request.session = {
          token: sessionResult.sessionToken,
          expiresAt: sessionResult.principal.sessionExpiresAt,
        };
        return true;
      }

      if (!failedResult) {
        failedResult = sessionResult;
      }
    }

    response.status(failedResult?.status || 401).json({
      error: failedResult?.message || "Unauthorized",
      code: failedResult?.code || "unauthorized",
    });
    return false;
  }

  function createSessionFromApiKey(request, response, allowedRoles = []) {
    const apiKeyResult = authenticateApiKey(extractBearerToken(request), allowedRoles);

    if (!apiKeyResult.ok) {
      response.status(apiKeyResult.status).json({
        error: apiKeyResult.message,
        code: apiKeyResult.code,
      });
      return null;
    }

    return setSessionCookie(response, apiKeyResult.principal);
  }

  function renewSession(request, response, allowedRoles = []) {
    let principal = null;
    let previousToken = "";

    const bearerToken = extractBearerToken(request);

    if (bearerToken) {
      const bearerResult = authenticateApiKey(bearerToken, allowedRoles);

      if (!bearerResult.ok) {
        response.status(bearerResult.status).json({
          error: bearerResult.message,
          code: bearerResult.code,
        });
        return null;
      }

      principal = bearerResult.principal;
    } else {
      const sessionResult = getSessionPrincipal(request, allowedRoles);

      if (!sessionResult.ok) {
        response.status(sessionResult.status).json({
          error: sessionResult.message,
          code: sessionResult.code,
        });
        return null;
      }

      principal = sessionResult.principal;
      previousToken = sessionResult.sessionToken;
    }

    return setSessionCookie(response, principal, previousToken);
  }

  return {
    clearSessionCookie,
    createSessionFromApiKey,
    ensureAuthorized,
    extractBearerToken,
    getSessionPrincipal,
    renewSession,
    setSessionCookie,
  };
}

module.exports = {
  createApiKeyAuth,
};
