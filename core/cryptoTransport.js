const crypto = require("node:crypto");

const ALGORITHM = "aes-256-gcm";
const IV_LENGTH = 12;

function isEncryptionEnabled(options = {}) {
  const rawValue = options.enabled ?? process.env.ENABLE_ENCRYPTION ?? "false";
  return String(rawValue).toLowerCase() === "true";
}

function getEncryptionKey(options = {}) {
  const secret = options.secret || process.env.ENCRYPTION_KEY;

  if (!secret) {
    throw new Error("ENCRYPTION_KEY is required when encryption is enabled.");
  }

  return crypto.createHash("sha256").update(String(secret)).digest();
}

function isEncryptedPayload(payload) {
  return Boolean(
    payload
      && typeof payload === "object"
      && typeof payload.iv === "string"
      && typeof payload.content === "string"
      && typeof payload.tag === "string",
  );
}

function encryptPayload(payload, options = {}) {
  if (!isEncryptionEnabled(options)) {
    return payload;
  }

  const key = getEncryptionKey(options);
  const iv = crypto.randomBytes(IV_LENGTH);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);
  const plaintext = JSON.stringify(payload);
  const encrypted = Buffer.concat([
    cipher.update(plaintext, "utf8"),
    cipher.final(),
  ]);

  return {
    iv: iv.toString("base64"),
    content: encrypted.toString("base64"),
    tag: cipher.getAuthTag().toString("base64"),
  };
}

function decryptPayload(payload, options = {}) {
  if (!isEncryptionEnabled(options)) {
    return payload;
  }

  if (!isEncryptedPayload(payload)) {
    throw new Error("Encrypted payload must include iv, content, and tag.");
  }

  const key = getEncryptionKey(options);
  const decipher = crypto.createDecipheriv(
    ALGORITHM,
    key,
    Buffer.from(payload.iv, "base64"),
  );

  decipher.setAuthTag(Buffer.from(payload.tag, "base64"));

  const decrypted = Buffer.concat([
    decipher.update(Buffer.from(payload.content, "base64")),
    decipher.final(),
  ]);

  return JSON.parse(decrypted.toString("utf8"));
}

module.exports = {
  decryptPayload,
  encryptPayload,
  isEncryptedPayload,
  isEncryptionEnabled,
};
