import { createHash, createPublicKey, verify } from "crypto";

type Canonicalize = (value: unknown) => string | undefined;

const importEsm = new Function(
  "specifier",
  "return import(specifier)",
) as (specifier: string) => Promise<{ default?: Canonicalize }>;

export function validateAgentIdValue(value: unknown, path = "$"): void {
  if (value === null || typeof value === "boolean") return;
  if (typeof value === "string") {
    if (value.normalize("NFC") !== value) {
      throw new Error(`non_nfc_string: ${path}`);
    }
    return;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error(`non_finite_number: ${path}`);
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
      throw new Error(`unsafe_integer: ${path}`);
    }
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((child, index) =>
      validateAgentIdValue(child, `${path}[${index}]`),
    );
    return;
  }
  if (typeof value === "object") {
    for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
      validateAgentIdValue(key, `${path}.<key>`);
      validateAgentIdValue(child, `${path}.${key}`);
    }
    return;
  }
  throw new Error(`unsupported_json_type: ${path}`);
}

/** RFC 8785 bytes loaded through a preserved ESM dynamic-import boundary. */
export async function canonicalizeAgentId(value: unknown): Promise<Buffer> {
  validateAgentIdValue(value);
  const module = await importEsm("canonicalize");
  const canonicalize = module.default;
  if (typeof canonicalize !== "function") {
    throw new Error("canonicalize_module_invalid");
  }
  const result = canonicalize(value);
  if (typeof result !== "string") throw new Error("canonicalize_failed");
  return Buffer.from(result, "utf8");
}

export async function agentIdCanonicalSha256(value: unknown): Promise<string> {
  return createHash("sha256")
    .update(await canonicalizeAgentId(value))
    .digest("hex");
}

export async function verifyAgentIdSignature(
  unsigned: unknown,
  signatureBase64Url: string,
  publicKeyBase64Url: string,
): Promise<boolean> {
  const rawPublic = Buffer.from(publicKeyBase64Url, "base64url");
  if (rawPublic.length !== 32) throw new Error("invalid_ed25519_public_key");
  // RFC 8410 SubjectPublicKeyInfo prefix for a raw 32-byte Ed25519 key.
  const spki = Buffer.concat([
    Buffer.from("302a300506032b6570032100", "hex"),
    rawPublic,
  ]);
  const key = createPublicKey({ key: spki, format: "der", type: "spki" });
  return verify(
    null,
    await canonicalizeAgentId(unsigned),
    key,
    Buffer.from(signatureBase64Url, "base64url"),
  );
}
