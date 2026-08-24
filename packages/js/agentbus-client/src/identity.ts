import { createHash, createPublicKey, verify } from "crypto";

type Canonicalize = (value: unknown) => string | undefined;

export type AgentIdAction = {
  type:
    | "message"
    | "runner_ack"
    | "implementation"
    | "qa_verdict"
    | "agy_go"
    | "merge"
    | "push"
    | "release"
    | "identity_admin";
  [key: string]: string;
};

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
    for (let i = 0; i < value.length; i += 1) {
      const unit = value.charCodeAt(i);
      if (unit >= 0xd800 && unit <= 0xdbff) {
        const next = value.charCodeAt(i + 1);
        if (!(next >= 0xdc00 && next <= 0xdfff)) {
          throw new Error(`invalid_unicode_scalar: ${path}`);
        }
        i += 1;
      } else if (unit >= 0xdc00 && unit <= 0xdfff) {
        throw new Error(`invalid_unicode_scalar: ${path}`);
      }
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

export function validateAgentIdAction(value: unknown): AgentIdAction {
  validateAgentIdValue(value, "$.action");
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("invalid_typed_action");
  }
  const action = value as Record<string, unknown>;
  const types = new Set([
    "message",
    "runner_ack",
    "implementation",
    "qa_verdict",
    "agy_go",
    "merge",
    "push",
    "release",
    "identity_admin",
  ]);
  if (typeof action.type !== "string" || !types.has(action.type)) {
    throw new Error(`unknown_action_type: ${String(action.type)}`);
  }
  const allowedFields: Record<string, Set<string>> = {
    message: new Set(["type"]),
    runner_ack: new Set(["type", "source_event_id", "status"]),
    implementation: new Set(["type", "phase", "task"]),
    qa_verdict: new Set(["type", "result", "mission_id", "candidate"]),
    agy_go: new Set(["type", "phase", "scope"]),
    merge: new Set(["type", "target", "candidate"]),
    push: new Set(["type", "target", "candidate"]),
    release: new Set(["type", "version", "candidate"]),
    identity_admin: new Set(["type", "operation", "subject"]),
  };
  const extras = Object.keys(action).filter(
    (field) => !allowedFields[action.type as string].has(field),
  );
  if (extras.length > 0) {
    throw new Error(`unexpected_action_fields: ${extras.sort().join(",")}`);
  }
  if (
    action.type === "qa_verdict" &&
    action.result !== "green" &&
    action.result !== "red"
  ) {
    throw new Error("invalid_qa_verdict_result");
  }
  if (action.type === "agy_go" && typeof action.phase !== "string") {
    throw new Error("agy_go_phase_required");
  }
  if (
    action.type === "identity_admin" &&
    !new Set(["delegate", "enroll", "mode", "revoke", "rotate"]).has(
      action.operation as string,
    )
  ) {
    throw new Error("invalid_identity_admin_operation");
  }
  for (const [field, fieldValue] of Object.entries(action)) {
    if (
      field === "source_event_id" &&
      (typeof fieldValue !== "string" || !/^\d+$/.test(fieldValue))
    ) {
      throw new Error("invalid_runner_ack_source_event_id");
    }
    if (field !== "source_event_id" && typeof fieldValue !== "string") {
      throw new Error(`invalid_action_field: ${field}`);
    }
  }
  return action as AgentIdAction;
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
