import { readFileSync } from "fs";
import { resolve } from "path";
import { execFileSync } from "child_process";
import { describe, expect, it } from "vitest";

import { canonicalizeAgentId, validateAgentIdValue } from "../src/identity";

const fixture = JSON.parse(
  readFileSync(
    resolve(
      __dirname,
      "../../../../tests/fixtures/agentid/cross_language_v1.json",
    ),
    "utf8",
  ),
);

describe("AgentID RFC 8785 boundary", () => {
  it("matches and verifies the Python fixture", async () => {
    // Vitest's VM omits the dynamic-import callback used by the CommonJS/ESM
    // bridge. Exercise the built CommonJS consumer in a real Node process.
    const fixturePath = resolve(
      __dirname,
      "../../../../tests/fixtures/agentid/cross_language_v1.json",
    );
    const script = `
      const fs = require("fs");
      const identity = require("./dist/identity.js");
      const fixture = JSON.parse(fs.readFileSync(process.argv[1], "utf8"));
      (async () => {
        const digest = await identity.agentIdCanonicalSha256(fixture.envelope.signed);
        const verified = await identity.verifyAgentIdSignature(
          fixture.envelope.signed, fixture.envelope.signature, fixture.public_key
        );
        const tampered = structuredClone(fixture.envelope.signed);
        tampered.payload.summary = "tampered";
        const tamperVerified = await identity.verifyAgentIdSignature(
          tampered, fixture.envelope.signature, fixture.public_key
        );
        process.stdout.write(JSON.stringify({ digest, verified, tamperVerified }));
      })().catch((error) => { console.error(error); process.exit(1); });
    `;
    const result = JSON.parse(
      execFileSync(process.execPath, ["-e", script, fixturePath], {
        cwd: resolve(__dirname, ".."),
        encoding: "utf8",
      }),
    );
    expect(result.digest).toBe(fixture.canonical_sha256);
    expect(result.verified).toBe(true);
    expect(result.tamperVerified).toBe(false);
  });

  it("rejects values Node would otherwise round or normalize ambiguously", async () => {
    expect(() => validateAgentIdValue(Number("9007199254740993"))).toThrow(
      "unsafe_integer",
    );
    expect(() => validateAgentIdValue({ value: "e\u0301" })).toThrow(
      "non_nfc_string",
    );
    await expect(canonicalizeAgentId({ value: Number.NaN })).rejects.toThrow(
      "non_finite_number",
    );
  });
});
