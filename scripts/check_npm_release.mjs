#!/usr/bin/env node
// Integrity gate for idempotent npm release reruns.

import fs from "node:fs";
import { spawnSync } from "node:child_process";

export function evaluateRelease(localIntegrity, remoteIntegrity) {
  if (!remoteIntegrity) return true;
  if (localIntegrity !== remoteIntegrity) {
    throw new Error("existing npm package integrity differs from this build");
  }
  return false;
}

function argument(name) {
  const index = process.argv.indexOf(name);
  if (index < 0 || !process.argv[index + 1]) throw new Error(`missing ${name}`);
  return process.argv[index + 1];
}

export function main() {
  const packageName = argument("--package");
  const version = argument("--version");
  const pack = JSON.parse(fs.readFileSync(argument("--pack-json"), "utf8"));
  if (!Array.isArray(pack) || pack.length !== 1 || !pack[0].integrity || !pack[0].filename) {
    throw new Error("npm pack JSON did not contain exactly one tarball with integrity");
  }
  if (pack[0].name !== packageName || pack[0].version !== version) {
    throw new Error(
      `packed identity ${pack[0].name}@${pack[0].version} does not match ${packageName}@${version}`,
    );
  }

  const query = spawnSync(
    "npm",
    ["view", `${packageName}@${version}`, "dist.integrity", "--json"],
    { encoding: "utf8" },
  );
  let remoteIntegrity = null;
  if (query.status === 0) {
    remoteIntegrity = JSON.parse(query.stdout || "null");
  } else if (!`${query.stderr}\n${query.stdout}`.includes("E404")) {
    throw new Error(`npm registry query failed: ${(query.stderr || query.stdout).trim()}`);
  }

  const publishNeeded = evaluateRelease(pack[0].integrity, remoteIntegrity);
  process.stdout.write(`publish-needed=${publishNeeded ? "true" : "false"}\n`);
  process.stdout.write(`tarball=${pack[0].filename}\n`);
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  try {
    main();
  } catch (error) {
    process.stderr.write(`release preflight failed: ${error.message}\n`);
    process.exitCode = 1;
  }
}
