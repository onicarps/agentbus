/**
 * Resolve platform Go helper packages (esbuild-style optionalDependencies).
 * No runtime download — npm installs the matching optional package when available.
 */
import fs from "node:fs";
import path from "node:path";

const PLAT_MAP: Record<string, string> = {
  "linux x64": "linux-x64",
  "linux arm64": "linux-arm64",
  "darwin x64": "darwin-x64",
  "darwin arm64": "darwin-arm64",
  "win32 x64": "win32-x64",
};

export function platformKey(
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch,
): string {
  const key = `${platform} ${arch}`;
  const plat = PLAT_MAP[key];
  if (!plat) {
    throw new Error(`unsupported platform for Go worker: ${platform}/${arch}`);
  }
  return plat;
}

export function optionalPackageName(plat: string = platformKey()): string {
  return `@agentbus/go-worker-${plat}`;
}

/**
 * Path to agentbus-go-worker from optionalDependency, or null if not installed.
 */
export function resolveGoWorkerPath(): string | null {
  if (process.env.AGENTBUS_GO_WORKER && fs.existsSync(process.env.AGENTBUS_GO_WORKER)) {
    return process.env.AGENTBUS_GO_WORKER;
  }
  const plat = platformKey();
  const pkg = optionalPackageName(plat);
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const pkgJson = require.resolve(`${pkg}/package.json`);
    const root = path.dirname(pkgJson);
    const exe =
      process.platform === "win32"
        ? "agentbus-go-worker.exe"
        : "agentbus-go-worker";
    const bin = path.join(root, "bin", exe);
    if (fs.existsSync(bin)) return bin;
  } catch {
    // optional dep missing
  }
  return null;
}
