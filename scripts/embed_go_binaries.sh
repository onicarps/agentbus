#!/usr/bin/env bash
# Cross-compile Go helpers into src/agentbus/bin/<platform>/ for wheel packaging.
# Usage:
#   ./scripts/embed_go_binaries.sh [GOOS] [GOARCH] [platform-dir]
# Defaults: current host → inferred platform-dir (linux-x64, darwin-arm64, …)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="${HOME}/.local/go/bin:/usr/local/go/bin:${PATH}"

GOOS="${1:-$(go env GOOS)}"
GOARCH="${2:-$(go env GOARCH)}"
PLAT="${3:-}"

if [[ -z "${PLAT}" ]]; then
  case "${GOOS}-${GOARCH}" in
    linux-amd64|linux-x86_64) PLAT=linux-x64 ;;
    linux-arm64|linux-aarch64) PLAT=linux-arm64 ;;
    darwin-amd64) PLAT=darwin-x64 ;;
    darwin-arm64) PLAT=darwin-arm64 ;;
    windows-amd64) PLAT=win32-x64 ;;
    *) echo "unknown GOOS/GOARCH ${GOOS}/${GOARCH}; pass platform-dir as \$3" >&2; exit 1 ;;
  esac
fi

OUT="${ROOT}/src/agentbus/bin/${PLAT}"
mkdir -p "${OUT}"
EXT=""
if [[ "${GOOS}" == "windows" ]]; then EXT=".exe"; fi

export CGO_ENABLED=0
export GOOS GOARCH

echo "Building agentbus-go-worker for ${GOOS}/${GOARCH} → ${PLAT}"
( cd "${ROOT}/go-core" && go build -trimpath -ldflags="-s -w" \
  -o "${OUT}/agentbus-go-worker${EXT}" ./cmd/agentbus-go-worker )

echo "Building agentbus-go-serve for ${GOOS}/${GOARCH} → ${PLAT}"
( cd "${ROOT}/go-core" && go build -trimpath -ldflags="-s -w" \
  -o "${OUT}/agentbus-go-serve${EXT}" ./cmd/agentbus-go-serve )

chmod +x "${OUT}/agentbus-go-worker${EXT}" "${OUT}/agentbus-go-serve${EXT}" || true
ls -la "${OUT}"
