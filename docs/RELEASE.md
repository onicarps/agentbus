# AgentBus release hygiene

Tag-driven releases via `.github/workflows/release.yml` (`push` tags `v*`).

## What the workflow does

| Job | Artifact | Auth |
|-----|----------|------|
| `build` | sdist + wheel + GitHub Release assets | `contents: write` |
| `publish-pypi` | `okf-agentbus` → PyPI | OIDC Trusted Publisher (`environment: pypi`) |
| `publish-npm` | `@okf/agentbus-client` → npm | OIDC Trusted Publisher (`environment: npm`) |

## One-time: PyPI Trusted Publisher

**Status (2026-07-12):** GitHub environment `pypi` exists, but v0.11.2 job failed with
`invalid-publisher` — no matching publisher on PyPI yet. Packages on PyPI were published
out-of-band (manual/token). OIDC must be registered for tag→PyPI to go green.

1. Open https://pypi.org/manage/project/okf-agentbus/settings/publishing/
2. Add **GitHub** publisher with **exactly**:

| Field | Value |
|-------|-------|
| Owner | `onicarps` |
| Repository | `agentbus` |
| Workflow | `release.yml` |
| Environment | `pypi` |

Claims that must match (from failed run `v0.11.2`):

```
sub: repo:onicarps/agentbus:environment:pypi
repository: onicarps/agentbus
workflow_ref: .../.github/workflows/release.yml@refs/tags/v*
environment: pypi
```

3. Confirm GitHub repo environment **Settings → Environments → `pypi`** (already present).
4. Re-run a tag workflow or cut the next patch tag to verify.

## One-time: npm Trusted Publisher + scope

1. Ensure npm org/user can own scope `@okf` (create org `okf` if needed).
2. Create empty package **or** register trusted publisher for pending name
   `@okf/agentbus-client` (npm Trusted Publishers UI).
3. GitHub Actions publisher fields:

| Field | Value |
|-------|-------|
| Organization or user | `onicarps` |
| Repository | `agentbus` |
| Workflow filename | `release.yml` |
| Environment | `npm` |

4. Confirm GitHub environment **`npm`** exists (workflow references it). Create if missing:
   Settings → Environments → New → `npm`.
5. First publish requires npm CLI ≥ 11.5.1 in CI (workflow installs `npm@latest`) and
   `id-token: write` + `npm publish --provenance --access public`.

## Version alignment

- Python: `pyproject.toml` → `okf-agentbus==X.Y.Z`
- TypeScript: `packages/js/agentbus-client/package.json` → `@okf/agentbus-client@X.Y.Z`
- CI sets npm version from the git tag (`vX.Y.Z` → `X.Y.Z`) so tag is source of truth.

Keep both package versions equal when cutting a release.

## Manual fallback (emergency only)

```bash
# PyPI (API token in TWINE_PASSWORD)
python -m build && twine upload dist/*

# npm (logged-in human or granular token)
cd packages/js/agentbus-client && npm ci && npm test && npm publish --access public
```

Prefer fixing Trusted Publishers over long-lived tokens.
