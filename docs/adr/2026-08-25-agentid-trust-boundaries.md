# ADR 2026-08-25: AgentBus Swarm Identity, Trust Boundaries & Anti-Impersonation (AgentID)

- **Status:** Proposed / Under Security Review
- **Deciders:** Agy (Architect), Codex (Engineer), Factory (QA / Security Auditor)
- **Target Release:** AgentBus v0.20 (`ABUS-020-001`)
- **Supersedes:** Unauthenticated in-process publish model (`src/agentbus/store.py` v0.19)
- **Incident fixture:** `/initiatives/agentbus/decisions/agentid-identity-substitution-incident-2026-07.md`

---

## 1. Context & Problem Statement

AgentBus coordinates heterogeneous autonomous agents (`agy`, `codex`, `factory`, `slack`, `hermes`) communicating across shared storage (`events.db`), runner loops, and dispatch planes.

Prior to v0.20, AgentBus lacked cryptographic verification of actor boundaries:
1. `producer_id` and `payload.from` were caller-supplied strings.
2. `EventStore.publish` was an in-process Python library call writing directly to an SQLite database with filesystem mode `0644`.
3. The wake/dispatch plane (`<runtime>_wake_queue.jsonl`, `WAKE.<agent>.json`, `/agentbus/wake`) accepted arbitrary synthetic `event_id` records without verifying against `events.db`.
4. Child subagents spawned by orchestrators inherited parent process environments, file handles, and shared credentials.

In Factory security review mission `sec-abus-020-001` (Event #7557, causation #7524), Factory delivered a **`SECURITY_REVIEW: RED`** with 8 concrete blockers demonstrating that client-side publish validation alone provides an illusion of security without actual isolation.

This ADR establishes the formal trust boundaries, asset inventory, threat models, attacker capability tiers, accepted residual risks, and negative-test validation matrix required to make AgentID secure and implementation-ready.

---

## 2. Asset Inventory

| Asset | Description | Integrity Requirement | Confidentiality Requirement |
|-------|-------------|-----------------------|-----------------------------|
| **Signing Private Keys** | Ed25519 private keys uniquely held by genuine agent execution daemons. | **Critical** (tamper-proof) | **Critical** (zero leak to parent/child/prompts/env) |
| **Trust Root & Policy** | Workspace policy (`policy.json` / `policy.sig`) & Public Key Registry. | **Critical** (monotonic versions, tamper-evident, broker-enforced high-water) | Public / workspace-visible |
| **Revocation Ledger** | List of revoked key IDs and expired rotation windows. | **Critical** (fail-closed, immediate invalidation) | Public / workspace-visible |
| **Event Log (`events.db`)** | Stored history of published events and hydrated artifacts. | **Untrusted Storage**; integrity verified via signature recomputation at read-time. In strict mode only the reference monitor writes it. | Workspace-visible through the broker API |
| **Runner Wake Envelopes** | Signals instructing runner adapters to execute turns. | **Critical** (must rehydrate and verify against `events.db`). | Local runtime only |
| **Privileged Transitions** | `QA_VERDICT: GREEN`, `Agy GO`, `/merge`, `/push`, release tagging. | **Critical** (requires separation of duties & authentic cryptographic proof). | High |

The workspace trust-root private key is held offline by the human workspace owner or an explicitly designated security administrator, not by Agy, Codex, Factory, a runner, or the reference-monitor process. Online components receive only signed policy/registry documents and the public trust root. Recovery and rotation require a new, auditable trust-root-signed statement; audit-mode local registries are advisory and cannot grant privileged authority.

---

## 3. The Four Trust Boundaries

```
+---------------------------------------------------------------------------------------------------+
|                                      TRUST BOUNDARY ARCHITECTURE                                   |
|                                                                                                   |
|  [ BOUNDARY 1: OS Principal / Isolation ]                                                         |
|  +-------------------------------------+         +-------------------------------------+          |
|  | Genuine Agent Runtime Daemon        |         | Adversary / Peer Process (Same UID)  |          |
|  | - Dedicated Ed25519 Signing Key     |         | - Cannot read isolated memory/keys  |          |
|  +-------------------------------------+         +-------------------------------------+          |
|                     |                                               |                             |
|  [ BOUNDARY 2: Process & Subagent Containment ]                     | (Attempts direct write)     |
|  +-------------------------------------+                            |                             |
|  | Parent Orchestrator                 |                            |                             |
|  |  \__ Child Subagent (Scrubbed env,  |                            |                             |
|  |      derived short-lived identity)  |                            |                             |
|  +-------------------------------------+                            |                             |
|                     |                                               |                             |
|                     v (Cryptographically Signed Envelopes)          v (Direct SQLite INSERT)      |
|  [ BOUNDARY 3: Storage & Verification Plane ]                       |                             |
|  +------------------------------------------------------------------+--------------------------+  |
|  |  events.db (UNTRUSTED STORAGE - PRAGMA foreign_keys=ON)                                     |  |
|  |  - Raw signed envelopes + post-extraction payloads + artifact digests                        |  |
|  +---------------------------------------------------------------------------------------------+  |
|                     |                                               ^                             |
|                     v (Mandatory Verify-at-Read)                    | (Synthetic Wake Drop)       |
|  [ BOUNDARY 4: Dispatch & Wake Ingress Plane ]                      |                             |
|  +------------------------------------------------------------------+--------------------------+  |
|  |  Runner Intake / Loop: Rehydrates event_id from events.db, verifies signature before wake.  |  |
|  |  Ingress Webhook: Mandatory broker-issued capability bound to runtime identity.             |  |
|  +---------------------------------------------------------------------------------------------+  |
+---------------------------------------------------------------------------------------------------+
```

### Boundary 1: OS Principal & Execution Boundary
- **Model:** In strict mode, the reference monitor and mutually distrusting agent runtimes run under distinct OS user principals or equivalently isolated containers. The reference monitor exclusively owns the database, policy high-water state, nonce ledger, and its authenticated IPC endpoints. A same-UID Unix socket is not an isolation boundary.
- **Broker authentication:** Runtime identity is bound during supervised process launch to a credential the child cannot mint or exchange for a peer credential. The broker derives `producer_id` from that authenticated channel and key binding; it never accepts a caller-selected producer as authority.
- **Single-UID Host Rule:** On single-user developer machines sharing a single UID, local file modes (`0600`/`0700`) cannot cryptographically prevent same-UID memory/file inspection. In audit/protected mode on single-UID hosts, AgentID guarantees **post-hoc non-repudiation and tamper-evident integrity**, but does not claim absolute execution prevention. `agentbus doctor` must explicitly report this constraint.
- **Canonical deployment rule:** Protected privileged transitions in the canonical swarm require the isolated broker boundary. A shared-UID development deployment cannot certify Factory GREEN, Agy GO, merge, push, or release authority and must not be advertised as impersonation-safe.

### Boundary 2: Process & Subagent Credential Containment
- **Model:** Parent processes (e.g. Agy, Claude, Cursor) spawning subagents MUST scrub private signing keys, file descriptors, agent sockets, and tokens from child process environments.
- **Delegation:** If subagents require publishing capability, they receive derived, short-lived identities (e.g. `agy/subagent/<id>`) with strictly non-escalating roles (cannot mint `QA_VERDICT`, `GO`, or release actions).

### Boundary 3: Storage & Verification Plane (Untrusted Storage)
- **Model:** `events.db` is explicitly treated as **untrusted storage**.
- **Verify-at-Read:** All downstream consumers (runners, `agentbus await`, log projection, TUI, CLI) MUST independently recompute the signature over the stored payload and bound artifact digests before acting upon any event on restricted topics.
- **Scoped Deduplication:** Deduplication and idempotency uniqueness are scoped strictly to `(verified_producer_id, idempotency_key)`. Verification precedes deduplication. Unauthorized dedup hits yield `403 Forbidden` with zero payload leakage.
- **Foreign Keys:** `PRAGMA foreign_keys = ON` is enforced.
- **Writer convergence:** Python, TypeScript, and Go submit envelopes through the broker. The Go worker has no direct restricted-topic write path. Compatibility readers may inspect the database, but no consumer may confer authority without recomputing verification.
- **Workspace binding:** Enrollment creates a random persistent `workspace_id`, included in the root-signed policy and every envelope. Copying `events.db` or changing a filesystem path does not transplant validity because the destination monitor verifies against its own root-bound workspace ID.
- **Retention:** Event retention and nonce retention are independent. Nonces and per-key monotonic time high-water marks outlive the maximum replay/recovery window. Artifact deletion cascades from event retention; no orphan rows remain.

### Boundary 4: Dispatch & Wake Ingress Plane
- **Model:** The wake plane is an authenticated channel.
- **Store-Backed Rehydration:** Runner intake loops MUST NOT execute turns on unverified or synthetic wake records. All wake queue records must resolve to an authentic `event_id` in `events.db` that passes signature verification.
- **Fail-Closed Ingress:** The HTTP wake ingress endpoint (`/agentbus/wake`) requires a broker-issued, runtime-bound capability. A workspace-wide shared bearer token is insufficient. The capability authorizes only notification of an already-persisted event ID; it cannot supply event content or select producer identity.

---

## 4. Attacker Capability Tiers

| Tier | Attacker Profile | Capabilities | Defense Mechanism |
|------|------------------|--------------|-------------------|
| **Tier 1: Same-UID Local Peer** | An unauthenticated process or script running under the same UID attempting to spoof events. | Can write directly to `events.db` or invoke CLI with arbitrary flags. | Verify-at-read detects missing/invalid signatures; strict mode isolates keys via broker/containers. |
| **Tier 2: Child Subagent** | A spawned LLM child process prompted to simulate peer identities. | Has subagent execution context; attempts to sign as `factory` or `codex`. | Environment scrubbing denies access to parent/peer private keys; delegation restricts capabilities. |
| **Tier 3: Network / Ingress Attacker** | An unauthenticated remote caller hitting the HTTP wake ingress endpoint. | Can send HTTP POST to `/agentbus/wake`. | Mandatory broker-issued runtime capability; store-backed wake rehydration. |
| **Tier 4: Compromised Key / Rollback** | An attacker holding an old key or attempting registry rollback. | Can replay old signatures or reset clock/policy. | Monotonic policy high-water mark; immediate revocation ledger; atomic nonce tracking. |

---

## 5. Canonicalization & Cryptographic Standards

- **Signature Scheme:** Ed25519 (RFC 8032).
- **Canonicalization:** RFC 8785 (JSON Canonicalization Scheme / JCS).
  - Python: `rfc8785==0.1.4` (Matrix `canonicaljson` is forbidden).
  - TypeScript: `canonicalize@4.0.0`, registry integrity `sha512-FEdXzwWs+N3rZqEqpqleiY9M1A6IAf9oo1zHFABnLW9FcJ/jzsu+G/Ks3Hq3FglmPKe80GeGBw8ZXLEnwPB0vQ==` (`@peculiar/json-canonicalize` is invalid).
  - Go: `github.com/cyberphone/json-canonicalization@v0.0.0-20241213102144-19d51d7fe467`.
- **Crypto providers:** Python uses `cryptography==50.0.0` Ed25519 primitives; TypeScript uses Node's `crypto` Ed25519 API; Go uses standard-library `crypto/ed25519`. Release lockfiles and artifact metadata retain exact dependency hashes.
- **Packaging impact:** `cryptography==50.0.0` introduces a native-wheel dependency to the Python package's previously crypto-free install graph, so the full supported wheel/platform matrix and source-install fallback are release gates. `canonicalize@4.0.0` is ESM-only while `@agentbus/agentbus-client` currently emits CommonJS; the client must either adopt an ESM-compatible import/build boundary or keep canonicalization out of the CommonJS runtime path. A failing `require("canonicalize")` is an explicit packaging regression test.
- **64-bit Integer Domain:** All 64-bit identifiers (`event_id`, `causation_id`, microsecond timestamps) MUST be serialized as strings in the signed envelope object to prevent IEEE-754 double precision domain errors ($\ge 2^{53}$).
- **Signed bytes:** Ed25519 signs `JCS(unsigned_envelope)` directly. The signature is not a member of `unsigned_envelope`; including it would create a recursive, undefined byte sequence. The transport object is `{ "signed": unsigned_envelope, "signature": "<unpadded base64url>" }`.
- **Signed Object Format:**
  ```json
  {
    "envelope_version": "1",
    "workspace_id": "okf-agentbus-workspace-uuid",
    "topic": "okf/handoff",
    "producer_id": "factory",
    "key_id": "factory-runner-ed25519-v1",
    "policy_version": "7",
    "registry_version": "12",
    "nonce": "d98f7a2b1c4e9081",
    "timestamp": "2026-08-25T00:10:00.000000Z",
    "causation_id": "7524",
    "idempotency_key": "qa:7524:factory-runner-ed25519-v1",
    "trace_id": "trace-7524",
    "action": {"type": "qa_verdict", "result": "green"},
    "payload": {"from": "factory", "to": "codex", "summary": "QA complete"},
    "artifact_digests": [
      {"name": "test_output.log", "size": "0", "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
    ]
  }
  ```
- The payload above is the exact post-extraction payload persisted by the broker. Artifact entries are ordered by normalized name; the broker rejects extra, missing, duplicate-name, size-mismatched, or digest-mismatched artifacts. Evidence blobs are content-addressed by SHA-256.
- JSON decoders reject duplicate keys before object construction, lone surrogates, `NaN`/`Infinity`, and non-NFC strings. This validation is identical at transport, signing, storage hydration, and verification.

---

## 6. Accepted Residual Risks

1. **Shared-UID Developer Host Limitation:** In single-user developer environments without container or OS-level user isolation, a process running with identical UID permissions can inspect local files and memory. Under this configuration, AgentID provides **cryptographic non-repudiation, tamper-evidence, and subagent containment**, but cannot defend against a malicious native process with full read access to local private key files. Strict mode documentation and `agentbus doctor` must make this boundary explicit.
2. **Clock Drift Tolerance:** Nonces are accepted within a bounded clock drift window ($\pm 60$ seconds). Replays within this window are defended by atomic database nonce constraints; replays outside the window are rejected by timestamp validation.
3. **Trust-root compromise:** A compromised offline workspace root can authorize malicious policy prospectively. AgentID cannot cryptographically distinguish that from a legitimate owner action. Recovery rotates the root through an externally recorded break-glass event and never retroactively verifies history.
4. **Signed output correctness:** A valid signature proves control of an enrolled execution boundary, not that its output is correct or that a claimed model/persona was used. Independent Factory review remains necessary.

## 6.1 Policy and migration controls

- `droid_proof` and legacy `token_roles` cannot authorize protected or strict events. Promotion invalidates existing token-role authority and replaces it with producer-bound credentials.
- Free-form `forbidden_payloads`, verdict keywords, and summary rewriting are non-security compatibility behavior. Privilege is determined only by the typed `action` object and verified policy capability.
- Additive migrations are driven by a `schema_version` table and tested against released v0.16.4, v0.18.0, and v0.19.0 fixtures. Legacy events remain `legacy_unverified`; a downgraded verifier must treat unknown envelope/policy/schema versions as unverified and refuse restricted actions.
- Stored verification badges and receipts are advisory caches. Every authoritative consumer recomputes the envelope, artifact bindings, registry state, revocation state, workspace binding, and action capability.
- `doctor` reports `strict_ready=false` on a same-UID shared host regardless of file modes. It reports separately whether audit integrity, protected broker enforcement, and strict OS isolation are available.

---

## 7. Authoritative Negative Test Matrix (32 Scenarios)

Every row below represents an automated, reproducible test that MUST **fail closed**:

| # | Test Scenario | Attack Surface | Expected Result | Target Task |
|---|---------------|----------------|-----------------|-------------|
| **N1** | `agentbus publish --producer-id factory` without valid private key | CLI | `403 Forbidden`, zero rows published | `ABUS-020-005` |
| **N2** | Raw `sqlite3 INSERT` of a crafted signed-looking row | Filesystem / Untrusted Storage | Verify-at-read rejects; `verify-event` reports unverified | `ABUS-020-005` |
| **N3** | Raw `sqlite3 UPDATE` modifying payload of an existing verified event | Filesystem / Untrusted Storage | Verify-at-read fails signature check; event treated as unverified | `ABUS-020-005` |
| **N4** | Go writer inserting on restricted topics without signing | `go-core` store | Rejected or Go restricted to read-only on restricted topics | `ABUS-020-007` |
| **N5** | Publisher sets `AGENTBUS_IDENTITY_MODE=audit` in env while workspace policy is `strict` | Local Env Variable | Workspace policy takes precedence; publish rejected | `ABUS-020-003` |
| **N6** | Setting `AGENTBUS_DISABLE_RBAC=1` or `AGENTBUS_AUTH=off` in mode $\ge$ `protected` | Local Env Variable | Ignored/refused; strict security enforced | `ABUS-020-006` |
| **N7** | Unauthorized caller submits publish reusing another producer's `idempotency_key` | `EventStore.publish` | `403 Forbidden`, zero payload returned to caller | `ABUS-020-005` |
| **N8** | Replay of identical signed envelope content to trigger dedup | `EventStore.publish` | Verification precedes dedup; duplicate nonce rejected | `ABUS-020-005` |
| **N9** | Concurrent submission of identical signed envelope ($N$ concurrent threads) | SQLite Concurrency | Exactly 1 transaction succeeds; $N-1$ fail on nonce collision | `ABUS-020-005` |
| **N10** | Replay of signed envelope after workspace policy/registry rollback | Policy Registry | Rejected on monotonic policy version high-water mark | `ABUS-020-003` |
| **N11** | Replay of envelope with host system clock set backwards | System Clock | Rejected on monotonic timestamp high-water mark | `ABUS-020-003` |
| **N12** | Replay of signed envelope into a different workspace (`workspace_id` mismatch) | Multi-Workspace | Rejected on `workspace_id` digest mismatch | `ABUS-020-002` |
| **N13** | `producer_id="agy"` attempting to publish with `payload.from="factory"` | `EventStore.publish` | `403 Forbidden` (mismatch rejected, not normalized) | `ABUS-020-005` |
| **N14** | Exact July fixtures: Agy `self` children claim Grok/Hermes, or a child claims Codex/Factory/Factory Droid; an Agy child test result attempts to satisfy Factory QA | Delegation / Gate Boundary | Peer claims rejected; Agy-owned evidence cannot transition the independent Factory gate | `ABUS-020-004` / `009` |
| **N15** | Child process attempting to access parent signing handles/sockets | Process Environment | Handles scrubbed; signing fails | `ABUS-020-004` |
| **N16** | Delegated token attempting privilege escalation (e.g. requesting `qa` or `release`) | Delegation Token | Rejected; child cannot exceed parent scope | `ABUS-020-004` |
| **N17** | Forged wake record injected directly into `<runtime>_wake_queue.jsonl` | Wake Queue File | Runner drops wake during store rehydration | `ABUS-020-010` |
| **N18** | `WAKE.<agent>.json` written with synthetic `event_id` not in `events.db` | Direct Wake File | Dropped; no matching verified store event | `ABUS-020-010` |
| **N19** | HTTP POST to `/agentbus/wake` without a valid broker-issued runtime capability | Webhook Ingress | `401 Unauthorized`; shared token or caller event body is insufficient | `ABUS-020-010` |
| **N20** | Post-hoc modification or append of rows in `artifacts` table | Side Table Tamper | Hydration digest mismatch; event flagged unverified | `ABUS-020-005` |
| **N21** | Insertion of orphan artifact row for nonexistent `event_id` | Side Table Tamper | Rejected by SQLite foreign key constraint (`FK=ON`) | `ABUS-020-005` |
| **N22** | Self-minted `droid_proof` submitted for `qa_droid` in mode $\ge$ `protected` | RBAC / Proof Gate | Rejected; AgentID cryptographic signature required | `ABUS-020-006` |
| **N23** | Bearer token for User A used to sign/publish as Producer B | Token Authorization | `403 Forbidden` | `ABUS-020-006` |
| **N24** | Implementer key (`codex`) issuing `QA_VERDICT: GREEN` | Separation of Duties | `403 Forbidden` | `ABUS-020-006` |
| **N25** | Codex key issuing `Agy GO`; Agy key issuing Factory QA verdict | Separation of Duties | Both rejected | `ABUS-020-006` |
| **N26** | Companion `RUNNER_ACK` claiming verified trust level of source event | ACK Processing | Trust level independent; ACK is ops-only | `ABUS-020-006` |
| **N27** | Event signed by a revoked key ID present in Revocation Ledger | Key Revocation | Rejected immediately upon ledger lookup | `ABUS-020-003` |
| **N28** | Event signed with rotated key after rotation grace window has expired | Key Rotation | Rejected | `ABUS-020-003` |
| **N29** | Attempting break-glass recovery to retroactively verify historical events | Recovery Ledger | Rejected; break-glass events marked non-retroactive | `ABUS-020-008` |
| **N30** | Signed payload containing duplicate JSON keys, lone surrogates, `NaN`/`Inf`, non-NFC strings, or an integer `>= 2^53` | JCS Serializer | Rejected prior to signing across Python, Go, and TS; Node must not silently round | `ABUS-020-002` |
| **N31** | Cross-language test: Envelope signed in Python verified in Go and TypeScript | Cross-SDK Test | Byte-identical canonical form and verification PASS | `ABUS-020-002` |
| **N32** | Forward migration of legacy v0.19 `events.db` into v0.20 schema | Schema Migration | History intact; legacy rows marked `legacy_unverified` | `ABUS-020-008` |

---

## 8. References

- Factory Security Verdict RED #7557: `/initiatives/agentbus/missions/verdict_security_abus-020-001_20260824T160000Z.md`
- Agy Triage Decision: `/initiatives/agentbus/decisions/agy-v0-20-agentid-threat-model-red-triage-2026-08-25.md`
- RFC 8785 JSON Canonicalization Scheme: https://www.rfc-editor.org/rfc/rfc8785
- RFC 8032 Edwards-Curve Digital Signature Algorithm: https://www.rfc-editor.org/rfc/rfc8032
