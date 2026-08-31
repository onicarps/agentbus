# AgentID security model

AgentID binds an AgentBus producer to an Ed25519 key and a root-signed workspace policy. It prevents a caller-controlled `producer_id`, payload `from`, role name, or wake file from becoming identity evidence by itself.

## Identity commands

```bash
agentbus identity init --workspace /path/to/workspace
agentbus identity enroll codex --capability message --topic okf/handoff
agentbus identity delegate agy review-42 --capability message \
  --topic okf/handoff --ttl-seconds 900
agentbus identity list --workspace /path/to/workspace
agentbus identity rotate codex --grace-seconds 300
agentbus identity revoke codex-<key-id>
agentbus identity verify-event 42
agentbus identity issue-wake-capability factory
```

Staged promotion and recovery use explicit signed-policy operations:

```bash
# Stage 1: initialize/enroll and observe legacy_unverified rows in audit mode.
agentbus identity mode protected

# Stage 3 requires an externally established distinct-principal/container broker.
agentbus identity reference-monitor isolated_broker --isolation-attested
agentbus identity mode strict

# Recovery is prospective and never upgrades historical rows.
agentbus identity record-recovery --reason "rotate lost online signer"
```

`--isolation-attested` records operator acknowledgement in signed policy; it is
not proof by itself. `agentbus doctor` continues to report
`strict_ready=false` on a shared-UID host even when policy mode is strict.

In an identity-configured workspace, CLI publishing on restricted topics
requires `AGENTBUS_IDENTITY_PRIVATE_KEY` to name the caller's matching signing
handle. Merely passing `--producer-id factory` never selects Factory's key.

Initialization creates `.agentbus/identity/` with a root-signed policy and registry. Local private keys are mode `0600`; they are development/audit credentials, not a strict isolation boundary. The workspace policy owns the mode, workspace ID, registry digest, restricted topics, and runtime wake-capability hashes. Environment variables may raise the configured mode but cannot lower it.

Delegated identities are named `<parent>/subagent/<id>`, expire after at most
one hour, cannot delegate again, and may only receive a subset of the parent's
topics and non-privileged capabilities. QA verdict, Agy GO, identity admin,
merge, push, and release authority cannot be delegated. A child must publish
as its own delegated identity; claiming `codex`, `factory`, or another peer is
rejected.

Every signed event covers the exact post-extraction payload, ordered artifact type/name/size/SHA-256 records, topic, producer, schema, policy and registry versions, nonce, timestamp, causation, idempotency, trace, and typed action. Signatures are Ed25519 over RFC 8785 JCS bytes. Event IDs and other 64-bit values are decimal strings inside the signed object.

Privileged automation uses a closed typed-action vocabulary rather than
matching words in summaries. Factory or an enrolled Factory droid may sign
`qa_verdict`; only Agy may sign `agy_go`; Codex may sign `merge`, `push`, and
`release`; and the dedicated identity administrator owns `identity_admin`.
Capabilities and the root-signed producer allowlist must both authorize the
action. Publish typed actions with `agentbus publish --action '<json>'` or the
MCP `action` argument, and wait on them with `agentbus await --action-type ...`.
`runner_ack` is explicitly operations-only and cannot fulfill a QA or GO gate.

Python uses `rfc8785==0.1.4` and `cryptography==50.0.0`; TypeScript uses the ESM-only `canonicalize@4.0.0` through a preserved dynamic-import boundary; Go pins `github.com/cyberphone/json-canonicalization@v0.0.0-20241213102144-19d51d7fe467`. A shared fixture proves byte and signature parity across all three implementations.

## Wake security

In protected mode, webhook ingress requires the runtime-specific capability recorded in signed policy. The request may notify only an existing event ID. Ingress and runners reload that event from `events.db`, recompute its AgentID proof, and ignore caller-supplied wake payload fields. Missing, synthetic, tampered, or unverified events do not start turns.

Audit mode keeps legacy synthetic wakes temporarily for migration compatibility and labels them unverified. It must not be presented as impersonation prevention.

Headless adapter children receive only workspace, producer, wake-event, and
chain routing metadata. Inherited `AGENTBUS_*` signer/broker/token variables,
SSH/GPG agent sockets, and inherited file descriptors are removed before exec.

## Honest guarantees

`events.db` is untrusted storage. Stored verification badges are advisory; authoritative consumers recompute signatures and artifact digests.

Publish authentication runs before RBAC and before either idempotency or
content deduplication. Idempotency keys are scoped to the cryptographically
verified producer. Exact signed-envelope replay is rejected by nonce even when
the payload would otherwise deduplicate. Poll/await, runner intake, trace views,
TUI/monitor snapshots, HITL review, SLA processing, and log projection all
recompute proof material; protected/strict mode filters invalid restricted-topic
events without hydrating or executing their payloads.

In protected or strict mode, `AGENTBUS_AUTH=off`,
`AGENTBUS_DISABLE_RBAC=1`, token-to-role mappings, and legacy droid proofs cannot
grant authority. MCP publishing is bound to the supervised runtime producer,
and a private key enrolled for one producer cannot publish as another. The Go
direct store/worker path refuses restricted-topic publish and poll once AgentID
is configured; those operations must go through the verifying broker.

`agentbus doctor` verifies the signed policy and registry, key state,
high-water rollback protection, identity database migrations, credential scrub,
reference-monitor mode, and strict readiness. A normal shared-UID local setup
honestly reports `strict_ready=false`.

Opening a released v0.16.4, v0.18.0, or v0.19.0 database performs only additive
schema migration. Existing event IDs, payload bytes, timestamps, causation, and
idempotency values are preserved; unsigned history is labeled
`legacy_unverified`. Root-signed recovery-ledger entries contain an
`effective_after_event_id` boundary and `non_retroactive=true`, so break-glass
operations cannot certify old events.

On a host where mutually distrusting runtimes share one Unix UID, file permissions and same-UID sockets provide tamper evidence, not execution prevention. Strict readiness requires the reference monitor and peer runtimes to run under distinct OS principals or equivalently isolated containers. A software signature proves control of an enrolled execution boundary; it does not prove model vendor, model name, persona, or correctness.

The full threat model and accepted residual risks are in [the AgentID trust-boundary ADR](adr/2026-08-25-agentid-trust-boundaries.md).

## v0.21 isolated broker and offline ceremony

The v0.21 strict boundary uses a dedicated Unix principal and a framed Unix
socket. The broker reads `SO_PEERCRED`, resolves the kernel UID to the symbolic
username in the root-signed policy, requires the matching AgentID key, and owns
all authoritative database writes. A caller-supplied producer or credential
field is ignored.

Create the root on offline storage, initialize only its public descriptor in
the workspace, generate per-principal peer enrollment requests, and import a
detached signed bundle:

```bash
# Offline administrator machine or removable offline environment.
agentbus identity root generate \
  --private-key /offline/agentbus-root.pem \
  --descriptor /transfer/root-public.json

# Online host. This does not create workspace-root.pem.
agentbus identity init --workspace "$AGENTBUS_WORKSPACE" \
  --offline-root-pubkey /transfer/root-public.json
agentbus identity key generate codex --principal agentbus-codex \
  --private-key /var/lib/agentbus-codex/codex.pem \
  --request /transfer/codex-enrollment.json
agentbus identity export-policy-request --workspace "$AGENTBUS_WORKSPACE" \
  --enrollment /transfer/codex-enrollment.json \
  --output /transfer/policy-request.json

# Offline signing step.
agentbus identity ceremony sign --root-key /offline/agentbus-root.pem \
  --request /transfer/policy-request.json \
  --output /transfer/signed-policy.json

# Online verification/import and broker start.
agentbus identity import-signed-policy /transfer/signed-policy.json \
  --workspace "$AGENTBUS_WORKSPACE"
agentbus broker run --workspace "$AGENTBUS_WORKSPACE" \
  --socket /run/agentbus/agentbus.sock
```

Adding a new enrollment and revoking the previous key in one detached request
provides an offline-signed rotation:

```bash
agentbus identity export-policy-request --workspace "$AGENTBUS_WORKSPACE" \
  --enrollment /transfer/codex-v2-enrollment.json \
  --revoke-key codex-OLD_KEY_ID \
  --output /transfer/rotation-request.json
```

The initial broker is Linux/POSIX only. Merely starting it does not make the
workspace strict-ready; active deployment probes and client transports are
delivered by later v0.21 work packages, and non-Linux hosts remain audit or
protected only.
