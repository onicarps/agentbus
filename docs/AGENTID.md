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

On a host where mutually distrusting runtimes share one Unix UID, file permissions and same-UID sockets provide tamper evidence, not execution prevention. Strict readiness requires the reference monitor and peer runtimes to run under distinct OS principals or equivalently isolated containers. A software signature proves control of an enrolled execution boundary; it does not prove model vendor, model name, persona, or correctness.

The full threat model and accepted residual risks are in [the AgentID trust-boundary ADR](adr/2026-08-25-agentid-trust-boundaries.md).
