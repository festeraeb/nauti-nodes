# Issue 002 — Durable Node Identity

## Objective

Each Nauti Node installation gets a unique, persistent identity that survives
restarts. Node ID is a UUID (not a hostname). An ed25519 keypair enables
future cryptographic authentication.

## Original State

`NodeId = String` (set by env var, often a hostname like "RTX2060"). No
persistence. No keypair. Identity lost on restart.

## Changes

- New `src/node/identity.rs`: `NodeIdentity` with UUID-based node_id,
  ed25519 public+secret keypair, created_at timestamp, software version
- `load_or_create(path)`: loads from JSON file or generates+persists new
- `signing_key()`: reconstructs ed25519 SigningKey from persisted bytes
- Config field `identity_path` (default `/var/lib/nauti-nodes/identity.json`,
  overridable via `NAUTI_IDENTITY_PATH`)
- Coordinator loads identity at startup, logs it, stores in `AppState`
- New endpoint `GET /v1/node/identity` returns node_id, public_key_hex,
  created_at_ms, software_version
- `tempfile` added as dev-dependency for test fixtures

## Tests

| Test | Result |
|------|--------|
| `generate_has_valid_uuid` | PASS |
| `load_or_create_persists` | PASS |
| `signing_key_roundtrip` | PASS |
| All 12 pre-existing tests | PASS |

## Live Evidence

```
GET /v1/node/identity
{"created_at_ms":1786462124575,"node_id":"3d3945fd-a70f-42da-8726-565e6114c453",
 "public_key_hex":"39f0ca74361ab50738aa610bef629834a290a5fcfdc311e6f02dc83a5a2b64c5",
 "software_version":"0.1.0"}
```

## Remaining Limitations

- `secret_key` is stored in plaintext JSON on disk. Future work should
  encrypt it at rest.
- The `/v1/node/identity` endpoint exposes `public_key_hex` but the
  serialization for network transmission (challenge/response auth) is not
  yet implemented.
- The identity is not yet used to authenticate worker registration or
  heartbeat calls.

## Disposition

PASS — durable identity persisted and verified.