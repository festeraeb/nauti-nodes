# Chunk 7 — Persistent Fleet Operations

**Goal:** Make Nauti Nodes' GPU ownership model *authoritative and durable* so that
coordinator restarts, worker crashes, and runtime failures can never cause a
leased GPU to be double-booked for shared inference.

## Problem

Before Chunk 7, GPU ownership truth lived only in process memory. A coordinator
restart could:

- Forget an active Forge lease and schedule shared work onto a GPU that was
  still leased (double ownership).
- Refund orphaned token reservations twice (or never).
- Assume a worker's hardware was unchanged across restart.
- Assume a worker's runtime was still alive.

## Solution

### 1. Authoritative lease state machine (`src/lease.rs`)

A `LeaseManager` owns the single source of truth for Forge GPU leases. Each
lease has a UUID `lease_id` and a strict state machine:

```
REQUESTED → SCHEDULER_EXCLUDED → RUNTIME_EXITED → GPU_RELEASED → GRANTED
                                                                   ↓
                                                               RELEASING → RELEASED
                                                                   ↓
                                                              RECOVERING → READY
```

- **Idempotent reserve:** a duplicate `begin_reserve` for the same
  `(mission_id, node_id)` returns the existing lease with `idempotent: true`
  and performs no re-drain.
- **Explicit conflict:** a *different* mission attempting to claim an
  already-leased GPU returns `409 Conflict` and is journaled
  (`LEASE_CONFLICT`).
- **Fail-closed release:** releasing a lease on a node whose truth was not
  re-established since restart does **not** auto-restore it to the shared
  pool. The node must rejoin (re-register + heartbeat) first.

### 2. Persistent operational state (`src/opsstore.rs`)

A SQLite-backed `OpsStore` persists:

- Node snapshots (hardware claim, observed hardware, reconciliation status,
  lifecycle state, runtime state, last heartbeat).
- Lease rows (full state-machine history with timestamps).
- Token-reservation ledger rows (state: `ACTIVE` / `PREEMPTED` / `SETTLED` /
  `REFUNDED`).
- An append-only event/audit journal (`OpsEventKind`).

SQLite `busy_timeout` is set on every connection so concurrent handler
threads never corrupt the ledger.

### 3. Coordinator restart recovery (`src/state.rs`)

`recover_operational_state` runs at `AppState::new`:

- Recovers persisted nodes as **OFFLINE** with `runtime_state = Unknown` and
  `reconciliation = STALE` (hardware truth is never assumed unchanged).
- Preserves **active lease ownership** (granted/leased/releasing/recovering)
  and re-applies the `RESERVED_FORGE` overlay on recovered nodes.
- Refunds **orphaned** token reservations exactly once (the `settle_reservation`
  transition is atomic; `PREEMPTED`/`SETTLED` rows are skipped).

### 4. Fail-closed eligibility (`src/eligibility.rs`, `src/fleet.rs`)

`evaluate_node` / `evaluate_metadata` is the single eligibility predicate.
A node is eligible for shared work only when **all** of:

- `lifecycle_state` is not terminal (not Withdrawn/Revoked/Draining/Offline).
- `health_state` is not `RESERVED_FORGE` / `PREEMPTING` / `RECOVERING`.
- `runtime_state` is `Ready` (worker-reported runtime truth).
- Reconciliation is `MATCH` or `UNVERIFIED` (not `MISMATCH` / `STALE`).
- Heartbeat is fresh (≤ 45s).
- No active lease on the node.

`fleet::pick_node_for_role` consults this predicate exclusively — there is no
parallel eligibility path.

### 5. Lease-protected stale sweep (`src/node/heartbeat.rs`)

The sweeper marks stale nodes `OFFLINE` but **never** clears an active lease
overlay. A leased GPU stays excluded even if its heartbeat goes silent.

### 6. Metrics (`src/metrics/mod.rs`)

`FederationMetrics` is `Arc`-shared across `AppState` clones (the previous
by-value clone was lossy). Counters include:

- `leases_granted_total`, `leases_released_total`
- `jobs_preempted_total`, `jobs_requeued_total`
- `ownership_conflicts_blocked_total`
- `accounting_inconsistencies_total`
- `runtime_failures_total`, `node_reconnects_total`

### 7. Operator observability

- `GET /operator/fleet` — admin-authenticated machine-readable fleet view
  (node ownership, eligibility, active leases, reconciliation).
- `GET /operator/events` — admin-authenticated event history query.
- `nauti fleet` / `nauti events` CLI commands.

## Testing

### Failure-injection suite (`tests/chunk7_failure_injection_integration.rs`)

20 cases covering: coordinator restart (idle fleet, shared work, active lease,
lease recovery), worker SIGKILL, heartbeat timeout, reconnect, runtime
crash/recovery, lease-vs-work race, duplicate/conflicting lease, duplicate
release, revoked/withdrawn nodes during lease, hardware mismatch, accounting
survival, checkpoint survival, and stale-state fail-closed.

```
cargo test --test chunk7_failure_injection_integration
```

### Soak harness (`tests/chunk7_soak.rs`)

A configurable-duration soak that exercises heartbeats, inference jobs,
runtime up/down, worker disconnect/reconnect, lease cycles, coordinator
restart, and continuous invariant checks.

```
# CI smoke (5s)
cargo test --test chunk7_soak

# 1-hour local validation
NAUTI_SOAK_DURATION_SECS=3600 cargo test --test chunk7_soak -- --nocapture

# Overnight
NAUTI_SOAK_DURATION_SECS=28800 bash scripts/chunk7_soak.sh
```

**Required invariants (asserted at end of every soak run):**

- `ownership_conflicts = 0`
- `accounting_inconsistencies = 0`
- `leases_granted == leases_released`

## RC1 Compatibility

- All 8 RC1 clean-host acceptance tests pass unmodified.
- The signed worker protocol (RC1) is unchanged; Chunk 7 adds
  *authoritative* truth on top of it.
- `external_alpha_enabled` kill switch and admin auth are preserved.
