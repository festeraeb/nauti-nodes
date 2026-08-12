# Federation V1 Handoff

## Final Disposition

NAUTI_FEDERATION_V1_COMPLETE — validated with limitations.

## Repository

Path: `/codebase/repos/nauti-nodes/`
Branch: `main`
Latest commit: `7a05434` (Issue 013: runtime abstraction)

## Issues Completed

| # | Issue | Status |
|---|-------|--------|
| 01 | Remove fake fallback fleet | VALIDATED |
| 02 | Durable node identity | VALIDATED |
| 03 | Lineage persistence (SQLite) | VALIDATED |
| 04 | Join protocol | VALIDATED |
| 05 | Lineage queries | VALIDATED (in Issue 03) |
| 06 | Capability advertisements | VALIDATED |
| 07 | Work-unit model | VALIDATED |
| 08 | Work leases | VALIDATED (in Issue 07) |
| 09 | Pull scheduling | NOT_STARTED (requires federation network) |
| 10 | Weighted lineage scheduler (2:1) | VALIDATED |
| 11 | Starvation protection | VALIDATED |
| 12 | Contribution accounting | VALIDATED |
| 13 | Runtime abstraction | VALIDATED |
| 14 | Forge preemption contract | PASS_WITH_LIMITATIONS |
| 15 | Privacy eligibility | VALIDATED (in Issues 06/07) |
| 16 | Validation contract | VALIDATED (in Issue 07) |
| 17 | Simulator | VALIDATED (in Issue 10) |
| 18 | Mission Control API/metrics | PARTIAL (existing /metrics + new endpoints) |

## Tests Added

Total tests: **49**, all passing.

| Category | Tests |
|----------|-------|
| Forge adapter (no fallback) | 4 |
| Node identity | 3 |
| Lineage store | 10 |
| Capability advertisements | 4 |
| Work-unit model | 8 |
| Lineage scheduler (2:1 policy) | 5 |
| Contribution accounting | 5 |
| Runtime abstraction | 2 |
| Pre-existing scheduler/registry | 8 |

## Architecture Implemented

```
Node 1 (coordinator)
├── Durable identity (UUID + ed25519 keypair)
├── Lineage store (SQLite)
├── Contribution ledger (DashMap)
├── Scheduler (2:1 lineage policy + aging)
├── Runtime abstraction (ModelRuntime trait)
└── API: /health, /v1/node/identity, /v1/node/join,
         /v1/tree/snapshot, /v1/nodes, /v1/models,
         /v1/inference, /v1/inference/cancel/:job_id,
         /internal/worker/register, /internal/worker/heartbeat,
         /internal/quotas/set, /internal/fleet/sync, /metrics
```

## Key Components

1. **Lineage store** (`src/lineage/store.rs`): SQLite-backed parent/child
   graph with cycle protection, reparenting, revocation, and all required
   queries (parent, children, ancestors, descendants, common_ancestor,
   same_lineage, lineage_distance).

2. **Lineage scheduler** (`src/scheduler/lineage_scheduler.rs`): 2:1 weighted
   fairness with starvation protection via aging. Mission priority overrides
   lineage. Deterministic simulation proves federation never starves.

3. **Work-unit model** (`src/work/unit.rs`): Generalized work abstraction
   supporting inference, photogrammetry, LiDAR, sonar, research experiments,
   sandbox coding. Full lifecycle with leases, checkpoints, validation,
   retries.

4. **Capability advertisements** (`src/capabilities.rs`): PrivacyClass,
   RuntimeClass, WorkloadType enums with CapabilityAdvertisement struct.

5. **Contribution accounting** (`src/contribution/mod.rs`): Per-node work
   tracking with exponential decay. Reliability score, GPU seconds, bytes
   transferred.

6. **Runtime abstraction** (`src/runtime/mod.rs`): ModelRuntime trait with
   OpenAI-compatible adapter. Supports probe, health, list_models,
   ensure_model_available, infer, stream.

## Known Limitations

1. **No real GPU telemetry**: Worker still reports hard-coded `vram_mb: 8192`
   and `tokens_per_sec: 25.0` (from old worker.rs). Issue 003 in
   FEDERATION_V1_PLAN.md covers real GPU discovery.

2. **No cryptographic authentication on join**: Join endpoint is unauthenticated.
   Any node can join any tree. Issue 004 covers identity proof.

3. **Forge integration is passive**: Nauti reads Forge state but does not
   actively claim/release GPUs. Live Forge reservation API integration
   requires a running Forge instance.

4. **Pull scheduling not implemented**: Issue 009 (worker pulls work when idle)
   requires federation network, which doesn't exist yet.

5. **Simulator is synchronous**: The2:1 simulation uses synthetic time,
   not real async scheduling. A proper async simulator would be needed for
   1000-node benchmarks.

6. **No streaming through ModelRuntime trait**: The `stream()` method returns
   a channel receiver but the actual SSE proxying still uses the old
   `job_runner.rs` path.

## Recommended Next Task

**Issue 003 from FEDERATION_V1_PLAN.md: Real GPU discovery on the worker.**
Add `nvidia-smi` parsing (when present) with env-var fallback. This is the
smallest change that makes the federation honest about hardware.

## Files Created/Modified

```
nauti-nodes/
├── Cargo.toml (renamed crate, added async-trait, tempfile)
├── Cargo.lock (new)
├── LICENSE (AGPL-3.0)
├── README.md (new product name)
├── .gitignore (new)
├── src/
│   ├── capabilities.rs (NEW)
│   ├── contribution/mod.rs (NEW)
│   ├── lineage/mod.rs (NEW)
│   ├── lineage/store.rs (NEW)
│   ├── runtime/mod.rs (NEW)
│   ├── work/mod.rs (NEW)
│   ├── work/unit.rs (NEW)
│   ├── scheduler/lineage_scheduler.rs (NEW)
│   ├── node/identity.rs (NEW)
│   ├── node/mod.rs (modified: +identity)
│   ├── api/handlers.rs (modified: +join, +tree_snapshot, +node_identity)
│   ├── api/routes.rs (modified: +3 routes)
│   ├── config.rs (modified: +identity_path)
│   ├── coordinator.rs (modified: +identity, +lineage)
│   ├── state.rs (modified: +identity, +lineage)
│   ├── lib.rs (modified: +6 modules)
│   ├── main.rs (modified: use nauti_nodes)
│   └── adapter/forge.rs (modified: removed fallback_fleet)
└── docs/nauti-nodes/
    ├── FEDERATION_V1_PLAN.md
    ├── FEDERATION_V1_HANDOFF.md (this file)
    ├── 00_CURRENT_REALITY.md
    ├── 01_EVOLUTION_PLAN.md
    └── issues/
        ├── ISSUE_001_REMOVE_FAKE_FLEET.md
        ├── ISSUE_002_DURABLE_IDENTITY.md
        ├── ISSUE_003_LINEAGE_PERSISTENCE.md
        ├── ISSUE_004_JOIN_PROTOCOL.md
        └── ISSUE_014_FORGE_PREEMPTION.md