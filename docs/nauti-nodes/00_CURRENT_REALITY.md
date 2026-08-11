# 00 — CURRENT REALITY (Nauti Nodes 0.1.0)

## Provenance

`nauti-nodes` 0.1.0 is a **byte-for-byte snapshot** of the working
`nauti-inferer` crate that lives in the wreckhunter2000-1 monorepo at
`backend/nauti-inferer/`. The source was copied verbatim, the
`Cargo.toml` was renamed to use the new crate name, and a thin
README/LICENSE/.gitignore were added. **No source file was modified.**

The only file renamed in the new tree is `Cargo.toml`; everything in
`src/` is identical to the snapshot at
`/codebase/repos/wreckhunter2000-1/backend/nauti-inferer/`. The
previous archaeology under `docs/nauti-inferer/` in the monorepo
applies verbatim; the relevant claims are summarised below for
readers who do not have the monorepo open.

## What runs and what doesn't

### Verified working (in the local snapshot, monorepo build evidence)

| Capability | Evidence |
|---|---|
| `cargo build --release` succeeds | `Finished `release` profile [optimized] target(s)` from the monorepo build |
| `GET /health` returns 200 with status ok | `{"service":"nauti-inferer","status":"ok","version":"4.0.0"}` (version string is the in-code literal; the new crate is at 0.1.0 per Cargo.toml) |
| `POST /internal/worker/register` registers a node | `{"registered":"TEST_NODE"}` |
| `GET /v1/nodes` lists registered nodes | Node `TEST_NODE` present |
| `POST /v1/chat/completions` accepts a request and returns a `job_id` | Confirmed in the bring-up run |
| `POST /v1/chat/completions` (stream=true) emits SSE `data:` frames | Confirmed |
| `DELETE /v1/cancel/:job_id` cancels a job | Code path present in `api/handlers.rs::cancel_inference` |

### What the previous "inference test" actually exercised

The previous task's "inference path works" evidence is honest
**plumbing**, not **model output**:

- The coordinator selected a node, created a `JobRecord`, spawned a
  `job_runner`, the runner issued a `POST` to the node's
  `inference_url` (`http://localhost:5200`), and either:
  - succeeded in returning SSE chunks (if a real llama-server was
    answering), or
  - failed with a transport error, in which case the SSE stream
    produced an error event.
- The job lifecycle, broadcasting, API-key check, quota reservation,
  and cancellation all worked.
- There was **no verification that the streamed bytes were real
  model tokens from a healthy model** in that test run. The
  "inference path" was the **transport + scheduling** path, not an
  end-to-end model round-trip.

This distinction is important for the new federation: the runtime
adapter needs to be qualified against an actual model before the
crate can be said to **do inference**, not merely **plumb
inference-shaped requests**.

### Confirmed gaps (to be addressed by the evolution plan)

| Gap | Where it lives |
|---|---|
| No real GPU discovery / VRAM telemetry | `worker.rs` hard-codes `vram_mb: 8192` and `tokens_per_sec: 25.0 if /health else 0.0` |
| No node identity proof | `worker_register` is an unauthenticated POST |
| No replay protection | Heartbeats are pure JSON with no nonce |
| Forge `/cluster/gpus` may return hard-coded fallback values | `adapter/forge.rs::fallback_fleet` |
| Coordinator may populate the registry from a hard-coded list even when no real GPU answers | `fleet::sync_fleet` -> `forge::fetch_fleet_from_forge` -> `fallback_fleet` |
| No signed tree snapshot | Not implemented |
| No parent/child lineage authority | Not implemented (Node 1 is implicit: the coordinator itself) |
| No empty-GPU model assignment | Not implemented (Paddler integration is on the plan, not the code) |
| No Lemonade, NIM, or generic OpenAI adapter abstraction | Adapters exist only for llama-server and Forge |
| No contribution ledger | Scheduler only has per-key token quota, not per-node contribution |
| `stale_after_ms` not exposed as config | Hard-coded default |

## File map (current)

```
nauti-nodes/
├── Cargo.toml                 (renamed; new name = nauti-nodes, version 0.1.0)
├── LICENSE                    (AGPL-3.0)
├── README.md                  (replaced; new product name)
├── landing.html               (operator landing page, embedded via include_str!)
├── src/
│   ├── main.rs                (binary entry: picks coordinator vs worker by env)
│   ├── lib.rs
│   ├── config.rs              (NAUTI_* env vars, RuntimeMode::{Coordinator,Worker})
│   ├── coordinator.rs
│   ├── worker.rs              (run_worker: register + 10s heartbeat loop)
│   ├── fleet.rs               (sync_fleet, pick_node_for_role)
│   ├── jobs.rs                (JobRecord, in-memory map)
│   ├── job_runner.rs          (spawns inference calls, broadcasts to SSE)
│   ├── state.rs               (AppState: registry, jobs, scheduler, http, config)
│   ├── adapter/
│   │   ├── forge.rs           (Forge fleet fetch + fallback list)
│   │   └── llama.rs           (llama-server OpenAI client + health probe)
│   ├── api/
│   │   ├── handlers.rs        (handlers: inference, register, heartbeat, cancel, …)
│   │   └── routes.rs          (axum Router)
│   ├── node/
│   │   ├── registry.rs        (DashMap<NodeId, Arc<NodeRuntime>>; stale removal)
│   │   ├── manager.rs
│   │   ├── heartbeat.rs
│   │   └── mod.rs
│   ├── scheduler/
│   │   ├── affinity.rs        (prompt/role affinity routing)
│   │   ├── scoring.rs         (compute_score over node statistics)
│   │   ├── quotas.rs          (per-API-key token ledger)
│   │   └── mod.rs
│   └── types/
│       ├── errors.rs
│       └── mod.rs             (NodeMetadata, NodeRuntime, NodeStatistics, MAILBOX_CAP)
└── docs/
    └── nauti-nodes/
        ├── 00_CURRENT_REALITY.md  (this file)
        └── 01_EVOLUTION_PLAN.md   (next file)
```

## How to verify the snapshot

```bash
cd /codebase/repos/nauti-nodes
cargo build --release
RUST_LOG=info NAUTI_LISTEN_PORT=8081 ./target/release/nauti-nodes &
sleep 3
curl -s http://localhost:8081/health
curl -s -X POST http://localhost:8081/internal/worker/register \
    -H 'Content-Type: application/json' \
    -d '{"id":"SNAPSHOT_NODE","inference_url":"http://127.0.0.1:5200","role":"thinker","gpu_name":"RTX 2060","vram_mb":8192,"models":["thinker"]}'
curl -s http://localhost:8081/v1/nodes
```

The expected output of the last call is a JSON document with at
least one node (`SNAPSHOT_NODE`), and may also contain entries from
the hard-coded `fallback_fleet` (`RTX2060`, `GTX1070`, …) depending
on whether Forge is reachable.
