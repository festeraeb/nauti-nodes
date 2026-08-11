# 01 — EVOLUTION PLAN: from `nauti-inferer` snapshot to a real federation

This plan turns the v0.1.0 snapshot into a real Nauti Nodes
federation. Each issue is small, builds on the previous one, and
preserves existing working behaviour.

Status legend:

- `NOT_STARTED`  — not begun
- `IN_PROGRESS`  — implementation under way
- `BLOCKED`      — blocked on external dependency or evidence
- `IMPLEMENTED_AWAITING_VALIDATION` — code written, tests pending
- `VALIDATED`    — at least one receipt exists

---

## Issue 001 — Standalone public crate (this snapshot)

| field | value |
|---|---|
| **CURRENT** | Nauti Inferer lives at `wreckhunter2000-1/backend/nauti-inferer/`, name `nauti_inferer`, version 4.0.0, never published |
| **TARGET** | Standalone crate `nauti-nodes` v0.1.0 at `/codebase/repos/nauti-nodes`, AGPL-3.0, no source modifications, ready to be pushed to GitHub and published to crates.io |
| **FILES LIKELY AFFECTED** | new repo: `Cargo.toml`, `LICENSE`, `README.md`, `.gitignore`, `docs/nauti-nodes/00_CURRENT_REALITY.md`, `docs/nauti-nodes/01_EVOLUTION_PLAN.md`; copy of `src/`, `landing.html` |
| **RISKS** | accidental source modification, name collision on crates.io (`nauti-nodes` is currently free), licence mismatch (preserved AGPL-3.0) |
| **TEST** | `cargo build --release` succeeds, `/health` returns ok, register/list work |
| **ROLLBACK** | delete `/codebase/repos/nauti-nodes`; do not push; do not publish |
| **STATUS** | **IMPLEMENTED_AWAITING_VALIDATION** (local copy complete; build validation pending in v0.1.0 release prep) |

---

## Issue 002 — Honest "inference path" verification

| field | value |
|---|---|
| **CURRENT** | The "inference test" exercises the SSE/job/scheduling plumbing against any HTTP responder at `inference_url`; it is not proven to produce real model tokens |
| **TARGET** | Add an end-to-end receipt that proves one of (a) a real `llama-server` on a real GPU model, or (b) any other OpenAI-compatible runtime (Lemonade, NIM) actually returned tokens through the coordinator SSE stream, with the model name and token count recorded |
| **FILES LIKELY AFFECTED** | new `docs/nauti-nodes/issues/ISSUE_002_INFERENCE_RECEIPT.md`, new bring-up script that captures `curl -N` output and a real `llama-server` invocation |
| **RISKS** | If no real runtime is available, the issue must be recorded as BLOCKED, not faked |
| **TEST** | receipt file under `docs/nauti-nodes/issues/` showing model output |
| **ROLLBACK** | n/a (documentation issue) |
| **STATUS** | **NOT_STARTED** |

---

## Issue 003 — Real GPU discovery on the worker

| field | value |
|---|---|
| **CURRENT** | `worker.rs` hard-codes `vram_mb: 8192` and `tokens_per_sec: 25.0 if /health else 0.0` |
| **TARGET** | Add a `gpu::discover()` module that reads `nvidia-smi` (when present) for `name`, `memory.total`, `memory.free`, and reports real numbers. Falls back to env-vars `NAUTI_WORKER_GPU`, `NAUTI_WORKER_VRAM_MB` if `nvidia-smi` is missing |
| **FILES LIKELY AFFECTED** | new `src/gpu/mod.rs`, new `src/gpu/nvidia.rs`; `worker.rs` calls it |
| **RISKS** | shelling out to `nvidia-smi` is platform-specific; must not break non-NVIDIA workers |
| **TEST** | unit test: env-var fallback returns the configured value; integration test: when `nvidia-smi` is present, parsed values are sensible |
| **ROLLBACK** | keep the current hard-coded path behind a `legacy_fallback()`; feature flag `legacy-gpu-info` |
| **STATUS** | **NOT_STARTED** |

---

## Issue 004 — Node identity proof

| field | value |
|---|---|
| **CURRENT** | `worker_register` is unauthenticated; any client can claim any id |
| **TARGET** | On first registration, the coordinator generates a per-node shared secret, returns it. Subsequent calls must present `Authorization: Bearer <secret>` (or HMAC the body). Replay protection via monotonic nonce |
| **FILES LIKELY AFFECTED** | `src/api/handlers.rs::worker_register`, new `src/node/identity.rs` |
| **RISKS** | the bring-up test will need an updated register flow |
| **TEST** | unit test: register then heartbeat with secret; heartbeat without secret is rejected; replay of an old heartbeat nonce is rejected |
| **ROLLBACK** | env `NAUTI_AUTH_DISABLED=1` (staging only) keeps the old path |
| **STATUS** | **NOT_STARTED** |

---

## Issue 005 — Forge three-state discipline (Observed / Allocation / Effective)

| field | value |
|---|---|
| **CURRENT** | `forge::fetch_fleet_from_forge` reads `g.enabled` and treats enabled as online; there is no notion of "Forge reserved" |
| **TARGET** | Add `ForgeEligibility { observed, allocation, effective }`; treat a node as **not** routable when `observed == FREE` but `allocation == RESERVED`. Respect this in `fleet::pick_node_for_role` |
| **FILES LIKELY AFFECTED** | `src/adapter/forge.rs`, `src/fleet.rs` |
| **RISKS** | the bring-up test will only pass if a real Forge instance is present; otherwise the path stays as today |
| **TEST** | mock Forge response with `allocation: RESERVED`; verify node is excluded from candidates |
| **ROLLBACK** | `FORGE_ELIGIBILITY_DISABLED=1` |
| **STATUS** | **NOT_STARTED** |

---

## Issue 006 — Run-time adapter abstraction

| field | value |
|---|---|
| **CURRENT** | `adapter::llama` is hard-coded; no Lemonade or NIM support |
| **TARGET** | Introduce `trait ModelRuntime` with `probe / health / list_models / ensure_model_available / infer / stream / capabilities`. Move the existing llama-server client into `adapter::openai::OpenAIRuntime`. Add a `LemonadeRuntime` (skeleton) and `NimRuntime` (skeleton). Selection by URL pattern or explicit `--runtime` flag |
| **FILES LIKELY AFFECTED** | new `src/runtime/mod.rs`, `src/runtime/openai.rs`, `src/runtime/lemonade.rs`, `src/runtime/nim.rs`; `job_runner.rs` calls the trait |
| **RISKS** | changing the inference path could regress the bring-up test |
| **TEST** | unit test: `OpenAIRuntime` can hit a local mock SSE server; `LemonadeRuntime::probe` against a missing server returns `NotAvailable`, not panic |
| **ROLLBACK** | `OpenAIRuntime` is the existing path; new runtimes are opt-in |
| **STATUS** | **NOT_STARTED** |

---

## Issue 007 — Model-runtime confidence (small, not lock-in)

| field | value |
|---|---|
| **CURRENT** | All runtimes are equal |
| **TARGET** | Each `ModelRuntime` reports a `runtime_confidence: f32` (default 1.0). Lemonade +0.05, NIM +0.05, generic 1.0, unknown 0.95. Used as a small multiplier in `scoring::compute_score` |
| **FILES LIKELY AFFECTED** | `src/scheduler/scoring.rs` |
| **RISKS** | multiplier drift; cap at the 1.05 ceiling in `00_CURRENT_REALITY.md` |
| **TEST** | synthetic score with/without confidence, bounded |
| **ROLLBACK** | constant `RUNTIME_CONFIDENCE_DISABLED=1` |
| **STATUS** | **NOT_STARTED** |

---

## Issue 008 — Parent/child lineage authority (Node 1)

| field | value |
|---|---|
| **CURRENT** | The coordinator is a single process; there is no tree |
| **TARGET** | Add a `LineageAuthority` with `NodeId / ParentNodeId / RootNodeId / BranchId / Depth / RelationshipStatus / CreatedAt / UpdatedAt`. Persisted in SQLite at `~/.local/share/nauti-nodes/lineage.db`. A new node requests a parent; only `Node 1` (configurable via `NAUTI_ROOT_NODE_ID`) may authoritatively grant one. Re-parenting is an explicit event; history is preserved |
| **FILES LIKELY AFFECTED** | new `src/lineage/mod.rs`, `src/lineage/store.rs`, `src/lineage/api.rs`; new routes `/v1/tree/snapshot`, `/v1/tree/events`, `/v1/node/lineage` |
| **RISKS** | SQLite migrations, future schema change, need schema versioning |
| **TEST** | unit test: register a node, request a parent, root grants it, snapshot includes it; re-parent produces two history rows |
| **ROLLBACK** | feature flag `NAUTI_LINEAGE_DISABLED=1` keeps the old flat registry |
| **STATUS** | **NOT_STARTED** |

---

## Issue 009 — Signed tree snapshot

| field | value |
|---|---|
| **CURRENT** | No snapshot, no signature |
| **TARGET** | `TreeSnapshot { epoch, root, relationships, generated_at, previous_hash, signature }` produced by the lineage authority, signed with the existing `ed25519-dalek` dep, served at `GET /v1/tree/snapshot?since_epoch=N`; joining nodes persist it and verify the signature |
| **FILES LIKELY AFFECTED** | `src/lineage/snapshot.rs`; reuse `ed25519-dalek` from `Cargo.toml` |
| **RISKS** | key management; for v0.1.0, generate a self-signed key per coordinator startup, persist it under `auth_keypair_path` |
| **TEST** | unit test: sign, verify, tamper-detection |
| **ROLLBACK** | unsigned snapshot fallback (with a warning) |
| **STATUS** | **NOT_STARTED** |

---

## Issue 010 — Root failure semantics

| field | value |
|---|---|
| **CURRENT** | If the coordinator is down, nothing works (it's also the inference proxy in this snapshot) |
| **TARGET** | Document and test the explicit split: when Node 1 is unavailable, existing jobs continue, existing workers continue heartbeating, but **new** enrollments, re-parenting, and authoritative tree mutations are paused. Workers must not be required to talk to Node 1 per request |
| **FILES LIKELY AFFECTED** | `docs/nauti-nodes/FAILURE_SEMANTICS.md`, code comments |
| **RISKS** | snapshot is not actually a tree today; this issue is mostly documentation + a small test that simulates Node 1 down and shows existing worker SSE streams keep flowing |
| **TEST** | kill the coordinator mid-stream, observe SSE continues, observe new `register` returns 503 |
| **ROLLBACK** | n/a |
| **STATUS** | **NOT_STARTED** |

---

## Issue 011 — Hierarchical resource summary

| field | value |
|---|---|
| **CURRENT** | The coordinator sees all GPUs; nothing aggregates |
| **TARGET** | Define `BranchResourceSummary { available_gpu_classes, model_families, approximate_free_vram, queue_pressure, spare_compute, health, timestamp }`. Each node periodically computes its summary and sends it up the lineage only when it changes by more than a threshold (or every N seconds as a floor) |
| **FILES LIKELY AFFECTED** | new `src/resources/summary.rs` |
| **RISKS** | over-aggressive threshold; under-aggressive threshold |
| **TEST** | unit test: a summary is sent when free_vram changes by >10% within 5 s, not sent otherwise |
| **ROLLBACK** | disable summary, fall back to current full-fleet view at the root |
| **STATUS** | **NOT_STARTED** |

---

## Issue 012 — Lineage affinity (configurable, not exponential)

| field | value |
|---|---|
| **CURRENT** | Affinity is role/gpu-class-based, not lineage-based |
| **TARGET** | Add a `lineage_distance: u8` to `NodeMetadata` (set on grant). Affinity score becomes `weight * lineage_bonus`, where `lineage_bonus = 2.0` for same lineage, `1.0` otherwise. **Do not compound by depth.** Make the weights configurable via env |
| **FILES LIKELY AFFECTED** | `src/scheduler/affinity.rs`, `src/scheduler/scoring.rs` |
| **RISKS** | compounding bug; cap must be tested |
| **TEST** | unit test: depth 1, depth 4, unrelated — bonus is capped |
| **ROLLBACK** | keep the current non-lineage affinity |
| **STATUS** | **NOT_STARTED** |

---

## Issue 013 — Contribution ledger

| field | value |
|---|---|
| **CURRENT** | Only per-API-key quotas, no per-node contribution |
| **TARGET** | New `ContributionLedger` persisted in SQLite. Records: `node_id, gpu_seconds, jobs_completed, jobs_failed, success_runtime_s, recent_activity, runtime_qualification`. Decayable by half-life. Surfaced as a separate `contribution_credit` field in scoring, kept **independent** of `lineage_affinity` and `reliability` |
| **FILES LIKELY AFFECTED** | new `src/contribution/mod.rs`, `src/contribution/store.rs`; `job_runner.rs` reports completion |
| **RISKS** | SQLite migrations; decay math |
| **TEST** | unit test: 1000 GPU-seconds with a 7-day half-life decay to a known value |
| **ROLLBACK** | feature flag |
| **STATUS** | **NOT_STARTED** |

---

## Issue 014 — Peer discovery: search locally, expand outward

| field | value |
|---|---|
| **CURRENT** | All requests are answered by the coordinator |
| **TARGET** | A node first looks at descendants, then ancestors, then branch, then federation. Forward a `DiscoveryRequirement { workload_class, model/role, min_vram, capabilities, expected_duration }` — **not** the full fleet state |
| **FILES LIKELY AFFECTED** | new `src/discovery/mod.rs` |
| **RISKS** | combinatorial search; cache to avoid hot loops |
| **TEST** | unit test: a 3-node tree with the right model on a grandchild is found without touching the root |
| **ROLLBACK** | always-federation search |
| **STATUS** | **NOT_STARTED** |

---

## Issue 015 — Direct execution path

| field | value |
|---|---|
| **CURRENT** | Inference traffic flows through the coordinator's `job_runner` |
| **TARGET** | Once a peer is chosen, the requesting node establishes a direct authenticated connection to it (HTTP/SSE today; QUIC later only if it provides a real, measured benefit). Document explicitly that Node 1 must not relay tokens, model files, photogrammetry, sonar, LiDAR, or large artifacts |
| **FILES LIKELY AFFECTED** | new `src/peers/route.rs`, `src/peers/direct.rs` |
| **RISKS** | direct path bypasses quota and auth — must be done carefully |
| **TEST** | integration test: two nodes, a request from A reaches Z via direct A→Z stream, root is not on the path |
| **ROLLBACK** | keep the current coordinator-relay path |
| **STATUS** | **NOT_STARTED** |

---

## Issue 016 — Route cache

| field | value |
|---|---|
| **CURRENT** | Every inference re-evaluates the node set |
| **TARGET** | Local `PeerRoute` cache with `{ peer_id, last_rtt, throughput_estimate, reliability, capabilities, model_ids, lineage_distance, last_success, expires_at }`. Latency is measured out-of-band of the lineage. Scoring uses latency and lineage as independent factors |
| **FILES LIKELY AFFECTED** | new `src/routes/cache.rs` |
| **RISKS** | stale cache |
| **TEST** | unit test: cache hit returns same peer until expiry; cache invalidation on failure |
| **ROLLBACK** | cache off |
| **STATUS** | **NOT_STARTED** |

---

## Issue 017 — Paddler integration

| field | value |
|---|---|
| **CURRENT** | No model placement authority; the coordinator just picks by role and lets the runtime figure it out |
| **TARGET** | New `PaddlerClient` with `select_model_for_role`, `is_model_loaded`, `placement_suggestion`. If Paddler is reachable, use it; otherwise degrade honestly. Never re-implement Paddler's role/model registry inside Nauti |
| **FILES LIKELY AFFECTED** | new `src/paddler/mod.rs`, `src/paddler/client.rs` |
| **RISKS** | Paddler not running locally in CI |
| **TEST** | unit test with a mock Paddler; integration test with `PADDLER_DISABLED=1` |
| **ROLLBACK** | current role-based selection |
| **STATUS** | **NOT_STARTED** |

---

## Issue 018 — Empty-GPU model assignment

| field | value |
|---|---|
| **CURRENT** | The coordinator never asks a worker to load a model |
| **TARGET** | Worker has `ensure_model_available(model_id)` on its `ModelRuntime`. If a node joins without a model and Paddler has a placement for it, the parent (or the empty node's coordinator) sends a `LoadModel` instruction. Node pulls or loads, reports health, becomes routable |
| **FILES LIKELY AFFECTED** | `src/runtime/*`, `src/lineage/instructions.rs` |
| **RISKS** | model download size; cancellation; partial download recovery |
| **TEST** | integration test: empty node + model assignment + first inference end-to-end |
| **ROLLBACK** | n/a (new feature) |
| **STATUS** | **NOT_STARTED** |

---

## Issue 019 — Tauri operator shell (optional, headless-first)

| field | value |
|---|---|
| **CURRENT** | A static `landing.html` served at `/` |
| **TARGET** | Optional Tauri desktop app: node enrollment, branch view, contribution enable/disable, GPU inventory, Forge availability, runtime state, loaded models, jobs, contribution credit, logs, start/stop. Headless Nauti must remain fully functional without it |
| **FILES LIKELY AFFECTED** | new `crates/nauti-nodes-tauri/` (separate workspace member), does not change the core crate |
| **RISKS** | Tauri build complexity |
| **TEST** | desktop app shows the same data as `GET /v1/nodes` and `GET /metrics` |
| **ROLLBACK** | separate crate, never blocks core |
| **STATUS** | **NOT_STARTED** |

---

## Issue 020 — wgpu / WebGPU capability layer (optional)

| field | value |
|---|---|
| **CURRENT** | No GPU capability introspection |
| **TARGET** | New `nauti-gpu` capability provider using `wgpu` where useful. Reports adapter/backend/limits/features. Not on the wire protocol — the protocol reports capabilities, not wgpu |
| **FILES LIKELY AFFECTED** | new `crates/nauti-gpu/` |
| **RISKS** | wgpu build on minimal hosts |
| **TEST** | unit test: capability report is serializable and stable |
| **ROLLBACK** | feature-gated dependency |
| **STATUS** | **NOT_STARTED** |

---

## Issue 021 — End-to-end receipts (Flow A / B / C)

| field | value |
|---|---|
| **CURRENT** | No end-to-end receipt for any flow |
| **TARGET** | Three receipts under `docs/nauti-nodes/issues/`: FLOW A (existing model), FLOW B (external branch), FLOW C (empty GPU + preferred runtime). Each shows: request → Nauti → discovery → Forge eligibility → runtime → real model → streamed tokens → completion |
| **FILES LIKELY AFFECTED** | docs only |
| **RISKS** | each flow may be blocked on a real runtime |
| **TEST** | n/a (evidence collection) |
| **ROLLBACK** | n/a |
| **STATUS** | **NOT_STARTED** |

---

## Issue 022 — Performance report

| field | value |
|---|---|
| **CURRENT** | No measurements |
| **TARGET** | `docs/nauti-nodes/PERFORMANCE_REPORT.md` with measured: Node 1 req/s, snapshot size, startup injection time, incremental event cost, branch summary bandwidth, discovery latency, direct-route latency, Nauti overhead per inference, route-cache hit rate, Forge query overhead |
| **FILES LIKELY AFFECTED** | docs only |
| **RISKS** | none |
| **TEST** | n/a |
| **ROLLBACK** | n/a |
| **STATUS** | **NOT_STARTED** |

---

## Issue 023 — crates.io publish (gated)

| field | value |
|---|---|
| **CURRENT** | Crate is local-only |
| **TARGET** | `cargo publish --dry-run` clean, then `cargo publish` after the user supplies a `CARGO_REGISTRY_TOKEN` and confirms |
| **FILES LIKELY AFFECTED** | none |
| **RISKS** | crate name collisions, license metadata, README rendering on crates.io |
| **TEST** | `cargo publish --dry-run` returns 0; `cargo package --list` shows expected files |
| **ROLLBACK** | do not publish; if already published, `cargo yank --vers 0.1.0` |
| **STATUS** | **NOT_STARTED** |

---

## Issue 024 — GitHub push (gated)

| field | value |
|---|---|
| **CURRENT** | Local-only |
| **TARGET** | Create `github.com/festeraeb/nauti-nodes` (public) and push the local commit. CI workflow not required for v0.1.0 |
| **FILES LIKELY AFFECTED** | remote only |
| **RISKS** | typo in remote name; pushing secrets (none should exist) |
| **TEST** | `git ls-remote` shows the commit |
| **ROLLBACK** | delete the remote repo (only possible in the first few minutes) or `git push --delete` |
| **STATUS** | **NOT_STARTED** |

---

## Stop