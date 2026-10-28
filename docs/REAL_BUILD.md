# REAL_BUILD: nauti-nodes (what was faked, what real looks like)

Branch `cleanup/no-sim-2026-10-28`, from `main`.

## Faked parts and what they were meant to do

| Where | What it did | Meant to |
|---|---|---|
| `landing.html` live stats | When `/cluster/nodes` was unreachable (the normal case on a public page) it showed "1,240 GPUs, 85,000 tiles, 12 models, 4 missions, 142 wrecks" as if live. | Show live federation size. |
| `src/worker.rs`, `src/cli.rs` | With no GPU discovered and `strict_alpha` off, the worker registered and preflight reported `vram_mb = 8192`. `local_worker_meta` also hard-coded 8192. | Report the worker's real VRAM so the scheduler can place models. |

## What a real build needs

* Stats: the coordinator returns `count`, `tiles`, `models`, `missions`, `wrecks` from its registry and
  mission store; the page shows exactly those, and a dash when it cannot reach them. A figure
  may be claimed only if it is the registry's current count of workers that passed a signed heartbeat
  inside the liveness window.
* VRAM: from `all-smi` discovery (`crate::gpu::discover_gpus`). If nothing is discovered, 0 / refuse
  to register (strict mode, the default, already refuses).
* To claim scale ("works at 1000 nodes"): run it. `src/scheduler/scale_sim.rs` and
  `fairness.rs` are in-process scheduler simulators, useful for checking the algorithm, and say so in
  their headers; they are not measurements of a real federation and must not be cited as one.

## Removed or changed

* `landing.html`: invented fallback statistics replaced by dashes.
* `src/worker.rs` (two sites) and `src/cli.rs`: 8192 MiB invented VRAM replaced by 0.
* Kept: `scheduler/scale_sim.rs`, `scheduler/fairness.rs`, `lineage_scheduler::simulate_2to1_policy`
  (labelled simulators, only called from tests); `runtime/mod.rs` `OpenAICompatibleRuntime::infer/stream`
  (honest "not implemented" errors).
* `src/preemption.rs`: `vram_released_mb` no longer adds an assumed 8192 MiB per preempted job; it is
  0 until `PreemptibleJob` records its real allocation (read from the runtime / `all-smi` at start).
