# Nauti Nodes

> Distributed GPU node federation for inference workloads.

Nauti Nodes is the worker/coordinator control plane that lets a fleet of
heterogeneous GPU machines (an RTX 2060, a GTX 1070, a P100, an M2200, a
remote worker on the T440, …) advertise themselves, heartbeat, receive
work, and stream OpenAI-compatible inference back to a client.

This crate is the public, standalone packaging of the previously
internal `nauti-inferer` module that lived inside the
`wreckhunter2000-1` monorepo at `backend/nauti-inferer/`. The first
public release is a faithful **snapshot** of that code, renamed for
clarity. See `docs/nauti-nodes/00_CURRENT_REALITY.md` and
`docs/nauti-nodes/01_EVOLUTION_PLAN.md` for the archaeology, current
behaviour, and the issue-by-issue plan to evolve it into a true
federation root.

## What it does today (v0.1.0)

- **Coordinator mode** (default): runs an Axum HTTP server that
  - exposes `/health`, `/metrics`, `/v1/inference`,
    `/v1/inference/cancel/:job_id`, `/v1/models`, `/v1/nodes`,
    `/internal/worker/register`, `/internal/worker/heartbeat`,
    `/internal/quotas/set`, `/internal/fleet/sync`
  - pulls a GPU list from the configured Forge Fleet endpoint
    (`<FORGE_URL>/cluster/gpus`) on every inference and on
    `/internal/fleet/sync`, and falls back to a hard-coded list when
    Forge is unreachable
  - picks a node using an affinity-aware score (role + GPU class
    + scoring algorithm), runs a job, streams tokens back as
    Server-Sent Events, or returns a single JSON completion
  - charges a per-key token ledger (`scheduler::quotas`)
- **Worker mode** (`NAUTI_MODE=worker`): registers itself with a
  coordinator via `POST /internal/worker/register`, then
  heartbeats every 10 s with synthetic `{free_vram_mb, tokens_per_sec}`
  derived from a `/health` probe of the local llama-server
- **OpenAI-compatible streaming** out of the box (SSE), with job
  cancellation by `job_id`
- **No GPU telemetry, no real identity proof, no signed tree
  snapshot yet** — those are tracked in the evolution plan

## Quick start

```bash
# build
cargo build --release

# run the coordinator
RUST_LOG=info NAUTI_LISTEN_PORT=8081 \
    FORGE_URL=http://127.0.0.1:9100 \
    ./target/release/nauti-nodes

# run a worker on a remote GPU box
RUST_LOG=info NAUTI_MODE=worker \
    NAUTI_COORDINATOR_URL=http://cesarops3:8081 \
    NAUTI_WORKER_ID=RTX2060 \
    NAUTI_WORKER_ROLE=thinker \
    NAUTI_WORKER_GPU="RTX 2060 SUPER" \
    LOCAL_INFERENCE_URL=http://127.0.0.1:5200 \
    ./target/release/nauti-nodes

# send a request
curl -s http://localhost:8081/health
curl -s -X POST http://localhost:8081/v1/chat/completions \
    -H 'Content-Type: application/json' \
    -H 'X-API-Key: testkey' \
    -d '{"model":"thinker","messages":[{"role":"user","content":"hi"}]}'
```

## Crate metadata

| field        | value |
|--------------|-------|
| name         | `nauti-nodes` |
| version      | `0.1.0` |
| edition      | 2021 |
| license      | AGPL-3.0-or-later |
| repository   | https://github.com/festeraeb/nauti-nodes |
| lib name     | `nauti_nodes` |
| bin name     | `nauti-nodes` |
| description  | Nauti Nodes: distributed GPU node federation for inference workloads. Worker registration, heartbeat, affinity-aware routing, OpenAI-compatible streaming, and Forge Fleet / model runtime adapter integration. |

## License

GNU Affero General Public License v3.0 or later. See `LICENSE`.
