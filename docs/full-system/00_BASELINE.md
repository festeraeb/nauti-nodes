# 00 — RECOVERY BASELINE

**Date:** 2026-08-12T04:54:48Z
**Build Pass:** INKLING Handoff — Forge Cognitive Lab + Nauti Nodes

---

## Repositories

| Repository | Branch | HEAD | Dirty Files | Notes |
|---|---|---|---|---|
| `/codebase/repos/nauti-nodes` | main | `7c009ab2dca5f4ddde640310634646805228d5b2` | 0 | Clean. 55 tests passing. |
| `/codebase/repos/nauti-inferer` | main | `e235023e423d99e8d16825ab346aec846698be69` | 0 | Clean. |
| `/codebase/repos/wreckhunter2000-1` | master | `5056dcafea4524a84bd9768f8e29564ece122379` | 3704 | Very dirty working tree. |
| `/codebase/repos/forge-cognitive-lab` | — | — | — | Does not exist yet. To be created. |

---

## Nauti Nodes — Verified State

- **Commit:** `7c009ab` (main)
- **Tests:** 55 passed, 0 failed, 0 ignored
- **Test duration:** 0.27s
- **Edition:** Rust 2021
- **Key dependencies:** dashmap, tokio, rusqlite, ed25519-dalek, axum, reqwest, uuid, chrono
- **Source files:** 37 `.rs` files across 10 modules

### Module Map

```
src/
├── adapter/          # forge.rs, llama.rs, mod.rs — runtime adapters
├── api/              # handlers.rs, routes.rs, mod.rs — HTTP API
├── capabilities.rs   # node capability advertisement
├── config.rs         # configuration
├── contribution/     # contribution accounting
├── coordinator.rs    # coordination logic
├── fleet.rs          # fleet management
├── gpu/              # GPU discovery
├── job_runner.rs     # job execution
├── jobs.rs           # job definitions
├── lib.rs            # library root
├── lineage/          # mod.rs, store.rs — SQLite lineage
├── main.rs           # binary entry
├── metrics/          # metrics/telemetry
├── node/             # heartbeat.rs, identity.rs, manager.rs, registry.rs
├── runtime/          # ModelRuntime abstraction
├── scheduler/        # affinity.rs, lineage_scheduler.rs, quotas.rs, scoring.rs
├── state.rs          # application state
├── types/            # errors.rs, mod.rs
├── work/             # mod.rs, unit.rs — WorkUnit lifecycle
└── worker.rs         # worker logic
```

### Handoff Claim vs Reality

| Claim | Reality |
|---|---|
| 49 passing tests | **55 passing tests** (6 more than claimed) |
| Commit `ba584d4` | **Commit `7c009ab`** (newer) |
| Fake fallback fleet removed | Not verified yet |
| Durable UUID + Ed25519 identity | Present (`ed25519-dalek` in deps, `node/identity.rs` exists) |
| SQLite lineage store | Present (`rusqlite` in deps, `lineage/store.rs` exists) |
| WorkUnit lifecycle | Present (`work/unit.rs` exists) |
| ModelRuntime abstraction | Present (`runtime/mod.rs` exists) |
| OpenAI-compatible adapter | Present (`adapter/llama.rs` exists) |

---

## Running Services

| Port | Process | PID | Role |
|---|---|---|---|
| 3000 | cesarops-missio | 1895418 | Mission Control |
| 4190 | (unknown) | — | — |
| 4330 | (unknown) | — | — |
| 5003 | nautivecs-cli | 1947 | NautiVecs CLI |
| 5010 | cesarops-wso-se | 1943 | Web Search Oracle |
| 5210 | llama-server | 646569 | Inference runtime |
| 5678 | (unknown) | — | — |
| 7700 | (unknown) | — | — |
| 8081 | nauti-inferer | 907476 | Nauti Inferer |
| 8088 | cesarops-pingor | 437165 | Ping/health |
| 8099 | python (×5) | 2679443+ | Paddler (likely) |
| 8101 | cesarops-wrecks | 2145 | Wreckhunter |
| 19999 | (localhost) | — | — |
| 20241 | cloudflared | 13439 | Tunnel |
| 20242 | cloudflared | 13575 | Tunnel |

---

## GPU State

| Index | Name | Total VRAM | Used | Free |
|---|---|---|---|---|
| 0 | Tesla P100-PCIE-16GB | 16384 MiB | 3 MiB | 16268 MiB |
| 1 | Tesla P100-PCIE-16GB | 16384 MiB | 3 MiB | 16268 MiB |

Both GPUs effectively idle. Total available VRAM: ~32.5 GiB.

---

## Models on Disk

| Path | Size | Notes |
|---|---|---|
| `/data/cesarops/models/Qwen2.5-14B-Instruct-Q3_K_M.gguf` | — | Existing |
| `/data/cesarops/models/Qwen_Qwen3-Coder-Next-IQ4_XS.gguf` | — | Existing |
| `/data/cesarops/models/Qwen3-72B-Instruct.Q4_K_M.gguf` | 45 GiB | Downloaded this session |

---

## Known Incomplete Areas (from handoff)

- Real federation network/join
- Direct peer execution
- Pull scheduling
- Live Forge reservation/preemption integration
- Lemonade adapter
- NIM adapter
- Large simulator validation
- Authenticated federation protocol
- Streaming through ModelRuntime

---

## Next Steps

1. Create `forge-cognitive-lab` repository
2. Write `MASTER_IMPLEMENTATION_PLAN.md`
3. Begin issue-by-issue implementation