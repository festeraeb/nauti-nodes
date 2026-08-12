# MASTER IMPLEMENTATION PLAN

**Build Pass:** INKLING Handoff — Forge Cognitive Lab + Nauti Nodes
**Date:** 2026-08-12
**Status:** ACTIVE

---

## Repositories

| Repository | Purpose | Status |
|---|---|---|
| `nauti-nodes` | Federation + distributed workloads | Active (55 tests, commit `7c009ab`) |
| `forge-cognitive-lab` | Experiments + registry evidence | **Created** (37 tests, commit `9e4bd3a`) |
| `nauti-inferer` | Inference routing | Stable (commit `e235023`) |
| `wreckhunter2000-1` | Monorepo (Forge, Paddler, etc.) | Dirty (3704 uncommitted files) |

---

## Issue Sequence

### Phase 1: Foundation (Issues 001–009)

| Issue | Title | Status | Commit | Tests |
|---|---|---|---|---|
| 001 | Baseline | ✅ DONE | — | — |
| 002 | Cognitive Lab repository | ✅ DONE | `9e4bd3a` | — |
| 003 | Model identity schema | ✅ DONE | `9e4bd3a` | 7 |
| 004 | Capability observations | ✅ DONE | `9e4bd3a` | 4 |
| 005 | Failure observations | ✅ DONE | `9e4bd3a` | 5 |
| 006 | Pair/complementarity observations | ✅ DONE | `9e4bd3a` | 6 |
| 007 | Team observations | ✅ DONE | `9e4bd3a` | 5 |
| 008 | Experiment receipt | ✅ DONE | `9e4bd3a` | 4 |
| 009 | Registry API + SQLite store | ✅ DONE | `9e4bd3a` | 6 |

**Phase 1 total: 37 tests passing in forge-cognitive-lab**

### Phase 2: Integration (Issues 010–012)

| Issue | Title | Status | Notes |
|---|---|---|---|
| 010 | Forge registry adapter | 🔲 TODO | Bridge forge-cognitive-lab → nauti-nodes/forge |
| 011 | Hardware discovery adapter | 🔲 TODO | Evaluate all-smi, wgpu; create CESARops adapter contract |
| 012 | Hardware transfer/locality metrics | 🔲 TODO | host_to_device_gbps, model_cold_load_ms, etc. |

### Phase 3: Nauti Networking (Issues 013–017)

| Issue | Title | Status | Notes |
|---|---|---|---|
| 013 | Nauti authenticated join | 🔲 TODO | Ed25519 protocol auth, anti-replay |
| 014 | Peer discovery | 🔲 TODO | Peer advertisement, direct-peer-capable execution |
| 015 | Pull scheduling | 🔲 TODO | "I have compute, give me eligible work" |
| 016 | 2:1 fairness correction/validation | 🔲 TODO | Deterministic simulations at 30/300/3000/30000 |
| 017 | Scale simulator | 🔲 TODO | 1/10/100/1000 node synthetic federation tests |

### Phase 4: Runtime & Preemption (Issues 018–020)

| Issue | Title | Status | Notes |
|---|---|---|---|
| 018 | Live Forge preemption | 🔲 TODO | Checkpoint → release → resume |
| 019 | Lemonade adapter | 🔲 TODO | AMD runtime adapter |
| 020 | NIM adapter | 🔲 TODO | NVIDIA NIM runtime adapter |

### Phase 5: Research & Sandbox (Issues 021–023)

| Issue | Title | Status | Notes |
|---|---|---|---|
| 021 | Research-memory integration | 🔲 TODO | SearXNG → Qdrant → researcher pipeline |
| 022 | Idea Foundry sandbox contract | 🔲 TODO | Safe sandbox architecture |
| 023 | Observability/Mission Control APIs | 🔲 TODO | Structured state for Mission Control |

---

## Architecture Boundaries

```
forge-cognitive-lab     experiments + registry evidence
forge (wreckhunter)     planning/router/team selection
forge-fleet             hardware authority
paddler                 model/runtime orchestration
nauti-nodes             federation + distributed workloads
mission-control         operator UI
```

## Hard Rules

1. **Forge distributes cognition. Nauti distributes workloads.**
2. Distributed tensor/model-parallel inference is NOT required.
3. A specialist model runs conventionally on one runtime/node.
4. Never allow one opaque score to override hard constraints.
5. A model may NOT validate its own work.
6. Raw evidence must survive even if scoring formulas change.

---

## Next Issue: 010 — Forge Registry Adapter

Build a Rust API/library that Forge can consume. Questions Forge should ask:
- Best specialists for role X?
- Best reviewer for model Y?
- Models with low shared-failure rate?
- Models validated for Rust?
- Confidence in this capability measurement?

---

## Evidence Log

| Date | Event | Evidence |
|---|---|---|
| 2026-08-12 04:54 | Baseline recorded | `docs/full-system/00_BASELINE.md` |
| 2026-08-12 05:05 | forge-cognitive-lab created | commit `9e4bd3a`, 37 tests |
| 2026-08-12 05:05 | nauti-nodes verified | commit `7c009ab`, 55 tests |