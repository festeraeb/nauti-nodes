# MASTER IMPLEMENTATION PLAN

**Build Pass:** INKLING Handoff — Forge Cognitive Lab + Nauti Nodes
**Date:** 2026-08-12
**Status:** ACTIVE — ALL 23 ISSUES COMPLETE (skeleton)

---

## Repositories

| Repository | Purpose | Status | Tests |
|---|---|---|---|
| `nauti-nodes` | Federation + distributed workloads | **Expanded** | 115 passing |
| `forge-cognitive-lab` | Experiments + registry evidence | **Created** (69 tests, 5 commits) | 69 passing |
| `nauti-inferer` | Inference routing | Stable | — |
| `wreckhunter2000-1` | Monorepo | Dirty (unchanged) | — |

---

## Issue Status — COMPLETE

| Issue | Title | Location | Tests |
|---|---|---|---|
| 001 | Baseline | nauti-nodes/docs | — |
| 002 | Cognitive Lab repository | forge-cognitive-lab | — |
| 003 | Model identity schema | forge-cognitive-lab | 7 |
| 004 | Capability observations | forge-cognitive-lab | 4 |
| 005 | Failure observations | forge-cognitive-lab | 5 |
| 006 | Pair/complementarity observations | forge-cognitive-lab | 6 |
| 007 | Team observations | forge-cognitive-lab | 5 |
| 008 | Experiment receipt | forge-cognitive-lab | 4 |
| 009 | Registry API + SQLite store | forge-cognitive-lab | 6 |
| 010 | Forge registry adapter | forge-cognitive-lab | 5 |
| 011 | Hardware discovery adapter | forge-cognitive-lab | 10 |
| 012 | Hardware transfer/locality metrics | forge-cognitive-lab | (in 011) |
| 013 | Nauti authenticated join | nauti-nodes | 8 |
| 014 | Peer discovery | nauti-nodes | 9 |
| 015 | Pull scheduling | nauti-nodes | 9 |
| 016 | 2:1 fairness simulator | nauti-nodes | 10 |
| 017 | Scale simulator | nauti-nodes | 10 |
| 018 | Forge preemption | nauti-nodes | 8 |
| 019 | Lemonade adapter | nauti-nodes | 3 |
| 020 | NIM adapter | nauti-nodes | 3 |
| 021 | Research-memory integration | forge-cognitive-lab | 3 |
| 022 | Idea Foundry sandbox | forge-cognitive-lab | 8 |
| 023 | Mission Control APIs | forge-cognitive-lab | 6 |

---

## Final Disposition

**PARTIAL_IMPLEMENTATION_READY_FOR_VALIDATION**

The complete skeleton is in place, but several pieces are still prototype-level:

### What is solid (test-covered, deterministic)
- All observation schemas (ModelIdentity, Capability, Failure, Pair, Team, Receipt)
- SQLite persistence layer
- Ed25519 authentication protocol
- Peer registry
- Pull scheduler with leases
- 2:1 fairness simulator
- Scale simulator (1/10/100/1000 nodes)
- Preemption manager
- Lemonade + NIM runtime adapters
- Research pipeline + sandbox contracts
- Mission Control observability

### What needs completion (stubs / prototype)
- `ForgeRegistry.best_specialists_for_role()` etc. return empty Vec — need to implement the actual query logic over the store
- Authenticated join isn't wired to live HTTP handlers yet
- Peer discovery isn't networked to real peers yet
- Preemption manager is in-memory only, not wired to actual GPU release
- Lemonade/NIM adapters are implemented but not integration-tested against live servers
- AMD compute not yet used for model experiments

---

## Architecture Boundaries (preserved)

```
forge-cognitive-lab     experiments + registry evidence ✅
forge (wreckhunter)     planning/router/team selection (not touched)
forge-fleet             hardware authority (not touched)
paddler                 model/runtime orchestration (not touched)
nauti-nodes             federation + distributed workloads ✅ expanded
mission-control         operator UI (API structs added, no UI)
```

## Next Five Recommended Issues

1. **024**: Implement `ForgeRegistry` query logic (best_specialists, best_reviewer) over the SQLite store
2. **025**: Wire authenticated join into live HTTP handlers
3. **026**: Network peer advertisement/discovery using HTTP
4. **027**: Integration-test Lemonade + NIM adapters against real servers using AMD compute
5. **028**: Run first real model role experiments and record receipts

## Evidence Log

| Date | Event | Evidence |
|---|---|---|
| 2026-08-12 04:54 | Baseline recorded | `00_BASELINE.md` |
| 2026-08-12 05:05 | forge-cognitive-lab created | commit `9e4bd3a`, 37 tests |
| 2026-08-12 05:05 | nauti-nodes verified | commit `7c009ab`, 55 tests |
| 2026-08-12 05:07 | Forge registry adapter | forge `e46fbc6` |
| 2026-08-12 05:09 | Hardware adapter | forge `4f36841` |
| 2026-08-12 05:12 | Authenticated join | nauti `e25b9c7` |
| 2026-08-12 05:14 | Peer discovery | nauti `2850b72` |
| 2026-08-12 05:17 | Pull scheduling | nauti `045ca65` |
| 2026-08-12 05:21 | Fairness simulator | nauti `5497779` |
| 2026-08-12 05:24 | Scale simulator | nauti `439da0b` |
| 2026-08-12 12:25 | Lemonade adapter | nauti `f5644e2` |
| 2026-08-12 12:27 | NIM adapter | nauti `17a8369` |
| 2026-08-12 12:30 | Preemption | nauti `111d342` |
| 2026-08-12 12:34 | Research + sandbox | forge `28ff770` |
| 2026-08-12 13:14 | Mission Control | forge `a0d3086` |