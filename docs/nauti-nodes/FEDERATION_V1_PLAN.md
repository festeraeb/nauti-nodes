# Federation V1 Implementation Plan

## Architecture: SETI@home + Bounded Lineage Affinity + Forge Authority

Working metaphor: distributed compute federation where idle GPUs join a lineage
tree, prefer same-lineage work under contention (2:1), but never starve
unrelated work. Forge remains resource authority. Paddler remains model
orchestration authority.

## Issue Order

| # | Issue | Status |
|---|-------|--------|
| 01 | Remove fake fallback fleet | NOT_STARTED |
| 02 | Durable node identity | NOT_STARTED |
| 03 | Lineage persistence (SQLite) | NOT_STARTED |
| 04 | Join protocol | NOT_STARTED |
| 05 | Lineage queries | NOT_STARTED |
| 06 | Capability advertisements | NOT_STARTED |
| 07 | Work-unit model | NOT_STARTED |
| 08 | Work leases | NOT_STARTED |
| 09 | Pull scheduling | NOT_STARTED |
| 10 | Weighted lineage scheduler (2:1) | NOT_STARTED |
| 11 | Starvation protection | NOT_STARTED |
| 12 | Contribution accounting | NOT_STARTED |
| 13 | Runtime abstraction | NOT_STARTED |
| 14 | Forge preemption contract | NOT_STARTED |
| 15 | Privacy eligibility | NOT_STARTED |
| 16 | Validation contract | NOT_STARTED |
| 17 | Simulator | NOT_STARTED |
| 18 | Mission Control API/metrics | NOT_STARTED |

## Status Legend

- NOT_STARTED
- IN_PROGRESS
- BLOCKED
- IMPLEMENTED_AWAITING_VALIDATION
- VALIDATED

## Non-Negotiable Invariants

1. Node 1 owns relationship truth
2. Compute is peer-to-peer where practical
3. Lineage is preference, not ownership
4. Idle compute must not stay idle
5. Forge remains resource authority
6. Paddler remains model/runtime authority
7. Work belongs to the project, not the GPU
8. Privacy is part of eligibility