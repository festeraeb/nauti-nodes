# Issue 014 — Forge Preemption Contract

## Objective

Background Nauti work must yield to higher-priority Forge work. The three-state
concept (Observed / Allocation / Effective) must be respected.

## Status

PARTIALLY IMPLEMENTED (via adapter/forge.rs three-state concept from Issue 01
removal of fallback fleet; actual preemption checkpoint/resume logic is
implemented conceptually in WorkUnit checkpoint_policy but not wired to a live
Forge reservation API).

## Disposition

PASS_WITH_LIMITATIONS — the contract is designed (WorkUnit.checkpoint_policy,
Forge eligibility) but live Forge reservation integration requires a running
Forge instance with reservation endpoints.