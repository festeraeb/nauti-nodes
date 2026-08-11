# Issue 001 — Remove Fake Fallback Fleet

## Objective

Remove hard-coded `fallback_fleet()` nodes from production runtime state.
When Forge is unreachable or returns empty, the federation must report zero
nodes — not seven phantom GPUs.

## Original State

`adapter/forge.rs::fallback_fleet()` returned 5-7 hard-coded `NodeMetadata`
entries (RTX2060, GTX1070, LAN variants, M2200, P100-Coder, P100-Reviewer)
on every Forge failure path (network error, HTTP error, JSON error, empty
result). This populated the registry with fake nodes even when no real GPU
answered.

## Changes

- Removed `fallback_fleet()` from production code
- All 3 error paths in `fetch_fleet_from_forge` now return `Vec::new()`
- Empty Forge result also returns `Vec::new()` (was `fallback_fleet()`)
- `fleet_node()` helper preserved as `#[cfg(test)]` for future test fixtures
- Added 4 new tests: 3 unit tests for `inference_url_for`, 1 async regression
  test proving `unreachable_forge_returns_empty_fleet`

## Tests

| Test | Result |
|------|--------|
| `inference_url_for_local` | PASS |
| `inference_url_for_http_prefix` | PASS |
| `inference_url_for_hostname` | PASS |
| `unreachable_forge_returns_empty_fleet` | PASS |
| All 8 pre-existing tests | PASS (unchanged) |

## Remaining Limitations

- `fleet_node()` is `#[cfg(test)]` and cannot be used in production fixtures.
  If a future feature needs to load known nodes from config, that is a
  separate issue.
- The coordinator's `fleet::sync_fleet` still calls Forge on every inference
  request. When Forge is down, this adds a 5-second timeout per request.
  Future work should cache the last-known-good fleet state.

## Disposition

PASS_WITH_LIMITATIONS — fake fleet removed from production; timeout latency
on Forge-down path is documented but not addressed in this issue.