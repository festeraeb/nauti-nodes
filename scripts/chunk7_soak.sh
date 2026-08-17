#!/usr/bin/env bash
# Chunk 7 soak orchestration (Rust-first).
#
# Usage:
#   bash scripts/chunk7_soak.sh            # 5-minute CI smoke
#   NAUTI_SOAK_DURATION_SECS=3600 bash scripts/chunk7_soak.sh   # 1-hour local
#   NAUTI_SOAK_DURATION_SECS=28800 bash scripts/chunk7_soak.sh  # overnight
#
# Produces an invariant summary. Required:
#   ownership_conflicts = 0
#   accounting_inconsistencies = 0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "${SCRIPT_DIR}")"

DURATION="${NAUTI_SOAK_DURATION_SECS:-300}"
echo "============================================================"
echo "NAUTI NODES CHUNK 7 SOAK"
echo "Duration: ${DURATION}s"
echo "============================================================"

# Build the soak test binary once.
cargo test --manifest-path "${ROOT_DIR}/Cargo.toml" --test chunk7_soak --no-run 2>&1 | tail -2

# Run the soak with the configured duration.
NAUTI_SOAK_DURATION_SECS="${DURATION}" \
  cargo test --manifest-path "${ROOT_DIR}/Cargo.toml" --test chunk7_soak -- --nocapture 2>&1 \
  | tee /tmp/chunk7_soak.log

echo "============================================================"
echo "CHUNK 7 SOAK COMPLETE"
echo "============================================================"
grep -E '"ownership_conflicts"|"accounting_inconsistencies"' /tmp/chunk7_soak.log || true