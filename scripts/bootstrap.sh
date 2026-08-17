#!/usr/bin/env bash
# Nauti Nodes Testbed 1 — Volunteer Bootstrap Script
# Unpacks release package, verifies permissions, and prepares identity directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "${SCRIPT_DIR}")"

echo "============================================================"
echo "NAUTI NODES TESTBED 1 — VOLUNTEER BOOTSTRAP INITIALIZATION"
echo "============================================================"

# 1. Verify binary executable
BINARY_PATH="${ROOT_DIR}/nauti-nodes"
if [[ ! -f "${BINARY_PATH}" ]]; then
    BINARY_PATH="${ROOT_DIR}/target/release/nauti-nodes"
fi

if [[ ! -x "${BINARY_PATH}" ]]; then
    echo "ERROR: nauti-nodes binary not found or not executable at ${BINARY_PATH}"
    exit 1
fi

# 2. Print version info
echo "Binary location: ${BINARY_PATH}"
"${BINARY_PATH}" --version

# 3. Create identity directory with 0700 permissions
ID_DIR="${HOME}/.config/nauti"
mkdir -p "${ID_DIR}"
chmod 0700 "${ID_DIR}"
echo "Identity storage: ${ID_DIR} (permissions 0700 verified)"

# 4. Copy example environment configuration if not present
ENV_DEST="${ROOT_DIR}/.env"
if [[ ! -f "${ENV_DEST}" ]] && [[ -f "${ROOT_DIR}/example.env" ]]; then
    cp "${ROOT_DIR}/example.env" "${ENV_DEST}"
    echo "Created initial .env configuration template from example.env"
fi

echo "------------------------------------------------------------"
echo "Bootstrap complete."
echo "Next step: Edit .env (set NAUTI_COORDINATOR_URL) and run:"
echo "  ./scripts/preflight.sh"
echo "============================================================"
