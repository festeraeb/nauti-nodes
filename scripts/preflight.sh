#!/usr/bin/env bash
# Nauti Nodes Testbed 1 — Volunteer Preflight Verification Script

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "${SCRIPT_DIR}")"

# Load local environment configuration if present
if [[ -f "${ROOT_DIR}/.env" ]]; then
    set -a
    source "${ROOT_DIR}/.env"
    set +a
fi

BINARY_PATH="${ROOT_DIR}/nauti-nodes"
if [[ ! -f "${BINARY_PATH}" ]]; then
    BINARY_PATH="${ROOT_DIR}/target/release/nauti-nodes"
fi

if [[ ! -x "${BINARY_PATH}" ]]; then
    echo "ERROR: nauti-nodes binary not found at ${BINARY_PATH}"
    exit 1
fi

exec "${BINARY_PATH}" preflight "$@"
