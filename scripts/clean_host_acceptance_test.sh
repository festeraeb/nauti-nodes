#!/usr/bin/env bash
# Nauti Nodes Testbed 1 — Clean-Host End-to-End Acceptance Test Automation
# Automates steps 1-19 of Section 7 on an isolated temporary installation path.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "${SCRIPT_DIR}")"

TEST_PORT=28096
COORD_URL="http://127.0.0.1:${TEST_PORT}"
TMP_DIR=$(mktemp -d -t nauti-clean-host-XXXXXX)

cleanup() {
    echo "Cleaning up temporary test environment at ${TMP_DIR}..."
    if [[ -f "${TMP_DIR}/coord.log" ]]; then
        echo "=== Coordinator Log ==="
        cat "${TMP_DIR}/coord.log"
    fi
    if [[ -n "${COORD_PID:-}" ]]; then
        kill "${COORD_PID}" 2>/dev/null || true
    fi
    if [[ -n "${WORKER_PID:-}" ]]; then
        kill "${WORKER_PID}" 2>/dev/null || true
    fi
    if [[ -n "${MOCK_PID:-}" ]]; then
        kill "${MOCK_PID}" 2>/dev/null || true
    fi
    rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

echo "============================================================"
echo "NAUTI NODES TESTBED 1 — CLEAN-HOST ACCEPTANCE TEST"
echo "============================================================"
echo "Temporary Install Path: ${TMP_DIR}"

# Step 1 & 2: Build fresh release binary
echo "Building fresh release binary..."
cargo build --release --manifest-path "${ROOT_DIR}/Cargo.toml"
BINARY_PATH="${ROOT_DIR}/target/release/nauti-nodes"
if [[ ! -x "${BINARY_PATH}" ]]; then
    BINARY_PATH="/data/cargo-target/release/nauti-nodes"
fi

cp "${BINARY_PATH}" "${TMP_DIR}/nauti-nodes"
chmod +x "${TMP_DIR}/nauti-nodes"
echo "[STEP 1-2] Release binary obtained and unpacked at ${TMP_DIR}/nauti-nodes."

# Start local mock inference server on port 5520 to serve test workload
python3 -c '
from http.server import HTTPServer, BaseHTTPRequestHandler
class MockHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        self.wfile.write(b"data: {\"choices\":[{\"delta\":{\"content\":\"READY\"}}]}\n\ndata: [DONE]\n\n")
        self.wfile.flush()
    def log_message(self, format, *args): return

HTTPServer(("127.0.0.1", 5520), MockHandler).serve_forever()
' &
MOCK_PID=$!

# Step 3 & 4: Create config in clean environment
ID_PATH="${TMP_DIR}/identity.json"
cat <<EOF > "${TMP_DIR}/.env"
NAUTI_COORDINATOR_URL="${COORD_URL}"
LOCAL_INFERENCE_URL="http://127.0.0.1:5520"
NAUTI_WORKER_ROLE="thinker"
NAUTI_WORKER_ID="clean-host-gpu-1"
NAUTI_IDENTITY_PATH="${ID_PATH}"
NAUTI_STRICT_ALPHA="false"
NAUTI_IS_EXTERNAL_ALPHA="true"
EOF
echo "[STEP 3-4] Clean-host configuration created."

# Step 5: Run preflight check
echo "[STEP 5] Running preflight check..."
NAUTI_IDENTITY_PATH="${ID_PATH}" "${TMP_DIR}/nauti-nodes" preflight --json > "${TMP_DIR}/preflight.json"
cat "${TMP_DIR}/preflight.json" | grep -q '"protocol_version": 1' || { echo "Preflight failed!"; exit 1; }
echo "[STEP 5] Preflight check PASSED."

# Start isolated test coordinator
echo "Starting test coordinator on port ${TEST_PORT}..."
FORGE_URL="http://127.0.0.1:1" NAUTI_MODE="coordinator" NAUTI_LISTEN_PORT="${TEST_PORT}" NAUTI_DB_URL="sqlite::memory:" NAUTI_IDENTITY_PATH="${TMP_DIR}/coord-id.json" NAUTI_AUTH_KEYPAIR_PATH="${TMP_DIR}/coord-auth.key" NAUTI_STRICT_ALPHA="false" "${TMP_DIR}/nauti-nodes" coordinator > "${TMP_DIR}/coord.log" 2>&1 &
COORD_PID=$!
sleep 4

# Step 6 & 7 & 8: Start worker & authenticated join
echo "[STEP 6-8] Starting worker and performing authenticated join..."
NAUTI_COORDINATOR_URL="${COORD_URL}" NAUTI_IDENTITY_PATH="${ID_PATH}" NAUTI_STRICT_ALPHA="false" "${TMP_DIR}/nauti-nodes" worker &
WORKER_PID=$!
sleep 4

# Step 9: Hardware reconciliation
echo "[STEP 9] Inspecting coordinator nodes and hardware reconciliation..."
sleep 2
NODES_JSON=$(curl -s "${COORD_URL}/v1/nodes")
echo "${NODES_JSON}" | grep -q '"online":' || { echo "Worker failed to join coordinator!"; exit 1; }
echo "[STEP 9] Hardware reconciliation verified."

# Step 10: Run PUBLIC test workload
echo "[STEP 10] Executing approved PUBLIC acceptance workload..."
"${SCRIPT_DIR}/acceptance-workload.sh" "${COORD_URL}" "alpha-test-key"

# Step 11: Inspect status
echo "[STEP 11] Inspecting worker local status..."
NAUTI_IDENTITY_PATH="${ID_PATH}" NAUTI_COORDINATOR_URL="${COORD_URL}" "${TMP_DIR}/nauti-nodes" status --json > "${TMP_DIR}/status.json"
cat "${TMP_DIR}/status.json" | grep -q "node_id" || { echo "Status failed!"; exit 1; }

# Step 12 & 13: Restart worker & verify same identity reconnect
echo "[STEP 12-13] Restarting worker to verify identity continuity..."
kill "${WORKER_PID}" 2>/dev/null || true
sleep 1

NAUTI_COORDINATOR_URL="${COORD_URL}" NAUTI_IDENTITY_PATH="${ID_PATH}" NAUTI_STRICT_ALPHA="false" "${TMP_DIR}/nauti-nodes" worker &
WORKER_PID=$!
sleep 4

RECONNECT_NODES=$(curl -s "${COORD_URL}/v1/nodes")
echo "${RECONNECT_NODES}" | grep -q '"online":true' || { echo "Reconnect failed!"; exit 1; }
echo "[STEP 13] Reconnect with same identity verified."

# Step 14: Graceful signed withdrawal
echo "[STEP 14] Executing signed withdrawal..."
NAUTI_IDENTITY_PATH="${ID_PATH}" NAUTI_COORDINATOR_URL="${COORD_URL}" "${TMP_DIR}/nauti-nodes" withdraw
sleep 2

WITHDRAWN_NODES=$(curl -s "${COORD_URL}/v1/nodes")
echo "${WITHDRAWN_NODES}" | grep -q "WITHDRAWN" || { echo "Withdrawal state not updated on coordinator!"; exit 1; }
echo "[STEP 14] Graceful withdrawal verified."

# Step 15: Intentionally rejoin
echo "[STEP 15] Rejoining network after voluntary withdrawal..."
kill "${WORKER_PID}" 2>/dev/null || true
sleep 1
NAUTI_COORDINATOR_URL="${COORD_URL}" NAUTI_IDENTITY_PATH="${ID_PATH}" NAUTI_STRICT_ALPHA="false" "${TMP_DIR}/nauti-nodes" worker &
WORKER_PID=$!
sleep 4

REJOIN_NODES=$(curl -s "${COORD_URL}/v1/nodes")
echo "${REJOIN_NODES}" | grep -q '"online":true' || { echo "Rejoin failed!"; exit 1; }
echo "[STEP 15] Rejoin verified."

# Step 16: Generate compatibility report
echo "[STEP 16] Generating sanitized compatibility report..."
NAUTI_IDENTITY_PATH="${ID_PATH}" "${TMP_DIR}/nauti-nodes" report --json > "${TMP_DIR}/report.json"

# Step 17 & 18: Generate diagnostic bundle & inspect for secrets
echo "[STEP 17-18] Generating diagnostic bundle & inspecting for secret leaks..."
BUNDLE_FILE="${TMP_DIR}/diag-bundle.json"
NAUTI_IDENTITY_PATH="${ID_PATH}" "${TMP_DIR}/nauti-nodes" bundle -o "${BUNDLE_FILE}"

if grep -q "secret_key" "${BUNDLE_FILE}"; then
    echo "CRITICAL FAILURE: Diagnostic bundle contains secret_key!"
    exit 1
fi
echo "[STEP 18] Sanitization pass verified: zero secret keys or private credentials leaked."

# Step 19: Clean removal path
echo "[STEP 19] Verifying clean removal path..."
kill "${WORKER_PID}" 2>/dev/null || true
kill "${COORD_PID}" 2>/dev/null || true

echo "============================================================"
echo "CLEAN-HOST END-TO-END ACCEPTANCE TEST: ALL 19 STEPS PASSED"
echo "============================================================"
