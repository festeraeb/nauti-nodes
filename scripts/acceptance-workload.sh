#!/usr/bin/env bash
# Nauti Nodes Testbed 1 — Approved PUBLIC Acceptance Workload Generator
# Executes a harmless, deterministic PUBLIC inference workload to verify end-to-end execution.

set -euo pipefail

COORDINATOR_URL="${1:-http://127.0.0.1:8099}"
API_KEY="${2:-alpha-test-key}"

echo "============================================================"
echo "NAUTI NODES TESTBED 1 — PUBLIC ACCEPTANCE WORKLOAD EXECUTION"
echo "============================================================"
echo "Target Coordinator: ${COORDINATOR_URL}"
echo "Privacy Class:      PUBLIC (PrivacyClass::Public)"
echo "Prompt:             Reply with one word: 'READY'."
echo "------------------------------------------------------------"

# Initialize quota for test API key
curl -s -X POST "${COORDINATOR_URL}/v1/quotas" \
  -H "Content-Type: application/json" \
  -d "{\"api_key\": \"${API_KEY}\", \"balance_tokens\": 1000000}" > /dev/null || true

RESPONSE=$(curl -s -X POST "${COORDINATOR_URL}/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: ${API_KEY}" \
  -d '{
    "model": "thinker",
    "messages": [
      {"role": "user", "content": "Reply with one word: '\''READY'\''."}
    ],
    "privacy_class": "Public",
    "avoid_p100": false,
    "max_tokens": 16,
    "temperature": 0.0
  }')

echo "Raw Response:"
echo "${RESPONSE}" | jq . || echo "${RESPONSE}"
echo "------------------------------------------------------------"

if echo "${RESPONSE}" | grep -qi "READY"; then
    echo "SUCCESS: PUBLIC acceptance workload executed and returned verified response."
    echo "Status: PASSED"
    exit 0
else
    echo "ERROR: Acceptance workload response did not contain expected verification token ('READY')."
    echo "Status: FAILED"
    exit 1
fi
