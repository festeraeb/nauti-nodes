# Nauti Nodes Testbed 1 — Internal Operator Runbook

This document details administrative controls and operational procedures for managing the Nauti Testbed 1 coordinator and external alpha nodes.

---

## 1. Coordinator Startup

Start the coordinator in strict alpha mode:

```bash
export NAUTI_MODE="coordinator"
export NAUTI_LISTEN_PORT=8095
export NAUTI_DB_URL="sqlite:nauti.db"
export NAUTI_STRICT_ALPHA="true"
export NAUTI_EXTERNAL_ALPHA_ENABLED="true"

./target/release/nauti-nodes
```

---

## 2. Operator Kill Switch (Emergency Stop External Alpha)

The operator kill switch halts new external worker joins and stops scheduling work to external nodes without requiring a coordinator recompile.

### Engage Kill Switch via HTTP API
```bash
curl -X POST http://127.0.0.1:8095/v1/admin/alpha/disable
```

### Disengage Kill Switch (Re-enable External Alpha)
```bash
curl -X POST http://127.0.0.1:8095/v1/admin/alpha/enable
```

### Check Kill Switch Status
```bash
curl -s http://127.0.0.1:8095/v1/admin/alpha/status | jq .
```

### Engage via Environment Variable at Coordinator Startup
```bash
export NAUTI_KILL_SWITCH="1"
```

---

## 3. Node Revocation & Unrevocation

Revoking a node ID or public key permanently bars it from joining, heartbeating, or receiving work (`403 Forbidden`).

### Revoke a Malicious or Compromised Node
```bash
curl -X POST http://127.0.0.1:8095/v1/admin/revoke \
  -H "Content-Type: application/json" \
  -d '{"identifier": "bad-node-id-or-pubkey", "reason": "compromised identity"}'
```

### Unrevoke a Node
```bash
curl -X POST http://127.0.0.1:8095/v1/admin/unrevoke \
  -H "Content-Type: application/json" \
  -d '{"identifier": "bad-node-id-or-pubkey"}'
```

### List Active Revocations
```bash
curl -s http://127.0.0.1:8095/v1/admin/revocations | jq .
```

---

## 4. Hardware Reconciliation & Fleet Inspection

### Inspect Active Nodes
```bash
curl -s http://127.0.0.1:8095/v1/nodes | jq .
```

### Inspect Hardware Reconciliation Details
```bash
curl -s http://127.0.0.1:8095/v1/admin/reconciliation | jq .
```

---

## 5. Forge Preemption & Lease Controls

### Reserve a GPU for High-Priority Mission
```bash
curl -X POST http://127.0.0.1:8095/v1/forge/reserve \
  -H "Content-Type: application/json" \
  -d '{
    "mission_id": "mission-001",
    "node_id": "target-node-id",
    "vram_mb": 16384,
    "priority": 100,
    "allow_preemption": true
  }'
```

### Release GPU Reservation (Restore Shared Compute)
```bash
curl -X POST http://127.0.0.1:8095/v1/forge/release \
  -H "Content-Type: application/json" \
  -d '{
    "mission_id": "mission-001",
    "node_id": "target-node-id"
  }'
```

### List Active Forge Leases
```bash
curl -s http://127.0.0.1:8095/v1/forge/reservations | jq .
```
