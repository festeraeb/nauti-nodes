# Nauti Nodes Testbed 1 — External Alpha Volunteer Guide

Welcome to the Nauti Nodes Testbed 1 Limited External Alpha.

---

## 1. What is Nauti Nodes?

Nauti Nodes is a distributed inference control-plane designed to coordinate heterogeneous GPU compute nodes across physical hosts.

---

## 2. Testbed 1 Objectives

Testbed 1 validates:
- Authenticated volunteer node lifecycle (Ed25519 node identity)
- Dynamic model runtime management (`llama-server` process supervisor)
- Owner & Forge mission sovereignty (preemption & recovery)
- Public-only workload boundaries for external volunteer nodes

---

## 3. Core Volunteer Rights & Privacy

1. **You Own the Hardware:** Local operations and high-priority missions always take precedence over shared compute.
2. **Voluntary Withdrawal:** You can leave the network at any time using `nauti-nodes withdraw`.
3. **PUBLIC Workloads Only:** External volunteer nodes process **PUBLIC inference workloads ONLY**. No confidential or internal data is ever assigned to external nodes.
4. **No Private Key Transmission:** Your node's Ed25519 private key is generated locally and stored at `$HOME/.config/nauti/identity.json` (`0600` permissions). It is **NEVER transmitted**.

---

## 4. Volunteer CLI Quick Reference

### Check Readiness (Preflight)
```bash
nauti-nodes preflight
```

### Inspect Local Status
```bash
nauti-nodes status
```

### Start Worker
```bash
nauti-nodes worker
```

### Graceful Signed Withdrawal
```bash
nauti-nodes withdraw
```

### Rejoin Network
Simply restart the worker with the same identity:
```bash
nauti-nodes worker
```

### Generate Sanitized Compatibility Report
```bash
nauti-nodes report
```

### Generate Diagnostic Bundle
```bash
nauti-nodes bundle -o /tmp/my-diagnostic-bundle.json
```

---

## 5. Reporting Compatibility Results

If you encounter issues or want to share hardware compatibility findings:
1. Generate a diagnostic bundle: `nauti-nodes bundle`
2. Inspect the bundle JSON to verify no sensitive data is included.
3. Submit the bundle and feedback to the Nauti Testbed 1 team.
