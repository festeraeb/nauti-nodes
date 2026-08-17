# Nauti Shared Compute — Release Candidate 1 (RC1)

**Release Version:** `0.1.0-alpha.1`  
**Target Architecture:** `x86_64-unknown-linux-gnu`  
**Testbed Scope:** Testbed 1 (Volunteer Node Lifecycle, Multi-GPU Sovereignty, Dynamic Pre-Staged Runtimes, Preemption)

---

## Overview

Nauti Shared Compute RC1 (`0.1.0-alpha.1`) is the first external volunteer release candidate of the Nauti distributed GPU inference network. RC1 enables heterogeneous external Linux volunteer nodes to join the Nauti compute network safely and securely while maintaining sovereign GPU hardware ownership via Forge preemption.

---

## Key Capabilities

1. **Persistent Cryptographic Identity**
   - Autonomous Ed25519 keypair generation stored with `0700` user permissions (`~/.config/nauti/identity.json`).
   - Reconnect continuity: Workers preserve identity across node restarts and re-registrations.
   - Nonce replay protection and timestamp verification on all control messages.

2. **Volunteer Lifecycle & Clean Withdrawal**
   - Automated registration and handshake via `nauti-nodes worker`.
   - Voluntary signed withdrawal (`nauti-nodes worker withdraw`) transitioning node to `WITHDRAWN` state on coordinator.
   - Immediate voluntary rejoin support without identity corruption.

3. **Multi-GPU Sovereign Preemption**
   - Sovereign mission priority enforcement: Forge can reclaim any assigned GPU at any time.
   - Atomic job checkpointing: In-flight shared compute inference jobs are checkpointed when Forge reserves hardware.
   - Automatic recovery: When Forge releases reservation, shared compute state is restored and checkpointed jobs resume.

4. **Public Workload Boundary Enforcement**
   - External alpha nodes process exclusively `PUBLIC` workloads (`PrivacyClass::Public`).
   - Internal mission telemetry and private tasks remain on sovereign infrastructure.

5. **Sanitization & Operator Diagnostics**
   - Diagnostic bundle generator (`nauti-nodes worker bundle`) redacting all secret keys (`X-Admin-Key`, Ed25519 private keys).
   - Compatibility reports (`nauti-nodes worker report`) for volunteer environment validation.

6. **Admin Endpoint Authentication Boundary**
   - Administrative endpoints (`/v1/admin/*`, `/v1/forge/*`) protected via `X-Admin-Key` or `Authorization: Bearer <key>`.
   - Emergency kill switch (`/v1/admin/disable-external-alpha`) instantly blocking new volunteer joins.

---

## Tested Hardware Baseline

- **NVIDIA Tesla P100 (16GB VRAM)**
- **NVIDIA GeForce RTX 2060 (6GB VRAM)**
- **NVIDIA GeForce GTX 1070 (8GB VRAM)**
- **Host Drivers:** NVIDIA 535.xx / CUDA 12.x
- **Host OS:** Linux x86_64 (Ubuntu 22.04 LTS / Debian 12 / Arch Linux)

---

## Known Limitations (Testbed 1 Alpha Scope)

- **Federation / Replicated State:** Replicated state algorithms (Raft, multi-coordinator consensus) are out of scope for Testbed 1. Single coordinator deployment per cluster.
- **Model Downloads:** Automatic arbitrary HuggingFace model downloading is deferred. Models must be pre-staged on volunteer host disks.
- **GUI Installer:** CLI-only operation in RC1.

---

## Operator Quickstart

```bash
# Extract Release Bundle
tar -xzf nauti-testbed1-rc1-linux-x86_64.tar.gz -C /tmp/nauti-rc1

# Run Preflight & Bootstrap
/tmp/nauti-rc1/scripts/preflight.sh
/tmp/nauti-rc1/scripts/bootstrap.sh

# Edit Configuration
nano ~/.config/nauti/.env

# Start Volunteer Worker
/tmp/nauti-rc1/nauti-nodes worker
```
