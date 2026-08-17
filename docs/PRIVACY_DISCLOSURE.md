# Nauti Nodes Testbed 1 — External Alpha Privacy Disclosure

**Effective Date:** August 2026  
**Target System:** Nauti Shared Compute Limited Alpha — Testbed 1  

This document explicitly defines what data is transmitted from a volunteer compute node to the Nauti federation coordinator.

---

## 1. Core Sovereignty & Privacy Boundary

1. **Hardware Owner Sovereignty:** The volunteer node operator retains complete sovereignty over their physical GPU hardware.
2. **Subordinate Workloads:** All shared compute workloads assigned by Nauti are subordinate to local or Forge priority missions.
3. **PUBLIC Workload Restriction:** External alpha volunteer nodes receive **PUBLIC workloads ONLY** (`PrivacyClass::Public`). Confidential, internal, or sensitive data workloads are NEVER routed to external volunteer nodes.
4. **Local Private Key Retention:** The node's Ed25519 private key is generated locally, stored on disk with `0600` permissions (`identity.json`), and **NEVER transmitted** over the network.

---

## 2. Information Transmitted to Coordinator

The following telemetry and state information is transmitted to the coordinator during join, heartbeat, and job execution:

| Data Category | Transmitted Fields | Purpose |
| :--- | :--- | :--- |
| **Node Identity** | `node_id` (UUID), `public_key_hex` (Ed25519 256-bit public key) | Cryptographic node authentication and replay protection |
| **Network Metadata** | Host IP address, configured `inference_url` | Routing public inference requests to the worker node |
| **Hardware Specs** | GPU Model Name, Total VRAM (MiB), Vendor Name | Hardware discovery, role placement, and reconciliation |
| **Telemetry & Health**| Free VRAM (MiB), active job count, tokens/sec throughput | Scheduler load balancing and capacity tracking |
| **Model Availability**| Loaded model ID (e.g., `Gemma-4-E2B-it-GGUF`), runtime status | Model readiness and prompt affinity scheduling |
| **Authentication** | Request Unix timestamp, random nonce, SHA-512 Ed25519 signature | Anti-replay verification (`SignedJoinRequest` / `SignedHeartbeat`) |

---

## 3. Information NEVER Transmitted or Collected

The Nauti Nodes worker software **DOES NOT** collect or transmit:
- Ed25519 private keys or secret key bytes
- Host user files, personal documents, or home directory contents
- Environment variables or system credentials
- User browser history, shell history, or personal identifiers
- Raw prompt or completion data from private workloads (external nodes process PUBLIC jobs only)

---

## 4. Volunteer Control & Withdrawal

- **Voluntary Withdrawal:** A volunteer can withdraw their node at any time by running `nauti-nodes withdraw`. Upon signed withdrawal, the node immediately transitions to state `WITHDRAWN` and receives no further work.
- **Identity Control:** Deleting `identity.json` revokes the local cryptographic identity and causes the worker to generate a brand new network identity upon next startup.
