# Nauti Nodes Testbed 1 — Installation & Build Guide

This guide describes how to build, configure, and run a Nauti Nodes volunteer worker on Linux.

---

## 1. Prerequisites

### System Requirements
- **OS:** Linux (Ubuntu 22.04 / Debian 12 / RHEL 9 recommended)
- **Architecture:** x86_64 or aarch64
- **Toolchain:** Rust 1.75+ (`cargo`, `rustc`)
- **GPU Hardware:** NVIDIA GPU (CUDA driver 525+) or AMD Radeon/Instinct GPU (ROCm 5.7+ / Vulkan)

### Required Dependencies
1. `all-smi` or `nvidia-smi` / `rocm-smi` (for hardware telemetry & discovery)
2. `llama-server` (llama.cpp server binary) or pre-installed inference runtime
3. Pre-staged model GGUF artifact (e.g. `Qwen2.5-Coder-1.5B-Instruct-Q8_0.gguf` or `Gemma-4-E2B-it-GGUF`)

---

## 2. Build Instructions

Clone the repository and build using Cargo:

```bash
cd /codebase/repos/nauti-nodes
cargo build --release
```

The compiled binary will be located at `target/release/nauti-nodes`.

---

## 3. Preflight Readiness Verification

Before running a worker node, execute the read-only preflight check:

```bash
./target/release/nauti-nodes preflight
```

Or for machine-readable JSON output:

```bash
./target/release/nauti-nodes preflight --json
```

If preflight outputs `READY FOR TESTBED 1`, proceed to worker startup.

---

## 4. Environment Configuration

The worker node can be configured via environment variables:

```bash
export NAUTI_COORDINATOR_URL="http://10.0.0.61:8095"
export LOCAL_INFERENCE_URL="http://127.0.0.1:5200"
export NAUTI_WORKER_ROLE="thinker"
export NAUTI_IDENTITY_PATH="$HOME/.config/nauti/identity.json"
export NAUTI_STRICT_ALPHA="true"
```

---

## 5. Starting the Worker

Start the worker node:

```bash
./target/release/nauti-nodes worker
```

Upon startup, the worker will:
1. Load or generate a persistent Ed25519 node identity (`0600` permissions).
2. Discover GPU hardware via `all-smi`.
3. Submit a signed join request (`SignedJoinRequest`) to the coordinator.
4. Begin sending heartbeats every 10 seconds.
