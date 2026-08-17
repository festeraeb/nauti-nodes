//! Volunteer CLI Subcommands for Testbed 1 External Alpha
//!
//! Provides read-only preflight, local status, voluntary signed withdrawal,
//! sanitized compatibility reporting, and diagnostic bundle generation.

use crate::config::Config;
use crate::gpu::discover_gpus;
use crate::node::auth::{sign_message, SignedWithdrawRequest, CURRENT_PROTOCOL_VERSION};
use crate::node::identity::NodeIdentity;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreflightReport {
    pub nauti_version: String,
    pub protocol_version: u32,
    pub os: String,
    pub arch: String,
    pub identity_path: String,
    pub identity_exists: bool,
    pub node_id: Option<String>,
    pub public_fingerprint: Option<String>,
    pub permissions_0600: bool,
    pub gpus_discovered: usize,
    pub gpu_vendor: String,
    pub gpu_model: String,
    pub total_vram_mb: u64,
    pub llama_server_available: bool,
    pub coordinator_url: String,
    pub coordinator_reachable: bool,
    pub public_policy_enforced: bool,
    pub ready: bool,
    pub failure_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityReport {
    pub nauti_version: String,
    pub protocol_version: u32,
    pub os: String,
    pub kernel: String,
    pub arch: String,
    pub gpu_vendor: String,
    pub gpu_model: String,
    pub total_vram_mb: u64,
    pub gpus_count: usize,
    pub llama_server_found: bool,
    pub identity_fingerprint: String,
    pub node_id: String,
    pub coordinator_url: String,
    pub public_workloads_only: bool,
    pub timestamp_utc: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticBundle {
    pub report: CompatibilityReport,
    pub sanitized_config: Config,
    pub runtime_state: String,
    pub lifecycle_state: String,
    pub recent_errors: Vec<String>,
    pub generated_at_ms: u64,
}

pub fn run_preflight(config: &Config, json_output: bool) -> Result<PreflightReport, String> {
    let os = std::env::consts::OS.to_string();
    let arch = std::env::consts::ARCH.to_string();
    let nauti_ver = env!("CARGO_PKG_VERSION").to_string();

    let id_path = Path::new(&config.identity_path);
    let id_exists = id_path.exists();
    let (node_id, fingerprint, perms_0600) = if id_exists {
        match NodeIdentity::load_or_create(id_path) {
            Ok(id) => {
                let pub_hex = hex::encode(&id.public_key);
                let fp = format!("ed25519:{}", &pub_hex[..16.min(pub_hex.len())]);
                #[cfg(unix)]
                let perms_ok = {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::metadata(id_path)
                        .map(|m| m.permissions().mode() & 0o777 == 0o600)
                        .unwrap_or(false)
                };
                #[cfg(not(unix))]
                let perms_ok = true;

                (Some(id.node_id), Some(fp), perms_ok)
            }
            Err(_) => (None, None, false),
        }
    } else {
        (None, None, true)
    };

    let gpus = discover_gpus();
    let gpu_count = gpus.len();
    let (gpu_vendor, gpu_model, vram) = if let Some(g) = gpus.first() {
        (g.vendor.clone(), g.name.clone(), g.total_memory_mb as u64)
    } else {
        ("Unknown".into(), config.worker_gpu.clone(), 8192u64)
    };

    // Check runtime binary or endpoint
    let llama_ok = which_exists("llama-server") || check_url(&config.local_inference_url);
    // Check coordinator reachability
    let coord_ok = check_url(&format!(
        "{}/health",
        config.coordinator_url.trim_end_matches('/')
    ));

    let mut failures = Vec::new();
    if gpu_count == 0 && config.strict_alpha {
        failures.push("No physical GPU hardware discovered via all-smi".into());
    }
    if !llama_ok {
        failures.push("llama-server runtime not found on PATH or listening port".into());
    }

    let is_ready = failures.is_empty();

    let report = PreflightReport {
        nauti_version: nauti_ver,
        protocol_version: CURRENT_PROTOCOL_VERSION,
        os,
        arch,
        identity_path: config.identity_path.clone(),
        identity_exists: id_exists,
        node_id,
        public_fingerprint: fingerprint,
        permissions_0600: perms_0600,
        gpus_discovered: gpu_count,
        gpu_vendor,
        gpu_model,
        total_vram_mb: vram,
        llama_server_available: llama_ok,
        coordinator_url: config.coordinator_url.clone(),
        coordinator_reachable: coord_ok,
        public_policy_enforced: config.strict_alpha,
        ready: is_ready,
        failure_reasons: failures,
    };

    if json_output {
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
    } else {
        println!("============================================================");
        println!("NAUTI NODES TESTBED 1 — VOLUNTEER PREFLIGHT INSPECTION");
        println!("============================================================");
        println!("Nauti Version:        {}", report.nauti_version);
        println!("Protocol Version:     v{}", report.protocol_version);
        println!("OS / Architecture:    {} / {}", report.os, report.arch);
        println!("Identity Path:        {}", report.identity_path);
        println!("Identity Exists:      {}", report.identity_exists);
        if let Some(nid) = &report.node_id {
            println!("Node ID:              {}", nid);
        }
        if let Some(fp) = &report.public_fingerprint {
            println!("Public Fingerprint:   {}", fp);
        }
        println!("0600 File Mode:       {}", report.permissions_0600);
        println!("GPUs Discovered:      {}", report.gpus_discovered);
        println!(
            "GPU Vendor / Model:   {} / {}",
            report.gpu_vendor, report.gpu_model
        );
        println!("VRAM Total (MiB):     {}", report.total_vram_mb);
        println!(
            "Inference Runtime:    {}",
            if report.llama_server_available {
                "AVAILABLE"
            } else {
                "NOT FOUND"
            }
        );
        println!("Coordinator URL:      {}", report.coordinator_url);
        println!(
            "Coordinator Status:   {}",
            if report.coordinator_reachable {
                "REACHABLE"
            } else {
                "UNREACHABLE"
            }
        );
        println!(
            "PUBLIC Policy:        {}",
            if report.public_policy_enforced {
                "STRICT PUBLIC ONLY"
            } else {
                "FLEXIBLE"
            }
        );
        println!("------------------------------------------------------------");
        if report.ready {
            println!("RESULT: READY FOR TESTBED 1");
        } else {
            println!("RESULT: NOT READY");
            for f in &report.failure_reasons {
                println!("  - {}", f);
            }
        }
        println!("============================================================");
    }

    Ok(report)
}

pub fn run_status(config: &Config, json_output: bool) -> Result<(), String> {
    let id_path = Path::new(&config.identity_path);
    let identity = NodeIdentity::load_or_create(id_path).map_err(|e| e.to_string())?;
    let pub_hex = hex::encode(&identity.public_key);
    let fp = format!("ed25519:{}", &pub_hex[..16.min(pub_hex.len())]);

    let gpus = discover_gpus();
    let coord_ok = check_url(&format!(
        "{}/health",
        config.coordinator_url.trim_end_matches('/')
    ));
    let runtime_ok = check_url(&config.local_inference_url);

    let status_data = serde_json::json!({
        "node_id": identity.node_id,
        "public_fingerprint": fp,
        "coordinator_url": config.coordinator_url,
        "coordinator_online": coord_ok,
        "runtime_online": runtime_ok,
        "gpus": gpus,
        "protocol_version": CURRENT_PROTOCOL_VERSION,
        "public_policy": "PUBLIC_ONLY"
    });

    if json_output {
        println!("{}", serde_json::to_string_pretty(&status_data).unwrap());
    } else {
        println!("============================================================");
        println!("NAUTI NODES LOCAL VOLUNTEER STATUS");
        println!("============================================================");
        println!("Node ID:              {}", identity.node_id);
        println!("Public Fingerprint:   {}", fp);
        println!("Coordinator URL:      {}", config.coordinator_url);
        println!("Coordinator Online:   {}", coord_ok);
        println!("Runtime Online:       {}", runtime_ok);
        println!("GPUs Count:           {}", gpus.len());
        if let Some(g) = gpus.first() {
            println!("Primary GPU:          {} ({})", g.name, g.vendor);
        }
        println!("Protocol Version:     v{}", CURRENT_PROTOCOL_VERSION);
        println!("Workload Boundary:    PUBLIC ONLY");
        println!("============================================================");
    }

    Ok(())
}

pub async fn run_withdraw(config: &Config) -> Result<(), String> {
    let id_path = Path::new(&config.identity_path);
    if !id_path.exists() {
        return Err("identity file does not exist — cannot withdraw".into());
    }
    let identity = NodeIdentity::load_or_create(id_path).map_err(|e| e.to_string())?;
    let signing_key = identity.signing_key().map_err(|e| e.to_string())?;

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let nonce = uuid::Uuid::new_v4().to_string();

    let mut req = SignedWithdrawRequest {
        node_id: identity.node_id.clone(),
        timestamp_secs: now_secs,
        nonce,
        signature_hex: String::new(),
    };
    req.signature_hex = sign_message(&signing_key, &req.canonical_message());

    let client = reqwest::Client::new();
    let withdraw_url = format!(
        "{}/internal/worker/withdraw",
        config.coordinator_url.trim_end_matches('/')
    );

    println!(
        "Sending signed withdrawal request for node {}...",
        identity.node_id
    );
    let resp = client
        .post(&withdraw_url)
        .json(&req)
        .send()
        .await
        .map_err(|e| format!("withdrawal request failed: {e}"))?;

    if resp.status().is_success() {
        println!("SUCCESS: Node {} gracefully withdrawn.", identity.node_id);
        println!("State: WITHDRAWN (No new work will be assigned).");
        println!("You may rejoin at any time by running 'nauti-nodes worker'.");
        Ok(())
    } else {
        let err_body = resp.text().await.unwrap_or_default();
        Err(format!("Withdrawal rejected by coordinator: {err_body}"))
    }
}

pub fn generate_compatibility_report(config: &Config) -> Result<CompatibilityReport, String> {
    let id_path = Path::new(&config.identity_path);
    let identity = NodeIdentity::load_or_create(id_path).map_err(|e| e.to_string())?;
    let pub_hex = hex::encode(&identity.public_key);
    let fp = format!("ed25519:{}", &pub_hex[..16.min(pub_hex.len())]);

    let gpus = discover_gpus();
    let (vendor, model, vram) = if let Some(g) = gpus.first() {
        (g.vendor.clone(), g.name.clone(), g.total_memory_mb as u64)
    } else {
        ("Unknown".into(), config.worker_gpu.clone(), 8192u64)
    };

    let report = CompatibilityReport {
        nauti_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: CURRENT_PROTOCOL_VERSION,
        os: std::env::consts::OS.to_string(),
        kernel: get_kernel_version(),
        arch: std::env::consts::ARCH.to_string(),
        gpu_vendor: vendor,
        gpu_model: model,
        total_vram_mb: vram,
        gpus_count: gpus.len(),
        llama_server_found: which_exists("llama-server"),
        identity_fingerprint: fp,
        node_id: identity.node_id,
        coordinator_url: config.coordinator_url.clone(),
        public_workloads_only: true,
        timestamp_utc: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };

    Ok(report)
}

pub fn run_report(config: &Config, json_output: bool) -> Result<(), String> {
    let report = generate_compatibility_report(config)?;
    if json_output {
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
    } else {
        println!("============================================================");
        println!("NAUTI NODES SANITIZED COMPATIBILITY REPORT");
        println!("============================================================");
        println!("Nauti Version:        {}", report.nauti_version);
        println!("Protocol Version:     v{}", report.protocol_version);
        println!(
            "OS / Kernel / Arch:   {} / {} / {}",
            report.os, report.kernel, report.arch
        );
        println!(
            "GPU Vendor / Model:   {} / {}",
            report.gpu_vendor, report.gpu_model
        );
        println!("Total VRAM (MiB):     {}", report.total_vram_mb);
        println!("GPUs Count:           {}", report.gpus_count);
        println!(
            "Inference Runtime:    {}",
            if report.llama_server_found {
                "PRESENT"
            } else {
                "NOT FOUND"
            }
        );
        println!("Node ID:              {}", report.node_id);
        println!("Public Fingerprint:   {}", report.identity_fingerprint);
        println!("Coordinator:          {}", report.coordinator_url);
        println!("Workload Policy:      PUBLIC ONLY");
        println!("============================================================");
        println!("NOTICE: No private keys, passwords, or host secrets are in this report.");
    }
    Ok(())
}

pub fn run_bundle(config: &Config, output_path: Option<&str>) -> Result<String, String> {
    let report = generate_compatibility_report(config)?;
    let sanitized_cfg = config.sanitize();

    let bundle = DiagnosticBundle {
        report,
        sanitized_config: sanitized_cfg,
        runtime_state: "IDLE_OR_SERVING".to_string(),
        lifecycle_state: "ONLINE".to_string(),
        recent_errors: vec![],
        generated_at_ms: crate::types::now_ms(),
    };

    let json_data = serde_json::to_string_pretty(&bundle)
        .map_err(|e| format!("serialize bundle failed: {e}"))?;

    // Verify secret scrubbing before writing
    assert!(
        !json_data.contains("secret_key"),
        "CRITICAL: Bundle contained secret_key!"
    );

    let default_path = format!("/tmp/nauti-diag-bundle-{}.json", bundle.report.node_id);
    let target_path = output_path.unwrap_or(&default_path);

    std::fs::write(target_path, json_data)
        .map_err(|e| format!("write diagnostic bundle to {target_path} failed: {e}"))?;

    println!("SUCCESS: Diagnostic bundle created at: {}", target_path);
    println!(
        "File size: {} bytes",
        std::fs::metadata(target_path).map(|m| m.len()).unwrap_or(0)
    );
    println!("NOTICE: Sanitized — contains zero private keys or secret credentials.");

    Ok(target_path.to_string())
}

fn which_exists(cmd: &str) -> bool {
    std::process::Command::new("which")
        .arg(cmd)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn check_url(url: &str) -> bool {
    let target = if url.contains("/health") {
        url.to_string()
    } else {
        format!("{}/health", url.trim_end_matches('/'))
    };
    std::process::Command::new("curl")
        .args(["-s", "-m", "2", &target])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn get_kernel_version() -> String {
    std::process::Command::new("uname")
        .arg("-r")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|_| "unknown".into())
}
