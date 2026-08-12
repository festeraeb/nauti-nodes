//! Vendor-agnostic GPU discovery using all-smi.
//!
//! `all-smi` (https://github.com/lablup/all-smi) is an open-source CLI tool
//! for unified heterogeneous accelerator monitoring — NVIDIA, AMD, Intel GPUs
//! and NPUs through a single interface.
//!
//! This module calls `all-smi snapshot --format json` to discover all GPUs
//! on the local machine, extracting:
//! - GPU name, UUID, vendor/architecture
//! - Total and free VRAM (bytes)
//! - Current utilization (%)
//! - Temperature (°C)
//! - Power consumption (W)
//! - Driver/CUDA version
//! - Performance state
//!
//! Falls back to environment variables if all-smi is not available.

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// Discovered GPU information from all-smi snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuInfo {
    /// GPU name (e.g. "Tesla P100-PCIE-16GB", "RTX 4090", "Radeon RX 7900 XTX").
    pub name: String,
    /// GPU UUID (unique per device).
    pub uuid: String,
    /// Vendor (NVIDIA, AMD, Intel, Apple, etc.).
    pub vendor: String,
    /// Architecture (Pascal, Ampere, RDNA3, etc.).
    pub architecture: String,
    /// Total VRAM in bytes.
    pub total_memory_bytes: u64,
    /// Used VRAM in bytes.
    pub used_memory_bytes: u64,
    /// Free VRAM in bytes.
    pub free_memory_bytes: u64,
    /// Total VRAM in MB (convenience).
    pub total_memory_mb: u32,
    /// Free VRAM in MB (convenience).
    pub free_memory_mb: u32,
    /// GPU utilization (0.0 to 100.0).
    pub utilization_pct: f64,
    /// Temperature in °C.
    pub temperature_c: Option<f64>,
    /// Power consumption in watts.
    pub power_watts: Option<f64>,
    /// Power limit in watts.
    pub power_limit_watts: Option<f64>,
    /// Driver version string.
    pub driver_version: Option<String>,
    /// CUDA/HIP/Level-Zero version.
    pub compute_version: Option<String>,
    /// Performance state (0 = max, 15 = min, for NVIDIA).
    pub performance_state: Option<u32>,
    /// GPU index (local ordinal).
    pub index: u32,
    /// Hostname where this GPU lives.
    pub hostname: String,
    /// Timestamp of the snapshot.
    pub timestamp: String,
    /// Whether this GPU is currently available (not locked by reservation).
    pub available: bool,
}

impl GpuInfo {
    /// Free VRAM in MB.
    pub fn free_vram_mb(&self) -> u32 {
        self.free_memory_mb
    }

    /// Whether this GPU appears idle (low utilization, low memory usage).
    pub fn appears_idle(&self) -> bool {
        self.utilization_pct < 5.0 && self.used_memory_bytes == 0
    }
}

/// The all-smi snapshot JSON structure (partial, GPU-relevant fields only).
#[derive(Debug, Deserialize)]
struct AllSmiSnapshot {
    hostname: Option<String>,
    gpus: Option<Vec<AllSmiGpu>>,
    #[serde(default)]
    errors: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AllSmiGpu {
    name: Option<String>,
    uuid: Option<String>,
    index: Option<u32>,
    hostname: Option<String>,
    host_id: Option<String>,
    total_memory: Option<u64>,
    used_memory: Option<u64>,
    utilization: Option<f64>,
    temperature: Option<f64>,
    power_consumption: Option<f64>,
    performance_state: Option<u32>,
    time: Option<String>,
    detail: Option<AllSmiGpuDetail>,
}

#[derive(Debug, Deserialize)]
struct AllSmiGpuDetail {
    #[serde(rename = "Brand")]
    brand: Option<String>,
    #[serde(rename = "architecture")]
    architecture: Option<String>,
    #[serde(rename = "Driver Version")]
    driver_version: Option<String>,
    #[serde(rename = "CUDA Version")]
    cuda_version: Option<String>,
    #[serde(rename = "power_limit_current")]
    power_limit: Option<String>,
}

/// Discover all GPUs on the local machine using all-smi.
/// Returns a list of discovered GPU info, or an empty list if all-smi is
/// not available or fails.
pub fn discover_gpus() -> Vec<GpuInfo> {
    // Try all-smi snapshot first
    match std::process::Command::new("all-smi")
        .args([
            "snapshot",
            "--format",
            "json",
            "--pretty=false",
            "--include",
            "gpu",
        ])
        .output()
    {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            match parse_all_smi_snapshot(&stdout) {
                Ok(gpus) if !gpus.is_empty() => {
                    info!(count = gpus.len(), "discovered GPUs via all-smi");
                    return gpus;
                }
                Ok(_) => {
                    debug!("all-smi returned no GPUs");
                }
                Err(e) => {
                    warn!("failed to parse all-smi output: {e}");
                }
            }
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("all-smi snapshot failed: {stderr}");
        }
        Err(e) => {
            debug!("all-smi not found: {e}");
        }
    }

    // Fallback: try nvidia-smi (NVIDIA only)
    match discover_gpus_nvidia_smi() {
        gpus if !gpus.is_empty() => {
            info!(
                count = gpus.len(),
                "discovered GPUs via nvidia-smi fallback"
            );
            gpus
        }
        _ => {
            // Fallback: environment variables
            discover_gpus_from_env()
        }
    }
}

/// Parse all-smi JSON snapshot output into GpuInfo list.
fn parse_all_smi_snapshot(json: &str) -> Result<Vec<GpuInfo>, String> {
    let snapshot: AllSmiSnapshot =
        serde_json::from_str(json).map_err(|e| format!("JSON parse error: {e}"))?;

    let gpus = snapshot.gpus.unwrap_or_default();
    let hostname = snapshot.hostname.unwrap_or_else(|| {
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string())
    });

    let mut result = Vec::new();
    for (i, gpu) in gpus.iter().enumerate() {
        let name = gpu.name.clone().unwrap_or_else(|| format!("GPU-{i}"));
        let uuid = gpu.uuid.clone().unwrap_or_else(|| format!("unknown-{i}"));
        let total = gpu.total_memory.unwrap_or(0);
        let used = gpu.used_memory.unwrap_or(0);
        let free = total.saturating_sub(used);

        let (vendor, arch, driver, compute) = match &gpu.detail {
            Some(d) => (
                d.brand.clone().unwrap_or_else(|| "unknown".to_string()),
                d.architecture
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                d.driver_version.clone(),
                d.cuda_version.clone(),
            ),
            None => ("unknown".to_string(), "unknown".to_string(), None, None),
        };

        let power_limit = gpu
            .detail
            .as_ref()
            .and_then(|d| d.power_limit.as_ref().and_then(|s| s.parse::<f64>().ok()));

        result.push(GpuInfo {
            name,
            uuid,
            vendor,
            architecture: arch,
            total_memory_bytes: total,
            used_memory_bytes: used,
            free_memory_bytes: free,
            total_memory_mb: (total / (1024 * 1024)) as u32,
            free_memory_mb: (free / (1024 * 1024)) as u32,
            utilization_pct: gpu.utilization.unwrap_or(0.0),
            temperature_c: gpu.temperature,
            power_watts: gpu.power_consumption,
            power_limit_watts: power_limit,
            driver_version: driver,
            compute_version: compute,
            performance_state: gpu.performance_state,
            index: gpu.index.unwrap_or(i as u32),
            hostname: gpu.hostname.clone().unwrap_or_else(|| hostname.clone()),
            timestamp: gpu.time.clone().unwrap_or_default(),
            available: true,
        });
    }

    Ok(result)
}

/// Fallback: parse nvidia-smi CSV output (NVIDIA only).
fn discover_gpus_nvidia_smi() -> Vec<GpuInfo> {
    let output = match std::process::Command::new("nvidia-smi")
        .args([
            "--query-gpu=index,name,uuid,memory.total,memory.free,memory.used,utilization.gpu,temperature.gpu,power.draw,power.limit,driver_version,pci.bus_id",
            "--format=csv,noheader,nounits",
        ])
        .output()
    {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).to_string(),
        _ => return Vec::new(),
    };

    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    let mut result = Vec::new();
    for line in output.lines() {
        let fields: Vec<&str> = line.split(", ").map(|s| s.trim()).collect();
        if fields.len() < 12 {
            continue;
        }
        let index: u32 = fields[0].parse().unwrap_or(0);
        let name = fields[1].to_string();
        let uuid = fields[2].to_string();
        let total_mb: u32 = fields[3].parse().unwrap_or(0);
        let free_mb: u32 = fields[4].parse().unwrap_or(0);
        let used_mb: u32 = fields[5].parse().unwrap_or(0);
        let util: f64 = fields[6].parse().unwrap_or(0.0);
        let temp: f64 = fields[7].parse().unwrap_or(0.0);
        let power: f64 = fields[8].parse().unwrap_or(0.0);
        let power_limit: f64 = fields[9].parse().unwrap_or(0.0);
        let driver = fields[10].to_string();

        result.push(GpuInfo {
            name,
            uuid,
            vendor: "NVIDIA".to_string(),
            architecture: "unknown".to_string(),
            total_memory_bytes: (total_mb as u64) * 1024 * 1024,
            used_memory_bytes: (used_mb as u64) * 1024 * 1024,
            free_memory_bytes: (free_mb as u64) * 1024 * 1024,
            total_memory_mb: total_mb,
            free_memory_mb: free_mb,
            utilization_pct: util,
            temperature_c: Some(temp),
            power_watts: Some(power),
            power_limit_watts: Some(power_limit),
            driver_version: Some(driver),
            compute_version: None,
            performance_state: None,
            index,
            hostname: hostname.clone(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            available: true,
        });
    }
    result
}

/// Fallback: read GPU info from environment variables.
fn discover_gpus_from_env() -> Vec<GpuInfo> {
    let gpu_name = match std::env::var("NAUTI_WORKER_GPU") {
        Ok(v) if !v.is_empty() => v,
        _ => return Vec::new(),
    };
    let vram_mb: u32 = std::env::var("NAUTI_WORKER_VRAM_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8192);

    let hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    warn!(
        gpu_name = %gpu_name,
        vram_mb = vram_mb,
        "using env-var fallback for GPU discovery (no all-smi or nvidia-smi found)"
    );

    vec![GpuInfo {
        name: gpu_name.clone(),
        uuid: format!("env-{}", hostname),
        vendor: "unknown".to_string(),
        architecture: "unknown".to_string(),
        total_memory_bytes: (vram_mb as u64) * 1024 * 1024,
        used_memory_bytes: 0,
        free_memory_bytes: (vram_mb as u64) * 1024 * 1024,
        total_memory_mb: vram_mb,
        free_memory_mb: vram_mb,
        utilization_pct: 0.0,
        temperature_c: None,
        power_watts: None,
        power_limit_watts: None,
        driver_version: None,
        compute_version: None,
        performance_state: None,
        index: 0,
        hostname,
        timestamp: chrono::Utc::now().to_rfc3339(),
        available: true,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_all_smi_snapshot_sample() {
        let json = r#"{
            "hostname": "test-host",
            "gpus": [{
                "name": "RTX 4090",
                "uuid": "GPU-12345",
                "index": 0,
                "hostname": "test-host",
                "total_memory": 25769803776,
                "used_memory": 1073741824,
                "utilization": 45.5,
                "temperature": 72.0,
                "power_consumption": 320.5,
                "performance_state": 0,
                "time": "2026-08-12 10:00:00",
                "detail": {
                    "Brand": "NVIDIA",
                    "architecture": "Ada Lovelace",
                    "Driver Version": "560.35.03",
                    "CUDA Version": "12.6",
                    "power_limit_current": "450.00"
                }
            }],
            "errors": []
        }"#;

        let gpus = parse_all_smi_snapshot(json).unwrap();
        assert_eq!(gpus.len(), 1);
        let gpu = &gpus[0];
        assert_eq!(gpu.name, "RTX 4090");
        assert_eq!(gpu.uuid, "GPU-12345");
        assert_eq!(gpu.vendor, "NVIDIA");
        assert_eq!(gpu.architecture, "Ada Lovelace");
        assert_eq!(gpu.total_memory_bytes, 25769803776);
        assert_eq!(gpu.used_memory_bytes, 1073741824);
        assert_eq!(gpu.free_memory_bytes, 25769803776 - 1073741824);
        assert_eq!(gpu.total_memory_mb, 24576);
        assert!((gpu.utilization_pct - 45.5).abs() < 0.01);
        assert_eq!(gpu.temperature_c, Some(72.0));
        assert_eq!(gpu.power_watts, Some(320.5));
        assert_eq!(gpu.driver_version.as_deref(), Some("560.35.03"));
        assert_eq!(gpu.compute_version.as_deref(), Some("12.6"));
        assert!(gpu.available);
    }

    #[test]
    fn parse_empty_snapshot() {
        let json = r#"{"hostname":"h","gpus":[],"errors":[]}"#;
        let gpus = parse_all_smi_snapshot(json).unwrap();
        assert!(gpus.is_empty());
    }

    #[test]
    fn gpu_info_appears_idle() {
        let gpu = GpuInfo {
            name: "test".into(),
            uuid: "u".into(),
            vendor: "NVIDIA".into(),
            architecture: "Ampere".into(),
            total_memory_bytes: 8 * 1024 * 1024 * 1024,
            used_memory_bytes: 0,
            free_memory_bytes: 8 * 1024 * 1024 * 1024,
            total_memory_mb: 8192,
            free_memory_mb: 8192,
            utilization_pct: 2.0,
            temperature_c: Some(35.0),
            power_watts: Some(25.0),
            power_limit_watts: Some(250.0),
            driver_version: None,
            compute_version: None,
            performance_state: None,
            index: 0,
            hostname: "h".into(),
            timestamp: "t".into(),
            available: true,
        };
        assert!(gpu.appears_idle());
        assert_eq!(gpu.free_vram_mb(), 8192);
    }
}
