//! Node capability advertisements for discovery and scheduling.
//!
//! Nodes advertise their GPU capabilities, supported runtimes, available models,
//! privacy class, and current availability. This metadata is used for discovery,
//! scheduling eligibility, and direct-peer routing.

use serde::{Deserialize, Serialize};

/// Privacy/work class for eligibility filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrivacyClass {
    /// Work can run on any node in the federation.
    Public,
    /// Work can run on federation-trusted nodes.
    FederationSafe,
    /// Work restricted to trusted lineage (same root, verified).
    TrustedLineage,
    /// Work must never leave the local fleet.
    LocalOnly,
}

impl Default for PrivacyClass {
    fn default() -> Self {
        Self::Public
    }
}

impl PrivacyClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::FederationSafe => "federation_safe",
            Self::TrustedLineage => "trusted_lineage",
            Self::LocalOnly => "local_only",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "public" => Self::Public,
            "federation_safe" => Self::FederationSafe,
            "trusted_lineage" => Self::TrustedLineage,
            "local_only" => Self::LocalOnly,
            _ => Self::Public,
        }
    }
}

/// Supported runtime class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeClass {
    /// Lemonade Server.
    Lemonade,
    /// NVIDIA NIM.
    NvidiaNim,
    /// Generic OpenAI-compatible endpoint.
    OpenAICompatible,
    /// Native CESARops runtime.
    NativeCesarops,
    /// Unknown/custom runtime.
    Unknown,
}

impl Default for RuntimeClass {
    fn default() -> Self {
        Self::Unknown
    }
}

impl RuntimeClass {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Lemonade => "lemonade",
            Self::NvidiaNim => "nvidia_nim",
            Self::OpenAICompatible => "openai_compatible",
            Self::NativeCesarops => "native_cesarops",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "lemonade" => Self::Lemonade,
            "nvidia_nim" => Self::NvidiaNim,
            "openai_compatible" => Self::OpenAICompatible,
            "native_cesarops" => Self::NativeCesarops,
            _ => Self::Unknown,
        }
    }
}

/// Workload type that a node can execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkloadType {
    Inference,
    Photogrammetry,
    Lidar,
    Sonar,
    GaussianSplatting,
    DenseReconstruction,
    SatelliteProcessing,
    ResearchExperiment,
    ModelBenchmark,
    SandboxCoding,
    Other,
}

impl Default for WorkloadType {
    fn default() -> Self {
        Self::Inference
    }
}

impl WorkloadType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Inference => "inference",
            Self::Photogrammetry => "photogrammetry",
            Self::Lidar => "lidar",
            Self::Sonar => "sonar",
            Self::GaussianSplatting => "gaussian_splatting",
            Self::DenseReconstruction => "dense_reconstruction",
            Self::SatelliteProcessing => "satellite_processing",
            Self::ResearchExperiment => "research_experiment",
            Self::ModelBenchmark => "model_benchmark",
            Self::SandboxCoding => "sandbox_coding",
            Self::Other => "other",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "inference" => Self::Inference,
            "photogrammetry" => Self::Photogrammetry,
            "lidar" => Self::Lidar,
            "sonar" => Self::Sonar,
            "gaussian_splatting" => Self::GaussianSplatting,
            "dense_reconstruction" => Self::DenseReconstruction,
            "satellite_processing" => Self::SatelliteProcessing,
            "research_experiment" => Self::ResearchExperiment,
            "model_benchmark" => Self::ModelBenchmark,
            "sandbox_coding" => Self::SandboxCoding,
            _ => Self::Other,
        }
    }
}

/// Full capability advertisement from a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityAdvertisement {
    /// Node's durable identity (UUID).
    pub node_id: String,
    /// Reachable endpoint for direct inference/work requests.
    pub inference_url: String,
    /// Reachable endpoint for health checks.
    pub health_url: String,
    /// Runtime class (Lemonade, NIM, OpenAI, etc.).
    pub runtime: RuntimeClass,
    /// GPU name (e.g. "RTX 2060 SUPER").
    pub gpu_name: String,
    /// Total VRAM in MB.
    pub vram_total_mb: u32,
    /// Currently free VRAM in MB.
    pub vram_free_mb: u32,
    /// Supported workload types.
    pub workload_types: Vec<WorkloadType>,
    /// Available models (names/IDs).
    pub available_models: Vec<String>,
    /// Privacy/trust class.
    pub privacy_class: PrivacyClass,
    /// Software version.
    pub software_version: String,
    /// Protocol version (for future compatibility).
    pub protocol_version: u32,
    /// Whether the node is currently available for work.
    pub available: bool,
    /// Number of currently active jobs.
    pub active_jobs: u32,
    /// Observed tokens per second (for inference workloads).
    pub tokens_per_sec: f64,
    /// Observation timestamp (millis since epoch).
    pub observed_at_ms: u64,
}

impl CapabilityAdvertisement {
    /// Create a minimal advertisement for a node with an inference runtime.
    pub fn inference_node(
        node_id: &str,
        inference_url: &str,
        gpu_name: &str,
        vram_total_mb: u32,
        vram_free_mb: u32,
        models: Vec<String>,
    ) -> Self {
        Self {
            node_id: node_id.to_string(),
            inference_url: inference_url.to_string(),
            health_url: format!("{}/health", inference_url.trim_end_matches('/')),
            runtime: RuntimeClass::OpenAICompatible,
            gpu_name: gpu_name.to_string(),
            vram_total_mb,
            vram_free_mb,
            workload_types: vec![WorkloadType::Inference],
            available_models: models,
            privacy_class: PrivacyClass::default(),
            software_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: 1,
            available: true,
            active_jobs: 0,
            tokens_per_sec: 0.0,
            observed_at_ms: crate::types::now_ms(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn privacy_class_roundtrip() {
        for cls in [
            PrivacyClass::Public,
            PrivacyClass::FederationSafe,
            PrivacyClass::TrustedLineage,
            PrivacyClass::LocalOnly,
        ] {
            let s = cls.as_str();
            assert_eq!(PrivacyClass::from_str(s), cls);
        }
    }

    #[test]
    fn runtime_class_roundtrip() {
        for cls in [
            RuntimeClass::Lemonade,
            RuntimeClass::NvidiaNim,
            RuntimeClass::OpenAICompatible,
            RuntimeClass::NativeCesarops,
            RuntimeClass::Unknown,
        ] {
            let s = cls.as_str();
            assert_eq!(RuntimeClass::from_str(s), cls);
        }
    }

    #[test]
    fn workload_type_roundtrip() {
        for wt in [
            WorkloadType::Inference,
            WorkloadType::Photogrammetry,
            WorkloadType::Lidar,
            WorkloadType::Sonar,
            WorkloadType::GaussianSplatting,
            WorkloadType::DenseReconstruction,
            WorkloadType::SatelliteProcessing,
            WorkloadType::ResearchExperiment,
            WorkloadType::ModelBenchmark,
            WorkloadType::SandboxCoding,
            WorkloadType::Other,
        ] {
            let s = wt.as_str();
            assert_eq!(WorkloadType::from_str(s), wt);
        }
    }

    #[test]
    fn inference_node_advertisement() {
        let adv = CapabilityAdvertisement::inference_node(
            "test-node",
            "http://10.0.0.1:5200",
            "RTX 2060 SUPER",
            8192,
            7000,
            vec!["thinker".into()],
        );
        assert_eq!(adv.node_id, "test-node");
        assert_eq!(adv.runtime, RuntimeClass::OpenAICompatible);
        assert_eq!(adv.vram_total_mb, 8192);
        assert_eq!(adv.vram_free_mb, 7000);
        assert!(adv.available);
        assert_eq!(adv.protocol_version, 1);
        assert_eq!(adv.workload_types, vec![WorkloadType::Inference]);
    }
}
