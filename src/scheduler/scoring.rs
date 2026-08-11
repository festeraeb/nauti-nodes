use crate::types::NodeStatistics;

pub fn compute_score(stats: &NodeStatistics) -> f64 {
    let queue_penalty = stats.active_jobs as f64 * 1.5;
    let latency_penalty = stats.avg_latency_ms / 1000.0;
    let vram_bonus = stats.free_vram_mb as f64 / 1024.0;
    let throughput_bonus = stats.tokens_per_sec;
    throughput_bonus + vram_bonus - queue_penalty - latency_penalty
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeStatistics;

    #[test]
    fn higher_throughput_scores_higher() {
        let a = NodeStatistics {
            tokens_per_sec: 100.0,
            free_vram_mb: 4096,
            ..Default::default()
        };
        let b = NodeStatistics {
            tokens_per_sec: 10.0,
            free_vram_mb: 4096,
            ..Default::default()
        };
        assert!(compute_score(&a) > compute_score(&b));
    }

    #[test]
    fn queue_penalizes() {
        let idle = NodeStatistics::default();
        let busy = NodeStatistics {
            active_jobs: 10,
            ..Default::default()
        };
        assert!(compute_score(&idle) > compute_score(&busy));
    }
}
