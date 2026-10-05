//! 002a — snapshot merge. Port of `cesaropsserver/fleet/awareness.py` (F1).
//!
//! Union N per-node snapshots into one fleet snapshot.
//!
//! Pure: no network, no filesystem, no clock reads. Every field comes from the
//! input snapshots.
//!
//! On a duplicate GPU `identity` the entry from the snapshot with the greater
//! `taken_at` wins **whole** — fields are never averaged or blended. Equal
//! `taken_at` resolves to the first occurrence, so the result does not depend
//! on how the caller ordered its inputs. That determinism is the point: a
//! merge whose output depends on input order cannot be reasoned about or
//! diffed.
//!
//! Empty input yields an empty snapshot, never `None`.

use std::collections::BTreeSet;

/// A GPU as the awareness layer understands it.
///
/// Mirrors `GpuAwareness`. `identity` is the dedupe key (e.g.
/// `"t440:GPU-de3ce5bc-…"`); equality and hashing are on `identity` alone,
/// exactly like the Python `__eq__`/`__hash__` pair.
#[derive(Debug, Clone, PartialEq)]
pub struct GpuAwareness {
    pub identity: String,
    pub host_key: String,
    pub hostname: String,
    pub index: i32,
    pub name: String,
    pub vram_total_gb: f64,
    pub vram_used_gb: f64,
    pub util_pct: f64,
    pub online: bool,
    pub loaded_model: String,
    /// `Free` | `BusyForge` | `BusyExternal` | `Offline` | `Locked`
    pub health: String,
    pub gpu_class: String,
    pub vram_free_gb: f64,
    pub can_inference: bool,
    pub note: String,
}

impl GpuAwareness {
    /// A minimal GPU record; the rest default to "free and usable".
    pub fn new(identity: &str, host_key: &str, index: i32) -> Self {
        GpuAwareness {
            identity: identity.to_string(),
            host_key: host_key.to_string(),
            hostname: host_key.to_string(),
            index,
            name: "UNKNOWN".to_string(),
            vram_total_gb: 16.0,
            vram_used_gb: 0.0,
            util_pct: 0.0,
            online: true,
            loaded_model: String::new(),
            health: "Free".to_string(),
            gpu_class: "P100-16GB".to_string(),
            vram_free_gb: 16.0,
            can_inference: true,
            note: String::new(),
        }
    }
}

/// A single moment in time: every GPU plus the planner's view.
///
/// Mirrors `AwarenessSnapshot`. Map-typed fields are kept as sorted
/// key/value pairs so the type stays `Ord`-friendly and needs no serde
/// dependency.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AwarenessSnapshot {
    pub taken_at: f64,
    pub gpus: Vec<GpuAwareness>,
    pub locked_hosts: Vec<String>,
    pub offline_hosts: Vec<String>,
    pub live_benchmarks: Vec<(String, String)>,
    pub planner_view: Vec<(String, String)>,
}

/// Order-preserving union across sequences.
///
/// Mirrors `_dedupe_union`: first-seen order is kept, later repeats dropped.
fn dedupe_union<'a, I, T>(seqs: I) -> Vec<T>
where
    I: IntoIterator<Item = &'a [T]>,
    T: Clone + Ord + 'a,
{
    let mut seen: BTreeSet<T> = BTreeSet::new();
    let mut out: Vec<T> = Vec::new();
    for seq in seqs {
        for item in seq {
            if seen.insert(item.clone()) {
                out.push(item.clone());
            }
        }
    }
    out
}

/// Value from the freshest snapshot that has a non-empty one.
///
/// Mirrors `_freshest_nonempty`: snapshots are walked newest-first and the
/// first truthy `attr` wins. An empty value is skipped rather than taken,
/// so a snapshot that merely has the field present-but-blank cannot mask a
/// real value from an older snapshot.
fn freshest_nonempty<T: Clone + Ord>(
    snaps: &[&AwarenessSnapshot],
    get: impl Fn(&AwarenessSnapshot) -> Vec<T>,
) -> Vec<T> {
    let mut ordered: Vec<&AwarenessSnapshot> = snaps.to_vec();
    ordered.sort_by(|a, b| {
        b.taken_at
            .partial_cmp(&a.taken_at)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    for s in ordered {
        let v = get(s);
        if !v.is_empty() {
            return v;
        }
    }
    Vec::new()
}

/// Union N per-node snapshots into one fleet snapshot.
///
/// Mirrors `merge_snapshots`. Empty input yields an empty snapshot with
/// `taken_at == 0.0`, never `None`.
pub fn merge_snapshots(snaps: &[AwarenessSnapshot]) -> AwarenessSnapshot {
    if snaps.is_empty() {
        return AwarenessSnapshot::default();
    }

    // Newest snapshot per GPU identity. Strict `>` on taken_at means ties keep
    // the first seen, so the result is independent of input order.
    let mut freshest: std::collections::HashMap<&str, (f64, GpuAwareness)> =
        std::collections::HashMap::new();
    let mut order: Vec<&str> = Vec::new();
    for snap in snaps {
        for gpu in &snap.gpus {
            match freshest.get(gpu.identity.as_str()) {
                None => {
                    freshest.insert(gpu.identity.as_str(), (snap.taken_at, gpu.clone()));
                    order.push(gpu.identity.as_str());
                }
                Some((prior_at, _)) if snap.taken_at > *prior_at => {
                    freshest.insert(gpu.identity.as_str(), (snap.taken_at, gpu.clone()));
                }
                _ => {}
            }
        }
    }
    let gpus: Vec<GpuAwareness> = order
        .iter()
        .filter_map(|id| freshest.get(id).map(|(_, g)| g.clone()))
        .collect();

    let refs: Vec<&AwarenessSnapshot> = snaps.iter().collect();

    AwarenessSnapshot {
        taken_at: snaps
            .iter()
            .map(|s| s.taken_at)
            .fold(f64::NEG_INFINITY, f64::max),
        gpus,
        locked_hosts: dedupe_union(refs.iter().map(|s| s.locked_hosts.as_slice())),
        offline_hosts: dedupe_union(refs.iter().map(|s| s.offline_hosts.as_slice())),
        live_benchmarks: freshest_nonempty(&refs, |s| s.live_benchmarks.clone()),
        planner_view: freshest_nonempty(&refs, |s| s.planner_view.clone()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn gpu(identity: &str, host: &str, index: i32, model: &str) -> GpuAwareness {
        let mut g = GpuAwareness::new(identity, host, index);
        g.loaded_model = model.to_string();
        g
    }

    fn snap(taken_at: f64, gpus: Vec<GpuAwareness>) -> AwarenessSnapshot {
        AwarenessSnapshot {
            taken_at,
            gpus,
            ..Default::default()
        }
    }

    #[test]
    fn empty_input_is_empty_not_none() {
        let m = merge_snapshots(&[]);
        assert_eq!(m.taken_at, 0.0);
        assert!(m.gpus.is_empty());
        assert!(m.locked_hosts.is_empty());
        assert_eq!(m, AwarenessSnapshot::default());
    }

    #[test]
    fn unions_gpus_from_every_snapshot() {
        let a = snap(10.0, vec![gpu("t440:0", "t440", 0, "KAT")]);
        let b = snap(11.0, vec![gpu("c4:0", "c4", 0, "apex")]);
        let m = merge_snapshots(&[a, b]);
        assert_eq!(m.gpus.len(), 2);
        assert_eq!(m.taken_at, 11.0, "taken_at is the max across inputs");
    }

    #[test]
    fn duplicate_identity_fresher_snapshot_wins_whole() {
        // Same identity, newer snapshot says Busy. The entry must be taken
        // WHOLE from the newer snapshot -- no blending of fields.
        let mut stale = gpu("t440:0", "t440", 0, "old-model");
        stale.health = "Free".into();
        stale.util_pct = 0.0;

        let mut fresh = gpu("t440:0", "t440", 0, "new-model");
        fresh.health = "BusyForge".into();
        fresh.util_pct = 99.0;

        let m = merge_snapshots(&[snap(10.0, vec![stale]), snap(20.0, vec![fresh])]);
        assert_eq!(m.gpus.len(), 1, "dedupe by identity");
        assert_eq!(m.gpus[0].health, "BusyForge");
        assert_eq!(m.gpus[0].util_pct, 99.0);
        assert_eq!(m.gpus[0].loaded_model, "new-model");
    }

    #[test]
    fn equal_taken_at_keeps_first_seen_regardless_of_input_order() {
        // Two snapshots at the same instant disagree about one GPU. Whichever
        // order the caller passes, the FIRST one wins -- so the merge is
        // deterministic for a given input order and never silently averages.
        let a_gpu = gpu("t440:0", "t440", 0, "from-a");
        let b_gpu = gpu("t440:0", "t440", 0, "from-b");
        let m1 = merge_snapshots(&[snap(5.0, vec![a_gpu.clone()]), snap(5.0, vec![b_gpu.clone()])]);
        assert_eq!(m1.gpus[0].loaded_model, "from-a");
        let m2 = merge_snapshots(&[snap(5.0, vec![b_gpu]), snap(5.0, vec![a_gpu])]);
        assert_eq!(m2.gpus[0].loaded_model, "from-b");
    }

    #[test]
    fn gpu_order_follows_first_appearance() {
        let a = snap(
            10.0,
            vec![gpu("t440:0", "t440", 0, "x"), gpu("t440:1", "t440", 1, "y")],
        );
        let b = snap(11.0, vec![gpu("c4:0", "c4", 0, "z")]);
        let m = merge_snapshots(&[a, b]);
        let ids: Vec<&str> = m.gpus.iter().map(|g| g.identity.as_str()).collect();
        assert_eq!(ids, vec!["t440:0", "t440:1", "c4:0"]);
    }

    #[test]
    fn host_lists_are_order_preserving_unions() {
        let a = snap(10.0, vec![]);
        let mut a = a;
        a.locked_hosts = vec!["t440".into(), "c2".into()];
        a.offline_hosts = vec!["c3".into()];

        let mut b = snap(11.0, vec![]);
        b.locked_hosts = vec!["c2".into(), "c4".into()];
        b.offline_hosts = vec!["c3".into(), "c2".into()];

        let m = merge_snapshots(&[a, b]);
        assert_eq!(m.locked_hosts, vec!["t440", "c2", "c4"], "dedupe, first-seen order");
        assert_eq!(m.offline_hosts, vec!["c3", "c2"]);
    }

    #[test]
    fn scalar_maps_take_the_freshest_non_empty() {
        let mut old = snap(1.0, vec![]);
        old.planner_view = vec![("mode".into(), "coding".into())];
        let mut blank = snap(9.0, vec![]);
        blank.planner_view = vec![]; // present but EMPTY -> must be skipped
        let mut new = snap(5.0, vec![]);
        new.planner_view = vec![("mode".into(), "review".into())];

        let m = merge_snapshots(&[old, blank, new]);
        assert_eq!(m.planner_view, vec![("mode".to_string(), "review".to_string())]);
    }

    #[test]
    fn all_blank_maps_yield_empty_not_a_stale_value() {
        let mut a = snap(1.0, vec![]);
        a.planner_view = vec![];
        a.live_benchmarks = vec![];
        let m = merge_snapshots(&[a]);
        assert!(m.planner_view.is_empty());
        assert!(m.live_benchmarks.is_empty());
    }
}
