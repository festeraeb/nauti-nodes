//! 002e — swappable `FleetPlanner`. Port of `cesaropsserver/fleet/planner.py` (F5).
//!
//! One trait, two implementations, identical signature, so the scorecard can
//! A/B them on `plan_quality`:
//!
//!   [`MonolithicMoEPlanner`]    — one fleet-wide view; any healthy host may
//!                                take the role, best free VRAM wins.
//!   [`FederatedPerNodePlanner`] — consults only the asking node's own slots.
//!
//! # The contract that matters
//!
//! Both return `Option<PlanFragmentSpec>`, and **`None` is the only correct
//! answer for a role that cannot be satisfied.** An impl must never fabricate
//! a fragment with empty `host_key`/`slot`/`model_id` to look successful —
//! that is precisely the failure where a plan reads as backed while nothing
//! backs it.
//!
//! The Python original keeps its shared helpers (`_hosts`, `_best_slot`,
//! `_serves`) on the ABC so the two impls cannot drift. Rust cannot have
//! default methods on a trait object the same way, so they live in
//! [`planner_util`] and both impls call the same functions — the guarantee is
//! "one implementation of the rules", enforced by review, rather than by
//! inheritance.

use std::collections::BTreeMap;

/// A host's slots, as seen by the planner.
#[derive(Debug, Clone, Default)]
pub struct HostView {
    /// Slot name -> slot record.
    pub slots: BTreeMap<String, SlotView>,
}

/// One schedulable slot.
#[derive(Debug, Clone, Default)]
pub struct SlotView {
    /// Model this slot serves. Empty means the slot holds nothing usable.
    pub model_id: String,
    /// Roles this slot can serve. Empty means "any role, if it has a model".
    pub roles: Vec<String>,
    /// Unhealthy slots are never offered.
    pub healthy: bool,
    /// Free VRAM in MB — the tie-break metric between candidate slots.
    pub free_mb: f64,
}

impl SlotView {
    /// A healthy slot serving `roles`, holding `model_id`.
    pub fn new(model_id: &str, roles: &[&str], free_mb: f64) -> Self {
        SlotView {
            model_id: model_id.to_string(),
            roles: roles.iter().map(|s| s.to_string()).collect(),
            healthy: true,
            free_mb,
        }
    }

    /// Mark unhealthy. Mirrors the Python `healthy=False` case.
    pub fn unhealthy(mut self) -> Self {
        self.healthy = false;
        self
    }
}

/// The fragment both impls must produce.
///
/// Field names and order mirror `fleet.api.PlanFragment` so a fragment can be
/// handed straight to the /v1/plan response model without translation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanFragmentSpec {
    pub node_id: String,
    pub role: String,
    pub host_key: String,
    pub slot: String,
    pub model_id: String,
    pub reason: String,
}

/// Why a planner could not produce a fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerError {
    /// No known planner kind. Never defaulted — a silent default would make
    /// an A/B comparison meaningless, since both arms would be the same impl.
    UnknownKind(String),
    /// The snapshot envelope could not be read at all.
    MalformedSnapshot(String),
}

/// Which planner to build. Parsed from the wire form used by Python's
/// `get_planner(kind)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlannerKind {
    Monolithic,
    Federated,
}

impl PlannerKind {
    /// `"monolithic"` / `"federated"` — exactly the two strings Python's
    /// `get_planner(kind)` accepts. Anything else is an error; there are no
    /// undocumented aliases, so a typo surfaces instead of silently selecting
    /// a planner and quietly corrupting an A/B comparison.
    pub fn parse(s: &str) -> Result<Self, PlannerError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "monolithic" => Ok(PlannerKind::Monolithic),
            "federated" => Ok(PlannerKind::Federated),
            other => Err(PlannerError::UnknownKind(other.to_string())),
        }
    }
}

/// Tier-2 predictor interface.
pub trait FleetPlanner {
    /// Stable name, used in scorecard rows.
    fn name(&self) -> &'static str;

    /// Return a fragment, or `None` when the role cannot be satisfied.
    fn plan(
        &self,
        node_id: &str,
        role: &str,
        snapshots: &BTreeMap<String, HostView>,
    ) -> Option<PlanFragmentSpec>;
}
/// Shared rule implementations. Both planners route through these so the two
/// impls cannot disagree about what "can serve" means.
pub mod planner_util {
    use super::{HostView, SlotView};

    /// Does this slot serve `role`?
    ///
    /// Mirrors Python `_serves`: unhealthy never serves; a slot with no
    /// declared roles accepts any role *provided it actually holds a model*.
    pub fn serves(slot: &SlotView, role: &str) -> bool {
        if !slot.healthy {
            return false;
        }
        if slot.roles.is_empty() {
            return !slot.model_id.is_empty();
        }
        slot.roles.iter().any(|r| r == role)
    }

    /// The slot on this host with the most free VRAM that serves `role`.
    ///
    /// Mirrors Python `_best_slot`. Returns `None` rather than an empty slot
    /// when nothing qualifies.
    pub fn best_slot<'a>(host: &'a HostView, role: &str) -> Option<(&'a String, &'a SlotView)> {
        let mut best: Option<(&String, &SlotView)> = None;
        for (name, slot) in &host.slots {
            if !serves(slot, role) {
                continue;
            }
            match best {
                Some((_, b)) if b.free_mb >= slot.free_mb => {}
                _ => best = Some((name, slot)),
            }
        }
        best
    }
}

/// Fleet-wide view: any healthy host may take the role.
///
/// Mirrors Python `MonolithicMoEPlanner`. The asking node is excluded from the
/// candidate set — the head may not hand a node its own work back.
#[derive(Debug, Clone, Copy, Default)]
pub struct MonolithicMoEPlanner;

impl FleetPlanner for MonolithicMoEPlanner {
    fn name(&self) -> &'static str {
        "monolithic"
    }

    fn plan(
        &self,
        node_id: &str,
        role: &str,
        snapshots: &BTreeMap<String, HostView>,
    ) -> Option<PlanFragmentSpec> {
        let mut best: Option<(f64, &String, &String, &SlotView)> = None;
        for (host_key, host) in snapshots {
            if host_key == node_id {
                continue;
            }
            if let Some((slot_name, slot)) = planner_util::best_slot(host, role) {
                match best {
                    Some((free, ..)) if free >= slot.free_mb => {}
                    _ => best = Some((slot.free_mb, host_key, slot_name, slot)),
                }
            }
        }
        let (_, host_key, slot_name, slot) = best?;
        Some(PlanFragmentSpec {
            node_id: node_id.to_string(),
            role: role.to_string(),
            host_key: host_key.clone(),
            slot: slot_name.clone(),
            model_id: slot.model_id.clone(),
            reason: format!("fleet-wide: {host_key} has a healthy {role} slot"),
        })
    }
}

/// Per-node view: only the asking node's own slots are consulted.
///
/// Mirrors Python `FederatedPerNodePlanner`. Returns `None` when the node
/// itself cannot serve the role, even if a remote host could — the decision to
/// widen the scope belongs to the caller, not to this planner.
#[derive(Debug, Clone, Copy, Default)]
pub struct FederatedPerNodePlanner;

impl FleetPlanner for FederatedPerNodePlanner {
    fn name(&self) -> &'static str {
        "federated"
    }

    fn plan(
        &self,
        node_id: &str,
        role: &str,
        snapshots: &BTreeMap<String, HostView>,
    ) -> Option<PlanFragmentSpec> {
        let host = snapshots.get(node_id)?;
        let (slot_name, slot) = planner_util::best_slot(host, role)?;
        Some(PlanFragmentSpec {
            node_id: node_id.to_string(),
            role: role.to_string(),
            host_key: node_id.to_string(),
            slot: slot_name.clone(),
            model_id: slot.model_id.clone(),
            reason: format!("local: {node_id} holds a healthy {role} slot"),
        })
    }
}

/// Factory. Port of Python's `get_planner(kind)`.
///
/// An unknown kind is an error, never a default: a silent default would make
/// an A/B comparison meaningless because both arms would be the same impl.
pub fn get_planner(kind: PlannerKind) -> Box<dyn FleetPlanner> {
    match kind {
        PlannerKind::Monolithic => Box::new(MonolithicMoEPlanner),
        PlannerKind::Federated => Box::new(FederatedPerNodePlanner),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleetplan::planner::planner_util::{best_slot, serves};

    /// Same fixture as the Python `test_planner.py`: two healthy hosts plus one
    /// unhealthy one that must never be chosen.
    fn snapshot() -> BTreeMap<String, HostView> {
        let mut m = BTreeMap::new();

        let mut t440 = HostView::default();
        t440.slots.insert(
            "coder-lane-c4".into(),
            SlotView::new("KAT-Coder-V2.5", &["coder", "coding"], 4000.0),
        );
        t440.slots.insert(
            "idle-0".into(),
            SlotView { healthy: true, ..SlotView::default() },
        );
        m.insert("t440".into(), t440);

        let mut c4 = HostView::default();
        c4.slots.insert(
            "judge".into(),
            SlotView::new("bugtrace-apex", &["reviewer", "validator"], 9000.0),
        );
        c4.slots.insert(
            "coder-b".into(),
            SlotView::new("kimi-k3", &["coder", "coding"], 7000.0),
        );
        m.insert("c4".into(), c4);

        let mut c2 = HostView::default();
        c2.slots.insert(
            "hermes".into(),
            SlotView::new("Hermes-3-8B", &["coder"], 12000.0).unhealthy(),
        );
        m.insert("c2".into(), c2);

        m
    }

    fn planners() -> Vec<Box<dyn FleetPlanner>> {
        vec![
            get_planner(PlannerKind::Monolithic),
            get_planner(PlannerKind::Federated),
        ]
    }

    #[test]
    fn both_impls_satisfy_the_same_contract() {
        let snap = snapshot();
        for p in planners() {
            let frag = p.plan("t440", "coding", &snap)
                .unwrap_or_else(|| panic!("{} produced no fragment", p.name()));
            assert_eq!(frag.node_id, "t440");
            assert_eq!(frag.role, "coding");
            assert!(!frag.host_key.is_empty());
            assert!(!frag.slot.is_empty());
            assert!(!frag.model_id.is_empty(), "empty model_id looks like success");
            assert!(!frag.reason.is_empty());
        }
    }

    #[test]
    fn same_ask_different_vantage_point() {
        let snap = snapshot();
        let mono = get_planner(PlannerKind::Monolithic);
        let fed = get_planner(PlannerKind::Federated);
        assert_eq!(mono.plan("t440", "coding", &snap).unwrap().host_key, "c4");
        assert_eq!(fed.plan("t440", "coding", &snap).unwrap().host_key, "t440");
    }

    #[test]
    fn unsatisfiable_role_yields_none_from_both() {
        let snap = snapshot();
        for p in planners() {
            assert!(
                p.plan("t440", "nonexistent-role", &snap).is_none(),
                "{} returned a fragment for an unsatisfiable role",
                p.name()
            );
        }
    }

    #[test]
    fn unhealthy_host_is_never_chosen() {
        let snap = snapshot();
        let frag = get_planner(PlannerKind::Monolithic)
            .plan("t440", "coding", &snap)
            .unwrap();
        assert_ne!(frag.host_key, "c2");
        assert!(!frag.model_id.to_lowercase().contains("hermes"));
    }

    #[test]
    fn federated_declines_when_only_a_remote_host_could_serve() {
        let snap = snapshot();
        // t440 holds no reviewer slot.
        assert!(get_planner(PlannerKind::Federated)
            .plan("t440", "reviewer", &snap)
            .is_none());
        // The fleet-wide planner can still satisfy it from c4.
        assert_eq!(
            get_planner(PlannerKind::Monolithic)
                .plan("t440", "reviewer", &snap)
                .unwrap()
                .host_key,
            "c4"
        );
    }

    #[test]
    fn monolithic_never_returns_the_askers_own_host() {
        let snap = snapshot();
        // c4 is the only other host with a coding slot; asking from c4 must not
        // hand c4 back to itself.
        let frag = get_planner(PlannerKind::Monolithic)
            .plan("c4", "coding", &snap)
            .unwrap();
        assert_ne!(frag.host_key, "c4");
    }

    #[test]
    fn unknown_kind_is_an_error_not_a_default() {
        assert!(matches!(
            PlannerKind::parse("unknown"),
            Err(PlannerError::UnknownKind(_))
        ));
        for bad in ["", "MoE", "singular"] {
            assert!(PlannerKind::parse(bad).is_err(), "{bad} should not parse");
        }
        assert_eq!(PlannerKind::parse("monolithic").unwrap(), PlannerKind::Monolithic);
        assert_eq!(PlannerKind::parse(" Federated ").unwrap(), PlannerKind::Federated);
    }

    #[test]
    fn empty_snapshot_is_none_from_both() {
        let empty = BTreeMap::new();
        for p in planners() {
            assert!(p.plan("t440", "coding", &empty).is_none());
        }
    }

    #[test]
    fn slot_with_no_model_and_no_roles_serves_nothing() {
        // The "idle-0" fixture: healthy but empty. It must never be picked,
        // which is what stops a blank slot from masquerading as a plan.
        let s = SlotView::default();
        assert!(!serves(&s, "coding"));
    }

    #[test]
    fn role_less_slot_serves_any_role_when_it_holds_a_model() {
        let s = SlotView::new("some-model", &[], 100.0);
        assert!(serves(&s, "coding"));
        assert!(serves(&s, "reviewer"));
    }

    #[test]
    fn best_slot_picks_the_most_free_vram() {
        let mut h = HostView::default();
        h.slots.insert("small".into(), SlotView::new("m", &["coding"], 10.0));
        h.slots.insert("big".into(), SlotView::new("m", &["coding"], 99.0));
        assert_eq!(best_slot(&h, "coding").unwrap().0, "big");
        assert!(best_slot(&h, "nonexistent-role").is_none());
    }
}
