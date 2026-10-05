//! 002b — planning scope. Port of `cesaropsserver/fleet/scope.py` (F2).
//!
//! The planner is scope-agnostic by construction: it plans over whatever GPU
//! list it is handed and never asks whether that is one host or four. `Scope`
//! makes the extent explicit so the same code path serves both.
//!
//!   `Local` — the GPUs this node can see alone. No network, no latency
//!             matrix. What a node does before escalating.
//!   `Fleet` — a merged snapshot of every node, latency-aware. What the head
//!             does when a node escalates.
//!
//! The narrowing rule is deliberately forgiving: when `Local` is asked for
//! without a `node_id`, or with one that matches no GPU, we return the FULL set
//! rather than an empty one. Silently dropping GPUs because the caller forgot
//! an argument would make the plan quietly wrong, and a wrong plan is worse
//! than a broad one. This mirrors `hosts_in_scope`/`gpus_in_scope` in Python,
//! where both end in `or hosts` / `or gpus`.

use std::collections::BTreeSet;

/// Planning scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
    /// This node only.
    Local,
    /// Every host in the merged snapshot.
    Fleet,
}

impl Scope {
    /// Parse the wire form used by the Python reference (`"local"`/`"fleet"`).
    ///
    /// An unrecognised value is `None` rather than a silent default to
    /// `Fleet`: defaulting would widen the scope without telling anyone.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "local" => Some(Scope::Local),
            "fleet" => Some(Scope::Fleet),
            _ => None,
        }
    }
}

impl Default for Scope {
    /// Defaults to `Fleet`, matching Python where `scope=None` resolves to
    /// `Scope.FLEET` so an unscoped call stays identical to a fleet call.
    fn default() -> Self {
        Scope::Fleet
    }
}

/// Anything that knows which host it belongs to.
///
/// The Python original reaches for the attribute dynamically (`getattr(g,
/// "host_key", None)`), so a missing host key yields `None` rather than
/// raising. [`NoHostKey`] reproduces that case so the port can be tested
/// against it.
pub trait HasHostKey {
    fn host_key(&self) -> Option<&str>;
}

/// A GPU-like record with no host association, mirroring Python's
/// `getattr(g, "host_key", None) -> None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoHostKey {
    pub id: String,
}

impl HasHostKey for NoHostKey {
    fn host_key(&self) -> Option<&str> {
        None
    }
}

/// Host keys visible at this scope.
///
/// `Local` narrows to `node_id` when the caller names one **and** it is
/// present. Otherwise every host is returned — see the module note.
pub fn hosts_in_scope<T: HasHostKey>(
    gpus: &[T],
    scope: Scope,
    node_id: Option<&str>,
) -> BTreeSet<String> {
    let all: BTreeSet<String> = gpus
        .iter()
        .filter_map(|g| g.host_key())
        .filter(|h| !h.is_empty())
        .map(str::to_string)
        .collect();

    match (scope, node_id) {
        (Scope::Fleet, _) | (_, None) => all,
        (Scope::Local, Some(node)) if all.contains(node) => {
            let mut s = BTreeSet::new();
            s.insert(node.to_string());
            s
        }
        // Asked for one node that has no GPUs here: fall back to the full set
        // rather than planning over nothing.
        (Scope::Local, Some(_)) => all,
    }
}

/// GPUs visible at this scope, in input order.
pub fn gpus_in_scope<'a, T: HasHostKey>(
    gpus: &'a [T],
    scope: Scope,
    node_id: Option<&str>,
) -> Vec<&'a T> {
    match (scope, node_id) {
        (Scope::Fleet, _) | (_, None) => gpus.iter().collect(),
        (Scope::Local, Some(node)) => {
            let narrowed: Vec<&T> = gpus
                .iter()
                .filter(|g| g.host_key() == Some(node))
                .collect();
            if narrowed.is_empty() {
                gpus.iter().collect()
            } else {
                narrowed
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct G {
        id: &'static str,
        host: Option<&'static str>,
    }

    impl HasHostKey for G {
        fn host_key(&self) -> Option<&str> {
            self.host
        }
    }

    fn snap() -> Vec<G> {
        vec![
            G { id: "g0", host: Some("t440") },
            G { id: "g1", host: Some("t440") },
            G { id: "g2", host: Some("c4") },
        ]
    }

    #[test]
    fn fleet_scope_sees_every_host() {
        let hs = hosts_in_scope(&snap(), Scope::Fleet, None);
        assert_eq!(hs, BTreeSet::from(["c4".to_string(), "t440".to_string()]));
    }

    #[test]
    fn fleet_scope_ignores_node_id() {
        // Naming a node must not narrow a FLEET scope.
        let hs = hosts_in_scope(&snap(), Scope::Fleet, Some("t440"));
        assert_eq!(hs.len(), 2, "FLEET must not narrow to one host");
    }

    #[test]
    fn local_scope_narrows_to_the_named_node() {
        let binding = snap();
        let hs = hosts_in_scope(&binding, Scope::Local, Some("t440"));
        assert_eq!(hs, BTreeSet::from(["t440".to_string()]));
        let gpus = gpus_in_scope(&binding, Scope::Local, Some("t440"));
        assert_eq!(gpus.len(), 2);
        assert!(gpus.iter().all(|g| g.host_key() == Some("t440")));
    }

    #[test]
    fn local_scope_without_node_id_keeps_everything() {
        // Port of Python's `or hosts`: a forgotten argument must not silently
        // empty the plan.
        let hs = hosts_in_scope(&snap(), Scope::Local, None);
        assert_eq!(hs.len(), 2);
        assert_eq!(gpus_in_scope(&snap(), Scope::Local, None).len(), 3);
    }

    #[test]
    fn local_scope_with_unknown_node_falls_back_to_all() {
        // Same reasoning: a node with no GPUs here is not a reason to plan
        // over nothing.
        let hs = hosts_in_scope(&snap(), Scope::Local, Some("c2"));
        assert_eq!(hs.len(), 2);
        assert_eq!(gpus_in_scope(&snap(), Scope::Local, Some("c2")).len(), 3);
    }

    #[test]
    fn missing_host_key_is_dropped_not_fatal() {
        let mixed = vec![
            G { id: "g0", host: Some("t440") },
            G { id: "gx", host: None },
        ];
        let hs = hosts_in_scope(&mixed, Scope::Fleet, None);
        assert_eq!(hs, BTreeSet::from(["t440".to_string()]));
        // The GPU itself is still in scope — only the host key is skipped.
        assert_eq!(gpus_in_scope(&mixed, Scope::Fleet, None).len(), 2);
    }

    #[test]
    fn no_host_key_impl_matches_python_getattr_none() {
        let g = vec![NoHostKey { id: "x".into() }];
        assert!(hosts_in_scope(&g, Scope::Fleet, None).is_empty());
        assert_eq!(gpus_in_scope(&g, Scope::Fleet, None).len(), 1);
    }

    #[test]
    fn empty_input_is_empty_not_an_error() {
        let empty: Vec<G> = Vec::new();
        assert!(hosts_in_scope(&empty, Scope::Fleet, None).is_empty());
        assert!(gpus_in_scope(&empty, Scope::Local, Some("t440")).is_empty());
    }

    #[test]
    fn default_scope_is_fleet_so_unscoped_matches_scoped() {
        assert_eq!(Scope::default(), Scope::Fleet);
        let binding = snap();
        let unscoped = gpus_in_scope(&binding, Scope::default(), None);
        let explicit = gpus_in_scope(&binding, Scope::Fleet, None);
        assert_eq!(unscoped.len(), explicit.len());
    }

    #[test]
    fn scope_parse_round_trips() {
        assert_eq!(Scope::parse("local"), Some(Scope::Local));
        assert_eq!(Scope::parse("FLEET"), Some(Scope::Fleet));
        assert_eq!(Scope::parse(" fleet "), Some(Scope::Fleet));
        assert_eq!(Scope::parse("nonsense"), None);
    }
}
