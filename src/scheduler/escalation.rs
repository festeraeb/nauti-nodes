//! 002c — escalation. Port of `cesaropsserver/fleet/policy.py` (F3).
//!
//! A **HARD** constraint is one where no amount of slot fallback on this node
//! can help: the model does not fit in VRAM, or the host is unavailable/locked.
//! A **PREFERENCE** is anything else (an unhealthy sibling slot, an ordering
//! choice, contention).
//!
//! `escalate` must never fire on a preference. Escalating on a preference would
//! push routine reordering to the head and turn the local plan into a
//! network round-trip for something the node could have decided itself.
//!
//! Ported as a pure predicate + record builder rather than the Python version's
//! closure over `_can_fit_model_global`, because that fit check is a Rust-side
//! concern (see [`EscalationInput::fit_fails`]); keeping the *classification*
//! here lets it be tested without a GPU model registry.

/// Reason prefixes that mark a hard constraint.
///
/// Lowercased and prefix-matched, exactly as Python does with
/// `HARD_CONSTRAINT_PREFIXES`. Note the spacing: `"host "` has a trailing space
/// on purpose so it matches `"host locked"` and not `"hostile"`.
pub const HARD_CONSTRAINT_PREFIXES: [&str; 3] = ["unknown host", "no vram estimate", "host "];

/// True only when `reason` describes an unworkable constraint on this node.
///
/// Preference-level failures (an unhealthy sibling slot, an ordering choice)
/// must return `false` so the local plan is still attempted first.
pub fn is_hard_constraint_violation(reason: &str) -> bool {
    let r = reason.trim().to_lowercase();
    HARD_CONSTRAINT_PREFIXES.iter().any(|p| r.starts_with(p))
}

/// What the node knows about its own failed local plan.
#[derive(Debug, Clone, Default)]
pub struct EscalationInput {
    /// Node asking for help.
    pub node_id: String,
    /// The role that could not be placed.
    pub role: String,
    /// An explicit constraint named by the caller. When set, it is classified
    /// rather than re-derived.
    pub constraint: String,
    /// Human-readable detail, shown to the operator.
    pub detail: String,
    /// Model the local plan tried to place. Empty means "nothing to fit",
    /// which is not an escalation on its own.
    pub model_id: String,
    /// Host the plan targeted; defaults to `node_id`.
    pub host_key: String,
    /// Whether the VRAM fit check failed, when the caller ran one. `None`
    /// means "not checked" and suppresses escalation.
    pub fit_fails: Option<bool>,
}

/// The escalation record. Field names mirror the Python dict so the head
/// endpoint can consume either without translation.
///
/// `PartialEq` only (not `Eq`) because `asked_at` is an `f64`.
#[derive(Debug, Clone, PartialEq)]
pub struct EscalationRecord {
    pub node_id: String,
    pub role: String,
    pub constraint: String,
    pub detail: String,
    pub asked_at: f64,
}

/// Decide whether a failed local plan must escalate, and build the record.
///
/// Returns `None` when the local plan is workable — including when the caller
/// supplied no fit result, mirroring Python's early `return None` for callers
/// that pass no snapshot. Callers that pass nothing therefore keep exactly the
/// behaviour they had before escalation existed.
pub fn escalate(input: &EscalationInput, now: f64) -> Option<EscalationRecord> {
    let (constraint, detail, hard) = if !input.constraint.is_empty() {
        let hard = is_hard_constraint_violation(&input.constraint);
        (input.constraint.clone(), input.detail.clone(), hard)
    } else {
        // Derived from the VRAM fit check for the planned model.
        if input.model_id.is_empty() {
            return None;
        }
        // Not checked, or checked and it fits: no escalation.
        match input.fit_fails {
            Some(false) | None => return None,
            Some(true) => {}
        }
        let reason = if input.detail.is_empty() {
            format!("no vram estimate for {}", input.model_id)
        } else {
            input.detail.clone()
        };
        let hard = is_hard_constraint_violation(&reason);
        (reason.clone(), reason, hard)
    };

    if !hard {
        return None;
    }

    Some(EscalationRecord {
        node_id: input.node_id.clone(),
        role: input.role.clone(),
        constraint,
        detail,
        asked_at: now,
    })
}
#[cfg(test)]
mod tests {
    use super::*;

    fn inp() -> EscalationInput {
        EscalationInput {
            node_id: "t440".into(),
            role: "coding".into(),
            ..Default::default()
        }
    }

    #[test]
    fn hard_constraint_prefixes_are_classified() {
        assert!(is_hard_constraint_violation("unknown host t440"));
        assert!(is_hard_constraint_violation("no vram estimate for KAT-Coder"));
        assert!(is_hard_constraint_violation("host locked"));
        assert!(is_hard_constraint_violation("  UNKNOWN HOST x  "), "case/space tolerant");
    }

    #[test]
    fn preferences_are_not_hard() {
        // The whole point of the split: routine failures must NOT escalate.
        assert!(!is_hard_constraint_violation("no healthy slot"));
        assert!(!is_hard_constraint_violation("prefer cheaper host"));
        assert!(!is_hard_constraint_violation("contended"));
        assert!(!is_hard_constraint_violation("hostile naming"), "prefix needs the space");
    }

    #[test]
    fn explicit_hard_constraint_escalates() {
        let mut i = inp();
        i.constraint = "host locked".into();
        i.detail = "t440 is locked for maintenance".into();
        let r = escalate(&i, 100.0).expect("hard constraint must escalate");
        assert_eq!(r.node_id, "t440");
        assert_eq!(r.role, "coding");
        assert_eq!(r.constraint, "host locked");
        assert_eq!(r.detail, "t440 is locked for maintenance");
        assert_eq!(r.asked_at, 100.0);
    }

    #[test]
    fn explicit_preference_never_escalates() {
        let mut i = inp();
        i.constraint = "no healthy slot".into();
        assert!(escalate(&i, 100.0).is_none());
    }

    #[test]
    fn fit_failure_escalates_only_when_the_reason_is_hard() {
        let mut i = inp();
        i.model_id = "KAT-Coder-V2.5".into();
        i.fit_fails = Some(true);
        i.detail = "no vram estimate for KAT-Coder-V2.5".into();
        let r = escalate(&i, 1.0).expect("a failed fit with a hard reason escalates");
        assert_eq!(r.constraint, "no vram estimate for KAT-Coder-V2.5");
    }

    #[test]
    fn a_fit_that_passes_never_escalates() {
        let mut i = inp();
        i.model_id = "KAT".into();
        i.fit_fails = Some(false);
        assert!(escalate(&i, 1.0).is_none());
    }

    #[test]
    fn an_unchecked_fit_never_escalates() {
        // Mirrors Python: callers that pass no snapshot keep today's behaviour.
        let mut i = inp();
        i.model_id = "KAT".into();
        i.fit_fails = None;
        assert!(escalate(&i, 1.0).is_none());
    }

    #[test]
    fn no_model_and_no_constraint_never_escalates() {
        let i = inp(); // nothing set at all
        assert!(escalate(&i, 1.0).is_none());
    }

    #[test]
    fn record_shape_matches_the_python_dict() {
        let mut i = inp();
        i.constraint = "unknown host c2".into();
        let r = escalate(&i, 42.5).unwrap();
        // {node_id, role, constraint, detail, asked_at}
        assert_eq!(
            vec!["node_id", "role", "constraint", "detail", "asked_at"],
            ["node_id", "role", "constraint", "detail", "asked_at"]
        );
        assert_eq!(r.asked_at, 42.5);
        assert!(!r.constraint.is_empty());
    }
}
