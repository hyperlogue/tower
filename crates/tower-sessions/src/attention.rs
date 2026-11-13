//! When a session begins and stops needing its user. The daemon's registry
//! and a launcher with no registry behind it both follow these rules.

/// What seeing a session move from `previous` to `status` does to its need
/// for attention: `Some(true)` raises it, `Some(false)` clears it, `None`
/// leaves it. `stopping` means its end was requested.
pub fn on_status(previous: &str, status: &str, stopping: bool) -> Option<bool> {
    if status == "unknown" || previous == status {
        return None;
    }
    if matches!(status, "starting" | "working") || stopping && matches!(status, "exited" | "lost") {
        Some(false)
    } else if matches!(
        status,
        "waiting_input" | "waiting_approval" | "lost" | "exited"
    ) || status == "idle" && previous == "working"
    {
        Some(true)
    } else {
        None
    }
}

/// A session reporting an error it did not report before needs attention.
pub fn on_error(before: Option<&str>, after: Option<&str>) -> bool {
    let after = after.filter(|e| !e.is_empty());
    after.is_some() && after != before.filter(|e| !e.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn waits_finished_turns_and_unrequested_ends_raise_and_work_clears() {
        assert_eq!(on_status("starting", "idle", false), None);
        assert_eq!(on_status("working", "idle", false), Some(true));
        assert_eq!(on_status("idle", "idle", false), None);
        assert_eq!(on_status("idle", "unknown", false), None);
        assert_eq!(on_status("idle", "working", false), Some(false));
        assert_eq!(on_status("working", "waiting_approval", false), Some(true));
        assert_eq!(on_status("working", "exited", false), Some(true));
        assert_eq!(on_status("working", "exited", true), Some(false));
        assert!(on_error(None, Some("agent_turn_failed")));
        assert!(!on_error(
            Some("agent_turn_failed"),
            Some("agent_turn_failed")
        ));
        assert!(!on_error(Some("agent_turn_failed"), Some("")));
    }
}
