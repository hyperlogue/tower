//! Rule evaluation has no I/O. Local and received rules are evaluated separately.
use tower_protocol::Rule;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Ask,
    Deny,
}
#[derive(Clone, Copy, Debug)]
pub struct Subject<'a> {
    pub id: &'a str,
    pub harness: Option<&'a str>,
    pub cwd: Option<&'a str>,
}
pub fn matches(selector: &str, subject: Subject<'_>) -> bool {
    if selector == "*" {
        true
    } else if let Some(h) = selector.strip_prefix("harness:") {
        subject.harness == Some(h)
    } else if let Some(p) = selector.strip_prefix("cwd:") {
        subject
            .cwd
            .is_some_and(|cwd| tower_pal::path_prefix(cwd, p))
    } else {
        selector == subject.id
    }
}
pub fn evaluate(
    rules: &[Rule],
    from: Subject<'_>,
    to: Subject<'_>,
    fallback: Decision,
) -> Decision {
    rules
        .iter()
        .find(|r| matches(&r.from, from) && matches(&r.to, to))
        .map_or(fallback, |r| match r.action.as_str() {
            "allow" => Decision::Allow,
            "ask" => Decision::Ask,
            _ => Decision::Deny,
        })
}
pub fn combine(local: Decision, remote: Decision) -> Decision {
    use Decision::*;
    match (local, remote) {
        (Deny, _) | (_, Deny) => Deny,
        (Ask, _) | (_, Ask) => Ask,
        _ => Allow,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_tables_never_override_a_restriction() {
        use Decision::*;
        for a in [Allow, Ask, Deny] {
            for b in [Allow, Ask, Deny] {
                let d = combine(a, b);
                assert_eq!(d == Allow, a == Allow && b == Allow);
                assert_eq!(d == Deny, a == Deny || b == Deny);
            }
        }
        let s = Subject {
            id: "opaque-id",
            harness: Some("codex"),
            cwd: Some("~/work/xy"),
        };
        assert!(!matches("cwd:~/work/x", s));
        assert!(matches("harness:codex", s));
        assert!(matches("*", s));
        assert!(!matches("opaque-*", s));
    }
}
