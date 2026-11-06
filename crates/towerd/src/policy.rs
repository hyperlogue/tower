//! Rule evaluation has no I/O. Local and received rules are evaluated separately.
use tower_protocol::Rule;

pub const MESSAGE: &str = "message";
pub const SESSION: &str = "session";
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
        subject.harness.is_some_and(|s| wildcard(h, s))
    } else if let Some(p) = selector.strip_prefix("cwd:") {
        subject.cwd.is_some_and(|cwd| {
            if p.contains(['*', '?']) {
                wildcard(p, cwd)
            } else {
                tower_pal::path_prefix(cwd, p)
            }
        })
    } else {
        wildcard(selector, subject.id)
    }
}
fn wildcard(pattern: &str, text: &str) -> bool {
    let p: Vec<_> = pattern.chars().collect();
    let t: Vec<_> = text.chars().collect();
    let (mut i, mut j, mut star, mut retry) = (0, 0, None, 0);
    while j < t.len() {
        if i < p.len() && (p[i] == '?' || p[i] == t[j]) {
            i += 1;
            j += 1;
        } else if i < p.len() && p[i] == '*' {
            star = Some(i);
            i += 1;
            retry = j;
        } else if let Some(s) = star {
            retry += 1;
            j = retry;
            i = s + 1;
        } else {
            return false;
        }
    }
    while i < p.len() && p[i] == '*' {
        i += 1;
    }
    i == p.len()
}
/// `kind` selects local rules by kind; `None` applies every rule.
pub fn evaluate(
    rules: &[Rule],
    kind: Option<&str>,
    from: Subject<'_>,
    to: Subject<'_>,
    fallback: Decision,
) -> Decision {
    rules
        .iter()
        .filter(|r| kind.is_none_or(|k| r.kind.as_deref().unwrap_or(MESSAGE) == k))
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
        assert!(matches("opaque-*", s));
    }
}
