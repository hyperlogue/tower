//! Program status reports (OSC 7501, Program Status Protocol rev 0.4).
//!
//! A program tells its terminal whether it is idle, working, done, blocked or
//! failed. The terminal keeps one record per reported id; a report replaces
//! its record completely.
use base64::Engine;
use tower_pool_proto::{Program, ProgramKind, ProgramState};

/// Terminals must hold at least 64 records and may hold 256.
const RECORDS: usize = 256;
const KEY: usize = 16;
const ID: usize = 128;
const SEGMENT: usize = 32;
const DEPTH: usize = 8;
const APP: usize = 32;
const TITLE: (usize, usize) = (256, 192);
const MESSAGE: (usize, usize) = (2732, 2048);

pub enum Report {
    /// The feature query, answered by the terminal.
    Query,
    /// A record was set or removed.
    Changed,
    /// Not a valid report; nothing was applied.
    Ignored,
}
#[derive(Default)]
pub struct Programs {
    /// Least recently updated first. The root record's id is empty.
    records: Vec<(String, Program)>,
}
fn name(value: &str, max: usize) -> bool {
    (1..=max).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.+-".contains(&b))
}
/// Decoded text, or `None` when the report must be discarded: too long, not
/// base64, not UTF-8, or containing a control character.
fn text(value: &str, (encoded, decoded): (usize, usize)) -> Option<String> {
    const BASE64: base64::engine::GeneralPurpose = base64::engine::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        base64::engine::GeneralPurposeConfig::new()
            .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
    );
    if value.len() > encoded {
        return None;
    }
    let text = String::from_utf8(BASE64.decode(value).ok()?).ok()?;
    (text.len() <= decoded && !text.chars().any(char::is_control)).then_some(text)
}
impl Programs {
    /// Applies the body of an OSC 7501 sequence, the bytes after `7501;`.
    pub fn report(&mut self, body: &[u8]) -> Report {
        let Ok(body) = std::str::from_utf8(body) else {
            return Report::Ignored;
        };
        if body.trim() == "?" {
            return Report::Query;
        }
        let (mut state, mut id, mut kind, mut progress) = (None, "", None, None);
        let (mut app, mut title, mut message) = (None, None, None);
        for pair in body.split(':') {
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };
            let (key, value) = (key.trim(), value.trim());
            if key.len() > KEY {
                return Report::Ignored;
            }
            if key.is_empty()
                || !key.bytes().all(|b| b.is_ascii_lowercase())
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.,+/=-".contains(&b))
            {
                continue;
            }
            // A repeated key keeps its last value; unknown keys are ignored.
            match key {
                "state" => state = Some(value),
                "id" => id = value,
                "kind" => kind = Some(value),
                "progress" => progress = Some(value),
                "app" => app = Some(value),
                "title" => title = Some(value),
                "msg" => message = Some(value),
                _ => {}
            }
        }
        if !id.is_empty()
            && (id.len() > ID
                || id.split('/').count() > DEPTH
                || !id.split('/').all(|segment| name(segment, SEGMENT)))
            || app.is_some_and(|app| app.len() > APP)
        {
            return Report::Ignored;
        }
        let state = match state {
            Some("idle") => ProgramState::Idle,
            Some("working") => ProgramState::Working,
            Some("done") => ProgramState::Done,
            Some("blocked") => ProgramState::Blocked,
            Some("error") => ProgramState::Error,
            Some("clear") => {
                // Removes the record and every record beneath it.
                let before = self.records.len();
                self.records.retain(|(record, _)| {
                    !(id.is_empty()
                        || record == id
                        || record
                            .strip_prefix(id)
                            .is_some_and(|rest| rest.starts_with('/')))
                });
                return if self.records.len() == before {
                    Report::Ignored
                } else {
                    Report::Changed
                };
            }
            _ => return Report::Ignored,
        };
        // Absent text is fine; text that cannot be used discards the report.
        let decoded = |value: Option<&str>, limit| match value {
            Some(value) => text(value, limit).map(Some),
            None => Some(None),
        };
        let (Some(title), Some(message)) = (decoded(title, TITLE), decoded(message, MESSAGE))
        else {
            return Report::Ignored;
        };
        let active = matches!(state, ProgramState::Working | ProgramState::Blocked);
        let program = Program {
            state,
            kind: kind
                .filter(|_| state == ProgramState::Blocked)
                .and_then(|kind| match kind {
                    "permission" => Some(ProgramKind::Permission),
                    "question" => Some(ProgramKind::Question),
                    "auth" => Some(ProgramKind::Auth),
                    _ => None,
                }),
            progress: progress
                .filter(|_| active)
                .and_then(|v| v.parse().ok())
                .filter(|v| *v <= 100),
            app: app.filter(|app| name(app, APP)).map(str::to_owned),
            title,
            message,
        };
        let existing = self.records.iter().position(|(record, _)| record == id);
        if existing.is_some_and(|at| self.records[at].1 == program) {
            return Report::Ignored;
        }
        if let Some(at) = existing {
            self.records.remove(at);
        } else if self.records.len() == RECORDS {
            self.records.remove(0);
        }
        self.records.push((id.to_owned(), program));
        Report::Changed
    }
    /// The program exited or its shell printed a new prompt: work in
    /// progress is over, while results stay until replaced or cleared.
    pub fn finish(&mut self) -> bool {
        let before = self.records.len();
        self.records.retain(|(_, program)| {
            matches!(program.state, ProgramState::Done | ProgramState::Error)
        });
        self.records.len() != before
    }
    /// The status of the whole terminal: the record that most needs its
    /// user, and of equals the most recently reported.
    pub fn summary(&self) -> Option<&Program> {
        let urgency = |state| match state {
            ProgramState::Blocked => 4,
            ProgramState::Working => 3,
            ProgramState::Error => 2,
            ProgramState::Done => 1,
            ProgramState::Idle => 0,
        };
        self.records
            .iter()
            .enumerate()
            .max_by_key(|(at, (_, program))| (urgency(program.state), *at))
            .map(|(_, (_, program))| program)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn state(programs: &Programs) -> Option<ProgramState> {
        programs.summary().map(|p| p.state)
    }
    #[test]
    fn reports_replace_their_record_and_clear_removes_a_subtree() {
        let mut p = Programs::default();
        assert!(matches!(p.report(b"?"), Report::Query));
        assert!(matches!(
            p.report(b"state=working:app=cargo:progress=40:msg=YnVpbGRpbmc="),
            Report::Changed
        ));
        let root = p.summary().unwrap();
        assert_eq!(
            (root.app.as_deref(), root.progress, root.message.as_deref()),
            (Some("cargo"), Some(40), Some("building"))
        );
        // The same report again changes nothing; a new one drops omitted keys.
        assert!(matches!(
            p.report(b"state=working:app=cargo:progress=40:msg=YnVpbGRpbmc="),
            Report::Ignored
        ));
        p.report(b"state=working");
        assert_eq!(p.summary().unwrap().app, None);

        p.report(b"state=blocked:kind=permission:id=deploy/us-east");
        p.report(b"state=done:id=deploy/eu");
        p.report(b"state=idle");
        let top = p.summary().unwrap();
        assert_eq!(
            (top.state, top.kind),
            (ProgramState::Blocked, Some(ProgramKind::Permission))
        );
        p.report(b"state=clear:id=deploy");
        assert_eq!(state(&p), Some(ProgramState::Idle));
        p.report(b"state=clear");
        assert_eq!(state(&p), None);
    }
    #[test]
    fn invalid_reports_apply_nothing_and_invalid_pairs_are_skipped() {
        let mut p = Programs::default();
        p.report(b"state=working");
        for report in [
            b"".as_slice(),
            b"app=cargo",
            b"state=paused",
            b"state=done:id=bad,id",
            b"state=done:id=a//b",
            b"state=done:id=1/2/3/4/5/6/7/8/9",
            b"state=done:msg=a",
            b"state=done:msg=YQph",
            b"state=done:title=//79",
            b"state=done:abcdefghijklmnopq=1",
            b"state=done:app=a-name-that-is-longer-than-thirty-two-bytes",
        ] {
            assert!(matches!(p.report(report), Report::Ignored), "{report:?}");
            assert_eq!(state(&p), Some(ProgramState::Working));
        }
        // Unknown keys, malformed pairs and unusable optional values are
        // dropped without discarding the report. Padding is optional.
        assert!(matches!(
            p.report(b" state = blocked :kind=sudo:progress=101:app=my app:novalue:=x:STATE=idle:future=1:msg=aGk"),
            Report::Changed
        ));
        let top = p.summary().unwrap();
        assert_eq!(top.state, ProgramState::Blocked);
        assert_eq!(
            (top.kind, top.progress, top.app.as_deref()),
            (None, None, None)
        );
        assert_eq!(top.message.as_deref(), Some("hi"));
        // The last value of a repeated key wins; kind and progress apply
        // only to the states that define them.
        p.report(b"state=working:state=done:kind=auth:progress=5");
        let top = p.summary().unwrap();
        assert_eq!(
            (top.state, top.kind, top.progress),
            (ProgramState::Done, None, None)
        );
    }
    #[test]
    fn finishing_drops_work_in_progress_and_keeps_results() {
        let mut p = Programs::default();
        p.report(b"state=error:id=test");
        p.report(b"state=working");
        p.report(b"state=idle:id=watch");
        assert!(p.finish());
        assert_eq!(state(&p), Some(ProgramState::Error));
        assert!(!p.finish());
        // The oldest record makes room for a new one.
        for n in 0..RECORDS {
            p.report(format!("state=done:id=job{n}").as_bytes());
        }
        assert_eq!(p.records.len(), RECORDS);
        assert!(p.records.iter().all(|(id, _)| id != "test"));
    }
}
