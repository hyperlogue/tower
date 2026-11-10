use crate::{
    events::{self, AgentEvent, EventKind},
    hook::HookSession,
    runtime::RuntimeState,
};
use std::collections::BTreeMap;

#[derive(Clone, Copy)]
enum Prompt {
    Approval,
    Decision,
}
fn prompt(value: &str) -> Option<Prompt> {
    match value {
        "approval" => Some(Prompt::Approval),
        "decision" => Some(Prompt::Decision),
        _ => None,
    }
}
#[derive(Default)]
pub(super) struct Hooks {
    underlying: Option<String>,
    parent: Option<Prompt>,
    children: BTreeMap<String, Prompt>,
}
impl Hooks {
    fn display(&self, state: &mut RuntimeState) {
        let mut prompts = self.parent.iter().chain(self.children.values()).peekable();
        if prompts.peek().is_some() {
            state.status = if prompts.any(|p| matches!(p, Prompt::Approval)) {
                "waiting_approval"
            } else {
                "waiting_input"
            }
            .into();
        }
    }
}
impl HookSession for Hooks {
    fn apply(&mut self, state: &mut RuntimeState, mut event: AgentEvent, now: u64) {
        if state.stop_requested || state.exit_code.is_some() {
            return;
        }
        if event.child {
            let (Some(id), Some(signal)) = (&event.native_id, &event.blocking_prompt) else {
                return;
            };
            if !events::native_id(id) {
                return;
            }
            match prompt(signal) {
                Some(value) if self.children.contains_key(id) || self.children.len() < 64 => {
                    self.children.insert(id.clone(), value);
                }
                None if signal == "none" => {
                    self.children.remove(id);
                }
                _ => return,
            }
            self.underlying.get_or_insert_with(|| state.status.clone());
            let before = state.status.clone();
            state.status = self.underlying.clone().unwrap();
            self.display(state);
            if state.status != before {
                state.seq = state.seq.saturating_add(1);
                state.updated_at = now;
            }
            return;
        }
        let before = state.status.clone();
        if let Some(underlying) = &self.underlying {
            state.status = underlying.clone();
        }
        let old_native = state.native.as_ref().map(|n| n.id.clone());
        let signal = event.blocking_prompt.clone();
        let ask = matches!(event.kind, EventKind::ToolStart | EventKind::Approval)
            && event.tool.as_deref() == Some("ask");
        if !ask && event.kind == EventKind::Approval && signal.as_deref() == Some("approval") {
            event.kind = EventKind::Metadata;
        }
        let seq = state.seq;
        events::apply(state, event, now);
        if state.seq == seq {
            state.status = before;
            return;
        }
        if old_native != state.native.as_ref().map(|n| n.id.clone()) {
            self.parent = None;
            self.children.clear();
        }
        if ask {
            state.status = "waiting_input".into();
            self.parent = None;
        } else if let Some(signal) = signal {
            if signal == "none" {
                self.parent = None;
            } else if let Some(prompt) = prompt(&signal) {
                self.parent = Some(prompt);
            }
        }
        self.underlying = Some(state.status.clone());
        self.display(state);
    }
}
