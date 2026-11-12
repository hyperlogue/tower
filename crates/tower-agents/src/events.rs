use crate::{
    Harness,
    runtime::{NativeSession, RuntimeState},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Ready,
    Working,
    Compacting,
    Compacted,
    ToolStart,
    ToolEnd,
    Approval,
    ApprovalResolved,
    Idle,
    End,
    Metadata,
    Failure,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentEvent {
    pub kind: EventKind,
    pub native_id: Option<String>,
    pub resume_ref: Option<String>,
    #[serde(default)]
    pub transcript_path: Option<String>,
    pub cwd: Option<String>,
    pub title: Option<String>,
    pub tool: Option<String>,
    pub tool_id: Option<String>,
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub clear_effort: bool,
    #[serde(default)]
    pub clear_context: bool,
    #[serde(default)]
    pub coarse_status: bool,
    #[serde(default)]
    pub reset_on_session_change: bool,
    pub context_tokens: Option<u64>,
    pub child: bool,
    #[serde(default)]
    pub root_confirmed: bool,
    #[serde(default)]
    pub resumable: Option<bool>,
    pub background_jobs: Option<u32>,
    #[serde(default)]
    pub background_active: Option<bool>,
    #[serde(default)]
    pub blocking_prompt: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    /// `Some` names every live background job; `None` reports nothing.
    #[serde(default)]
    pub background_commands: Option<Vec<String>>,
    pub continuing: bool,
    pub authoritative_status: Option<String>,
}
pub(crate) fn text(value: &Value, paths: &[&str], max: usize) -> Option<String> {
    paths
        .iter()
        .find_map(|path| value.pointer(path).and_then(Value::as_str))
        .filter(|v| !v.is_empty())
        .map(|s| s.chars().filter(|c| !c.is_control()).take(max).collect())
        .filter(|s: &String| !s.is_empty())
}
const PROMPT_CHARS: usize = 500;
const COMMAND_CHARS: usize = 200;
const COMMANDS: usize = 16;
/// One display line: whitespace and control runs become single spaces.
pub(crate) fn line(text: &str, max: usize) -> Option<String> {
    let mut words = text.split(|c: char| c.is_whitespace() || c.is_control());
    let joined = words
        .by_ref()
        .filter(|w| !w.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    (!joined.is_empty()).then(|| joined.chars().take(max).collect())
}
/// The commands of live background jobs, bounded for a state file.
pub(crate) fn commands<'a>(found: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut commands: Vec<_> = found.filter_map(|c| line(c, COMMAND_CHARS)).collect();
    commands.sort();
    commands.dedup();
    commands.truncate(COMMANDS);
    commands
}
pub(crate) fn count(value: &Value, paths: &[&str]) -> Option<u64> {
    paths
        .iter()
        .find_map(|p| value.pointer(p).and_then(Value::as_u64))
}
pub fn native_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphanumeric())
        && value.len() <= 512
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.:".contains(&b))
}
fn event_kind(name: &str) -> Option<EventKind> {
    Some(match name {
        "SessionStart" | "session-start" => EventKind::Ready,
        "UserPromptSubmit" | "TurnStarted" | "PreInvocation" | "prompt-submit" | "working" => {
            EventKind::Working
        }
        "PreToolUse" | "pre-tool-use" => EventKind::ToolStart,
        "PostToolUse" | "PostToolUseFailure" | "post-tool-use" | "post-tool-use-failure" => {
            EventKind::ToolEnd
        }
        "PermissionRequest" | "permission-request" | "Elicitation" => EventKind::Approval,
        "PermissionResult" | "ElicitationResult" | "permission-resolved" | "elicitation-result" => {
            EventKind::ApprovalResolved
        }
        "Stop" | "Interrupt" | "StopCancelled" | "stop" => EventKind::Idle,
        "SessionEnd" | "session-end" => EventKind::End,
        "PreCompact" | "pre-compact" => EventKind::Compacting,
        "PostCompact" | "post-compact" => EventKind::Compacted,
        "model-switch" | "status-update" | "cwd-changed" | "metadata" | "PostModelSwitch" => {
            EventKind::Metadata
        }
        "StopFailure" | "stop-failure" => EventKind::Failure,
        _ => return None,
    })
}
pub fn parse(harness: Harness, name: &str, bytes: &[u8]) -> Result<AgentEvent, &'static str> {
    if bytes.len() > 1024 * 1024 {
        return Err("hook_too_large");
    }
    let value: Value = serde_json::from_slice(bytes).map_err(|_| "invalid_hook")?;
    let mut kind = event_kind(name).ok_or("unknown_event")?;
    let id = [
        "/session_id",
        "/sessionId",
        "/sessionID",
        "/conversationId",
        "/conversation_id",
        "/payload/0/event/properties/sessionID",
        "/payload/0/sessionID",
    ]
    .iter()
    .find_map(|p| value.pointer(p).and_then(Value::as_str))
    .filter(|s| native_id(s))
    .map(str::to_owned);
    let mut event = AgentEvent {
        kind,
        native_id: id,
        resume_ref: text(&value, &["/session_file", "/resume_ref"], 4096),
        cwd: text(
            &value,
            &["/cwd", "/workspace_root", "/directory", "/workspacePaths/0"],
            4096,
        ),
        title: text(
            &value,
            &[
                "/session_title",
                "/title",
                "/payload/0/event/properties/info/title",
            ],
            200,
        ),
        tool: text(
            &value,
            &[
                "/tool_name",
                "/toolName",
                "/toolCall/name",
                "/payload/0/tool",
            ],
            128,
        ),
        tool_id: text(
            &value,
            &[
                "/tool_use_id",
                "/tool_call_id",
                "/toolCallId",
                "/toolUseId",
                "/toolCall/id",
            ],
            256,
        ),
        model: text(
            &value,
            &[
                "/model_id",
                "/to_model",
                "/modelId",
                "/model_name",
                "/modelName",
                "/model/id",
                "/model",
            ],
            128,
        ),
        effort: text(
            &value,
            &["/effort/level", "/effort", "/reasoningEffort"],
            64,
        )
        .filter(|s| !s.trim().is_empty()),
        clear_effort: false,
        clear_context: false,
        coarse_status: false,
        reset_on_session_change: harness
            .definition()
            .status_behavior()
            .reset_on_session_change,
        transcript_path: None,
        context_tokens: count(&value, &["/context_tokens", "/contextTokens"]),
        child: value.get("is_child").and_then(Value::as_bool) == Some(true)
            || value.get("session_is_child").and_then(Value::as_bool) == Some(true)
            || text(
                &value,
                &[
                    "/subagent_type",
                    "/subagentType",
                    "/parent_session_id",
                    "/parentSessionId",
                ],
                512,
            )
            .is_some()
            || text(&value, &["/transcript_path"], 4096)
                .is_some_and(|p| p.split('/').any(|part| part == "subagents")),
        root_confirmed: value.get("root_confirmed").and_then(Value::as_bool) == Some(true)
            || (kind == EventKind::Ready
                && harness.definition().status_behavior().session_start_is_root),
        background_active: None,
        background_jobs: count(&value, &["/background_jobs"]).and_then(|n| n.try_into().ok()),
        blocking_prompt: None,
        prompt: matches!(name, "UserPromptSubmit" | "prompt-submit")
            .then(|| value.get("prompt").and_then(Value::as_str))
            .flatten()
            .and_then(|prompt| line(prompt, PROMPT_CHARS)),
        background_commands: None,
        continuing: value.get("will_continue").and_then(Value::as_bool) == Some(true)
            || value.get("will_retry").and_then(Value::as_bool) == Some(true),
        authoritative_status: None,
        resumable: value.get("ephemeral").and_then(Value::as_bool).map(|b| !b),
    };
    harness
        .definition()
        .normalize_hook(name, &value, &mut event);
    if name == "Elicitation" {
        event.authoritative_status = Some("waiting_input".into());
    }
    kind = event.kind;
    if matches!(name, "stop-failure" | "StopFailure")
        && value.get("aborted").and_then(Value::as_bool) == Some(true)
    {
        kind = EventKind::Idle;
    }
    if event.continuing && matches!(kind, EventKind::Idle | EventKind::Failure) {
        kind = EventKind::Working;
    }
    event.kind = kind;
    Ok(event)
}

pub fn apply(state: &mut RuntimeState, mut event: AgentEvent, now: u64) {
    if event
        .native_id
        .as_ref()
        .is_some_and(|id| state.private.get("fork_parent").and_then(Value::as_str) == Some(id))
    {
        return;
    }
    if event.native_id.as_ref().is_some_and(|s| !native_id(s)) {
        return;
    }
    let clean = |value: &mut Option<String>, limit: usize| {
        if let Some(s) = value {
            *s = s.chars().filter(|c| !c.is_control()).take(limit).collect();
        }
    };
    clean(&mut event.title, 200);
    clean(&mut event.model, 128);
    clean(&mut event.effort, 64);
    clean(&mut event.tool, 128);
    clean(&mut event.tool_id, 256);
    event.prompt = event.prompt.and_then(|p| line(&p, PROMPT_CHARS));
    event.background_commands = event
        .background_commands
        .map(|found| commands(found.iter().map(String::as_str)));
    if event
        .cwd
        .as_ref()
        .is_some_and(|s| s.len() > 4096 || s.chars().any(char::is_control))
    {
        event.cwd = None;
    }
    if event.resume_ref.as_ref().is_some_and(|s| {
        s.len() > 4096 || !std::path::Path::new(s).is_absolute() || s.chars().any(char::is_control)
    }) {
        event.resume_ref = None;
    }
    if event.authoritative_status.as_deref().is_some_and(|s| {
        !matches!(
            s,
            "starting" | "idle" | "working" | "waiting_input" | "waiting_approval" | "unknown"
        )
    }) {
        event.authoritative_status = None;
    }
    if event.child || state.exit_code.is_some() || state.stop_requested {
        return;
    }
    if let Some(id) = event.native_id {
        if state.native.as_ref().is_some_and(|n| n.id != id)
            && !(event.root_confirmed
                && matches!(event.kind, EventKind::Ready | EventKind::Working))
        {
            return;
        }
        if event.reset_on_session_change
            && event.kind == EventKind::Ready
            && state.native.as_ref().is_some_and(|old| old.id != id)
        {
            state.detail = Default::default();
            state.display_name = None;
            state.status = "idle".into();
            state.private.remove("waiting_tool");
        }
        state.native = Some(NativeSession {
            id: id.clone(),
            resume_ref: event.resume_ref.or_else(|| {
                state
                    .native
                    .as_ref()
                    .filter(|n| n.id == id)
                    .and_then(|n| n.resume_ref.clone())
            }),
        });
    }
    if let Some(resumable) = event.resumable {
        state
            .private
            .insert("native_resumable".into(), resumable.into());
    }
    if let Some(cwd) = event
        .cwd
        .filter(|cwd| std::path::Path::new(cwd).is_absolute())
    {
        state.cwd = cwd;
    }
    if let Some(title) = event.title {
        state.display_name = Some(title);
    }
    if let Some(model) = event.model {
        state.detail.model = Some(model);
    }
    if event.clear_effort {
        state.detail.effort = None;
    }
    if let Some(effort) = event.effort {
        state.detail.effort = Some(effort);
    }
    if event.clear_context {
        state.detail.context_tokens = None;
    }
    if let Some(tokens) = event.context_tokens {
        state.detail.context_tokens = Some(tokens);
    }
    if let Some(tool) = &event.tool {
        state.detail.last_tool = Some(tool.clone());
    }
    if event.background_active == Some(false) {
        state.detail.background_jobs = Some(0);
    }
    if event.background_active == Some(true) && state.detail.background_jobs == Some(0) {
        state.detail.background_jobs = None;
    }
    if let Some(jobs) = event.background_jobs {
        state.detail.background_jobs = Some(jobs);
    }
    if let Some(prompt) = event.prompt {
        state.detail.last_prompt = Some(prompt);
    }
    if let Some(commands) = event.background_commands {
        state.detail.background_commands = commands;
    }
    let waiting = |state: &RuntimeState| {
        matches!(state.status.as_str(), "waiting_input" | "waiting_approval")
    };
    match event.kind {
        EventKind::Ready if matches!(state.status.as_str(), "starting" | "unknown") => {
            state.status = "idle".into()
        }
        EventKind::Compacting => {
            if !waiting(state) {
                state.status = "working".into();
            }
            state.detail.phase = Some("compacting".into());
            state.detail.context_tokens = None;
        }
        EventKind::Compacted => {
            if !waiting(state) {
                state.status = "idle".into();
            }
            state.detail.phase = Some("compacted".into());
            state.detail.context_tokens = event.context_tokens;
        }
        EventKind::Working => {
            state.detail.phase = None;
            state.status = "working".into();
            state.detail.error = None;
            state.private.remove("waiting_tool");
        }
        EventKind::ToolStart => {
            if event.tool.as_deref().is_some_and(|t| {
                matches!(
                    t,
                    "AskUserQuestion" | "ask" | "ask_user_question" | "question"
                )
            }) {
                state.status = "waiting_input".into();
                state.private.insert(
                    "waiting_tool".into(),
                    serde_json::json!(event.tool_id.or(event.tool)),
                );
            } else if !waiting(state) {
                state.status = "working".into();
            }
        }
        EventKind::ToolEnd => {
            let key = event.tool_id.or(event.tool);
            let matches = state
                .private
                .get("waiting_tool")
                .and_then(Value::as_str)
                .is_some_and(|w| key.as_deref() == Some(w));
            if !waiting(state) || matches {
                state.status = "working".into();
                state.private.remove("waiting_tool");
            }
        }
        EventKind::Approval => {
            state.status = if event.tool.as_deref() == Some("AskUserQuestion") {
                "waiting_input"
            } else {
                "waiting_approval"
            }
            .into();
            state.private.insert(
                "waiting_tool".into(),
                serde_json::json!(event.tool_id.or(event.tool)),
            );
        }
        EventKind::ApprovalResolved => {
            state.status = "working".into();
            state.private.remove("waiting_tool");
        }
        EventKind::Idle | EventKind::Failure => {
            let background = event
                .background_active
                .unwrap_or(state.detail.background_jobs.unwrap_or(0) > 0);
            state.status = if background { "working" } else { "idle" }.into();
            state.detail.phase = background.then(|| "background".into());
            state.private.remove("waiting_tool");
            if event.kind == EventKind::Failure {
                state.detail.error = Some("agent_turn_failed".into());
            }
        }
        EventKind::Metadata | EventKind::Ready | EventKind::End => {}
    }
    if state.detail.phase.as_deref() != Some("background") {
        state.detail.background_commands.clear();
    }
    if let Some(status) = event.authoritative_status
        && !(event.coarse_status && state.status == "waiting_input" && status != "waiting_approval")
    {
        state.status = status;
    }
    state.seq = state.seq.saturating_add(1);
    state.updated_at = now;
}
