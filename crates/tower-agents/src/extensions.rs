use crate::Harness;
use std::path::Path;

fn transport(exe: &Path, h: Harness) -> String {
    let exe = serde_json::to_string(&exe.to_string_lossy()).expect("string");
    format!(
        r#"import {{ spawn }} from 'node:child_process';
const executable = {exe};
let pending = Promise.resolve();
let queued = 0;
function send(event, data) {{
  if (queued >= 128) return Promise.resolve();
  let body;
  try {{ body = JSON.stringify(data); }} catch {{ return Promise.resolve(); }}
  if (body.length > 65536) return Promise.resolve();
  queued++;
  const deliver = () => new Promise(resolve => {{
    let child, timer, done = false;
    const finish = () => {{ if (!done) {{ done = true; clearTimeout(timer); queued--; resolve(); }} }};
    const abort = () => {{ try {{ child?.kill('SIGKILL'); }} catch {{}} finish(); }};
    try {{ child = spawn(executable, ['hook', '{h}', event], {{stdio:['pipe','ignore','ignore']}}); }}
    catch {{ finish(); return; }}
    timer = setTimeout(abort, 1500);
    child.on('error', finish); child.on('close', finish); child.stdin.on('error', abort);
    try {{ child.stdin.end(body); }} catch {{ abort(); }}
  }});
  pending = pending.then(deliver, deliver);
  return pending;
}}
function safely(read) {{ try {{ return read(); }} catch {{ return undefined; }} }}
"#,
        h = h.id()
    )
}
pub fn extension(exe: &Path, h: Harness) -> String {
    let mut events = vec![
        ("session_start", "SessionStart"),
        ("before_agent_start", "UserPromptSubmit"),
        ("tool_execution_start", "PreToolUse"),
        ("tool_execution_end", "PostToolUse"),
        ("session_before_compact", "PreCompact"),
        ("session_compact", "PostCompact"),
    ];
    if h == Harness::Omp {
        events.extend([
            ("session_switch", "SessionStart"),
            ("tool_approval_requested", "PermissionRequest"),
            ("tool_approval_resolved", "PermissionResult"),
            ("agent_end", "Stop"),
        ]);
    } else {
        events.extend([
            ("session_info_changed", "metadata"),
            ("model_select", "metadata"),
            ("session_tree", "metadata"),
            ("agent_settled", "Stop"),
            ("session_compact_failed", "StopFailure"),
        ]);
    }
    let table = serde_json::to_string(&events).expect("static event names");
    format!(
        r#"{}
const events = {table};
export default function (api) {{
  for (const [name, forward] of events) {{
    try {{ api.on(name, (event, ctx) => {{
      const tokens = safely(() => ctx.getContextUsage()?.tokens);
      const running = safely(() => ctx.getAsyncJobSnapshot()?.running);
      return send(forward, {{
        session_id: safely(() => ctx.sessionManager.getSessionId()),
        session_file: safely(() => ctx.sessionManager.getSessionFile()),
        session_title: safely(() => api.getSessionName()), cwd: ctx?.cwd,
        tool_name: event?.toolName, tool_call_id: event?.toolCallId,
        will_continue: event?.willContinue, will_retry: event?.willRetry, aborted: event?.aborted,
        background_jobs: Array.isArray(running) ? running.length : undefined,
        context_tokens: Number.isFinite(tokens) && tokens >= 0 ? Math.round(tokens) : undefined,
        model: ctx?.model?.provider && ctx?.model?.id ? `${{ctx.model.provider}}/${{ctx.model.id}}` : ctx?.model?.id,
      }});
    }}); }} catch {{}}
  }}
}}
"#,
        transport(exe, h)
    )
}
pub fn opencode(exe: &Path) -> String {
    format!(
        r#"{}
export const Tower = async ({{directory}}) => {{
  const roots = new Set(), children = new Set();
  function facts(input) {{
    const props = input?.event?.properties ?? {{}};
    const info = props.info ?? {{}};
    const session = typeof info.directory === 'string';
    const id = info.sessionID ?? (session ? info.id : undefined) ?? props.sessionID ?? input?.sessionID;
    if (session && id) {{ (info.parentID ? children : roots).add(id); }}
    const tokens = info.role === 'assistant' && Number.isFinite(info.tokens?.input)
      ? info.tokens.input + (info.tokens.cache?.read ?? 0) + (info.tokens.cache?.write ?? 0) : undefined;
    const model = input?.model ?? info;
    return {{session_id: id, cwd: session ? info.directory : directory,
      session_title: session ? info.title : undefined, is_child: children.has(id),
      root_confirmed: roots.has(id), tool_name: input?.tool, tool_call_id: input?.callID,
      context_tokens: tokens, model: model.providerID && model.modelID ? `${{model.providerID}}/${{model.modelID}}` : undefined,
      aborted: props.error?.name === 'MessageAbortedError'}};
  }}
  const direct = name => (...args) => send(name, facts(args[0]));
  return {{
    'chat.message': direct('UserPromptSubmit'),
    'tool.execute.before': direct('PreToolUse'),
    'tool.execute.after': direct('PostToolUse'),
    'experimental.session.compacting': direct('PreCompact'),
    event: async (input) => {{
      const e = input?.event, p = e?.properties ?? {{}};
      let name = ({{'session.created':'SessionStart','session.updated':'metadata',
        'permission.asked':'PermissionRequest','permission.replied':'PermissionResult',
        'session.idle':'Stop','session.error':'StopFailure','session.compacted':'PostCompact'}})[e?.type];
      if (e?.type === 'session.status' && p.status?.type === 'idle') name = 'Stop';
      if (e?.type === 'message.updated' && p.info?.role === 'assistant' && p.info?.time?.completed) name = 'metadata';
      if (name) await send(name, facts(input));
    }}
  }};
}};
"#,
        transport(exe, Harness::Opencode)
    )
}
