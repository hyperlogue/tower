use crate::{Harness, runtime::AgentSpec};
use serde_json::{Value, json};
use std::{
    io,
    path::{Path, PathBuf},
};
use tower_pal::{Environment, read_config, write_private};

pub struct Invocation {
    pub argv: Vec<String>,
    pub env: Environment,
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}
fn hook(exe: &Path, h: Harness, event: &str) -> String {
    format!(
        "if [ -n \"${{TOWER_LAUNCHER_SOCKET:-${{TOWER_WRAPPER:-}}}}\" ]; then {} hook {} {}; fi; :",
        shell_quote(&exe.to_string_lossy()),
        h.id(),
        event
    )
}
fn home(env: &Environment, key: &str, fallback: &str) -> io::Result<PathBuf> {
    let path = env.get(key).map(PathBuf::from).unwrap_or_else(|| {
        Path::new(env.get("HOME").map(String::as_str).unwrap_or("")).join(fallback)
    });
    if !path.is_absolute() {
        return Err(invalid("harness home must be absolute"));
    }
    Ok(path)
}
fn json_file(path: &Path) -> io::Result<Value> {
    let value = read_config(path)?
        .map(|s| serde_json::from_str(&s))
        .transpose()?
        .unwrap_or_else(|| json!({}));
    if !value.is_object() {
        return Err(invalid("harness configuration must be an object"));
    }
    Ok(value)
}
fn save_json(path: &Path, value: &Value) -> io::Result<()> {
    write_private(path, &serde_json::to_vec_pretty(value)?)
}
// Never write through a mirrored configuration file. Unknown state stays native.
fn mirror(
    real: &Path,
    overlay: &Path,
    owned: &[&str],
    seed_dirs: &[&str],
    seed_files: &[&str],
) -> io::Result<()> {
    tower_pal::ensure_user_dir(real)?;
    tower_pal::private_dir(overlay)?;
    for name in seed_dirs {
        tower_pal::ensure_user_dir(&real.join(name))?;
    }
    let mut names = std::fs::read_dir(real)?
        .map(|e| e.map(|e| e.file_name()))
        .collect::<io::Result<Vec<_>>>()?;
    names.extend(seed_files.iter().map(std::ffi::OsString::from));
    names.sort();
    names.dedup();
    for name in names {
        if owned.iter().any(|s| name == *s) {
            continue;
        }
        let dest = overlay.join(&name);
        match std::fs::symlink_metadata(&dest) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                tower_pal::symlink(&real.join(name), &dest)?
            }
            Ok(m)
                if m.file_type().is_symlink() && std::fs::read_link(&dest)? == real.join(name) => {}
            _ => return Err(invalid("unexpected entry in managed harness home")),
        }
    }
    Ok(())
}
fn extend_events(config: &mut Value, events: Value) -> io::Result<()> {
    let hooks = config
        .as_object_mut()
        .ok_or_else(|| invalid("invalid hook config"))?
        .entry("hooks")
        .or_insert(json!({}));
    let hooks = hooks
        .as_object_mut()
        .ok_or_else(|| invalid("hooks must be an object"))?;
    for (event, group) in events
        .as_object()
        .ok_or_else(|| invalid("invalid generated hooks"))?
    {
        let array = hooks
            .entry(event)
            .or_insert(json!([]))
            .as_array_mut()
            .ok_or_else(|| invalid("hook event must be an array"))?;
        array.extend(
            group
                .as_array()
                .ok_or_else(|| invalid("invalid generated group"))?
                .clone(),
        );
    }
    Ok(())
}
const COMMON: &[&str] = &[
    "SessionStart",
    "UserPromptSubmit",
    "PreToolUse",
    "PostToolUse",
    "PostToolUseFailure",
    "PermissionRequest",
    "Stop",
    "StopFailure",
    "PreCompact",
];
fn nested(exe: &Path, h: Harness, names: &[&str]) -> Value {
    let mut map = serde_json::Map::new();
    for name in names {
        map.insert(
            (*name).into(),
            json!([{"hooks":[{"type":"command","command":hook(exe,h,name),"timeout":5}]}]),
        );
    }
    Value::Object(map)
}
pub fn validate(spec: &AgentSpec) -> io::Result<()> {
    if !Path::new(&spec.cwd).is_absolute()
        || spec.args.iter().any(|s| s.contains('\0'))
        || spec.args.iter().map(String::len).sum::<usize>() > 64 * 1024
    {
        return Err(invalid("invalid agent arguments or working directory"));
    }
    let prohibited: &[&str] = match spec.harness {
        Harness::ClaudeCode => &["--tmux"],
        Harness::Codex => &["--remote", "--profile", "-p"],
        Harness::Antigravity => &["--gemini_dir"],
        _ => &[],
    };
    if spec.args.iter().any(|arg| {
        prohibited.iter().any(|key| {
            arg == key
                || arg.starts_with(&format!("{key}="))
                || (*key == "-p" && arg.starts_with("-p") && !arg.starts_with("--"))
        })
    }) {
        return Err(invalid(
            "launch argument conflicts with Tower's session ownership",
        ));
    }
    if spec.harness != Harness::Codex && spec.mode.is_some() {
        return Err(invalid("mode is only supported for Codex"));
    }
    if spec.harness == Harness::Codex && spec.mode.as_deref() != Some("app-server") {
        return Err(invalid(
            "Tower launches Codex through its host app-server only",
        ));
    }
    if let Some(native) = &spec.resume {
        let selectors: &[&str] = match spec.harness {
            Harness::ClaudeCode | Harness::Grok | Harness::Omp => &["--resume", "--continue"],
            Harness::Codex => &[],
            Harness::Reasonix => &["-r", "--resume"],
            Harness::Kimi | Harness::Pi => &["--session", "--continue"],
            Harness::Opencode => &["-s", "--session", "--continue"],
            Harness::Antigravity => &["--conversation"],
        };
        if spec.args.iter().any(|arg| {
            selectors
                .iter()
                .any(|s| arg == s || arg.starts_with(&format!("{s}=")))
        }) || spec.harness == Harness::Codex
            && spec
                .args
                .first()
                .is_some_and(|s| matches!(s.as_str(), "resume" | "fork"))
        {
            return Err(invalid(
                "native identity arguments conflict with the selected conversation",
            ));
        }
        if !crate::events::native_id(&native.id) {
            return Err(invalid("invalid native session identity"));
        }
        if let Some(path) = &native.resume_ref
            && !Path::new(path).is_absolute()
        {
            return Err(invalid("resume file must be absolute"));
        }
    }
    if spec.fork && (spec.resume.is_none() || !spec.harness.can_fork()) {
        return Err(invalid(
            "this harness cannot fork the selected conversation",
        ));
    }
    Ok(())
}
pub fn resume_options(harness: Harness, args: &[String]) -> io::Result<Vec<String>> {
    let specific: &[&str] = match harness {
        Harness::Codex => &[
            "--config",
            "-c",
            "--sandbox",
            "-s",
            "--ask-for-approval",
            "-a",
            "--add-dir",
            "--enable",
            "--disable",
            "--model-provider",
        ],
        Harness::ClaudeCode => &[
            "--permission-mode",
            "--system-prompt",
            "--append-system-prompt",
            "--settings",
            "--setting-sources",
            "--mcp-config",
            "--permission-prompt-tool",
            "--output-format",
            "--input-format",
            "--max-budget-usd",
            "--max-turns",
        ],
        Harness::Pi | Harness::Omp => &[
            "--provider",
            "--thinking",
            "--thinking-level",
            "--session-dir",
        ],
        Harness::Reasonix => &["--provider", "--config", "--approval-mode"],
        Harness::Opencode => &["--agent"],
        Harness::Kimi => &["--config", "--config-file"],
        Harness::Grok | Harness::Antigravity => &[],
    };
    let values = [&["--model", "-m"][..], specific].concat();
    let switches = [
        "--dangerously-skip-permissions",
        "--dangerously-bypass-approvals-and-sandbox",
        "--full-auto",
        "--yolo",
        "--no-alt-screen",
        "--verbose",
        "--debug",
        "--strict-mcp-config",
        "--bare",
        "--json",
        "--disable-slash-commands",
    ];
    let mut result = Vec::new();
    let mut iter = args.iter().peekable();
    while let Some(arg) = iter.next() {
        if arg == "--" {
            break;
        }
        if matches!(arg.as_str(), "-c" | "-p")
            && matches!(harness, Harness::ClaudeCode | Harness::Pi | Harness::Omp)
        {
            continue;
        }
        if arg == "-s" && harness == Harness::Opencode {
            let _ = iter.next();
            continue;
        }
        if matches!(arg.as_str(), "--resume" | "--session" | "--worktree" | "-r")
            || arg == "--fork" && matches!(harness, Harness::Pi | Harness::Omp)
        {
            if iter.peek().is_some_and(|s| !s.starts_with('-')) {
                iter.next();
            }
            continue;
        }
        if matches!(
            arg.as_str(),
            "--fork" | "--fork-session" | "--copy" | "--continue" | "--print"
        ) {
            continue;
        }
        if harness == Harness::ClaudeCode
            && matches!(
                arg.as_str(),
                "--allowedTools"
                    | "--allowed-tools"
                    | "--disallowedTools"
                    | "--disallowed-tools"
                    | "--tools"
                    | "--add-dir"
            )
        {
            result.push(arg.clone());
            while iter.peek().is_some_and(|s| !s.starts_with('-')) {
                result.push(iter.next().expect("peeked").clone());
            }
            continue;
        }
        if harness == Harness::ClaudeCode
            && [
                "--allowedTools",
                "--allowed-tools",
                "--disallowedTools",
                "--disallowed-tools",
                "--tools",
                "--add-dir",
            ]
            .iter()
            .any(|key| arg.starts_with(&format!("{key}=")))
        {
            result.push(arg.clone());
            continue;
        }
        if values.contains(&arg.as_str()) {
            let value = iter
                .next()
                .ok_or_else(|| invalid("stored launch option is missing its value"))?;
            result.extend([arg.clone(), value.clone()]);
        } else if switches.contains(&arg.as_str())
            || values.iter().any(|key| arg.starts_with(&format!("{key}=")))
        {
            result.push(arg.clone());
        } else if arg.starts_with('-') {
            return Err(invalid(
                "cannot safely preserve an unrecognized launch option; supply explicit replacement arguments with restart/resume -- ...",
            ));
        }
    }
    Ok(result)
}
pub fn prepare(
    spec: &AgentSpec,
    dir: &Path,
    exe: &Path,
    mut env: Environment,
) -> io::Result<Invocation> {
    validate(spec)?;
    tower_pal::private_dir(dir)?;
    let bin = dir.join("bin");
    tower_pal::private_dir(&bin)?;
    let command = bin.join("tower");
    match std::fs::symlink_metadata(&command) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => tower_pal::symlink(exe, &command)?,
        Ok(metadata)
            if metadata.file_type().is_symlink() && std::fs::read_link(&command)? == exe => {}
        _ => return Err(invalid("Unexpected Tower command shim")),
    }
    let mut path = vec![bin];
    path.extend(env.get("PATH").into_iter().flat_map(std::env::split_paths));
    let path = std::env::join_paths(path)
        .map_err(io::Error::other)?
        .into_string()
        .map_err(|_| invalid("PATH must be UTF-8"))?;
    env.insert("PATH".into(), path);
    let h = spec.harness;
    let mut argv = vec![h.program().into()];
    match h {
        Harness::ClaudeCode => {
            let mut names = COMMON.to_vec();
            names.extend([
                "Elicitation",
                "ElicitationResult",
                "PostCompact",
                "PostModelSwitch",
            ]);
            let path = dir.join("claude-settings.json");
            save_json(&path, &json!({"hooks":nested(exe,h,&names)}))?;
            argv.extend(["--settings".into(), path.to_string_lossy().into_owned()]);
            let mcp = dir.join("tower-mcp.json");
            save_json(
                &mcp,
                &json!({"mcpServers":{"tower":{"command":exe,"args":["mcp"]}}}),
            )?;
            argv.extend(["--mcp-config".into(), mcp.to_string_lossy().into_owned()]);
            env.insert(
                "CLAUDE_BASH_MAINTAIN_PROJECT_WORKING_DIR".into(),
                "1".into(),
            );
        }
        Harness::Codex => {}
        Harness::Reasonix => {
            let real = home(&env, "REASONIX_HOME", ".reasonix")?;
            let overlay = dir.join("reasonix");
            mirror(
                &real,
                &overlay,
                &["settings.json"],
                &[],
                &[".env", "config.toml"],
            )?;
            let mut settings = json_file(&real.join("settings.json"))?;
            let events = COMMON
                .iter()
                .map(|e| {
                    (
                        (*e).to_owned(),
                        json!([{"match":"*","command":hook(exe,h,e)}]),
                    )
                })
                .collect();
            extend_events(&mut settings, Value::Object(events))?;
            save_json(&overlay.join("settings.json"), &settings)?;
            env.entry("REASONIX_STATE_HOME".into())
                .or_insert_with(|| real.to_string_lossy().into_owned());
            let cache = if env.contains_key("REASONIX_HOME") {
                real.join("cache")
            } else {
                tower_pal::cache_home(&env)?.join("reasonix")
            };
            env.entry("REASONIX_CACHE_HOME".into())
                .or_insert_with(|| cache.to_string_lossy().into_owned());
            env.insert(
                "REASONIX_HOME".into(),
                overlay.to_string_lossy().into_owned(),
            );
            argv.extend(["--dir".into(), spec.cwd.clone()]);
        }
        Harness::Kimi => {
            let real = home(&env, "KIMI_CODE_HOME", ".kimi")?;
            let overlay = dir.join("kimi");
            mirror(
                &real,
                &overlay,
                &["config.toml"],
                &["sessions", "credentials", "logs", "updates", "user-history"],
                &["session_index.jsonl"],
            )?;
            let mut config: toml::Table = read_config(&real.join("config.toml"))?
                .map(|s| s.parse())
                .transpose()
                .map_err(io::Error::other)?
                .unwrap_or_default();
            let hooks = config
                .entry("hooks")
                .or_insert(toml::Value::Array(vec![]))
                .as_array_mut()
                .ok_or_else(|| invalid("Kimi hooks must be an array"))?;
            let mut names = COMMON.to_vec();
            names.extend([
                "TurnStarted",
                "PermissionResult",
                "PostCompact",
                "Interrupt",
            ]);
            for name in names {
                hooks.push(
                    toml::Value::try_from(
                        json!({"event":name,"command":hook(exe,h,name),"timeout":5}),
                    )
                    .map_err(io::Error::other)?,
                );
            }
            write_private(
                &overlay.join("config.toml"),
                toml::to_string(&config)
                    .map_err(io::Error::other)?
                    .as_bytes(),
            )?;
            env.insert(
                "KIMI_CODE_HOME".into(),
                overlay.to_string_lossy().into_owned(),
            );
        }
        Harness::Grok => {
            let real = home(&env, "GROK_HOME", ".grok")?;
            let hooks_dir = real.join("hooks");
            tower_pal::ensure_user_dir(&hooks_dir)?;
            let mut names = COMMON.to_vec();
            names.extend(["StopCancelled", "PostCompact"]);
            let mut hooks = nested(exe, h, &names);
            hooks["Notification"] = json!([
                {"matcher":"permission_prompt","hooks":[{"type":"command","command":hook(exe,h,"PermissionRequest"),"timeout":5}]},
                {"matcher":"idle_prompt","hooks":[{"type":"command","command":hook(exe,h,"Stop"),"timeout":5}]}
            ]);
            let path = hooks_dir.join("tower.json");
            if path.exists()
                && json_file(&path)?.get("_tower_generated") != Some(&Value::Bool(true))
            {
                return Err(invalid("Grok tower.json exists and is not owned by Tower"));
            }
            tower_pal::write_owned_config(
                &path,
                &serde_json::to_vec_pretty(&json!({"_tower_generated":true,"hooks":hooks}))?,
            )?;
            env.insert("GROK_HOME".into(), real.to_string_lossy().into_owned());
        }
        Harness::Opencode => {
            let overlay = dir.join("opencode");
            tower_pal::private_dir(&overlay)?;
            if let Some(real) = env.get("OPENCODE_CONFIG_DIR").map(PathBuf::from) {
                if !real.is_absolute() {
                    return Err(invalid("OPENCODE_CONFIG_DIR must be absolute"));
                }
                mirror(&real, &overlay, &["plugins"], &[], &[])?;
                mirror(
                    &real.join("plugins"),
                    &overlay.join("plugins"),
                    &["tower.ts"],
                    &[],
                    &[],
                )?;
            } else {
                tower_pal::private_dir(&overlay.join("plugins"))?;
            }
            write_private(
                &overlay.join("plugins/tower.ts"),
                crate::extensions::opencode(exe).as_bytes(),
            )?;
            env.insert(
                "OPENCODE_CONFIG_DIR".into(),
                overlay.to_string_lossy().into_owned(),
            );
        }
        Harness::Pi | Harness::Omp => {
            let path = dir.join("tower-extension.ts");
            write_private(&path, crate::extensions::extension(exe, h).as_bytes())?;
            argv.extend(["-e".into(), path.to_string_lossy().into_owned()]);
        }
        Harness::Antigravity => {
            let real = home(&env, "GEMINI_DIR", ".gemini")?;
            let overlay = dir.join("gemini");
            mirror(&real, &overlay, &["config", "antigravity-cli"], &[], &[])?;
            mirror(
                &real.join("config"),
                &overlay.join("config"),
                &["hooks.json"],
                &[],
                &[],
            )?;
            let cli = overlay.join("antigravity-cli");
            mirror(
                &real.join("antigravity-cli"),
                &cli,
                &["settings.json"],
                &[
                    "annotations",
                    "bin",
                    "brain",
                    "builtin",
                    "cache",
                    "conversations",
                    "crashes",
                    "implicit",
                    "log",
                    "presence",
                    "updater",
                ],
                &[
                    "conversation_summaries.db",
                    "conversation_summaries.db-wal",
                    "conversation_summaries.db-shm",
                    "history.jsonl",
                    "installation_id",
                    "jetski_state.pbtxt",
                    "jetbox_summaries_proto.pb",
                    "last_check.timestamp",
                    "cli.log",
                ],
            )?;
            let mut hooks = json_file(&real.join("config/hooks.json"))?;
            let mut events = serde_json::Map::new();
            for name in ["SessionStart", "PreInvocation", "PostToolUse", "Stop"] {
                let handler = json!({"type":"command","command":format!("{} >/dev/null; printf '{{}}'",hook(exe,h,name)),"timeout":5});
                events.insert(
                    name.into(),
                    if name == "PostToolUse" {
                        json!([{"matcher":"*","hooks":[handler]}])
                    } else {
                        json!([handler])
                    },
                );
            }
            hooks["tower"] = Value::Object(events);
            save_json(&overlay.join("config/hooks.json"), &hooks)?;
            let mut settings = json_file(&real.join("antigravity-cli/settings.json"))?;
            let original = settings
                .get("statusLine")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let tail = original
                .get("command")
                .and_then(Value::as_str)
                .filter(|_| original["enabled"] != false)
                .map(|c| {
                    format!(
                        "printf '%s' \"$tower_status\" | /bin/sh -c {}",
                        shell_quote(c)
                    )
                })
                .unwrap_or_else(|| ":".into());
            let mut line = original.as_object().cloned().unwrap_or_default();
            line.insert("enabled".into(), true.into());
            line.insert("type".into(), "command".into());
            if original.get("command").is_none() {
                line.insert("stack_with_default".into(), true.into());
            }
            line.insert(
                "command".into(),
                format!(
                    "tower_status=$(cat); printf '%s' \"$tower_status\" | {} >/dev/null; {tail}",
                    hook(exe, h, "status-update")
                )
                .into(),
            );
            settings["statusLine"] = Value::Object(line);
            save_json(&cli.join("settings.json"), &settings)?;
            argv.extend([
                "--gemini_dir".into(),
                overlay.to_string_lossy().into_owned(),
            ]);
        }
    }
    argv.extend(spec.args.clone());
    if let Some(native) = &spec.resume {
        let reference = if matches!(h, Harness::Pi | Harness::Omp) {
            native.resume_ref.as_deref().unwrap_or(&native.id)
        } else {
            &native.id
        };
        argv.extend(if spec.fork {
            h.fork_args(reference)
                .ok_or_else(|| invalid("native fork is unsupported"))?
        } else {
            h.resume_args(reference)
        });
    }
    Ok(Invocation { argv, env })
}
