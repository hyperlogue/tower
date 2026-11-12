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
pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}
/// Names the program a session's hooks run. Agents that read hooks from a
/// file in the user's agent home share that file between every program that
/// launches them, so the command takes the helper from the session.
pub const HELPER: &str = "TOWER_HELPER";
/// Shell that runs `helper hook <arguments>` inside a launched session.
/// `exe` serves sessions started before their launcher exported [`HELPER`].
pub(crate) fn hook_command(guard: &str, exe: &Path, arguments: &str) -> String {
    format!(
        "if [ -n \"{guard}\" ]; then tower_helper=${{{HELPER}:-{}}}; \"$tower_helper\" hook {arguments}; fi; :",
        shell_quote(&exe.to_string_lossy()),
    )
}
pub(crate) fn hook(exe: &Path, h: Harness, event: &str) -> String {
    hook_command(
        "${TOWER_LAUNCHER_SOCKET:-${TOWER_WRAPPER:-}}",
        exe,
        &format!("{} {event}", h.id()),
    )
}
pub(crate) fn home(env: &Environment, key: &str, fallback: &str) -> io::Result<PathBuf> {
    let path = env.get(key).map(PathBuf::from).unwrap_or_else(|| {
        Path::new(env.get("HOME").map(String::as_str).unwrap_or("")).join(fallback)
    });
    if !path.is_absolute() {
        return Err(invalid("harness home must be absolute"));
    }
    Ok(path)
}
pub(crate) fn json_file(path: &Path) -> io::Result<Value> {
    let value = read_config(path)?
        .map(|s| serde_json::from_str(&s))
        .transpose()?
        .unwrap_or_else(|| json!({}));
    if !value.is_object() {
        return Err(invalid("harness configuration must be an object"));
    }
    Ok(value)
}
pub(crate) fn save_json(path: &Path, value: &Value) -> io::Result<()> {
    write_private(path, &serde_json::to_vec_pretty(value)?)
}
// Never write through a mirrored configuration file. Unknown state stays native.
pub(crate) fn mirror(
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
pub(crate) fn extend_events(config: &mut Value, events: Value) -> io::Result<()> {
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
pub(crate) const COMMON: &[&str] = &[
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
pub(crate) fn nested(exe: &Path, h: Harness, names: &[&str]) -> Value {
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
    let prohibited = spec.harness.definition().prohibited_options();
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
    if spec.harness == Harness::Codex
        && !matches!(spec.mode.as_deref(), Some("app-server" | "native"))
    {
        return Err(invalid("Codex mode must be app-server or native"));
    }
    if let Some(native) = &spec.resume {
        let selectors = spec.harness.definition().identity_options();
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
    let options = harness.definition().resume_options();
    let values = [&["--model", "-m"][..], options.values].concat();
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
        if options.switches_to_remove.contains(&arg.as_str()) {
            continue;
        }
        if options.values_to_remove.contains(&arg.as_str()) {
            let _ = iter.next();
            continue;
        }
        if matches!(arg.as_str(), "--resume" | "--session" | "--worktree" | "-r")
            || options.optional_values_to_remove.contains(&arg.as_str())
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
        if options.multiple_values.contains(&arg.as_str()) {
            result.push(arg.clone());
            while iter.peek().is_some_and(|s| !s.starts_with('-')) {
                result.push(iter.next().expect("peeked").clone());
            }
            continue;
        }
        if options
            .multiple_values
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
    h.definition()
        .configure_launch(spec, dir, exe, &mut env, &mut argv)?;
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
