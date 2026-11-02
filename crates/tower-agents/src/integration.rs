use crate::{Harness, config::shell_quote};
use serde_json::{Value, json};
use std::{
    io,
    path::{Path, PathBuf},
};

const MARKER: &str = " # tower-managed-hook";
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn location(h: Harness, project: bool, mcp: bool) -> io::Result<PathBuf> {
    let env = tower_pal::environment();
    let home = env.get("HOME").ok_or_else(|| invalid("HOME is required"))?;
    if project {
        Ok(std::env::current_dir()?.join(match (h, mcp) {
            (Harness::ClaudeCode, true) => ".mcp.json",
            (Harness::ClaudeCode, false) => ".claude/settings.json",
            _ => ".codex/config.toml",
        }))
    } else {
        Ok(match (h, mcp) {
            (Harness::ClaudeCode, true) => Path::new(home).join(".claude.json"),
            (Harness::ClaudeCode, false) => env
                .get("CLAUDE_CONFIG_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| Path::new(home).join(".claude"))
                .join("settings.json"),
            _ => env
                .get("CODEX_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| Path::new(home).join(".codex"))
                .join("config.toml"),
        })
    }
}
fn read(path: &Path, h: Harness) -> io::Result<Value> {
    let text = tower_pal::read_config(path)?;
    match text {
        None => Ok(json!({})),
        Some(text) if h == Harness::ClaudeCode => {
            serde_json::from_str(&text).map_err(io::Error::other)
        }
        Some(text) => {
            let table: toml::Table = text.parse().map_err(io::Error::other)?;
            serde_json::to_value(table).map_err(io::Error::other)
        }
    }
}
fn save(path: &Path, h: Harness, value: Value) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("Invalid config path"))?;
    tower_pal::ensure_user_dir(parent)?;
    let bytes = if h == Harness::ClaudeCode {
        serde_json::to_vec_pretty(&value)?
    } else {
        toml::to_string_pretty(&toml::Value::try_from(value).map_err(io::Error::other)?)
            .map_err(io::Error::other)?
            .into_bytes()
    };
    tower_pal::write_owned_config(path, &bytes)
}
fn harness(name: &str) -> io::Result<Harness> {
    Harness::parse(name)
        .filter(|h| matches!(h, Harness::ClaudeCode | Harness::Codex))
        .ok_or_else(|| invalid("Supported integrations: claude, codex"))
}
pub fn hooks(name: &str, project: bool, install: bool) -> io::Result<PathBuf> {
    let h = harness(name)?;
    let path = location(h, project, false)?;
    let mut config = read(&path, h)?;
    let hooks = config
        .as_object_mut()
        .ok_or_else(|| invalid("Config must be an object"))?
        .entry("hooks")
        .or_insert(json!({}))
        .as_object_mut()
        .ok_or_else(|| invalid("Hooks must be an object"))?;
    for groups in hooks.values_mut().filter_map(Value::as_array_mut) {
        for group in groups.iter_mut() {
            if let Some(commands) = group.get_mut("hooks").and_then(Value::as_array_mut) {
                commands.retain(|v| !v["command"].as_str().is_some_and(|s| s.ends_with(MARKER)));
            }
        }
        groups.retain(|g| g["hooks"].as_array().is_none_or(|a| !a.is_empty()));
    }
    if install {
        let exe = tower_pal::executable()?;
        let events = if h == Harness::ClaudeCode {
            vec![
                "SessionStart",
                "UserPromptSubmit",
                "PreToolUse",
                "PostToolUse",
                "PermissionRequest",
                "Stop",
                "SessionEnd",
            ]
        } else {
            vec![
                "SessionStart",
                "UserPromptSubmit",
                "PreToolUse",
                "PostToolUse",
                "PermissionRequest",
                "Stop",
            ]
        };
        for event in events {
            let command = format!(
                "{} hook {} {}{}",
                shell_quote(&exe.to_string_lossy()),
                h.id(),
                event,
                MARKER
            );
            let groups = hooks
                .entry(event)
                .or_insert(json!([]))
                .as_array_mut()
                .ok_or_else(|| invalid("Hook group must be an array"))?;
            let mut group = json!({"hooks":[{"type":"command","command":command}]});
            if h == Harness::Codex && !matches!(event, "UserPromptSubmit" | "Stop") {
                group["matcher"] = "*".into();
            }
            groups.push(group);
        }
    }
    save(&path, h, config)?;
    Ok(path)
}
pub fn mcp(name: &str, project: bool, install: bool) -> io::Result<PathBuf> {
    let h = harness(name)?;
    let path = location(h, project, true)?;
    let mut config = read(&path, h)?;
    let servers = config
        .as_object_mut()
        .ok_or_else(|| invalid("Config must be an object"))?
        .entry(if h == Harness::ClaudeCode {
            "mcpServers"
        } else {
            "mcp_servers"
        })
        .or_insert(json!({}))
        .as_object_mut()
        .ok_or_else(|| invalid("MCP servers must be an object"))?;
    let exe = tower_pal::executable()?;
    if let Some(existing) = servers.get("tower")
        && (existing["args"] != json!(["mcp"]) || existing["command"] != json!(exe))
    {
        return Err(invalid(
            "An existing MCP server named tower has different settings",
        ));
    }
    if install {
        servers.insert("tower".into(), json!({"command":exe,"args":["mcp"]}));
    } else {
        servers.remove("tower");
    }
    save(&path, h, config)?;
    Ok(path)
}
