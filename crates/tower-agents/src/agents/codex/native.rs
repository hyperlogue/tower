use crate::config::{home, invalid, shell_quote};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    io,
    path::Path,
    time::{Duration, Instant},
};
use tower_pal::{Environment, SingletonLock};

const MARKER: &str = "# Managed by Tower: native Codex hooks and Tower MCP.";
const EVENTS: &[(&str, &str, bool)] = &[
    ("SessionStart", "session_start", true),
    ("UserPromptSubmit", "user_prompt_submit", false),
    ("PreToolUse", "pre_tool_use", true),
    ("PostToolUse", "post_tool_use", true),
    ("PermissionRequest", "permission_request", true),
    ("Stop", "stop", false),
    ("PreCompact", "pre_compact", true),
    ("PostCompact", "post_compact", true),
];
pub(crate) fn configure(
    exe: &Path,
    env: &mut Environment,
    argv: &mut Vec<String>,
) -> io::Result<()> {
    let home = home(env, "CODEX_HOME", ".codex")?;
    tower_pal::ensure_user_dir(&home)?;
    let home = std::fs::canonicalize(home)?;
    let until = Instant::now() + Duration::from_secs(2);
    let _lock = loop {
        match SingletonLock::acquire(&home.join("tower-profile.lock")) {
            Ok(lock) => break lock,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock && Instant::now() < until => {
                std::thread::sleep(Duration::from_millis(10))
            }
            Err(e) => return Err(e),
        }
    };
    let path = home.join("tower.config.toml");
    let previous = match std::fs::symlink_metadata(&path) {
        Ok(meta) if !meta.is_file() || meta.file_type().is_symlink() => {
            return Err(invalid("Tower Codex profile is not a regular owned file"));
        }
        Ok(_) => tower_pal::read_config(&path)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };
    if previous
        .as_ref()
        .is_some_and(|s| !s.lines().any(|line| line.trim_end() == MARKER))
    {
        return Err(invalid("Existing tower.config.toml is not Tower-owned"));
    }
    if previous.is_some() {
        let _owned = tower_pal::private_file(&path, false)?;
    }
    let profile = build(&path, exe, previous.as_deref())?;
    if previous.as_deref() != Some(&profile) {
        tower_pal::write_owned_config(&path, profile.as_bytes())?;
    }
    env.insert("CODEX_HOME".into(), home.to_string_lossy().into_owned());
    argv.extend([
        "--profile".into(),
        "tower".into(),
        "--enable".into(),
        "hooks".into(),
    ]);
    Ok(())
}
fn build(path: &Path, exe: &Path, previous: Option<&str>) -> io::Result<String> {
    let mut profile: toml::Table = previous
        .map(str::parse)
        .transpose()
        .map_err(io::Error::other)?
        .unwrap_or_default();
    let mut hooks = toml::Table::new();
    let mut trust = toml::Table::new();
    for (name, label, matcher) in EVENTS {
        let command = format!(
            "if [ -n \"${{TOWER_LAUNCHER_SOCKET:-}}\" ]; then {} hook codex {name}; fi; :",
            shell_quote(&exe.to_string_lossy())
        );
        let mut group =
            json!({"hooks":[{"type":"command","command":command,"timeout":600,"async":false}]});
        if *matcher {
            group["matcher"] = "*".into();
        }
        let mut identity = group.clone();
        identity["event_name"] = (*label).into();
        let bytes = serde_json::to_vec(&canonical(&identity)).map_err(io::Error::other)?;
        let digest = Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        trust.insert(
            format!("{}:{label}:0:0", path.display()),
            toml::Value::try_from(json!({"trusted_hash":format!("sha256:{digest}")}))
                .map_err(io::Error::other)?,
        );
        hooks.insert(
            (*name).into(),
            toml::Value::try_from(json!([group])).map_err(io::Error::other)?,
        );
    }
    let prefix = format!("{}:", path.display());
    if let Some(old) = profile.get("hooks").and_then(toml::Value::as_table) {
        let same = old
            .iter()
            .filter(|(k, _)| k.as_str() != "state")
            .eq(hooks.iter());
        if let Some(carried) = old.get("state").and_then(toml::Value::as_table) {
            for (key, value) in carried {
                if !key.starts_with(&prefix) || (same && trust.contains_key(key)) {
                    trust.insert(key.clone(), value.clone());
                }
            }
        }
    }
    hooks.insert("state".into(), toml::Value::Table(trust));
    profile.insert("hooks".into(), toml::Value::Table(hooks));
    let servers = profile
        .entry("mcp_servers")
        .or_insert_with(|| toml::Value::Table(Default::default()))
        .as_table_mut()
        .ok_or_else(|| invalid("Invalid native profile MCP table"))?;
    servers.insert("tower".into(), toml::Value::try_from(json!({"command":exe,"args":["mcp"],"env_vars":["TOWER_AGENT_ID","TOWER_AGENT_TOKEN","TOWER_SOCKET"]})).map_err(io::Error::other)?);
    Ok(format!(
        "{MARKER}\n# Other profile settings and foreign hook trust are preserved.\n{}",
        toml::to_string(&profile).map_err(io::Error::other)?
    ))
}
fn canonical(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: std::collections::BTreeMap<_, _> = map.iter().collect();
            serde_json::Value::Object(
                sorted
                    .into_iter()
                    .map(|(k, v)| (k.clone(), canonical(v)))
                    .collect(),
            )
        }
        serde_json::Value::Array(a) => a.iter().map(canonical).collect(),
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_profile_preserves_codex_edits_and_accepts_relocated_marker() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("codex");
        let mut env =
            Environment::from([("CODEX_HOME".into(), root.to_string_lossy().into_owned())]);
        let mut argv = vec!["codex".into()];
        let exe = Path::new("/opt/tower-cli");
        configure(exe, &mut env, &mut argv).unwrap();
        assert_eq!(argv, ["codex", "--profile", "tower", "--enable", "hooks"]);
        let path = root.join("tower.config.toml");
        let first = std::fs::read_to_string(&path).unwrap();
        assert!(!first.contains("dangerously"));
        assert!(!root.join("config.toml").exists());
        let mut table: toml::Table = first.parse().unwrap();
        table.insert("model".into(), toml::Value::String("saved-model".into()));
        table
            .get_mut("hooks")
            .unwrap()
            .get_mut("state")
            .unwrap()
            .as_table_mut()
            .unwrap()
            .insert(
                "foreign:stop:0:0".into(),
                toml::Value::try_from(json!({"trusted_hash":"user-choice"})).unwrap(),
            );
        let our_key = format!("{}:stop:0:0", path.display());
        table["hooks"]["state"][&our_key] =
            toml::Value::try_from(json!({"trusted_hash":"native-normalized-hash"})).unwrap();
        std::fs::write(
            &path,
            format!("{}\n{MARKER}\n", toml::to_string(&table).unwrap()),
        )
        .unwrap();
        configure(exe, &mut env, &mut vec![]).unwrap();
        let updated: toml::Table = std::fs::read_to_string(&path).unwrap().parse().unwrap();
        assert_eq!(updated["model"].as_str(), Some("saved-model"));
        assert_eq!(
            updated["hooks"]["state"][&our_key]["trusted_hash"].as_str(),
            Some("native-normalized-hash")
        );
        assert_eq!(
            updated["hooks"]["state"]["foreign:stop:0:0"]["trusted_hash"].as_str(),
            Some("user-choice")
        );
        assert!(updated["hooks"]["Stop"][0].get("matcher").is_none());
    }
    #[test]
    fn native_profile_refuses_unowned_symlinked_and_malformed_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut env =
            Environment::from([("CODEX_HOME".into(), root.to_string_lossy().into_owned())]);
        let profile = root.join("tower.config.toml");
        let exe = Path::new("/opt/tower-cli");
        for value in ["model='user'".to_owned(), format!("{MARKER}\ninvalid = [")] {
            std::fs::write(&profile, &value).unwrap();
            assert!(configure(exe, &mut env, &mut vec![]).is_err());
            assert_eq!(std::fs::read_to_string(&profile).unwrap(), value);
        }
        std::fs::remove_file(&profile).unwrap();
        let other = root.join("other");
        std::fs::write(&other, MARKER).unwrap();
        tower_pal::symlink(&other, &profile).unwrap();
        assert!(configure(exe, &mut env, &mut vec![]).is_err());
        assert_eq!(std::fs::read_to_string(other).unwrap(), MARKER);
    }
}
