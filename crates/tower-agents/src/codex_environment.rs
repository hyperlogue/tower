// Shell-policy handling is adapted from Captain Miao; see LICENSE-CAPTAIN-MIAO.
use crate::config::shell_quote;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
};
use tower_pal::Environment;
use tower_protocol::{CallerContext, Secret};

const MAX_ENVIRONMENT_BYTES: usize = 8 * 1024 * 1024;
const MAX_VARIABLES: usize = 4096;
const STARTUP_NAMES: &[&str] = &["BASH_ENV", "ZDOTDIR", "SHLVL"];
const CORE_NAMES: &[&str] = &[
    "PATH", "SHELL", "TMPDIR", "TEMP", "TMP", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "LOGNAME",
    "USER",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionMcpContext {
    pub agent_id: String,
    pub agent_token: Secret,
    pub socket: PathBuf,
}

impl SessionMcpContext {
    pub fn caller_context(&self) -> CallerContext {
        CallerContext {
            agent_id: Some(self.agent_id.clone()),
            agent_token: Some(self.agent_token.clone()),
            ..Default::default()
        }
    }

    fn from_environment(env: &Environment) -> io::Result<Self> {
        let field = |name: &str| {
            env.get(name)
                .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
                .cloned()
                .ok_or_else(|| invalid("Managed Codex requires a complete Tower identity"))
        };
        let context = Self {
            agent_id: field("TOWER_AGENT_ID")?,
            agent_token: Secret(field("TOWER_AGENT_TOKEN")?),
            socket: field("TOWER_SOCKET")?.into(),
        };
        if context.agent_id.len() > 512 || context.agent_token.0.len() > 4096 {
            return Err(invalid("Invalid managed Codex identity"));
        }
        path_text(&context.socket)?;
        Ok(context)
    }
}

pub fn prepare(
    root: &Path,
    executable: &Path,
    env: &Environment,
    base_config: &Value,
    request_config: &mut Value,
    execution_shell: &Path,
) -> io::Result<()> {
    path_text(root)?;
    let executable = path_text(executable)?;
    path_text(execution_shell)?;
    if !matches!(
        execution_shell.file_name().and_then(|name| name.to_str()),
        Some("bash" | "zsh")
    ) {
        return Err(unsupported(
            "Codex project environments require a verified Bash or Zsh execution shell",
        ));
    }
    let context = SessionMcpContext::from_environment(env)?;
    let base = normalized(base_config, false)?;
    let mut output = normalized(request_config, true)?;
    reject_broker(&base)?;
    reject_request_policy_nulls(&output)?;
    reject_mixed_filters(&output)?;
    let mut effective = base.clone();
    merge(&mut effective, output.clone());
    reject_broker(&effective)?;
    let policy = Policy::parse(&effective)?;
    let variables = policy.apply(env)?;
    if env.get("BASH_ENV").is_some_and(|value| !value.is_empty()) {
        return Err(unsupported(
            "An existing BASH_ENV cannot be combined with the managed Codex environment",
        ));
    }
    validate_tower_mcp(&effective)?;
    validate_feature_table(&effective)?;

    let script_path = root.join("environment.sh");
    let context_path = root.join("mcp.json");
    let script_path_text = path_text(&script_path)?;
    let context_path_text = path_text(&context_path)?;
    let script = environment_script(&variables);
    if script.len() > MAX_ENVIRONMENT_BYTES * 4 {
        return Err(invalid("Codex environment startup file exceeds its limit"));
    }
    let zsh_script = format!("unsetopt RCS\n. {}\n", shell_quote(script_path_text));
    let context_bytes =
        serde_json::to_vec(&context).map_err(|_| invalid("Cannot encode managed MCP context"))?;

    output["shell_environment_policy"] = policy.bootstrap(script_path_text, path_text(root)?);
    let features = output
        .as_object_mut()
        .expect("normalized object")
        .entry("features")
        .or_insert_with(|| json!({}));
    if features.is_null() {
        *features = json!({});
    }
    features
        .as_object_mut()
        .ok_or_else(|| invalid("Invalid Codex feature settings"))?
        .insert("shell_snapshot".into(), Value::Bool(false));
    output["allow_login_shell"] = Value::Bool(false);
    let servers = output
        .as_object_mut()
        .expect("normalized object")
        .entry("mcp_servers")
        .or_insert_with(|| json!({}));
    if servers.is_null() {
        *servers = json!({});
    }
    let tower = servers
        .as_object_mut()
        .ok_or_else(|| invalid("Invalid Codex MCP settings"))?
        .entry("tower")
        .or_insert_with(|| json!({}));
    if tower.is_null() {
        *tower = json!({});
    }
    let tower = tower
        .as_object_mut()
        .ok_or_else(|| invalid("Invalid Tower MCP settings"))?;
    for name in [
        "env",
        "env_vars",
        "url",
        "http_headers",
        "env_http_headers",
        "bearer_token_env_var",
    ] {
        tower.remove(name);
    }
    tower.insert("command".into(), Value::String(executable.into()));
    tower.insert("args".into(), json!(["__session-mcp", context_path_text]));
    remove_managed_hooks(&effective, &mut output)?;

    tower_pal::private_dir(root)?;
    let _lock = tower_pal::SingletonLock::acquire(&root.join(".prepare.lock"))?;
    let files = [
        (script_path, script.into_bytes()),
        (root.join(".zshenv"), zsh_script.into_bytes()),
        (context_path, context_bytes),
    ];
    // Validate all previous files before completing a partially published generation.
    for (path, bytes) in &files {
        if let Some(previous) = existing(path)?
            && previous != *bytes
        {
            return Err(changed());
        }
    }
    for (path, bytes) in files {
        if existing(&path)?.is_none() {
            tower_pal::write_new_private(&path, &bytes)?;
        }
    }
    *request_config = output;
    Ok(())
}

fn existing(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.len() > (MAX_ENVIRONMENT_BYTES * 4) as u64 {
                return Err(changed());
            }
            tower_pal::read_private(path).map(Some)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn changed() -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        "Codex environment snapshot differs; use a new session generation",
    )
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn unsupported(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, message)
}

fn path_text(path: &Path) -> io::Result<&str> {
    path.to_str()
        .filter(|value| path.is_absolute() && !value.chars().any(char::is_control))
        .ok_or_else(|| {
            invalid(
                "Codex environment paths must be absolute UTF-8 paths without control characters",
            )
        })
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Policy {
    inherit: Option<String>,
    ignore_default_excludes: Option<bool>,
    exclude: Vec<String>,
    include_only: Vec<String>,
    filters: BTreeMap<String, String>,
    set: BTreeMap<String, String>,
    experimental_use_profile: Option<bool>,
}

impl Policy {
    fn parse(config: &Value) -> io::Result<Self> {
        let mut value = config
            .get("shell_environment_policy")
            .cloned()
            .unwrap_or(Value::Null);
        if value.is_null() {
            value = json!({});
        }
        value
            .as_object_mut()
            .ok_or_else(|| invalid("Invalid Codex shell environment policy"))?
            .retain(|_, value| !value.is_null());
        let policy: Self = serde_json::from_value(value)
            .map_err(|_| unsupported("Unsupported Codex shell environment policy"))?;
        if !matches!(
            policy.inherit.as_deref(),
            None | Some("all" | "core" | "none")
        ) || policy
            .filters
            .values()
            .any(|action| !matches!(action.as_str(), "include" | "exclude"))
            || policy.experimental_use_profile == Some(true)
        {
            return Err(unsupported("Unsupported Codex shell environment policy"));
        }
        let patterns = policy
            .exclude
            .iter()
            .chain(&policy.include_only)
            .chain(policy.filters.keys());
        if patterns.clone().count() > 256
            || patterns
                .into_iter()
                .any(|pattern| pattern.len() > 256 || pattern.contains('\0'))
        {
            return Err(invalid("Codex environment filters exceed their limits"));
        }
        Ok(policy)
    }

    fn included(&self, name: &str) -> bool {
        let mut patterns = self
            .include_only
            .iter()
            .chain(
                self.filters
                    .iter()
                    .filter(|(_, action)| *action == "include")
                    .map(|(pattern, _)| pattern),
            )
            .peekable();
        patterns.peek().is_none() || patterns.any(|pattern| wildcard(pattern, name))
    }

    fn inherited(&self, name: &str) -> bool {
        match self.inherit.as_deref().unwrap_or("all") {
            "none" => false,
            "core" => CORE_NAMES
                .iter()
                .any(|core| core.eq_ignore_ascii_case(name)),
            _ => true,
        }
    }

    fn excluded(&self, name: &str) -> bool {
        (self.ignore_default_excludes == Some(false)
            && ["*KEY*", "*SECRET*", "*TOKEN*"]
                .iter()
                .any(|pattern| wildcard(pattern, name)))
            || self.exclude.iter().any(|pattern| wildcard(pattern, name))
            || self
                .filters
                .iter()
                .any(|(pattern, action)| action == "exclude" && wildcard(pattern, name))
    }

    fn apply(&self, env: &Environment) -> io::Result<Environment> {
        if env.len() > MAX_VARIABLES || self.set.len() > MAX_VARIABLES {
            return Err(invalid("Codex environment has too many variables"));
        }
        let mut selected = Environment::new();
        for (name, value) in env {
            if runtime_name(name) || removed_name(name) || name == "BASH_ENV" {
                continue;
            }
            if self.inherited(name) && !self.excluded(name) && self.included(name) {
                validate_variable(name, value)?;
                selected.insert(name.clone(), value.clone());
            }
        }
        // Native explicit values restore exclusions, but still obey include-only.
        for (name, value) in &self.set {
            if runtime_name(name)
                || removed_name(name)
                || name == "BASH_ENV"
                || name.starts_with("TOWER_")
            {
                return Err(unsupported(
                    "Codex explicit environment values cannot replace managed runtime identity or startup controls",
                ));
            }
            validate_variable(name, value)?;
            if self.included(name) {
                selected.insert(name.clone(), value.clone());
            }
        }
        let size = selected.iter().try_fold(0usize, |size, (name, value)| {
            size.checked_add(name.len())?.checked_add(value.len())
        });
        if selected.len() > MAX_VARIABLES || size.is_none_or(|size| size > MAX_ENVIRONMENT_BYTES) {
            return Err(invalid("Codex environment snapshot exceeds its limits"));
        }
        Ok(selected)
    }

    fn bootstrap(&self, bash_env: &str, zdotdir: &str) -> Value {
        let excluded = self
            .exclude
            .iter()
            .chain(
                self.filters
                    .iter()
                    .filter(|(_, action)| *action == "exclude")
                    .map(|(pattern, _)| pattern),
            )
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut included = self
            .include_only
            .iter()
            .chain(
                self.filters
                    .iter()
                    .filter(|(_, action)| *action == "include")
                    .map(|(pattern, _)| pattern),
            )
            .cloned()
            .collect::<BTreeSet<_>>();
        if !included.is_empty() {
            included.extend(STARTUP_NAMES.iter().map(|name| (*name).to_owned()));
        }
        json!({
            "inherit": self.inherit.as_deref().unwrap_or("all"),
            "ignore_default_excludes": self.ignore_default_excludes.unwrap_or(true),
            "exclude": excluded,
            "include_only": included,
            "set": {"BASH_ENV":bash_env,"ZDOTDIR":zdotdir,"SHLVL":"1"},
            "experimental_use_profile":false,
        })
    }
}

fn variable_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 256
        && name.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_alphabetic() || (index > 0 && byte.is_ascii_digit())
        })
}

fn validate_variable(name: &str, value: &str) -> io::Result<()> {
    if !variable_name(name) || value.contains('\0') {
        return Err(unsupported(
            "Codex project environment contains a value that Bash and Zsh cannot export safely",
        ));
    }
    Ok(())
}

fn runtime_name(name: &str) -> bool {
    name.starts_with("CODEX_")
        || name.starts_with("_TOWER_ENV_")
        || matches!(
            name,
            "SHLVL" | "_" | "PWD" | "OLDPWD" | "SHELLOPTS" | "BASHOPTS" | "UID" | "EUID" | "PPID"
        )
}

fn removed_name(name: &str) -> bool {
    name.starts_with("DIRENV_")
        || matches!(
            name.to_ascii_uppercase().as_str(),
            "NODE_REPL_AUTH_TOKEN"
                | "OPENAI_FEDERATION_RULE_ID"
                | "OPENAI_IDENTITY_TOKEN_FILE"
                | "OPENAI_WORKLOAD_IDENTITY_CONTEXT"
        )
}

fn proxy_name(name: &str) -> bool {
    name.to_ascii_uppercase().contains("PROXY")
        || matches!(
            name,
            "GIT_SSH_COMMAND"
                | "SSL_CERT_FILE"
                | "SSL_CERT_DIR"
                | "REQUESTS_CA_BUNDLE"
                | "CURL_CA_BUNDLE"
                | "NODE_EXTRA_CA_CERTS"
                | "GIT_SSL_CAINFO"
                | "CARGO_HTTP_CAINFO"
                | "PIP_CERT"
                | "BUNDLE_SSL_CA_CERT"
                | "npm_config_cafile"
                | "NPM_CONFIG_CAFILE"
        )
}

fn environment_script(values: &Environment) -> String {
    let mut script = String::from(
        "if [ -n \"${CODEX_NETWORK_PROXY_CREDENTIAL_BROKER_ACTIVE+x}\" ]; then\nprintf '%s\\n' 'tower: project environment is unsupported with the Codex credential broker' >&2\nexit 1\nfi\nif [ -z \"${BASH_VERSION-}${ZSH_VERSION-}\" ]; then\nprintf '%s\\n' 'tower: unsupported execution shell' >&2\nexit 1\nfi\nfor _TOWER_ENV_NAME in $(if [ -n \"${BASH_VERSION-}\" ]; then builtin export -p | while IFS= builtin read -r _TOWER_ENV_LINE; do case \"$_TOWER_ENV_LINE\" in 'declare -'*) _TOWER_ENV_LINE=${_TOWER_ENV_LINE#declare -}; _TOWER_ENV_LINE=${_TOWER_ENV_LINE#* };; 'export '*) _TOWER_ENV_LINE=${_TOWER_ENV_LINE#export };; *) continue;; esac; _TOWER_ENV_LINE=${_TOWER_ENV_LINE%%=*}; case \"$_TOWER_ENV_LINE\" in ''|[0-9]*|*[!a-zA-Z0-9_]*) continue;; esac; builtin printf '%s\\n' \"$_TOWER_ENV_LINE\"; done; else builtin print -rl -- ${(k)parameters[(R)*export*]}; fi); do\ncase \"$_TOWER_ENV_NAME\" in CODEX_*|BASH_ENV|ZDOTDIR|SHLVL|PWD|OLDPWD|_|SHELLOPTS|BASHOPTS|UID|EUID|PPID|_TOWER_ENV_*) continue;; esac\nif [ -n \"${CODEX_NETWORK_PROXY_ACTIVE+x}\" ]; then\ncase \"$_TOWER_ENV_NAME\" in *[pP][rR][oO][xX][yY]*|GIT_SSH_COMMAND|SSL_CERT_FILE|SSL_CERT_DIR|REQUESTS_CA_BUNDLE|CURL_CA_BUNDLE|NODE_EXTRA_CA_CERTS|GIT_SSL_CAINFO|CARGO_HTTP_CAINFO|PIP_CERT|BUNDLE_SSL_CA_CERT|npm_config_cafile|NPM_CONFIG_CAFILE) continue;; esac\nfi\n",
    );
    if !values.is_empty() {
        let retained = values
            .keys()
            .map(|name| shell_quote(name))
            .collect::<Vec<_>>()
            .join("|");
        script.push_str(&format!(
            "case \"$_TOWER_ENV_NAME\" in {retained}) continue;; esac\n"
        ));
    }
    script.push_str("builtin unset \"$_TOWER_ENV_NAME\" 2>/dev/null || { builtin printf '%s\\n' 'tower: cannot clear inherited environment' >&2; exit 1; }\ndone\nbuiltin unset _TOWER_ENV_NAME\n");
    for (name, value) in values {
        if proxy_name(name) {
            script.push_str("if [ -z \"${CODEX_NETWORK_PROXY_ACTIVE+x}\" ]; then\n");
        }
        script.push_str(&format!("builtin export {name}={} 2>/dev/null || {{ builtin printf '%s\\n' 'tower: cannot install project environment' >&2; exit 1; }}\n", shell_quote(value)));
        if proxy_name(name) {
            script.push_str("fi\n");
        }
    }
    if !values.contains_key("ZDOTDIR") {
        script.push_str("builtin unset ZDOTDIR\n");
    }
    script.push_str("builtin export BASH_ENV=/dev/null\n");
    script
}

fn wildcard(pattern: &str, name: &str) -> bool {
    let (pattern, name) = (pattern.as_bytes(), name.as_bytes());
    let (mut p, mut n, mut star) = (0usize, 0usize, None);
    while n < name.len() {
        if p < pattern.len() && pattern[p] == b'*' {
            star = Some((p, n));
            p += 1;
        } else if p < pattern.len()
            && (pattern[p] == b'?' || pattern[p].eq_ignore_ascii_case(&name[n]))
        {
            p += 1;
            n += 1;
        } else if let Some((index, matched)) = star {
            let matched = matched + 1;
            star = Some((index, matched));
            p = index + 1;
            n = matched;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

fn normalized(value: &Value, nullable: bool) -> io::Result<Value> {
    if nullable && value.is_null() {
        return Ok(json!({}));
    }
    let source = value
        .as_object()
        .ok_or_else(|| invalid("Codex configuration must be an object"))?;
    let mut result = Value::Object(Map::new());
    let mut leaves = BTreeSet::new();
    for (key, value) in source {
        let keys = key_parts(key)?;
        if keys[0].is_empty()
            || !keys[0]
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(unsupported("Unsupported Codex root configuration key"));
        }
        collect_leaves(&keys, value, &mut leaves)?;
        let mut patch = value.clone();
        for key in keys.into_iter().rev() {
            patch = json!({key:patch});
        }
        merge(&mut result, patch);
    }
    Ok(result)
}

fn key_parts(key: &str) -> io::Result<Vec<String>> {
    if key.len() > 1024 {
        return Err(invalid("Codex configuration key exceeds its limit"));
    }
    let parsed: toml::Table = toml::from_str(&format!("{key} = 0"))
        .map_err(|_| invalid("Invalid Codex dotted configuration key"))?;
    let mut current = toml::Value::Table(parsed);
    let mut parts = Vec::new();
    loop {
        match current {
            toml::Value::Table(table) if table.len() == 1 && parts.len() < 16 => {
                let (key, value) = table.into_iter().next().expect("one key");
                parts.push(key);
                current = value;
            }
            toml::Value::Integer(0) if !parts.is_empty() => return Ok(parts),
            _ => return Err(invalid("Invalid Codex dotted configuration key")),
        }
    }
}

fn collect_leaves(
    path: &[String],
    value: &Value,
    leaves: &mut BTreeSet<Vec<String>>,
) -> io::Result<()> {
    if path.len() > 32 || leaves.len() >= 8192 {
        return Err(invalid("Codex configuration exceeds its limits"));
    }
    if let Some(object) = value.as_object().filter(|object| !object.is_empty()) {
        for (key, value) in object {
            let mut path = path.to_vec();
            path.push(key.clone());
            collect_leaves(&path, value, leaves)?;
        }
        return Ok(());
    }
    if leaves
        .iter()
        .any(|existing| existing.starts_with(path) || path.starts_with(existing))
    {
        return Err(invalid(
            "Ambiguous Codex nested and dotted configuration overrides",
        ));
    }
    leaves.insert(path.to_vec());
    Ok(())
}

fn merge(base: &mut Value, patch: Value) {
    if let (Some(base), Some(patch)) = (base.as_object_mut(), patch.as_object()) {
        for (key, value) in patch {
            merge(
                base.entry(key.clone()).or_insert(Value::Null),
                value.clone(),
            );
        }
    } else {
        *base = patch;
    }
}

fn reject_broker(config: &Value) -> io::Result<()> {
    if config
        .pointer("/features/network_proxy/credential_broker")
        .is_some_and(|value| !matches!(value, Value::Null | Value::Bool(false)))
    {
        return Err(unsupported(
            "Codex project environments are unsupported with the credential broker",
        ));
    }
    Ok(())
}

fn reject_request_policy_nulls(config: &Value) -> io::Result<()> {
    if let Some(policy) = config.get("shell_environment_policy")
        && (policy.is_null()
            || policy
                .as_object()
                .is_some_and(|policy| policy.values().any(Value::is_null)))
    {
        return Err(unsupported(
            "Null Codex environment overrides are ambiguous",
        ));
    }
    Ok(())
}

fn reject_mixed_filters(config: &Value) -> io::Result<()> {
    if let Some(policy) = config
        .get("shell_environment_policy")
        .and_then(Value::as_object)
        && policy.contains_key("filters")
        && (policy.contains_key("exclude") || policy.contains_key("include_only"))
    {
        return Err(unsupported(
            "Codex environment filters cannot mix legacy and modern forms in one override",
        ));
    }
    Ok(())
}

fn validate_feature_table(config: &Value) -> io::Result<()> {
    if config
        .get("features")
        .is_some_and(|value| !value.is_null() && !value.is_object())
    {
        return Err(invalid("Invalid Codex feature settings"));
    }
    Ok(())
}

fn validate_tower_mcp(config: &Value) -> io::Result<()> {
    let Some(servers) = config.get("mcp_servers").filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let servers = servers
        .as_object()
        .ok_or_else(|| invalid("Invalid Codex MCP settings"))?;
    let Some(tower) = servers.get("tower").filter(|value| !value.is_null()) else {
        return Ok(());
    };
    let tower = tower
        .as_object()
        .ok_or_else(|| invalid("Invalid Tower MCP settings"))?;
    for name in [
        "env",
        "env_vars",
        "url",
        "http_headers",
        "env_http_headers",
        "bearer_token_env_var",
    ] {
        if tower.get(name).is_some_and(|value| match value {
            Value::Null => false,
            Value::Object(v) => !v.is_empty(),
            Value::Array(v) => !v.is_empty(),
            _ => true,
        }) {
            return Err(unsupported(
                "Tower MCP cannot inherit a configured transport or credential environment",
            ));
        }
    }
    Ok(())
}

fn remove_managed_hooks(effective: &Value, output: &mut Value) -> io::Result<()> {
    const MARKER: &str = " # tower-managed-hook";
    fn managed(value: &Value) -> bool {
        value
            .get("command")
            .and_then(Value::as_str)
            .is_some_and(|command| command.ends_with(MARKER))
    }
    fn contains_managed(value: &Value) -> bool {
        managed(value)
            || match value {
                Value::Array(values) => values.iter().any(contains_managed),
                Value::Object(values) => values.values().any(contains_managed),
                _ => false,
            }
    }
    let Some(hooks) = effective.get("hooks").filter(|hooks| !hooks.is_null()) else {
        return Ok(());
    };
    let hooks = hooks
        .as_object()
        .ok_or_else(|| unsupported("Unsupported Codex hook settings"))?;
    let mut replacements = Map::new();
    for (event, value) in hooks {
        if !contains_managed(value) {
            continue;
        }
        let groups = value.as_array().ok_or_else(|| {
            unsupported("Cannot identify managed commands in Codex hook settings")
        })?;
        let mut filtered = Vec::new();
        for group in groups {
            if !contains_managed(group) {
                filtered.push(group.clone());
                continue;
            }
            let mut group = group.clone();
            let commands = group
                .get_mut("hooks")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| {
                    unsupported("Cannot identify managed commands in Codex hook settings")
                })?;
            commands.retain(|command| !managed(command));
            if !commands.is_empty() {
                filtered.push(group);
            }
        }
        let replacement = Value::Array(filtered);
        if contains_managed(&replacement) {
            return Err(unsupported(
                "Cannot identify managed commands in Codex hook settings",
            ));
        }
        replacements.insert(event.clone(), replacement);
    }
    if !replacements.is_empty() {
        let hooks = output
            .as_object_mut()
            .expect("normalized object")
            .entry("hooks")
            .or_insert_with(|| json!({}));
        hooks
            .as_object_mut()
            .ok_or_else(|| unsupported("Ambiguous Codex hook override"))?
            .extend(replacements);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    struct Fixture {
        directory: tempfile::TempDir,
        env: Environment,
        shell: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let shell = tower_pal::find_program("bash", &tower_pal::environment())
                .expect("Bash is required for environment tests");
            let env = [
                (
                    "PATH".into(),
                    shell.parent().unwrap().to_str().unwrap().into(),
                ),
                ("HOME".into(), directory.path().to_str().unwrap().into()),
                ("SHELL".into(), "/project/selected/fish".into()),
                ("PROJECT".into(), "project-value".into()),
                ("PROJECT_SECRET".into(), "private-project-value".into()),
                ("TOWER_AGENT_ID".into(), "fixture-agent".into()),
                ("TOWER_AGENT_TOKEN".into(), "private-agent-token".into()),
                (
                    "TOWER_SOCKET".into(),
                    directory.path().join("tower.sock").to_str().unwrap().into(),
                ),
            ]
            .into();
            Self {
                directory,
                env,
                shell,
            }
        }

        fn root(&self) -> PathBuf {
            self.directory.path().join("generation")
        }

        fn prepare(&self, base: Value, mut request: Value) -> io::Result<Value> {
            prepare(
                &self.root(),
                &std::env::current_exe()?,
                &self.env,
                &base,
                &mut request,
                &self.shell,
            )?;
            Ok(request)
        }

        fn command(&self, config: &Value, shell: &Path, program: &str) -> Command {
            let mut command = Command::new(shell);
            command
                .arg("-c")
                .arg(program)
                .env_clear()
                .env("HOME", self.directory.path())
                .env("SERVER_ONLY_TOKEN", "must-be-removed")
                .env("OLD_PROJECT", "must-be-removed")
                .env("DIRENV_DIFF", "must-be-removed")
                .env("NODE_REPL_AUTH_TOKEN", "must-be-removed")
                .env("CODEX_THREAD_ID", "native-thread");
            for (name, value) in config["shell_environment_policy"]["set"]
                .as_object()
                .unwrap()
            {
                command.env(name, value.as_str().unwrap());
            }
            command
        }
    }

    #[test]
    fn only_private_paths_enter_native_config_and_mcp_identity_is_separate() {
        let fixture = Fixture::new();
        let config = fixture
            .prepare(
                json!({}),
                json!({"model":"fixture-model","mcp_servers":{"other":{"command":"other-tool"}}}),
            )
            .unwrap();
        let encoded = config.to_string();
        assert!(!encoded.contains("private-project-value"));
        assert!(!encoded.contains("private-agent-token"));
        assert_eq!(config["model"], "fixture-model");
        assert_eq!(config["mcp_servers"]["other"]["command"], "other-tool");
        assert_eq!(config["mcp_servers"]["tower"]["args"][0], "__session-mcp");
        assert!(config["mcp_servers"]["tower"].get("env").is_none());
        assert!(config["mcp_servers"]["tower"].get("env_vars").is_none());
        assert_eq!(config["features"]["shell_snapshot"], false);
        assert_eq!(config["allow_login_shell"], false);
        let bytes = tower_pal::read_private(&fixture.root().join("mcp.json")).unwrap();
        let context: SessionMcpContext = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes)
                .unwrap()
                .as_object()
                .unwrap()
                .len(),
            3
        );
        assert_eq!(context.agent_id, "fixture-agent");
        assert_eq!(
            context.caller_context().agent_token.unwrap().0,
            "private-agent-token"
        );
        assert!(!format!("{context:?}").contains("private-agent-token"));
        let output = fixture.command(&config, &fixture.shell, "builtin printf '%s|%s|%s|%s' \"$PROJECT\" \"$PROJECT_SECRET\" \"$CODEX_THREAD_ID\" \"${SERVER_ONLY_TOKEN+x}${OLD_PROJECT+x}${DIRENV_DIFF+x}${NODE_REPL_AUTH_TOKEN+x}\"").output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            output.stdout,
            b"project-value|private-project-value|native-thread|"
        );
    }

    #[test]
    fn filtering_preserves_native_set_precedence_and_dotted_launch_overrides() {
        let fixture = Fixture::new();
        let config = fixture.prepare(
            json!({"shell_environment_policy":{"inherit":"all","ignore_default_excludes":false,"filters":{"OLD*":"exclude","P*":"include","RESTORED":"include"},"set":{"PROJECT":"host-value"}}}),
            json!({"shell_environment_policy.set.PROJECT":"launch-value", "shell_environment_policy.set.RESTORED":"restored-value", "shell_environment_policy.filters.\"PROJ?CT\"":"exclude"}),
        ).unwrap();
        let script = std::fs::read_to_string(fixture.root().join("environment.sh")).unwrap();
        assert!(!script.contains("private-project-value"));
        assert!(!config.to_string().contains("launch-value"));
        let output = fixture.command(&config, &fixture.shell, "builtin printf '%s|%s|%s' \"$PROJECT\" \"$RESTORED\" \"${PROJECT_SECRET+x}${TOWER_AGENT_TOKEN+x}${HOME+x}\"").output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, b"launch-value|restored-value|");
        let includes = config["shell_environment_policy"]["include_only"]
            .as_array()
            .unwrap();
        for name in STARTUP_NAMES {
            assert!(includes.contains(&json!(name)));
        }
    }

    #[test]
    fn restrictive_inheritance_and_allowlists_apply_even_to_explicit_values() {
        for inherit in ["none", "core"] {
            let fixture = Fixture::new();
            let config = fixture.prepare(
                json!({"shell_environment_policy":{"inherit":inherit,"set":{"EXPLICIT":"configured"}}}),
                Value::Null,
            ).unwrap();
            let output = fixture
                .command(
                    &config,
                    &fixture.shell,
                    "builtin printf '%s|%s|%s' \"${PROJECT+x}\" \"$EXPLICIT\" \"${HOME+x}\"",
                )
                .output()
                .unwrap();
            assert!(output.status.success());
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                format!("|configured|{}", if inherit == "core" { "x" } else { "" })
            );
        }
        let fixture = Fixture::new();
        let config = fixture.prepare(json!({"shell_environment_policy":{"inherit":"none","set":{"DROP":"must-not-persist","ALLOWED":"kept"},"include_only":["ALLOWED"]}}), json!({})).unwrap();
        let script = std::fs::read_to_string(fixture.root().join("environment.sh")).unwrap();
        assert!(!script.contains("must-not-persist"));
        let output = fixture
            .command(
                &config,
                &fixture.shell,
                "builtin printf '%s|%s' \"$ALLOWED\" \"${DROP+x}\"",
            )
            .output()
            .unwrap();
        assert_eq!(output.stdout, b"kept|");
    }

    #[test]
    fn startup_quotes_values_clears_removed_names_and_does_not_reset_child_shells() {
        let mut fixture = Fixture::new();
        let marker = fixture.directory.path().join("must-not-exist");
        let value = format!(
            "quote'\n$(touch {}) `exit 61` \\",
            shell_quote(marker.to_str().unwrap())
        );
        fixture.env.insert("PROJECT".into(), value.clone());
        fixture
            .env
            .insert("HTTP_PROXY".into(), "project-proxy".into());
        let config = fixture.prepare(json!({}), json!({})).unwrap();
        let mut shells = vec![fixture.shell.clone()];
        if let Some(zsh) = tower_pal::find_program("zsh", &tower_pal::environment()) {
            shells.push(zsh);
        }
        for shell in shells {
            let nested = format!(
                "builtin printf '%s\\0%s\\0' \"$PROJECT\" \"$HTTP_PROXY\"; builtin export PROJECT=changed; {} -c 'test \"$PROJECT\" = changed'",
                shell_quote(shell.to_str().unwrap())
            );
            let output = fixture
                .command(&config, &shell, &nested)
                .env("CODEX_NETWORK_PROXY_ACTIVE", "1")
                .env("HTTP_PROXY", "managed-proxy")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                output.stdout,
                format!("{value}\0managed-proxy\0").into_bytes()
            );
            assert!(!marker.exists());
            let blocked = fixture
                .command(&config, &shell, "builtin printf command-ran")
                .env("CODEX_NETWORK_PROXY_CREDENTIAL_BROKER_ACTIVE", "1")
                .output()
                .unwrap();
            assert!(!blocked.status.success());
            assert!(blocked.stdout.is_empty());
            assert!(!String::from_utf8_lossy(&blocked.stderr).contains(&value));
        }
    }

    #[test]
    fn immutable_generation_accepts_identical_inputs_and_rejects_replacement() {
        let mut fixture = Fixture::new();
        let config = fixture.prepare(json!({}), json!({})).unwrap();
        let script = tower_pal::read_private(&fixture.root().join("environment.sh")).unwrap();
        assert_eq!(fixture.prepare(json!({}), json!({})).unwrap(), config);
        fixture
            .env
            .insert("PROJECT".into(), "different-value".into());
        assert_eq!(
            fixture.prepare(json!({}), json!({})).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(
            tower_pal::read_private(&fixture.root().join("environment.sh")).unwrap(),
            script
        );
        fixture.env.insert("PROJECT".into(), "project-value".into());
        fixture
            .env
            .insert("TOWER_AGENT_TOKEN".into(), "replacement-token".into());
        assert_eq!(
            fixture.prepare(json!({}), json!({})).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
    }

    #[test]
    fn unsupported_or_ambiguous_input_fails_before_publishing_snapshots() {
        let cases = [
            (
                json!({"features":{"network_proxy":{"credential_broker":true}}}),
                json!({"features.network_proxy":false}),
            ),
            (
                json!({}),
                json!({"features.network_proxy.credential_broker":true}),
            ),
            (
                json!({}),
                json!({"shell_environment_policy.experimental_use_profile":true}),
            ),
            (
                json!({}),
                json!({"shell_environment_policy.futureRestriction":true}),
            ),
            (
                json!({}),
                json!({"shell_environment_policy.inherit":"all","shell_environment_policy":{"inherit":"none"}}),
            ),
            (
                json!({}),
                json!({"shell_environment_policy":{"filters":{},"exclude":[]}}),
            ),
            (
                json!({}),
                json!({"shell_environment_policy.set.CODEX_THREAD_ID":"spoofed"}),
            ),
            (
                json!({}),
                json!({"shell_environment_policy.set.PROJECT":"has\0nul"}),
            ),
            (json!({}), json!({"shell_environment_policy.exclude":null})),
            (
                json!({"mcp_servers":{"tower":{"env_vars":["TOKEN"]}}}),
                json!({}),
            ),
            (
                json!({"mcp_servers":{"tower":{"url":"https://invalid.example/mcp"}}}),
                json!({}),
            ),
        ];
        for (base, request) in cases {
            let fixture = Fixture::new();
            assert!(fixture.prepare(base, request).is_err());
            assert!(!fixture.root().exists());
        }
        let fixture = Fixture::new();
        let mut request = json!({});
        assert!(
            prepare(
                &fixture.root(),
                &std::env::current_exe().unwrap(),
                &fixture.env,
                &json!({}),
                &mut request,
                Path::new("/bin/fish")
            )
            .is_err()
        );
        assert!(!fixture.root().exists());
    }

    #[test]
    fn matching_uses_literal_punctuation_and_case_insensitive_star_and_question() {
        assert!(wildcard("pr?j*", "PROJECT"));
        assert!(wildcard("*TOKEN*", "my_token_value"));
        assert!(wildcard("literal[1]", "literal[1]"));
        assert!(!wildcard("literal[1]", "literal1"));
        assert!(!wildcard("a?", "a"));
        assert!(wildcard("*", ""));
        assert!(wildcard("*", "*"));
    }

    #[test]
    fn only_tower_owned_hooks_are_removed_without_changing_user_commands_or_trust() {
        let fixture = Fixture::new();
        let user = json!({"type":"command","command":"printf user-command","timeout":10});
        let other = json!({"matcher":"Bash","hooks":[{"type":"command","command":"printf user-command # another-marker"}]});
        let managed = json!({"type":"command","command":"tower hook codex SessionStart # tower-managed-hook"});
        let base = json!({"hooks":{
            "SessionStart":[{"matcher":"*","hooks":[managed.clone(),user.clone()]}, other.clone()],
            "Stop":[{"hooks":[managed.clone()]}],
            "PreToolUse":[{"hooks":[user.clone()]}],
            "trusted_hashes":["existing-user-trust"]
        }});
        let config = fixture.prepare(base, json!({})).unwrap();
        assert_eq!(
            config["hooks"]["SessionStart"],
            json!([{"matcher":"*","hooks":[user.clone()]},other])
        );
        assert_eq!(config["hooks"]["Stop"], json!([]));
        assert!(config["hooks"].get("PreToolUse").is_none());
        assert!(config["hooks"].get("trusted_hashes").is_none());
        let launch = Fixture::new().prepare(json!({}), json!({"hooks.SessionStart":[{"hooks":[managed,user.clone()]}], "hooks.PreToolUse":[{"hooks":[user.clone()]}]})).unwrap();
        assert_eq!(
            launch["hooks"]["SessionStart"],
            json!([{"hooks":[user.clone()]}])
        );
        assert_eq!(launch["hooks"]["PreToolUse"], json!([{"hooks":[user]}]));
    }

    #[test]
    fn readonly_inherited_values_fail_instead_of_bypassing_the_environment_filter() {
        let fixture = Fixture::new();
        let config = fixture.prepare(json!({}), json!({})).unwrap();
        let startup = fixture.root().join("readonly-startup.sh");
        tower_pal::write_private(
            &startup,
            format!(
                "export PROFILE_SECRET=must-not-leak\nreadonly PROFILE_SECRET\n. {}\n",
                shell_quote(fixture.root().join("environment.sh").to_str().unwrap())
            )
            .as_bytes(),
        )
        .unwrap();
        let output = fixture
            .command(&config, &fixture.shell, "builtin printf command-ran")
            .env("BASH_ENV", startup)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("cannot clear inherited environment")
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("must-not-leak"));
    }
}
