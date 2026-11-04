use crate::{Binding, Client, Result, error, internal, random_id};
use std::sync::Arc;
use tower_protocol::{Capabilities, Secret, SessionOpenParams, Value};
use tower_sessions::{
    ManagedBackend, ManagedSession, Observation, PreparedSession, SessionMetadata,
    execution::{ExecutionHost, RuntimeView},
};
pub struct Backend {
    client: Arc<Client>,
    protocol: String,
}
impl Backend {
    pub fn new(client: Arc<Client>, protocol: &str) -> Self {
        Self {
            client,
            protocol: protocol.into(),
        }
    }
}
fn decode<T: serde::de::DeserializeOwned>(v: &Value) -> Result<T> {
    serde_json::from_value(serde_json::to_value(v).map_err(internal)?).map_err(internal)
}
fn public(v: impl serde::Serialize) -> Result<Value> {
    serde_json::from_value(serde_json::to_value(v).map_err(internal)?).map_err(internal)
}
pub fn capabilities(active: bool, resume: bool) -> Capabilities {
    let mut caps = Capabilities::unavailable("structured_control_required");
    caps.kill = active;
    caps.resume = resume;
    caps
}
fn observation(v: &RuntimeView) -> Result<Observation> {
    let m: serde_json::Value = decode(&v.metadata)?;
    let native = m["native_session_id"]
        .as_str()
        .filter(|v| !v.is_empty())
        .map(str::to_owned);
    let status = if v.fenced {
        if v.termination == "confirmed" {
            "exited"
        } else {
            "lost"
        }
    } else {
        &v.state
    };
    let mut detail = serde_json::Map::new();
    for key in ["model", "context_tokens", "last_tool", "error"] {
        if let Some(value) = m.get(key).filter(|v| !v.is_null()) {
            detail.insert(key.into(), value.clone());
        }
    }
    if let Some(model) = m.pointer("/config/model") {
        detail.insert("model".into(), model.clone());
    }
    Ok(Observation {
        id: v.binding.tower_session_id.clone(),
        binding: None,
        status: status.into(),
        pid: m["pid"].as_u64().and_then(|v| u32::try_from(v).ok()),
        pid_start_time: m["pid_start_time"].as_u64(),
        caps: capabilities(
            !v.fenced,
            native.is_some()
                && (!v.fenced || v.termination == "confirmed")
                && (v.binding.protocol != "codex-app-server" || m["native_resumable"] == true),
        ),
        terminal: None,
        recovery_pending: false,
        metadata: SessionMetadata {
            shared_process: Some(v.binding.protocol == "codex-app-server"),
            native_session_id: native,
            display_name: None,
            cwd: Some(v.binding.cwd.clone()),
            detail: (!detail.is_empty())
                .then(|| public(serde_json::Value::Object(detail)))
                .transpose()?,
        },
    })
}
impl ManagedBackend for Backend {
    fn protocol(&self) -> &str {
        &self.protocol
    }
    fn harnesses(&self) -> Vec<tower_protocol::HarnessInfo> {
        self.client
            .controls()
            .into_iter()
            .filter(|(_, c)| c.protocol == self.protocol)
            .map(|(h, c)| tower_protocol::HarnessInfo {
                harness: h.clone(),
                label: h.clone(),
                protocol: c.protocol,
                executable: match h.as_str() {
                    "claude-code" => "claude",
                    "antigravity" => "agy",
                    _ => "codex",
                }
                .into(),
                available: true,
                modes: if h == "codex" {
                    vec!["app-server".into()]
                } else {
                    vec![]
                },
                resume: true,
            })
            .collect()
    }
    fn prepare(&self, id: &str, p: &SessionOpenParams) -> Result<PreparedSession> {
        if !p.args.is_empty()
            || p.resume.is_some()
            || p.mode.as_deref().is_some_and(|v| v != "app-server")
        {
            return Err(error(
                "unsupported",
                "Structured runtimes use native configuration and known managed history",
            ));
        }
        let harness = tower_agents::Harness::parse(&p.harness)
            .map(|h| h.id())
            .unwrap_or(&p.harness);
        if self
            .client
            .controls()
            .get(harness)
            .is_none_or(|c| c.protocol != self.protocol)
        {
            return Err(error(
                "unsupported",
                "Harness has no available adapter for this protocol",
            ));
        }
        let b = Binding {
            origin: "local".into(),
            machine_id: "local".into(),
            session_id: id.into(),
            runtime_id: random_id("runtime_")?,
            generation: random_id("generation_")?,
            tower_session_id: id.into(),
            owner_id: random_id("owner_")?,
            protocol: self.protocol.clone(),
            harness: harness.into(),
            cwd: std::fs::canonicalize(&p.cwd)
                .map_err(|_| error("invalid_params", "Working directory must exist"))?
                .to_string_lossy()
                .into_owned(),
            resume_session_id: None,
        };
        self.prepared(b)
    }
    fn start(&self, s: &ManagedSession, _token: &Secret) -> Result<Observation> {
        let b: Binding = decode(&s.binding)?;
        let c = crate::command(
            &b,
            if b.resume_session_id.is_some() {
                "resume"
            } else {
                "launch"
            },
            serde_json::json!({"harness":b.harness,"cwd":b.cwd,"protocol":b.protocol,"generation":b.generation,"resume_session_id":b.resume_session_id}),
        )?;
        self.client.execute(&b, &c)?;
        Ok(Observation {
            id: s.id.clone(),
            binding: None,
            status: "starting".into(),
            pid: None,
            pid_start_time: None,
            caps: capabilities(true, false),
            terminal: None,
            recovery_pending: false,
            metadata: Default::default(),
        })
    }
    fn observe(&self, sessions: &[ManagedSession]) -> Result<Vec<Observation>> {
        let views = self.client.list(None)?;
        sessions
            .iter()
            .filter_map(|s| {
                let b: Binding = match decode(&s.binding) {
                    Ok(v) => v,
                    Err(e) => return Some(Err(e)),
                };
                let v = views.iter().find(|v| {
                    v.binding.namespace() == b.namespace()
                        && v.binding.runtime_id == b.runtime_id
                        && v.binding.generation == b.generation
                });
                v.map(|v| self.observed(v))
            })
            .collect()
    }
    fn stop(&self, s: &ManagedSession) -> Result<Observation> {
        let b: Binding = decode(&s.binding)?;
        self.client.stop(&b)?;
        let v = self
            .client
            .list(Some(&b.namespace()))?
            .into_iter()
            .find(|v| v.binding.runtime_id == b.runtime_id)
            .ok_or_else(|| error("not_found", "Runtime disappeared"))?;
        self.observed(&v)
    }
    fn prepare_resume(
        &self,
        s: &ManagedSession,
        mode: Option<&str>,
        args: Option<&[String]>,
    ) -> Result<PreparedSession> {
        let old: Binding = decode(&s.binding)?;
        if old.origin != "local" {
            return Err(error("denied", "Resume this execution through Allgather"));
        }
        if args.is_some() || mode.is_some_and(|v| v != "app-server") {
            return Err(error(
                "unsupported",
                "Structured resume does not accept native arguments",
            ));
        }
        let view = self
            .client
            .list(Some(&old.namespace()))?
            .into_iter()
            .find(|v| v.binding.runtime_id == old.runtime_id)
            .ok_or_else(|| error("not_found", "Runtime disappeared"))?;
        let m: serde_json::Value = decode(&view.metadata)?;
        let native = m[if old.protocol == "acp" {
            "acp_session_id"
        } else {
            "native_session_id"
        }]
        .as_str()
        .ok_or_else(|| error("resume_unavailable", "Native history identity unavailable"))?;
        let mut b = old;
        b.runtime_id = random_id("runtime_")?;
        b.generation = random_id("generation_")?;
        b.owner_id = random_id("owner_")?;
        b.resume_session_id = Some(native.into());
        self.prepared(b)
    }
}
impl Backend {
    fn observed(&self, view: &RuntimeView) -> Result<Observation> {
        let mut observed = observation(view)?;
        observed.metadata.cwd = Some(
            self.client
                .paths
                .display_cwd(&view.binding.cwd)
                .map_err(internal)?,
        );
        Ok(observed)
    }
    fn prepared(&self, b: Binding) -> Result<PreparedSession> {
        Ok(PreparedSession {
            harness: b.harness.clone(),
            cwd: b.cwd.clone(),
            native_session_id: None,
            binding: public(&b)?,
            launch: public(&b)?,
            mode: if b.harness == "codex" {
                Some("app-server".into())
            } else {
                None
            },
            caps: capabilities(true, false),
            terminal: None,
        })
    }
}
