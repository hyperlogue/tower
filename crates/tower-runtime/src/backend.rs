use crate::{Binding, Result, Runtime, error, internal, random_id};
use std::sync::Arc;
use tower_protocol::{Capabilities, Secret, SessionOpenParams, Value};
use tower_sessions::{
    ManagedBackend, ManagedSession, Observation, PreparedSession, SessionMetadata,
    execution::{ExecutionHost, RuntimeView},
};
pub struct Backend {
    runtime: Arc<Runtime>,
}
impl Backend {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self { runtime }
    }
}
fn decode<T: serde::de::DeserializeOwned>(v: &Value) -> Result<T> {
    serde_json::from_value(serde_json::to_value(v).map_err(internal)?).map_err(internal)
}
fn public(v: impl serde::Serialize) -> Result<Value> {
    serde_json::from_value(serde_json::to_value(v).map_err(internal)?).map_err(internal)
}
fn capabilities(active: bool) -> Capabilities {
    let mut caps = Capabilities::unavailable("protocol_control_required");
    caps.kill = active;
    caps
}
pub(crate) fn observation(v: &RuntimeView) -> Result<Observation> {
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
    for key in ["model", "effort", "context_tokens", "last_tool", "error"] {
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
        caps: capabilities(!v.fenced),
        terminal: None,
        recovery_pending: false,
        metadata: SessionMetadata {
            shared_process: Some(false),
            native_session_id: native,
            display_name: tower_protocol::Patch::Missing,
            cwd: Some(v.binding.cwd.clone()),
            detail: Some(public(serde_json::Value::Object(detail))?),
        },
    })
}
impl ManagedBackend for Backend {
    fn protocol(&self) -> &str {
        "acp"
    }
    fn watch(&self, wake: std::sync::Arc<dyn Fn() + Send + Sync>) {
        let mut runtimes = tower_store::table_changes("runtimes");
        tokio::spawn(async move {
            while runtimes.changed().await.is_ok() {
                wake();
            }
        });
    }
    fn harnesses(&self) -> Vec<tower_protocol::HarnessInfo> {
        self.runtime.harnesses()
    }

    fn prepare(&self, id: &str, p: &SessionOpenParams) -> Result<PreparedSession> {
        if !p.args.is_empty() || p.resume.is_some() || p.mode.is_some() {
            return Err(error(
                "unsupported",
                "Configure and load sessions through native ACP frames",
            ));
        }
        let harness = tower_agents::Harness::parse(&p.harness)
            .map(|h| h.id())
            .unwrap_or(&p.harness);
        if self
            .runtime
            .controls()
            .get(harness)
            .is_none_or(|c| c.protocol != "acp")
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
            protocol: "acp".into(),
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
            "launch",
            serde_json::json!({"harness":b.harness,"cwd":b.cwd,"protocol":b.protocol,"generation":b.generation}),
        )?;
        self.runtime.execute(&b, &c)?;
        Ok(Observation {
            id: s.id.clone(),
            binding: None,
            status: "starting".into(),
            pid: None,
            pid_start_time: None,
            caps: capabilities(true),
            terminal: None,
            recovery_pending: false,
            metadata: Default::default(),
        })
    }
    fn observe(&self, sessions: &[ManagedSession]) -> Result<Vec<Observation>> {
        let views = self.runtime.list(None)?;
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
        self.runtime.stop(&b)?;
        let v = self
            .runtime
            .list(Some(&b.namespace()))?
            .into_iter()
            .find(|v| v.binding.runtime_id == b.runtime_id)
            .ok_or_else(|| error("not_found", "Runtime disappeared"))?;
        self.observed(&v)
    }
    fn prepare_resume(
        &self,
        _s: &ManagedSession,
        _mode: Option<&str>,
        _args: Option<&[String]>,
    ) -> Result<PreparedSession> {
        Err(error(
            "acp_required",
            "Open an ACP connection and send session/load",
        ))
    }
}
impl Backend {
    fn observed(&self, view: &RuntimeView) -> Result<Observation> {
        let mut observed = observation(view)?;
        observed.metadata.cwd = Some(
            self.runtime
                .paths()
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
            mode: None,
            caps: capabilities(true),
            terminal: None,
        })
    }
}
