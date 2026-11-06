use crate::{Binding, Result, error, host, internal, wire};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};
use tower_protocol::{
    ExecutionCommand, ExecutionReceipt, ExecutionStatus, RuntimeCursor, RuntimeEvents,
};
use tower_sessions::execution::{ExecutionHost, HarnessControls, RuntimeUpdates, RuntimeView};
#[derive(Clone)]
pub struct Client {
    pub(crate) paths: tower_pal::Paths,
}
impl Client {
    pub fn new(paths: tower_pal::Paths) -> Self {
        Self { paths }
    }
    pub(crate) fn terminal_session(
        &self,
        binding: &Binding,
        live: bool,
    ) -> Result<tower_sessions::ManagedSession> {
        self.ensure()?;
        self.request("terminal.session", json!({"binding":binding,"live":live}))
    }
    pub fn shutdown(&self) -> Result<()> {
        self.request("shutdown", json!({}))
    }
    fn request<T: Serialize, U: DeserializeOwned>(&self, method: &str, p: T) -> Result<U> {
        let mut socket = tower_pal::connect_blocking(&self.paths.run.join("runtime.sock"))
            .map_err(|_| {
                error(
                    "runtime_unavailable",
                    "Managed runtime service is unavailable",
                )
            })?;
        let result: Result<Value> = wire::call(
            &mut socket,
            &host::Call {
                credential: host::credential(&self.paths)?,
                method: method.into(),
                params: serde_json::to_value(p).map_err(internal)?,
            },
        )?;
        serde_json::from_value(result?).map_err(internal)
    }
    fn ensure(&self) -> Result<()> {
        if self.request::<_, Value>("hello", json!({})).is_ok() {
            return Ok(());
        }
        self.paths.prepare().map_err(internal)?;
        let _lock = tower_pal::SingletonLock::acquire(&self.paths.run.join("runtime-start.lock"))
            .map_err(internal)?;
        if self.request::<_, Value>("hello", json!({})).is_ok() {
            return Ok(());
        }
        host::prepare(&self.paths)?;
        tower_pal::start_runtime().map_err(|_| {
            error(
                "runtime_unavailable",
                "Cannot start managed runtime service",
            )
        })?;
        for _ in 0..100 {
            if self.request::<_, Value>("hello", json!({})).is_ok() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        Err(error(
            "runtime_unavailable",
            "Managed runtime service did not become ready",
        ))
    }
}
impl ExecutionHost for Client {
    fn controls(&self) -> BTreeMap<String, HarnessControls> {
        let env = tower_pal::environment();
        let mut out = BTreeMap::new();
        for (harness, bin, protocol, permissions, configure, cancel) in [
            ("codex", "codex", "codex-app-server", true, true, "turn"),
            ("claude-code", "claude", "acp", true, false, "turn"),
            ("antigravity", "agy", "acp", false, false, "runtime"),
        ] {
            if tower_pal::find_program(bin, &env).is_some() {
                let mut capabilities = vec!["prompt".into(), "resume".into()];
                if permissions {
                    capabilities.push("permission_reply".into());
                    capabilities.push("elicitation_reply".into());
                }
                if configure {
                    capabilities.push("configure".into());
                }
                out.insert(
                    harness.into(),
                    HarnessControls {
                        protocol: protocol.into(),
                        capabilities,
                        cancel_mode: Some(cancel.into()),
                    },
                );
            }
        }
        out
    }
    fn execute(&self, b: &Binding, c: &ExecutionCommand) -> Result<ExecutionReceipt> {
        self.ensure()?;
        self.request("execute", json!({"binding":b,"command":c}))
    }
    fn status(&self, ns: &str, c: &ExecutionStatus) -> Result<ExecutionReceipt> {
        self.ensure()?;
        self.request("status", json!({"namespace":ns,"command":c}))
    }
    fn list(&self, namespace: Option<&str>) -> Result<Vec<RuntimeView>> {
        if !self.paths.state.join("runtime/runtime.db").exists() {
            return Ok(Vec::new());
        }
        self.ensure()?;
        #[derive(serde::Deserialize)]
        struct Page {
            records: Vec<RuntimeView>,
            cursor: Option<u64>,
        }
        let mut records = Vec::new();
        let mut after = 0;
        loop {
            let page: Page = self.request("list", json!({"namespace":namespace,"after":after}))?;
            records.extend(page.records);
            let Some(next) = page.cursor else { break };
            if next <= after {
                return Err(error(
                    "runtime_protocol",
                    "Runtime pagination did not advance",
                ));
            }
            after = next;
        }
        Ok(records)
    }
    fn updates(&self, ns: &str, connection: &str) -> Result<RuntimeUpdates> {
        if !self.paths.state.join("runtime/runtime.db").exists() {
            return Ok(Default::default());
        }
        self.request("updates", json!({"namespace":ns,"connection":connection}))
    }
    fn batch(&self, b: &Binding) -> Result<Option<RuntimeEvents>> {
        self.request("batch", b)
    }
    fn cursor(&self, b: &Binding) -> Result<RuntimeCursor> {
        self.request("cursor", b)
    }
    fn reannounce(&self, b: &Binding) -> Result<()> {
        self.request("reannounce", b)
    }
    fn acknowledge(&self, ns: &str, kind: &str, value: tower_protocol::Value) -> Result<()> {
        self.request("ack", json!({"namespace":ns,"kind":kind,"value":value}))
    }
    fn fence(&self, b: &Binding, reason: &str) -> Result<()> {
        self.request("fence", json!({"binding":b,"reason":reason}))
    }
    fn stop(&self, b: &Binding) -> Result<()> {
        self.request::<_, ()>("stop", json!({"binding":b}))?;
        for _ in 0..100 {
            let views = self.list(Some(&b.namespace()))?;
            if views.iter().any(|v| {
                v.binding.runtime_id == b.runtime_id && v.fenced && v.termination == "confirmed"
            }) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        Err(error(
            "stop_unconfirmed",
            "Runtime termination was not confirmed",
        ))
    }
}
