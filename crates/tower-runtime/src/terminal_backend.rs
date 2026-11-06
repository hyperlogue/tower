use crate::{Binding, Result, Runtime, error, internal};
use std::sync::Arc;
use tower_protocol::{
    PoolInfo, Secret, SessionOpenParams,
    terminal::{AttachInfo, SnapshotResult},
};
use tower_sessions::{
    ManagedBackend, ManagedSession, Observation, PreparedSession, execution::ExecutionHost,
    terminal::TerminalAccess,
};

pub struct TerminalBackend {
    native: Arc<dyn ManagedBackend>,
    runtime: Arc<Runtime>,
}
impl TerminalBackend {
    pub fn new(native: Arc<dyn ManagedBackend>, runtime: Arc<Runtime>) -> Self {
        Self { native, runtime }
    }
    fn session(&self, s: &ManagedSession, live: bool) -> Result<ManagedSession> {
        match Binding::from_session(s)? {
            Some(binding) => self.runtime.terminal_session(&binding, live),
            None => Ok(s.clone()),
        }
    }
    fn access(&self) -> Result<&dyn TerminalAccess> {
        self.native
            .terminal()
            .ok_or_else(|| error("unsupported", "Terminal access is unavailable"))
    }
}
impl ManagedBackend for TerminalBackend {
    fn protocol(&self) -> &str {
        "terminal"
    }
    fn features(&self) -> &'static [&'static str] {
        self.native.features()
    }
    fn harnesses(&self) -> Vec<tower_protocol::HarnessInfo> {
        self.native.harnesses()
    }
    fn prepare(&self, id: &str, p: &SessionOpenParams) -> Result<PreparedSession> {
        self.native.prepare(id, p)
    }
    fn start(&self, s: &ManagedSession, token: &Secret) -> Result<Observation> {
        if Binding::from_session(s)?.is_some() {
            return Err(error(
                "unsupported_control",
                "Remote terminal launch requires a durable command",
            ));
        }
        self.native.start(s, token)
    }
    fn observe(&self, sessions: &[ManagedSession]) -> Result<Vec<Observation>> {
        let mut local = Vec::new();
        let mut remote = Vec::new();
        for session in sessions {
            if let Some(binding) = Binding::from_session(session)? {
                remote.push(binding);
            } else {
                local.push(session.clone());
            }
        }
        let mut observations = if local.is_empty() {
            Vec::new()
        } else {
            self.native.observe(&local)?
        };
        if !remote.is_empty() {
            let views = self.runtime.list(None)?;
            for binding in remote {
                if let Some(view) = views.iter().find(|v| {
                    v.binding.namespace() == binding.namespace()
                        && v.binding.runtime_id == binding.runtime_id
                        && v.binding.generation == binding.generation
                }) {
                    let m = serde_json::to_value(&view.metadata).map_err(internal)?;
                    {
                        let mut observation: Observation = match m.get("observation") {
                            Some(value) => {
                                serde_json::from_value(value.clone()).map_err(internal)?
                            }
                            None => crate::backend::observation(view)?,
                        };
                        observation.binding = None;
                        observation.caps.resume = false;
                        observation.recovery_pending = false;
                        if view.fenced {
                            observation.status = if view.termination == "confirmed" {
                                "exited"
                            } else {
                                "lost"
                            }
                            .into();
                            observation.caps.attach = false;
                            observation.caps.message = "none".into();
                            observation.caps.message_reason = Some("runtime_fenced".into());
                            observation.caps.kill = view.termination != "confirmed";
                        }
                        observations.push(observation);
                    }
                }
            }
        }
        Ok(observations)
    }
    fn stop(&self, s: &ManagedSession) -> Result<Observation> {
        if let Some(binding) = Binding::from_session(s)? {
            self.runtime.stop(&binding)?;
            self.observe(std::slice::from_ref(s))?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    error(
                        "stop_unconfirmed",
                        "Terminal stop has no durable observation",
                    )
                })
        } else {
            self.native.stop(s)
        }
    }
    fn prepare_resume(
        &self,
        s: &ManagedSession,
        mode: Option<&str>,
        args: Option<&[String]>,
    ) -> Result<PreparedSession> {
        if Binding::from_session(s)?.is_some() {
            return Err(error(
                "unsupported_control",
                "Remote terminal generations cannot be restarted or resumed locally",
            ));
        }
        self.native.prepare_resume(s, mode, args)
    }
    fn recover(&self, s: &ManagedSession) -> Result<Option<PreparedSession>> {
        if Binding::from_session(s)?.is_some() {
            Ok(None)
        } else {
            self.native.recover(s)
        }
    }
    fn cleanup(&self, s: &ManagedSession) -> Result<()> {
        self.native.cleanup(&self.session(s, false)?)
    }
    fn clipboard(&self, s: &ManagedSession) -> Result<Option<tower_sessions::ClipboardTarget>> {
        self.native.clipboard(&self.session(s, true)?)
    }
    fn send_message(
        &self,
        s: &ManagedSession,
        text: &str,
    ) -> Result<tower_protocol::DeliveryReceipt> {
        self.native.send_message(&self.session(s, true)?, text)
    }
    fn conversations(
        &self,
        p: &tower_protocol::ConversationListParams,
    ) -> Result<tower_protocol::ConversationList> {
        self.native.conversations(p)
    }
    fn terminal(&self) -> Option<&dyn TerminalAccess> {
        Some(self)
    }
}
impl TerminalAccess for TerminalBackend {
    fn attach_info(&self, s: &ManagedSession) -> Result<AttachInfo> {
        self.access()?.attach_info(&self.session(s, true)?)
    }
    fn snapshot(&self, s: &ManagedSession) -> Result<SnapshotResult> {
        self.access()?.snapshot(&self.session(s, false)?)
    }
    fn pool_info(&self) -> PoolInfo {
        self.native.terminal().map_or(
            PoolInfo {
                running: false,
                proto_version: None,
            },
            |t| t.pool_info(),
        )
    }
}
