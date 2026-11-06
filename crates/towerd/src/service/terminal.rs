use super::*;
use tower_sessions::{ManagedSession, terminal::TerminalAccess};

impl Service {
    fn terminal_session(
        &self,
        id: &str,
    ) -> std::result::Result<(AgentRecord, ManagedSession, &dyn TerminalAccess), RpcError> {
        let record = self.resolve(id)?.record;
        if record.ownership != "owned" {
            return Err(app(
                "not_owned",
                "No managed terminal interface for this session",
            ));
        }
        let session = self.store.managed_session(&record.id).map_err(storage)?;
        let terminal = self
            .backends
            .select(Some(&session.protocol))?
            .terminal()
            .ok_or_else(|| app("unsupported", "This session has no terminal interface"))?;
        Ok((record, session, terminal))
    }
    pub(super) fn terminal_attach_info(&self, id: &str) -> std::result::Result<Value, RpcError> {
        self.sync_managed().map_err(storage)?;
        let (record, session, terminal) = self.terminal_session(id)?;
        if !record.caps.attach {
            return Err(app(
                "unsupported",
                "Terminal attachment is unavailable for this session",
            ));
        }
        value(terminal.attach_info(&session)?)
    }
    pub(super) fn terminal_snapshot(&self, id: &str) -> std::result::Result<Value, RpcError> {
        let (record, session, terminal) = self.terminal_session(id)?;
        if !record.caps.snapshot {
            return Err(app(
                "unsupported",
                "Terminal snapshots are unavailable for this session",
            ));
        }
        value(terminal.snapshot(&session)?)
    }
    /// Replaces the terminal pool and resumes its sessions. With
    /// `require_idle`, refuses while any terminal session is mid-turn.
    pub(super) fn upgrade_pool(&self, require_idle: bool) -> std::result::Result<Value, RpcError> {
        let terminal = self
            .backends
            .iter()
            .find_map(|b| b.terminal())
            .ok_or_else(|| app("unsupported", "No terminal pool is configured"))?;
        if require_idle {
            for session in self.store.managed_sessions().map_err(storage)? {
                if session.protocol != "terminal" {
                    continue;
                }
                let status = self
                    .store
                    .agent(&session.id)
                    .map_err(storage)?
                    .record
                    .status;
                if matches!(
                    status.as_str(),
                    "starting" | "working" | "waiting_input" | "waiting_approval"
                ) {
                    return Err(app("sessions_busy", "A terminal session is not idle"));
                }
            }
        }
        terminal.restart_pool()?;
        self.sync_managed().map_err(storage)?;
        value(self.terminal_pool_info())
    }
    pub(super) fn terminal_pool_info(&self) -> PoolInfo {
        self.backends.iter().find_map(|b| b.terminal()).map_or(
            PoolInfo {
                running: false,
                proto_version: None,
            },
            |t| t.pool_info(),
        )
    }
}
