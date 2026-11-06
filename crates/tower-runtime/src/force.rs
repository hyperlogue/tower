use crate::{Binding, Result, engine, error, internal, millis, parse_time};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tower_protocol::{ExecutionCommand, ExecutionResult};

pub(crate) struct Work {
    pub ctx: engine::Context,
    pub binding: Binding,
    pub command: ExecutionCommand,
    pub targets: Vec<(Binding, Option<Arc<engine::Handle>>)>,
}
impl Work {
    pub async fn run(
        self,
        handle: Arc<engine::Handle>,
        normal: mpsc::Receiver<ExecutionCommand>,
        mut control: mpsc::Receiver<engine::Control>,
        recover: bool,
    ) {
        let outcome = tokio::select! { biased;
            stop=control.recv()=> match stop {
                Some(engine::Control::Suspend)=>return,
                Some(engine::Control::Command(c))=> {
                    if self.ctx.authority.check(&self.binding,&c).await.is_ok()
                        && self.ctx.journal.begin_dispatch(&self.binding,&c.command_id).unwrap_or(false) {
                        let _=self.ctx.journal.finish_dispatch(&self.binding,&c.command_id,false,Some(ExecutionResult {outcome:Some("stopped".into()),..Default::default()}),None);
                    }
                    Err(error("runtime_cancelled","Force resume was cancelled before replacement launch"))
                }
                _=>Err(error("runtime_cancelled","Force resume was stopped before replacement launch")),
            },
            result=self.clean(recover)=>result,
        };
        if let Err(e) = outcome {
            let code = match e.data.code.as_str() {
                "denied"
                | "approval_required"
                | "command_expired"
                | "runtime_cancelled"
                | "cleanup_target_conflict" => e.data.code.as_str(),
                _ => "cleanup_unconfirmed",
            };
            let _ = self
                .ctx
                .journal
                .force_failed(&self.binding, &self.command, code);
            return;
        }
        engine::run(
            self.ctx,
            self.binding,
            handle,
            normal,
            control,
            engine::Start::Launch,
        )
        .await;
    }
    async fn authorize(&self, target: Option<&Binding>, recover: bool) -> Result<()> {
        loop {
            if parse_time(&self.command.expires_at)? <= millis() {
                return Err(error("command_expired", "Force resume expired"));
            }
            if self.ctx.journal.state(&self.binding)?.1 {
                return Err(error("runtime_fenced", "Force resume is fenced"));
            }
            let result = match target {
                Some(t) => {
                    self.ctx
                        .authority
                        .check_target(&self.binding, &self.command, t)
                        .await
                }
                None => self.ctx.authority.check(&self.binding, &self.command).await,
            };
            match result {
                Err(e)
                    if recover
                        && matches!(
                            e.data.code.as_str(),
                            "authority_unavailable" | "control_unavailable"
                        ) =>
                {
                    tokio::time::sleep(Duration::from_millis(250)).await
                }
                result => return result,
            }
        }
    }
    async fn clean(&self, recover: bool) -> Result<()> {
        self.authorize(None, recover).await?;
        for (target, owner) in &self.targets {
            self.authorize(Some(target), recover).await?;
            let (_, fenced, termination) = self.ctx.journal.state(target)?;
            if !fenced {
                return Err(error(
                    "cleanup_target_conflict",
                    "Cleanup target fence was lost",
                ));
            }
            if termination == "confirmed" {
                continue;
            }
            self.ctx
                .journal
                .check_cleanup(&self.binding, &self.command)?;
            if let Some(owner) = owner {
                owner.requests.retire_all()?;
                if !owner.normal.is_closed() {
                    owner
                        .control
                        .try_send(engine::Control::Stop("force_resume".into()))
                        .map_err(|_| {
                            error(
                                "cleanup_unconfirmed",
                                "Targeted owner stop queue is unavailable",
                            )
                        })?;
                    tokio::time::timeout(Duration::from_secs(35), owner.normal.closed())
                        .await
                        .map_err(|_| {
                            error(
                                "cleanup_unconfirmed",
                                "Targeted owner stop was not confirmed",
                            )
                        })?;
                }
            }
            if self.ctx.journal.state(target)?.2 != "confirmed" {
                self.authorize(Some(target), recover).await?;
                cleanup_orphan(&self.ctx, target).await?;
                self.ctx
                    .journal
                    .fence(target, "force_resume", "confirmed")?;
                engine::cleanup_environment(&self.ctx.paths, target);
            }
        }
        self.authorize(None, recover).await?;
        self.ctx.journal.force_ready(&self.binding, &self.command)
    }
}
async fn cleanup_orphan(ctx: &engine::Context, b: &Binding) -> Result<()> {
    let unknown = || {
        error(
            "cleanup_unconfirmed",
            "Exact native execution identity or termination could not be established",
        )
    };
    if ctx.journal.never_dispatched(b)? {
        return Ok(());
    }
    let metadata = ctx.journal.metadata(b)?;
    let boot = metadata["boot_id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(unknown)?;
    if boot != tower_pal::boot_id().map_err(internal)? {
        return Ok(());
    }
    let pid = metadata["pid"]
        .as_u64()
        .and_then(|p| u32::try_from(p).ok())
        .filter(|p| *p > 1)
        .ok_or_else(unknown)?;
    let start = metadata["pid_start_time"].as_u64().ok_or_else(unknown)?;
    if b.protocol == "acp" {
        let identity = tower_pal::ProcessIdentity {
            pid,
            parent: 0,
            start_time: start,
        };
        if tower_pal::protocol_group_terminated(identity).map_err(|_| unknown())? {
            return Ok(());
        }
        tower_pal::kill_protocol_group(identity).map_err(|_| unknown())?;
        for _ in 0..200 {
            if tower_pal::protocol_group_terminated(identity).map_err(|_| unknown())? {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        return Err(unknown());
    }
    if b.protocol != "codex-app-server" {
        return Err(unknown());
    }
    let thread = metadata["native_session_id"]
        .as_str()
        .filter(|s| crate::identifier(s))
        .ok_or_else(unknown)?;
    let namespace = tower_agents::codex_host::namespace(&ctx.paths).map_err(|_| unknown())?;
    if metadata["native_home"] != namespace {
        return Err(unknown());
    }
    let config = tower_agents::codex_service::Config::load(&ctx.paths).map_err(|_| unknown())?;
    let mut client = tower_agents::codex_service::Client::connect(&config.endpoint)
        .await
        .map_err(|_| unknown())?;
    if !client
        .process()
        .is_some_and(|p| p.pid == pid && p.start_time == start)
    {
        return Err(unknown());
    }
    client
        .stop_thread(thread, metadata["turn"].as_str())
        .await
        .map_err(|_| unknown())?;
    client
        .wait_unloaded(thread, Duration::from_secs(5))
        .await
        .map_err(|_| unknown())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn context(root: &std::path::Path) -> engine::Context {
        engine::Context {
            paths: tower_pal::Paths {
                state: root.into(),
                run: root.join("run"),
                socket: root.join("tower.sock"),
                database: root.join("tower.db"),
                tokens: root.join("tokens"),
                logs: root.join("logs"),
                config_home: root.join("config"),
                home: root.into(),
            },
            journal: Arc::new(
                crate::Journal::open(&root.join("runtime.db"), crate::Limits::default()).unwrap(),
            ),
            authority: Default::default(),
        }
    }
    #[tokio::test]
    async fn missing_native_identity_cannot_authorize_targeted_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let ctx = context(root.path());
        let b = Binding {
            origin: "https://allgather.example".into(),
            machine_id: "machine".into(),
            session_id: "logical".into(),
            runtime_id: "old".into(),
            generation: "generation".into(),
            tower_session_id: "tower-id".into(),
            owner_id: "owner".into(),
            protocol: "codex-app-server".into(),
            harness: "codex".into(),
            cwd: "/work/project".into(),
            resume_session_id: Some("caller-supplied-native".into()),
        };
        let c = crate::command(&b, "launch", serde_json::json!({})).unwrap();
        ctx.journal.admit(&b, &c).unwrap();
        ctx.journal.begin_dispatch(&b, &c.command_id).unwrap();
        ctx.journal.fence(&b, "owner_lost", "unknown").unwrap();
        assert_eq!(
            cleanup_orphan(&ctx, &b).await.unwrap_err().data.code,
            "cleanup_unconfirmed"
        );
        assert_eq!(ctx.journal.state(&b).unwrap().2, "unknown");
    }
}
