use futures_util::future::BoxFuture;
use std::{io, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tower_agents::runtime::{LauncherPlan, RuntimeState};

pub(crate) type SessionTask = BoxFuture<'static, io::Result<SessionExit>>;
pub(crate) trait AgentAdapter: Send {
    fn run(self: Box<Self>, context: SessionContext) -> SessionTask;
}
pub(crate) struct SessionContext {
    pub plan: LauncherPlan,
    pub token: String,
    pub state: RuntimeState,
    pub publisher: SessionPublisher,
    pub commands: SessionCommands,
}
pub(crate) struct SessionExit {
    pub code: i32,
    pub state: RuntimeState,
}
pub(crate) enum SessionCommand {
    Stop,
}
pub(crate) struct SessionCommands(pub mpsc::Receiver<SessionCommand>);
impl SessionCommands {
    pub async fn stop(&mut self) {
        match self.0.recv().await {
            Some(SessionCommand::Stop) => (),
            None => std::future::pending().await,
        }
    }
    pub fn requested(&mut self) -> bool {
        self.0.try_recv().is_ok()
    }
}
pub(crate) struct SessionUpdate {
    state: RuntimeState,
    receipt: Option<oneshot::Sender<io::Result<()>>>,
}
#[derive(Clone)]
pub(crate) struct SessionPublisher(mpsc::Sender<SessionUpdate>);
impl SessionPublisher {
    pub fn channel() -> (Self, mpsc::Receiver<SessionUpdate>) {
        let (tx, rx) = mpsc::channel(64);
        (Self(tx), rx)
    }
    pub async fn publish(&self, state: &RuntimeState) -> io::Result<()> {
        self.0
            .send(SessionUpdate {
                state: state.clone(),
                receipt: None,
            })
            .await
            .map_err(|_| io::Error::other("session writer disconnected"))
    }
    pub async fn write(&self, state: &RuntimeState) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(SessionUpdate {
                state: state.clone(),
                receipt: Some(tx),
            })
            .await
            .map_err(|_| io::Error::other("session writer disconnected"))?;
        rx.await
            .map_err(|_| io::Error::other("session writer lost its receipt"))?
    }
}

/// Follows a session's recorded states to mark when it began to need its
/// user. Only a launcher with no daemon does this: the daemon's registry
/// applies the same rules to every session it observes.
pub(crate) struct Attention {
    status: String,
    error: Option<String>,
    since: Option<u64>,
}
impl Attention {
    pub fn new(state: &RuntimeState) -> Self {
        Self {
            status: state.status.clone(),
            error: state.detail.error.clone(),
            since: state.attention,
        }
    }
    pub fn note(&mut self, state: &mut RuntimeState) {
        use tower_sessions::attention;
        let status = state.status.as_str();
        match attention::on_status(&self.status, status, state.stop_requested) {
            Some(true) => self.since = Some(state.seq),
            Some(false) => self.since = None,
            None => {}
        }
        if status != "unknown" {
            self.status = status.into();
        }
        if attention::on_error(self.error.as_deref(), state.detail.error.as_deref()) {
            self.since = Some(state.seq);
        }
        self.error = state.detail.error.clone();
        state.attention = self.since;
    }
}

pub(crate) async fn drive(
    mut task: SessionTask,
    mut updates: mpsc::Receiver<SessionUpdate>,
    commands: mpsc::Sender<SessionCommand>,
    mut state: RuntimeState,
    mut terminated: mpsc::Receiver<()>,
    mut persist: impl FnMut(&RuntimeState) -> io::Result<()>,
) -> io::Result<i32> {
    let mut dirty = false;
    let mut exited = None;
    loop {
        tokio::select! {
            biased;
            Some(()) = terminated.recv() => {
                let _ = commands.try_send(SessionCommand::Stop);
            }
            _ = tokio::time::sleep(Duration::from_millis(250)), if dirty => {
                if persist(&state).is_ok() { dirty = false; }
            }
            Some(update) = updates.recv(), if exited.is_none() => {
                state = update.state;
                let result = persist(&state);
                dirty = result.is_err();
                if let Some(receipt) = update.receipt { let _ = receipt.send(result); }
            }
            result = &mut task, if exited.is_none() => {
                let code = match result {
                    Ok(exit) => {state = exit.state; exit.code}
                    Err(error) => {
                        state.detail.error = Some(match error.kind() {
                            io::ErrorKind::NotFound => "agent_not_found",
                            io::ErrorKind::PermissionDenied => "environment_not_approved",
                            _ => "launch_failed",
                        }.into());
                        eprintln!("tower: {}", state.detail.error.as_deref().unwrap());
                        1
                    }
                };
                state.status = "exited".into();
                state.exit_code = Some(code);
                state.seq = state.seq.saturating_add(1);
                state.updated_at = super::launcher::now();
                dirty = persist(&state).is_err();
                exited = Some(code);
            }
        }
        if let Some(code) = exited
            && !dirty
        {
            return Ok(code);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    fn state() -> RuntimeState {
        serde_json::from_value(serde_json::json!({
            "id":"session","generation":"one","harness":"claude-code","status":"starting",
            "cwd":"/work","native":null,"display_name":null,"detail":{},"launcher_pid":1,
            "launcher_start_time":1,"boot_id":"boot","agent_pid":null,"agent_start_time":null,
            "seq":0,"updated_at":0,"stop_requested":false,"exit_code":null
        }))
        .unwrap()
    }
    #[test]
    fn attention_starts_at_waits_and_finished_turns_and_ends_with_work() {
        let mut state = state();
        let mut attention = Attention::new(&state);
        let mut step = |status: &str, seq| {
            state.status = status.into();
            state.seq = seq;
            attention.note(&mut state);
            state.attention
        };
        assert_eq!(step("idle", 1), None);
        assert_eq!(step("working", 2), None);
        assert_eq!(step("unknown", 3), None);
        assert_eq!(step("idle", 4), Some(4));
        assert_eq!(step("idle", 5), Some(4));
        assert_eq!(step("working", 6), None);
        assert_eq!(step("waiting_approval", 7), Some(7));
        assert_eq!(step("working", 8), None);
        state.detail.error = Some("agent_turn_failed".into());
        state.seq = 9;
        attention.note(&mut state);
        assert_eq!(state.attention, Some(9));
        state.stop_requested = true;
        state.status = "exited".into();
        attention.note(&mut state);
        assert_eq!(state.attention, None);
    }
    #[tokio::test(start_paused = true)]
    async fn failed_writes_keep_the_task_and_stop_control_alive_and_retry_latest_facts() {
        let (publisher, updates) = SessionPublisher::channel();
        let (commands, receiver) = mpsc::channel(1);
        let mut control = SessionCommands(receiver);
        let (terminated, signal) = mpsc::channel(1);
        let storage = Arc::new(AtomicBool::new(false));
        let starts = Arc::new(AtomicUsize::new(0));
        let (failed, observed) = oneshot::channel();
        let count = starts.clone();
        let task: SessionTask = Box::pin(async move {
            count.fetch_add(1, Ordering::SeqCst);
            let mut state = state();
            state.status = "working".into();
            publisher.publish(&state).await?;
            assert!(publisher.write(&state).await.is_err());
            let _ = failed.send(());
            control.stop().await;
            state.stop_requested = true;
            publisher.write(&state).await?;
            Ok(SessionExit { code: 0, state })
        });
        let saved = Arc::new(Mutex::new(Vec::new()));
        let output = saved.clone();
        let ready = storage.clone();
        let writer = tokio::spawn(drive(
            task,
            updates,
            commands,
            state(),
            signal,
            move |state| {
                if !ready.load(Ordering::SeqCst) {
                    return Err(io::Error::other("fixture storage unavailable"));
                }
                output.lock().unwrap().push(state.clone());
                Ok(())
            },
        ));
        observed.await.unwrap();
        assert!(!writer.is_finished());
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        storage.store(true, Ordering::SeqCst);
        terminated.send(()).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), writer)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            0
        );
        let saved = saved.lock().unwrap();
        assert!(
            saved
                .iter()
                .any(|s| s.status == "working" && s.stop_requested)
        );
        assert_eq!(saved.last().unwrap().status, "exited");
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }
    #[tokio::test(start_paused = true)]
    async fn losing_the_control_sender_is_not_a_stop_command() {
        let (sender, receiver) = mpsc::channel(1);
        let mut control = SessionCommands(receiver);
        drop(sender);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), control.stop())
                .await
                .is_err()
        );
    }
    #[tokio::test(start_paused = true)]
    async fn exit_is_not_reported_until_its_final_record_is_saved() {
        let (_publisher, updates) = SessionPublisher::channel();
        let (commands, _receiver) = mpsc::channel(1);
        let tries = Arc::new(AtomicUsize::new(0));
        let attempts = tries.clone();
        let task: SessionTask = Box::pin(async {
            Ok(SessionExit {
                code: 7,
                state: state(),
            })
        });
        let code = drive(
            task,
            updates,
            commands,
            state(),
            mpsc::channel(1).1,
            move |state| {
                assert_eq!(state.exit_code, Some(7));
                if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(io::Error::other("retry"))
                } else {
                    Ok(())
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(code, 7);
        assert_eq!(tries.load(Ordering::SeqCst), 3);
    }
}
