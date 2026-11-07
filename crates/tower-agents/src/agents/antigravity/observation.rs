use crate::{
    events::{self, AgentEvent, EventKind},
    hook::HookSession,
    runtime::RuntimeState,
};
use futures_util::future::BoxFuture;
use std::{path::PathBuf, time::Duration};
use tokio::sync::watch;
mod reader;

#[derive(Clone, PartialEq)]
struct Request {
    id: String,
    revision: u64,
    idle: bool,
}
#[derive(Clone, Default)]
struct Facts {
    stale_usage: Option<bool>,
    compacting: bool,
    compacted: bool,
    tasks: Option<u32>,
    title: tower_protocol::Patch<String>,
}
#[derive(Clone)]
struct Update {
    request: Request,
    facts: Facts,
}
struct Worker {
    requests: watch::Sender<Option<Request>>,
    updates: watch::Receiver<Option<Update>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub(super) struct Session {
    root: Option<PathBuf>,
    worker: Option<Worker>,
    request: Option<Request>,
    pending: Option<Update>,
    facts: Facts,
    fully_idle: Option<bool>,
    revision: u64,
    idle: bool,
}
impl Session {
    pub fn new(env: &tower_pal::Environment) -> Self {
        let root = crate::config::home(env, "GEMINI_DIR", ".gemini")
            .ok()
            .map(|p| p.join("antigravity-cli"));
        Self {
            root,
            worker: None,
            request: None,
            pending: None,
            facts: Facts::default(),
            fully_idle: None,
            revision: 0,
            idle: false,
        }
    }
    fn apply_facts(&self, state: &mut RuntimeState) {
        if self.facts.stale_usage == Some(true) {
            state.detail.context_tokens = None;
        }
        match &self.facts.title {
            tower_protocol::Patch::Set(title) => state.display_name = Some(title.clone()),
            tower_protocol::Patch::Clear => state.display_name = None,
            tower_protocol::Patch::Missing => {}
        }
        if matches!(state.status.as_str(), "waiting_approval" | "waiting_input") {
            return;
        }
        if self.idle {
            let tasks = if self.fully_idle == Some(true) {
                Some(0)
            } else {
                self.facts.tasks
            };
            if let Some(count) = tasks {
                state.detail.background_jobs = Some(count);
            }
            if tasks
                .map(|n| n > 0)
                .unwrap_or(self.fully_idle == Some(false))
            {
                state.status = "working".into();
                state.detail.phase = Some("background".into());
            } else if state.detail.phase.as_deref() == Some("background") {
                state.status = "idle".into();
                state.detail.phase = None;
            }
            if self.facts.compacted && state.status == "idle" {
                state.detail.phase = Some("compacted".into());
            }
        } else if self.facts.compacting && state.status == "working" {
            state.detail.phase = Some("compacting".into());
        }
    }
}
impl HookSession for Session {
    fn start(&mut self) {
        if self.worker.is_some() {
            return;
        }
        if let Some(root) = self.root.clone() {
            let (requests, incoming) = watch::channel(None);
            let (updates, outgoing) = watch::channel(None);
            let task = tokio::spawn(observe(root, incoming, updates));
            self.worker = Some(Worker {
                requests,
                updates: outgoing,
                task,
            });
        }
    }
    fn apply(&mut self, state: &mut RuntimeState, mut event: AgentEvent, now: u64) {
        if event.child || state.stop_requested || state.exit_code.is_some() {
            return;
        }
        let old = state.native.as_ref().map(|n| n.id.clone());
        let rest = matches!(
            event.kind,
            EventKind::Idle | EventKind::Failure | EventKind::Compacted
        ) || event.authoritative_status.as_deref() == Some("idle");
        let working = event.kind == EventKind::Working
            || event.authoritative_status.as_deref() == Some("working");
        let fully_idle = event.background_active.map(|active| !active);
        let switching = event.root_confirmed
            && event
                .native_id
                .as_ref()
                .is_some_and(|id| old.as_ref() != Some(id));
        if !switching && self.facts.stale_usage == Some(true) {
            event.context_tokens = None;
            event.clear_context = true;
        }
        let seq = state.seq;
        events::apply(state, event, now);
        if state.seq == seq {
            return;
        }
        let Some(id) = state.native.as_ref().map(|n| n.id.clone()) else {
            return;
        };
        if old.as_ref() != Some(&id) || self.request.as_ref().is_none_or(|r| r.id != id) {
            self.facts = Facts::default();
            self.fully_idle = None;
        }
        if rest {
            self.idle = true;
            if let Some(value) = fully_idle {
                self.fully_idle = Some(value);
                if value {
                    self.facts.tasks = Some(0);
                } else if self.facts.tasks == Some(0) {
                    self.facts.tasks = None;
                }
            }
        } else if working {
            self.idle = false;
            self.fully_idle = None;
        }
        self.apply_facts(state);
        self.revision = self.revision.saturating_add(1);
        let request = Request {
            id,
            revision: self.revision,
            idle: self.idle && self.fully_idle != Some(true),
        };
        if let Some(worker) = &self.worker {
            worker.requests.send_replace(Some(request.clone()));
        }
        self.request = Some(request);
    }
    fn changed(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let Some(worker) = &mut self.worker else {
                return std::future::pending().await;
            };
            if worker.updates.changed().await.is_ok() {
                self.pending = worker.updates.borrow_and_update().clone();
            } else {
                std::future::pending().await
            }
        })
    }
    fn refresh(&mut self, state: &mut RuntimeState, now: u64) -> bool {
        let Some(update) = self.pending.take() else {
            return false;
        };
        if self.request.as_ref() != Some(&update.request) || state.stop_requested {
            return false;
        }
        let before = (
            state.status.clone(),
            state.detail.clone(),
            state.display_name.clone(),
        );
        if update.facts.stale_usage.is_some() {
            self.facts.stale_usage = update.facts.stale_usage;
        }
        self.facts.compacting = update.facts.compacting;
        self.facts.compacted = update.facts.compacted;
        if update.facts.tasks.is_some() {
            self.facts.tasks = update.facts.tasks;
        }
        if !update.facts.title.is_missing() {
            self.facts.title = update.facts.title;
        }
        self.apply_facts(state);
        if before
            != (
                state.status.clone(),
                state.detail.clone(),
                state.display_name.clone(),
            )
        {
            state.seq = state.seq.saturating_add(1);
            state.updated_at = now;
            true
        } else {
            false
        }
    }
}
async fn observe(
    root: PathBuf,
    mut requests: watch::Receiver<Option<Request>>,
    updates: watch::Sender<Option<Update>>,
) {
    let mut reader = None;
    loop {
        let request = requests.borrow_and_update().clone();
        let Some(request) = request else {
            if requests.changed().await.is_err() {
                return;
            }
            continue;
        };
        let mut current = reader
            .take()
            .filter(|r: &reader::Reader| r.id == request.id)
            .unwrap_or_else(|| reader::Reader::new(root.clone(), request.id.clone()));
        let mut watched = tower_pal::FileWatch::new(&current.paths()).ok();
        let idle = request.idle;
        let result = tokio::task::spawn_blocking(move || {
            let facts = current.read(idle);
            (current, facts)
        })
        .await;
        let Ok((current, facts)) = result else {
            return;
        };
        let more = current.more;
        reader = Some(current);
        if requests.borrow().as_ref() == Some(&request) {
            updates.send_replace(Some(Update { request, facts }));
        }
        if !more {
            tokio::select! {
                result=requests.changed()=>{if result.is_err(){return;}},
                _=async {match &mut watched {Some(w)=>w.changed().await,None=>std::future::pending().await}}=>{},
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
