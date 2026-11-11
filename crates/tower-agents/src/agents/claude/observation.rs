use super::{
    activity::Activity,
    transcript::{Facts, Reader},
};
use crate::{
    events::{self, AgentEvent, EventKind},
    hook::HookSession,
    runtime::RuntimeState,
};
use futures_util::future::BoxFuture;
use std::{path::PathBuf, time::Duration};
use tokio::sync::watch;
use tower_protocol::Patch;

#[derive(Clone, PartialEq)]
struct Request {
    path: PathBuf,
    id: String,
    revision: u64,
    model_after: u64,
    usage_after: u64,
}
#[derive(Clone)]
struct Update {
    request: Request,
    facts: Facts,
}
pub(super) struct Session {
    activity: Activity,
    root: Option<PathBuf>,
    worker: Option<tokio::task::JoinHandle<()>>,
    requests: Option<watch::Sender<Option<Request>>>,
    updates: Option<watch::Receiver<Option<Update>>>,
    request: Option<Request>,
    pending: Option<Update>,
}
impl Drop for Session {
    fn drop(&mut self) {
        if let Some(worker) = &self.worker {
            worker.abort();
        }
    }
}
impl Session {
    pub fn new(env: &tower_pal::Environment) -> Self {
        Self {
            activity: Activity::new(env),
            root: crate::config::home(env, "CLAUDE_CONFIG_DIR", ".claude")
                .ok()
                .map(|p| p.join("projects")),
            worker: None,
            requests: None,
            updates: None,
            request: None,
            pending: None,
        }
    }
}
impl HookSession for Session {
    fn start(&mut self) {
        if self.worker.is_some() {
            return;
        }
        let (requests, incoming) = watch::channel(None);
        let (updates, outgoing) = watch::channel(None);
        self.worker = Some(tokio::spawn(observe(incoming, updates)));
        self.requests = Some(requests);
        self.updates = Some(outgoing);
    }
    fn apply(&mut self, state: &mut RuntimeState, event: AgentEvent, now: u64) {
        if event.child || state.stop_requested || state.exit_code.is_some() {
            return;
        }
        let path = event.transcript_path.as_ref().map(PathBuf::from);
        let model = event.model.is_some();
        let compact = matches!(event.kind, EventKind::Compacting | EventKind::Compacted);
        let seq = state.seq;
        events::apply(state, event, now);
        if state.seq == seq {
            return;
        }
        let Some(id) = state.native.as_ref().map(|n| n.id.clone()) else {
            return;
        };
        let previous = self.request.as_ref().filter(|r| r.id == id);
        let path = path.or_else(|| previous.map(|r| r.path.clone()));
        let Some(path) = path else {
            return;
        };
        let Ok(path) = std::fs::canonicalize(path) else {
            return;
        };
        let Some(root) = self
            .root
            .as_ref()
            .and_then(|p| std::fs::canonicalize(p).ok())
        else {
            return;
        };
        if !path.starts_with(root) || path.components().any(|c| c.as_os_str() == "subagents") {
            return;
        }
        let cut = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let request = Request {
            path,
            id,
            revision: seq.saturating_add(1),
            model_after: if model {
                cut
            } else {
                previous.map_or(0, |r| r.model_after)
            },
            usage_after: if compact {
                cut
            } else {
                previous.map_or(0, |r| r.usage_after)
            },
        };
        if let Some(tx) = &self.requests {
            tx.send_replace(Some(request.clone()));
        }
        self.request = Some(request);
    }
    fn changed(&mut self) -> BoxFuture<'_, ()> {
        let Self {
            activity,
            updates,
            pending,
            ..
        } = self;
        Box::pin(async move {
            let transcript = async {
                let Some(rx) = updates else {
                    return std::future::pending().await;
                };
                if rx.changed().await.is_ok() {
                    *pending = rx.borrow_and_update().clone();
                } else {
                    std::future::pending().await
                }
            };
            tokio::select! {
                () = transcript => {}
                () = activity.changed() => {}
            }
        })
    }
    fn refresh(&mut self, state: &mut RuntimeState, now: u64) -> bool {
        let activity = self.activity.refresh(state, now);
        if state.stop_requested || state.exit_code.is_some() {
            return activity;
        }
        let Some(update) = self.pending.take().filter(|u| {
            self.request.as_ref() == Some(&u.request)
                && state.native.as_ref().is_some_and(|n| n.id == u.request.id)
        }) else {
            return activity;
        };
        let before = (state.detail.clone(), state.cwd.clone());
        match update.facts.tokens {
            Patch::Set(v) => state.detail.context_tokens = Some(v),
            Patch::Clear => state.detail.context_tokens = None,
            Patch::Missing => {}
        }
        if let Patch::Set(v) = update.facts.model {
            state.detail.model = Some(v);
        }
        if let Patch::Set(v) = update.facts.cwd {
            state.cwd = v;
        }
        if before != (state.detail.clone(), state.cwd.clone()) {
            state.seq = state.seq.saturating_add(1);
            state.updated_at = now;
            true
        } else {
            activity
        }
    }
}
async fn observe(
    mut requests: watch::Receiver<Option<Request>>,
    updates: watch::Sender<Option<Update>>,
) {
    let mut reader: Option<Reader> = None;
    let mut previous: Option<Request> = None;
    loop {
        let Some(request) = requests.borrow_and_update().clone() else {
            if requests.changed().await.is_err() {
                return;
            }
            continue;
        };
        let mut current = reader
            .take()
            .filter(|r| r.path == request.path)
            .unwrap_or_else(|| Reader::new(request.path.clone()));
        current.invalidate(
            previous
                .as_ref()
                .is_none_or(|r| r.model_after != request.model_after)
                && request.model_after > 0,
            previous
                .as_ref()
                .is_none_or(|r| r.usage_after != request.usage_after)
                && request.usage_after > 0,
        );
        let mut watched = tower_pal::FileWatch::new(std::slice::from_ref(&request.path)).ok();
        let (model, usage) = (request.model_after, request.usage_after);
        let Ok((current, facts)) = tokio::task::spawn_blocking(move || {
            let facts = current.read(model, usage);
            (current, facts)
        })
        .await
        else {
            return;
        };
        let more = current.more;
        reader = Some(current);
        previous = Some(request.clone());
        if requests.borrow().as_ref() == Some(&request)
            && let Ok(facts) = facts
        {
            updates.send_replace(Some(Update { request, facts }));
        }
        if !more {
            tokio::select! {
                result=requests.changed()=>{if result.is_err(){return;}},
                _=async {match &mut watched{Some(w)=>w.changed().await,None=>std::future::pending().await}}=>{},
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
