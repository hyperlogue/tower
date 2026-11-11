use crate::service::{Service, now_ms};
use serde::Deserialize;
use serde_json::json;
use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};
use tower_protocol::*;
use tower_store::{Store, StoredCheckout, StoredVcsOperation};
use tower_vcs::{Binding, Prepared};

type Result<T> = std::result::Result<T, RpcError>;
pub(crate) fn failure(_: impl std::fmt::Display) -> RpcError {
    RpcError::application("vcs_storage", "Checkout state is unavailable")
}
pub(crate) fn native(error: tower_vcs::Error) -> RpcError {
    let mut e = RpcError::application(&error.code, "VCS operation failed");
    e.data
        .details
        .insert("may_have_effect".into(), Value::Bool(error.may_have_effect));
    e
}
pub(crate) fn encode(v: impl serde::Serialize) -> Result<Value> {
    serde_json::to_value(v)
        .and_then(serde_json::from_value)
        .map_err(failure)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(v: &Value) -> Result<T> {
    serde_json::from_value(serde_json::to_value(v).map_err(failure)?)
        .map_err(|_| RpcError::application("invalid_params", "Invalid checkout parameters"))
}
fn id(prefix: &str) -> Result<String> {
    let mut b = [0; 16];
    getrandom::fill(&mut b).map_err(failure)?;
    Ok(format!(
        "{prefix}{}",
        b.iter().map(|b| format!("{b:02x}")).collect::<String>()
    ))
}
fn operation(kind: &str, options: Value) -> Result<tower_vcs::Operation> {
    let mut options = match serde_json::to_value(options).map_err(failure)? {
        serde_json::Value::Null => json!({}),
        serde_json::Value::Object(v) => v.into(),
        _ => {
            return Err(RpcError::application(
                "invalid_params",
                "VCS options must be an object",
            ));
        }
    };
    options["kind"] = kind.replace('.', "_").into();
    serde_json::from_value(options)
        .map_err(|_| RpcError::application("unsupported", "Unsupported VCS operation or options"))
}
pub(crate) fn authentication(
    auth: &VcsAuth,
    peer: tower_pal::Peer,
    paths: &tower_pal::Paths,
) -> Result<tower_vcs::Authentication> {
    match auth {
        VcsAuth::Host => Ok(tower_vcs::Authentication::host()),
        VcsAuth::SshAgent { socket } => {
            if peer.uid != tower_pal::uid() || peer.pid.is_none() {
                return Err(RpcError::application(
                    "unsupported_auth_transport",
                    "SSH-agent authentication requires a local UDS caller",
                ));
            }
            let path = paths.absolute_cwd(&socket.0).map_err(|_| {
                RpcError::application("auth_unavailable", "Invalid agent socket path")
            })?;
            tower_vcs::Authentication::ssh_agent(&path).map_err(native)
        }
    }
}
#[derive(Clone, Default, Deserialize)]
#[serde(default)]
pub(crate) struct Config {
    pub worktree_root: Option<String>,
}
pub(crate) struct Vcs {
    pub store: Arc<Store>,
    pub paths: tower_pal::Paths,
    pub root: PathBuf,
    pub gate: Arc<tokio::sync::Semaphore>,
    active: Arc<tokio::sync::watch::Sender<BTreeSet<String>>>,
}
/// Marks a plan as executing in this process and holds its worker slot until dropped.
pub(crate) struct Active {
    plan: String,
    set: Arc<tokio::sync::watch::Sender<BTreeSet<String>>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl Drop for Active {
    fn drop(&mut self) {
        self.set.send_modify(|set| {
            set.remove(&self.plan);
        });
    }
}
impl Vcs {
    pub fn new(
        paths: tower_pal::Paths,
        store: Arc<Store>,
        config: Config,
    ) -> std::io::Result<Self> {
        let root = match config.worktree_root {
            Some(p) => paths.absolute_cwd(&p)?,
            None => paths.home.join("worktrees"),
        };
        Ok(Self {
            store,
            paths,
            root,
            gate: Arc::new(tokio::sync::Semaphore::new(4)),
            active: Arc::new(tokio::sync::watch::channel(BTreeSet::new()).0),
        })
    }
    pub fn binding(c: &StoredCheckout) -> Result<Binding> {
        serde_json::from_value(c.binding.clone()).map_err(failure)
    }
    pub fn prepared(op: &StoredVcsOperation) -> Result<Prepared> {
        let invalid = || RpcError::application("internal_error", "Invalid stored VCS plan");
        let (data, forwarded) = match op.prepared.get("format") {
            None => (&op.prepared, false),
            Some(format) if format == "vcs.ssh-agent.v1" => (&op.prepared["prepared"], true),
            _ => return Err(invalid()),
        };
        let prepared = Prepared::deserialize(data).map_err(|_| invalid())?;
        if (prepared.review().authentication == tower_vcs::AuthMode::SshAgent) != forwarded {
            return Err(invalid());
        }
        Ok(prepared)
    }
    pub fn checkout(&self, id: &str) -> Result<StoredCheckout> {
        self.store.checkout(id).map_err(|e| match e {
            tower_store::Error::NotFound => {
                RpcError::application("not_found", "Checkout not found")
            }
            _ => failure(e),
        })
    }
    fn association(
        &self,
        s: &tower_vcs::Snapshot,
        requested: Option<&str>,
        existing: Option<&str>,
    ) -> Result<Option<String>> {
        let catalog = self.store.catalog().map_err(failure)?;
        let Some(catalog) = catalog else {
            return if requested.is_some() {
                Err(RpcError::application(
                    "catalog_unavailable",
                    "Repository catalog is unavailable",
                ))
            } else {
                Ok(None)
            };
        };
        let matches = catalog
            .repositories
            .iter()
            .filter(|r| {
                r.upstream_url
                    .iter()
                    .chain(&r.url_aliases)
                    .filter_map(|u| tower_vcs::canonical_remote(u))
                    .any(|u| s.remote_urls.contains(&u))
            })
            .collect::<Vec<_>>();
        if let Some(id) = requested {
            let repository = catalog
                .repositories
                .iter()
                .find(|r| r.id == id)
                .ok_or_else(|| {
                    RpcError::application("not_found", "Repository is not in the catalog")
                })?;
            if (repository.upstream_url.is_some() || !repository.url_aliases.is_empty())
                && !matches.iter().any(|r| r.id == id)
            {
                return Err(RpcError::application(
                    "repository_mismatch",
                    "Checkout remote does not match this repository or its approved aliases",
                ));
            }
            return Ok(Some(id.into()));
        }
        if let Some(existing) = existing
            && catalog.repositories.iter().any(|r| {
                r.id == existing
                    && ((r.upstream_url.is_none() && r.url_aliases.is_empty())
                        || matches.iter().any(|m| m.id == existing))
            })
        {
            return Ok(Some(existing.into()));
        }
        Ok((matches.len() == 1).then(|| matches[0].id.clone()))
    }
    pub async fn observe(
        &self,
        mut binding: Binding,
        repository: Option<&str>,
    ) -> Result<Checkout> {
        let snapshot = tower_vcs::status(&binding).await.map_err(native)?;
        let old = self
            .store
            .checkout_identity(&binding.identity_key())
            .map_err(failure)?;
        if old.is_none() && !snapshot.available {
            return Err(RpcError::application(
                "checkout_unavailable",
                "Working copy is not available",
            ));
        }
        if let Some(old) = &old {
            let previous = Self::binding(old)?;
            if !snapshot.available {
                binding = previous;
            } else if !binding.can_reconcile(&previous) {
                return Err(RpcError::application(
                    "checkout_identity_conflict",
                    "The directory no longer proves the registered checkout identity",
                ));
            }
        }
        let path = binding.directory.as_ref().ok_or_else(|| {
            RpcError::application("not_a_checkout", "Bare storage has no working directory")
        })?;
        let public = self
            .paths
            .display_cwd(&path.to_string_lossy())
            .map_err(failure)?;
        let repository_id = if snapshot.available {
            if repository.is_none() && old.as_ref().is_some_and(|c| c.association_manual) {
                old.as_ref().and_then(|c| c.record.repository_id.clone())
            } else {
                self.association(
                    &snapshot,
                    repository,
                    old.as_ref().and_then(|c| c.record.repository_id.as_deref()),
                )?
            }
        } else {
            old.as_ref().and_then(|c| c.record.repository_id.clone())
        };
        let record = Checkout {
            id: old
                .as_ref()
                .map(|c| Ok(c.record.id.clone()))
                .unwrap_or_else(|| id("checkout_"))?,
            repository_id,
            path: public,
            system: snapshot.system,
            branch: snapshot.branch,
            head: snapshot.head,
            dirty: snapshot.dirty,
            conflicts: snapshot.conflicts,
            locked: snapshot.locked,
            prunable: snapshot.prunable,
            available: snapshot.available,
            tracking_ref: snapshot.tracking_ref,
            ahead: snapshot.ahead,
            behind: snapshot.behind,
            observed_at: now_ms(),
            revision: old.as_ref().map_or(0, |c| c.record.revision),
        };
        self.store
            .save_checkout(StoredCheckout {
                identity: binding.identity_key(),
                association_manual: repository.is_some()
                    || old.as_ref().is_some_and(|c| c.association_manual),
                binding: serde_json::to_value(binding).map_err(failure)?,
                record,
            })
            .map_err(failure)
    }
    pub async fn register(
        &self,
        directory: &str,
        repository: Option<&str>,
    ) -> Result<Vec<Checkout>> {
        let directory = self.paths.absolute_cwd(directory).map_err(failure)?;
        let discovery = tower_vcs::discover(&directory).await.map_err(native)?;
        self.observe_worktrees(discovery.worktrees, repository)
            .await
    }
    pub async fn register_request(&self, p: CheckoutRegister, actor: String) -> Result<Value> {
        let directory = self.paths.absolute_cwd(&p.directory).map_err(failure)?;
        let discovery = tower_vcs::discover(&directory).await.map_err(native)?;
        let rows = self
            .observe_worktrees(discovery.worktrees, p.repository_id.as_deref())
            .await?;
        let plan = if let Some(options) = p.create {
            let operation = operation("checkout.create", options).map_err(|_| {
                RpcError::application("invalid_params", "Invalid worktree creation options")
            })?;
            Some(
                self.prepare_binding(
                    &discovery.source,
                    operation,
                    actor,
                    None,
                    p.repository_id,
                    &tower_vcs::Authentication::host(),
                )
                .await?,
            )
        } else {
            None
        };
        encode(json!({"checkouts": rows, "plan": plan}))
    }
    async fn observe_worktrees(
        &self,
        worktrees: Vec<Binding>,
        repository: Option<&str>,
    ) -> Result<Vec<Checkout>> {
        let mut results = Vec::new();
        for binding in worktrees {
            match self.observe(binding, repository).await {
                Ok(record) => results.push(record),
                Err(e) if e.data.code == "checkout_unavailable" => {}
                Err(e) => return Err(e),
            }
        }
        let checkouts = self
            .store
            .checkouts(&CheckoutFilter::default())
            .map_err(failure)?;
        for session in self
            .store
            .agents(&AgentFilter::default())
            .map_err(failure)?
        {
            self.link_from(&session, &checkouts)?;
        }
        Ok(results)
    }
    pub fn link_session(&self, session: &str) -> Result<()> {
        let session = self.store.agent(session).map_err(failure)?.record;
        if session.checkout_id.is_none() {
            self.link_from(
                &session,
                &self
                    .store
                    .checkouts(&CheckoutFilter::default())
                    .map_err(failure)?,
            )?;
        }
        Ok(())
    }
    fn link_from(&self, session: &AgentRecord, checkouts: &[StoredCheckout]) -> Result<()> {
        if session.checkout_id.is_some() {
            return Ok(());
        }
        let Ok(cwd) = self
            .paths
            .absolute_cwd(&session.cwd)
            .and_then(|p| p.canonicalize())
        else {
            return Ok(());
        };
        let best = checkouts
            .iter()
            .filter(|c| c.record.available)
            .filter_map(|c| {
                let binding = Self::binding(c).ok()?;
                let path = binding.directory.as_ref()?;
                if !cwd.starts_with(path) {
                    return None;
                }
                tower_vcs::validate_binding(&binding).ok()?;
                Some((path.components().count(), &c.record.id))
            })
            .max();
        if let Some((_, id)) = best {
            self.store
                .link_session_checkout(&session.id, id)
                .map_err(failure)?;
        }
        Ok(())
    }
    pub async fn refresh(&self, checkout: &str) -> Result<Checkout> {
        let mut c = self.checkout(checkout)?;
        let b = Self::binding(&c)?;
        match self.observe(b, None).await {
            Ok(row) => Ok(row),
            Err(e)
                if matches!(
                    e.data.code.as_str(),
                    "missing"
                        | "stale_checkout"
                        | "not_a_checkout"
                        | "identity_changed"
                        | "path_unavailable"
                        | "not_repository"
                        | "not_directory"
                ) =>
            {
                c.record.available = false;
                c.record.observed_at = now_ms();
                self.store.save_checkout(c).map_err(failure)
            }
            Err(e) => Err(e),
        }
    }
    /// Keeps checkout observations current from Git metadata changes and agent activity.
    pub async fn observe_changes(
        self: Arc<Self>,
        mut events: tokio::sync::broadcast::Receiver<Arc<EventNotification>>,
    ) {
        use std::collections::{BTreeMap, BTreeSet};
        let mut changes = tower_store::changes();
        let (due, mut pending) = tokio::sync::mpsc::unbounded_channel::<String>();
        let mut watched: BTreeMap<String, tokio::task::JoinHandle<()>> = BTreeMap::new();
        let mut attempted: BTreeSet<(String, String)> = BTreeSet::new();
        loop {
            if let Ok(checkouts) = self.store.checkouts(&CheckoutFilter::default()) {
                let ids: BTreeSet<_> = checkouts.iter().map(|c| c.record.id.clone()).collect();
                watched.retain(|id, task| {
                    let keep = ids.contains(id);
                    if !keep {
                        task.abort();
                    }
                    keep
                });
                for c in checkouts {
                    if watched.contains_key(&c.record.id) {
                        continue;
                    }
                    if let Ok(binding) = Self::binding(&c) {
                        let task =
                            tokio::spawn(watch_checkout(binding, c.record.id.clone(), due.clone()));
                        watched.insert(c.record.id, task);
                    }
                }
            }
            if let Ok(sessions) = self.store.agents(&AgentFilter {
                ownership: Some("owned".into()),
                ..Default::default()
            }) {
                for session in sessions {
                    if session.checkout_id.is_none()
                        && attempted.insert((session.id.clone(), session.cwd.clone()))
                        && let Ok(_permit) = self.gate.clone().acquire_owned().await
                    {
                        let _ = tokio::time::timeout(
                            Duration::from_secs(5),
                            self.register(&session.cwd, None),
                        )
                        .await;
                    }
                }
            }
            tokio::select! {
                _ = changes.changed() => {}
                event = events.recv() => match event {
                    Ok(event) if event.method.starts_with("agent.") => {
                        if let Ok(agent) = self.store.agent(&event.params.id)
                            && let Some(checkout) = agent.record.checkout_id
                        {
                            let _ = due.send(checkout);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    _ => {}
                },
                Some(first) = pending.recv() => {
                    // Coalesce a burst of Git writes into one observation per checkout.
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let mut ids = BTreeSet::from([first]);
                    while let Ok(id) = pending.try_recv() {
                        ids.insert(id);
                    }
                    let mut groups = BTreeSet::new();
                    for id in ids {
                        let Ok(_permit) = self.gate.clone().acquire_owned().await else { return };
                        if let Ok(c) = self.checkout(&id)
                            && let Ok(binding) = Self::binding(&c)
                            && groups.insert(binding.repository_key())
                        {
                            let _ = tokio::time::timeout(
                                Duration::from_secs(5),
                                self.register(&binding.common_dir.to_string_lossy(), None),
                            )
                            .await;
                        }
                        let _ = tokio::time::timeout(Duration::from_secs(5), self.refresh(&id)).await;
                    }
                }
            }
        }
    }
    pub fn begin(&self, plan: &str, permit: tokio::sync::OwnedSemaphorePermit) -> Active {
        self.active.send_modify(|set| {
            set.insert(plan.to_owned());
        });
        Active {
            plan: plan.to_owned(),
            set: self.active.clone(),
            _permit: permit,
        }
    }
    /// The operation record once no execution of `plan` is in flight here.
    pub async fn settled(&self, plan: &str) -> Result<VcsOperation> {
        let mut active = self.active.subscribe();
        let _ = active.wait_for(|set| !set.contains(plan)).await;
        Ok(self.operation(plan)?.record)
    }
    pub fn operation(&self, id: &str) -> Result<StoredVcsOperation> {
        self.store.vcs_operation(id).map_err(|e| match e {
            tower_store::Error::NotFound => {
                RpcError::application("not_found", "VCS operation not found")
            }
            _ => failure(e),
        })
    }
    pub async fn prepare(
        &self,
        p: VcsPrepare,
        actor: String,
        auth: &tower_vcs::Authentication,
    ) -> Result<VcsOperation> {
        let c = self.checkout(&p.checkout_id)?;
        let b = Self::binding(&c)?;
        let operation = operation(&p.operation, p.options)?;
        self.prepare_binding(
            &b,
            operation,
            actor,
            Some(p.checkout_id),
            c.record.repository_id,
            auth,
        )
        .await
    }
    async fn prepare_binding(
        &self,
        b: &Binding,
        operation: tower_vcs::Operation,
        actor: String,
        checkout_id: Option<String>,
        repository_id: Option<String>,
        auth: &tower_vcs::Authentication,
    ) -> Result<VcsOperation> {
        let prepared = tower_vcs::prepare_with_auth(b, operation, &self.root, auth)
            .await
            .map_err(native)?;
        let mut review = serde_json::to_value(prepared.review()).map_err(failure)?;
        if let Some(repository) = repository_id {
            review["repository_id"] = repository.into();
        }
        let record = VcsOperation {
            plan_id: id("vcs_")?,
            checkout_id,
            operation: prepared.operation_kind().into(),
            state: "prepared".into(),
            created_at: now_ms(),
            expires_at: now_ms() + 600_000,
            review: encode(review)?,
            released: false,
            result: None,
            error: None,
        };
        let prepared = serde_json::to_value(prepared).map_err(failure)?;
        self.store
            .save_vcs_plan(&StoredVcsOperation {
                actor,
                // Older daemons must fail to decode agent-bound plans rather than
                // ignore the new auth mode and execute them with host credentials.
                prepared: if auth.mode() == tower_vcs::AuthMode::SshAgent {
                    json!({"format":"vcs.ssh-agent.v1","prepared":prepared})
                } else {
                    prepared
                },
                record: record.clone(),
            })
            .map_err(failure)?;
        Ok(record)
    }
    pub async fn execute(
        self: Arc<Self>,
        service: Arc<Service>,
        request: Request,
        peer: tower_pal::Peer,
        mut op: StoredVcsOperation,
        auth: tower_vcs::Authentication,
        _active: Active,
    ) {
        let prepared = Self::prepared(&op);
        let result = match prepared {
            Ok(prepared) => {
                let check = service.clone();
                let recheck = request.clone();
                let operation = prepared.operation_kind().to_string();
                tower_vcs::execute_with_auth(&prepared, &auth, move |paths| {
                    check
                        .vcs_execution_check(&recheck, peer, paths, &operation)
                        .map_err(|e| tower_vcs::Error::new(e.data.code, false))
                })
                .await
                .map_err(native)
            }
            Err(e) => Err(e),
        };
        match result {
            Ok(outcome) => {
                op.record.state = "completed".into();
                op.record.result = encode(json!({"checkout_ids":[],"refresh_pending":true})).ok();
                if self.store.complete_vcs_operation(&op.record).is_err() {
                    return;
                }
                let repository = match &op.record.review {
                    Value::Object(fields) => match fields.get("repository_id") {
                        Some(Value::String(id)) => Some(id.as_str()),
                        _ => None,
                    },
                    _ => None,
                };
                let mut rows = Vec::new();
                let mut refresh_pending = outcome.refresh_pending;
                if let Some(b) = outcome.binding {
                    match tokio::time::timeout(
                        Duration::from_secs(10),
                        self.register(&b.common_dir.to_string_lossy(), repository),
                    )
                    .await
                    {
                        Ok(Ok(c)) => rows = c,
                        _ => refresh_pending = true,
                    }
                }
                if let Some(id) = &op.record.checkout_id
                    && !matches!(
                        tokio::time::timeout(Duration::from_secs(5), self.refresh(id)).await,
                        Ok(Ok(_))
                    )
                {
                    refresh_pending = true;
                }
                op.record.result=encode(json!({"checkout_ids":rows.iter().map(|c|&c.id).collect::<Vec<_>>(),"refresh_pending":refresh_pending})).ok();
            }
            Err(error) => {
                op.record.state =
                    if error.data.details.get("may_have_effect") == Some(&Value::Bool(true)) {
                        "unknown"
                    } else {
                        "failed"
                    }
                    .into();
                op.record.error = Some(error);
            }
        }
        let store = self.store.clone();
        let record = op.record;
        let _ = tokio::task::spawn_blocking(move || store.complete_vcs_operation(&record)).await;
    }
    pub fn active_paths(prepared: &Prepared) -> Vec<PathBuf> {
        if prepared.operation_kind() == "push" {
            return vec![];
        }
        if prepared.operation_kind() == "checkout.create" {
            return prepared.review().destination.iter().cloned().collect();
        }
        prepared.affected_paths().to_vec()
    }
}

/// Signals `checkout` whenever its Git metadata or working directory entry changes.
async fn watch_checkout(
    binding: Binding,
    checkout: String,
    due: tokio::sync::mpsc::UnboundedSender<String>,
) {
    let mut paths = vec![
        binding.git_dir.join("HEAD"),
        binding.git_dir.join("index"),
        binding.common_dir.join("packed-refs"),
        binding.common_dir.join("worktrees"),
    ];
    paths.extend(binding.directory.clone());
    let files = tower_pal::FileWatch::new(&paths);
    let refs = tower_pal::TreeWatch::new(&binding.common_dir.join("refs"), None);
    let (mut files, mut refs) = match (files, refs) {
        (Ok(files), Ok(refs)) => (files, refs),
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("tower: checkout {checkout} is unobservable: {e}");
            return;
        }
    };
    loop {
        tokio::select! {
            _ = files.changed() => {}
            _ = refs.changed() => {}
        }
        if due.send(checkout.clone()).is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod auth_tests {
    use super::*;
    #[test]
    fn forwarded_agents_require_a_local_peer_identity() {
        let auth = VcsAuth::SshAgent {
            socket: Secret("/unavailable-agent".into()),
        };
        let error = authentication(
            &auth,
            tower_pal::Peer {
                uid: tower_pal::uid(),
                pid: None,
            },
            &tower_pal::Paths::discover().unwrap(),
        )
        .err()
        .unwrap();
        assert_eq!(error.data.code, "unsupported_auth_transport");
        assert!(!format!("{auth:?}").contains("/unavailable-agent"));
    }
}
