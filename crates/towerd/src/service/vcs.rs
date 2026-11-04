use super::*;
use crate::vcs::Vcs;
use std::path::{Path, PathBuf};

impl Service {
    pub(crate) fn vcs_authorize(
        &self,
        r: &Request,
        peer: Peer,
    ) -> std::result::Result<String, RpcError> {
        let actor = self.actor(&r.context, peer)?;
        let read = matches!(
            r.method.as_str(),
            "checkout.list" | "checkout.get" | "vcs.status" | "vcs.operation" | "catalog.get"
        );
        require(&actor, if read { "read" } else { "admin" })?;
        match r.method.as_str() {
            "vcs.prepare" => {
                let p: VcsPrepare = params(&r.params)?;
                let checkout = self.vcs.checkout(&p.checkout_id)?;
                self.vcs_write_policy(&actor, None)?;
                self.vcs_write_policy(&actor, Some(&checkout.record.path))?;
                let binding = Vcs::binding(&checkout)?;
                self.vcs_repository_available(&binding.repository_key(), None)?;
            }
            "vcs.execute" => self.vcs_write_policy(&actor, None)?,
            "checkout.register" => {
                let p: CheckoutRegister = params(&r.params)?;
                if p.create.is_some() {
                    let cwd = self
                        .paths
                        .display_cwd(&p.directory)
                        .map_err(|_| invalid("Invalid directory"))?;
                    self.vcs_write_policy(&actor, Some(&cwd))?;
                }
            }
            _ => {}
        }
        Ok(actor.id())
    }
    fn vcs_write_policy(
        &self,
        actor: &Actor,
        cwd: Option<&str>,
    ) -> std::result::Result<(), RpcError> {
        require(actor, "admin")?;
        if self.store.host().map_err(storage)?.mode == "allgather" {
            if !self.policy_ready()
                || self
                    .catalog_epoch
                    .load(std::sync::atomic::Ordering::Acquire)
                    != self.remote_epoch.load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(app(
                    "offline_restricted",
                    "VCS mutations require a connected Allgather with current catalog and policy",
                ));
            }
            self.session_policy(
                actor,
                crate::policy::Subject {
                    id: "*",
                    harness: None,
                    cwd,
                },
            )?;
        }
        Ok(())
    }
    fn vcs_repository_available(
        &self,
        key: &str,
        except: Option<&str>,
    ) -> std::result::Result<(), RpcError> {
        for op in self.store.reserved_vcs_operations().map_err(storage)? {
            if except == Some(op.record.plan_id.as_str()) {
                continue;
            }
            let prepared = Vcs::prepared(&op)?;
            if prepared.repository_key() == key {
                return Err(app(
                    "vcs_busy",
                    "A running or uncertain operation reserves this local repository",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn vcs_execution_check(
        &self,
        r: &Request,
        peer: Peer,
        paths: &[PathBuf],
        operation: &str,
    ) -> std::result::Result<(), RpcError> {
        let _lock = self
            .mutation
            .lock()
            .map_err(|_| app("internal_error", "Mutation lock unavailable"))?;
        let actor = self.actor(&r.context, peer)?;
        let params: VcsPlanParams = params(&r.params)?;
        let op = self.vcs.operation(&params.plan_id)?;
        if op.actor != actor.id() {
            return Err(app("denied", "This VCS plan belongs to another caller"));
        }
        if now_ms() > op.record.expires_at {
            return Err(app("plan_expired", "VCS plan expired"));
        }
        self.vcs_write_policy(&actor, None)?;
        for path in paths {
            let cwd = self
                .paths
                .display_cwd(&path.to_string_lossy())
                .map_err(|_| invalid("Invalid checkout directory"))?;
            self.vcs_write_policy(&actor, Some(&cwd))?;
        }
        let prepared = Vcs::prepared(&op)?;
        if prepared.operation_kind() != operation {
            return Err(app("stale_plan", "VCS operation changed"));
        }
        self.vcs_unused(&Vcs::active_paths(&prepared))
    }
    pub(crate) fn vcs_unused(&self, paths: &[PathBuf]) -> std::result::Result<(), RpcError> {
        let paths = paths
            .iter()
            .map(|p| p.canonicalize().unwrap_or_else(|_| p.clone()))
            .collect::<Vec<_>>();
        let checkouts = self
            .store
            .checkouts(&CheckoutFilter::default())
            .map_err(storage)?;
        let directories = checkouts
            .iter()
            .filter_map(|c| {
                Vcs::binding(c)
                    .ok()?
                    .directory
                    .map(|p| (c.record.id.as_str(), p))
            })
            .collect::<BTreeMap<_, _>>();
        for a in self
            .store
            .agents(&AgentFilter {
                include_exited: true,
                ..Default::default()
            })
            .map_err(storage)?
        {
            if a.status == "exited" || a.status == "lost" && a.ownership != "owned" {
                continue;
            }
            let cwd = self
                .paths
                .absolute_cwd(&a.cwd)
                .map_err(|_| invalid("Invalid session directory"))?;
            let cwd = cwd.canonicalize().unwrap_or(cwd);
            let linked = a.checkout_id.as_deref().and_then(|id| directories.get(id));
            if paths.iter().any(|path| {
                cwd.starts_with(path) || linked.as_ref().is_some_and(|p| p.starts_with(path))
            }) {
                return Err(app(
                    "checkout_in_use",
                    "An active session uses this checkout",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn vcs_claim(
        &self,
        r: &Request,
        peer: Peer,
        id: &str,
    ) -> std::result::Result<(tower_store::StoredVcsOperation, bool), RpcError> {
        let _lock = self
            .mutation
            .lock()
            .map_err(|_| app("internal_error", "Mutation lock unavailable"))?;
        let actor = self.actor(&r.context, peer)?;
        self.vcs_write_policy(&actor, None)?;
        let op = self.vcs.operation(id)?;
        if op.actor != actor.id() {
            return Err(app("denied", "This VCS plan belongs to another caller"));
        }
        if op.record.state != "prepared" {
            return Ok((op, false));
        }
        let prepared = Vcs::prepared(&op)?;
        for path in prepared.affected_paths() {
            let cwd = self
                .paths
                .display_cwd(&path.to_string_lossy())
                .map_err(|_| invalid("Invalid checkout directory"))?;
            self.vcs_write_policy(&actor, Some(&cwd))?;
        }
        self.vcs_unused(&Vcs::active_paths(&prepared))?;
        self.vcs_repository_available(&prepared.repository_key(), Some(id))?;
        let claimed = self
            .store
            .claim_vcs_operation(id, &actor.id(), now_ms())
            .map_err(storage)?;
        Ok((self.vcs.operation(id)?, claimed))
    }
    pub(crate) fn vcs_directory_available(&self, path: &Path) -> std::result::Result<(), RpcError> {
        let path = path.canonicalize().unwrap_or_else(|_| path.to_owned());
        for op in self.store.reserved_vcs_operations().map_err(storage)? {
            let prepared = Vcs::prepared(&op)?;
            if Vcs::active_paths(&prepared)
                .iter()
                .map(|p| p.canonicalize().unwrap_or_else(|_| p.clone()))
                .any(|p| path.starts_with(&p) || p.starts_with(&path))
            {
                return Err(app(
                    "checkout_busy",
                    "A VCS operation currently reserves this directory",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn resolve_checkout_launch(
        &self,
        p: &mut SessionOpenParams,
    ) -> std::result::Result<(), RpcError> {
        if let Some(id) = &p.checkout_id {
            let c = self.vcs.checkout(id)?;
            if !c.record.available {
                return Err(app("checkout_unavailable", "Checkout is not available"));
            }
            let binding = Vcs::binding(&c)?;
            tower_vcs::validate_binding(&binding).map_err(crate::vcs::native)?;
            let root = binding
                .directory
                .ok_or_else(|| app("not_a_checkout", "Checkout has no working directory"))?;
            if p.cwd.is_empty() {
                p.cwd = c.record.path;
            } else {
                let path = self
                    .paths
                    .absolute_cwd(&p.cwd)
                    .and_then(|p| p.canonicalize())
                    .map_err(|_| invalid("Working directory does not exist"))?;
                if !path.starts_with(root) {
                    return Err(app(
                        "denied",
                        "Working directory is outside the selected checkout",
                    ));
                }
            }
        }
        let path = self
            .paths
            .absolute_cwd(&p.cwd)
            .map_err(|_| invalid("Invalid working directory"))?;
        let path = path.canonicalize().unwrap_or(path);
        self.vcs_directory_available(&path)
    }
    pub(crate) fn validate_session_checkout(
        &self,
        record: &AgentRecord,
    ) -> std::result::Result<(), RpcError> {
        if let Some(id) = &record.checkout_id {
            let checkout = self.vcs.checkout(id)?;
            let binding = Vcs::binding(&checkout)?;
            tower_vcs::validate_binding(&binding).map_err(crate::vcs::native)?;
        }
        Ok(())
    }
    pub(crate) fn forget_checkout(&self, id: &str) -> std::result::Result<(), RpcError> {
        let _lock = self
            .mutation
            .lock()
            .map_err(|_| app("internal_error", "Mutation lock unavailable"))?;
        let c = self.vcs.checkout(id)?;
        if c.record.available {
            return Err(app(
                "checkout_available",
                "Only unavailable checkouts can be forgotten; remove the worktree first",
            ));
        }
        let binding = Vcs::binding(&c)?;
        self.vcs_repository_available(&binding.repository_key(), None)?;
        self.vcs_unused(&binding.directory.into_iter().collect::<Vec<_>>())?;
        self.store.forget_checkout(id).map_err(storage)
    }
    pub(crate) fn sync_catalog_pairing(
        &self,
        url: &str,
        node: &str,
        epoch: u64,
    ) -> std::result::Result<(), RpcError> {
        self.remote_actor(epoch)?;
        let origin = crate::allgather::enrollment::origin(url)
            .map_err(|_| invalid("Invalid catalog pairing origin"))?;
        self.store
            .catalog_pairing(&format!("{origin}\n{node}"))
            .map_err(storage)?;
        self.catalog_epoch
            .store(0, std::sync::atomic::Ordering::Release);
        Ok(())
    }
    pub(crate) fn sync_catalog_pending(&self, epoch: u64) -> std::result::Result<(), RpcError> {
        self.remote_actor(epoch)?;
        self.catalog_epoch
            .store(0, std::sync::atomic::Ordering::Release);
        Ok(())
    }
    pub(crate) fn sync_catalog(
        &self,
        c: RepositoryCatalog,
        epoch: u64,
    ) -> std::result::Result<u64, RpcError> {
        let actor = self.remote_actor(epoch)?;
        require(&actor, "read")?;
        let revision = c.revision;
        self.store.apply_catalog(&c).map_err(storage)?;
        self.catalog_epoch
            .store(epoch, std::sync::atomic::Ordering::Release);
        Ok(revision)
    }
    pub(crate) fn sync_catalog_delta(
        &self,
        c: CatalogDelta,
        epoch: u64,
    ) -> std::result::Result<u64, RpcError> {
        let actor = self.remote_actor(epoch)?;
        require(&actor, "read")?;
        let revision = c.revision;
        self.store.apply_catalog_delta(&c).map_err(storage)?;
        self.catalog_epoch
            .store(epoch, std::sync::atomic::Ordering::Release);
        Ok(revision)
    }
    pub(crate) fn sync_checkouts(
        &self,
        epoch: u64,
        since: Option<u64>,
    ) -> std::result::Result<Option<(u64, Vec<Checkout>)>, RpcError> {
        let actor = self.remote_actor(epoch)?;
        require(&actor, "read")?;
        self.store.checkout_snapshot(since).map_err(storage)
    }
    pub(crate) fn catalog_view(&self) -> std::result::Result<Value, RpcError> {
        value(
            serde_json::json!({"catalog":self.store.catalog().map_err(storage)?,"current":self.policy_ready()&&self.catalog_epoch.load(std::sync::atomic::Ordering::Acquire)==self.remote_epoch.load(std::sync::atomic::Ordering::Acquire)}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower_sessions::{Backends, ManagedSession, Observation};

    fn setup() -> (tempfile::TempDir, Arc<Store>, Service) {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let paths = Paths {
            run: state.join("run"),
            socket: state.join("run/towerd.sock"),
            database: state.join("tower.db"),
            tokens: state.join("tokens"),
            logs: state.join("logs"),
            config_home: dir.path().join("config"),
            home: dir.path().join("home"),
            state,
        };
        paths.prepare().unwrap();
        tower_pal::private_file(&paths.database, true).unwrap();
        let store = Arc::new(Store::open(&paths.database).unwrap());
        let service = Service::new(paths, store.clone(), "test", Backends::default()).unwrap();
        (dir, store, service)
    }

    fn record(cwd: &Path, ownership: &str) -> AgentRecord {
        serde_json::from_value(serde_json::json!({
            "id":"guarded-agent", "node":"fixture-node", "harness":"fixture",
            "ownership":ownership,
            "protocol":(ownership=="owned").then_some("acp"),
            "trust":if ownership=="owned" {"strong"} else {"weak"},
            "cwd":cwd.to_string_lossy(), "status":"working",
            "caps":Capabilities::unavailable("harness_unsupported"),
            "attach_count":0, "sources":["fixture"],
            "registered_at":1, "updated_at":1, "last_seen":1, "revision":0,
            "sync_state":"synced"
        }))
        .unwrap()
    }

    fn register(store: &Store, cwd: &Path) -> String {
        let record = record(cwd, "registered");
        let id = record.id.clone();
        store.register(record, &[0; 32], &id).unwrap();
        id
    }

    #[test]
    fn owned_lost_session_blocks_vcs_even_when_automatic_resume_is_disabled() {
        for (status, blocked) in [("lost", true), ("exited", false)] {
            let (dir, store, service) = setup();
            let checkout = dir.path().join("checkout");
            std::fs::create_dir(&checkout).unwrap();
            let record = record(&checkout, "owned");
            let id = record.id.clone();
            store
                .create_managed(
                    record,
                    &[0; 32],
                    &ManagedSession {
                        id: id.clone(),
                        protocol: "acp".into(),
                        binding: Value::Null,
                        launch: None,
                        auto_resume: false,
                    },
                )
                .unwrap();
            store.disable_managed_recovery(&id).unwrap();
            store
                .observe_managed(
                    "acp",
                    Observation {
                        id: id.clone(),
                        binding: None,
                        status: status.into(),
                        pid: None,
                        pid_start_time: None,
                        caps: Capabilities::unavailable("structured_control_required"),
                        terminal: None,
                        recovery_pending: false,
                        metadata: Default::default(),
                    },
                    2,
                )
                .unwrap();
            assert!(!store.managed_session(&id).unwrap().auto_resume);
            assert_eq!(store.agent(&id).unwrap().record.status, status);
            let result = service.vcs_unused(&[checkout]);
            if blocked {
                assert_eq!(result.unwrap_err().data.code, "checkout_in_use");
            } else {
                result.unwrap();
            }
            service.vcs_unused(&[dir.path().join("unrelated")]).unwrap();
        }
    }

    #[test]
    fn registered_session_cwd_symlink_blocks_the_real_checkout_path() {
        let (dir, store, service) = setup();
        let checkout = dir.path().join("checkout");
        std::fs::create_dir_all(checkout.join("subdirectory")).unwrap();
        let alias = dir.path().join("alias");
        tower_pal::symlink(&checkout, &alias).unwrap();
        let cwd = alias.join("subdirectory");
        let id = register(&store, &cwd);
        assert_eq!(store.agent(&id).unwrap().record.cwd, cwd.to_string_lossy());
        assert_eq!(
            service.vcs_unused(&[checkout]).unwrap_err().data.code,
            "checkout_in_use"
        );
        service.vcs_unused(&[dir.path().join("unrelated")]).unwrap();
    }

    #[tokio::test]
    async fn checkout_link_blocks_mutation_when_session_cwd_is_stale_or_missing() {
        for exists in [false, true] {
            let (dir, store, service) = setup();
            let checkout = dir.path().join("checkout");
            std::fs::create_dir(&checkout).unwrap();
            let mut env = tower_pal::environment();
            env.retain(|key, _| !key.starts_with("GIT_"));
            env.insert("GIT_CONFIG_NOSYSTEM".into(), "1".into());
            env.insert("GIT_CONFIG_GLOBAL".into(), "/dev/null".into());
            let (code, _) = tower_pal::capture(
                &[
                    "git".into(),
                    "-c".into(),
                    "core.hooksPath=/dev/null".into(),
                    "init".into(),
                    "-b".into(),
                    "main".into(),
                ],
                &env,
                &checkout,
                std::time::Duration::from_secs(10),
                65536,
            )
            .await
            .unwrap();
            assert_eq!(code, 0);
            let rows = service
                .vcs
                .register(checkout.to_str().unwrap(), None)
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            let cwd = dir.path().join("stale-cwd");
            if exists {
                std::fs::create_dir(&cwd).unwrap();
            }
            let id = register(&store, &cwd);
            service.vcs_unused(std::slice::from_ref(&checkout)).unwrap();
            store.link_session_checkout(&id, &rows[0].id).unwrap();
            assert_eq!(
                service.vcs_unused(&[checkout]).unwrap_err().data.code,
                "checkout_in_use"
            );
            assert_eq!(cwd.exists(), exists);
        }
    }
}
