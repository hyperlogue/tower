use super::*;
use std::collections::{BTreeMap, BTreeSet};
use tower_protocol::{CatalogDelta, Checkout, CheckoutFilter, RepositoryCatalog, VcsOperation};

#[derive(Clone, Debug)]
pub struct StoredCheckout {
    pub identity: String,
    pub binding: serde_json::Value,
    pub association_manual: bool,
    pub record: Checkout,
}
#[derive(Clone)]
pub struct StoredVcsOperation {
    pub actor: String,
    pub prepared: serde_json::Value,
    pub record: VcsOperation,
}
fn tick(tx: &Transaction<'_>) -> Result<u64> {
    Ok(tx.query_row(
        "UPDATE counters SET value=value+1 WHERE name='checkouts' RETURNING value",
        [],
        |r| r.get::<_, i64>(0),
    )? as u64)
}
fn checkout_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(String, String, String, bool)> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
}
fn decode_checkout(
    (identity, binding, record, association_manual): (String, String, String, bool),
) -> Result<StoredCheckout> {
    Ok(StoredCheckout {
        identity,
        binding: serde_json::from_str(&binding)?,
        association_manual,
        record: serde_json::from_str(&record)?,
    })
}
fn operation_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(String, String, String)> {
    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
}
fn decode_operation(
    (actor, prepared, record): (String, String, String),
) -> Result<StoredVcsOperation> {
    Ok(StoredVcsOperation {
        actor,
        prepared: serde_json::from_str(&prepared)?,
        record: serde_json::from_str(&record)?,
    })
}
fn save(tx: &Transaction<'_>, c: &StoredCheckout) -> Result<()> {
    tx.execute("INSERT INTO checkouts(id,identity,binding,record,association_manual) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(id) DO UPDATE SET identity=excluded.identity,binding=excluded.binding,record=excluded.record,association_manual=excluded.association_manual",params![c.record.id,c.identity,serde_json::to_string(&c.binding)?,serde_json::to_string(&c.record)?,c.association_manual])?;
    Ok(())
}
fn detach_catalog(tx: &Transaction<'_>, valid: Option<&BTreeSet<&str>>) -> Result<()> {
    let rows = tx
        .prepare_cached("SELECT identity,binding,record,association_manual FROM checkouts")?
        .query_map([], checkout_row)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for row in rows {
        let mut c = decode_checkout(row)?;
        if c.record
            .repository_id
            .as_deref()
            .is_some_and(|id| valid.is_none_or(|v| !v.contains(id)))
        {
            c.record.repository_id = None;
            c.record.revision = tick(tx)?;
            save(tx, &c)?;
        }
    }
    Ok(())
}
fn validate_catalog(c: &RepositoryCatalog) -> Result<()> {
    let text =
        |v: &str, max: usize| !v.is_empty() && v.len() <= max && !v.chars().any(char::is_control);
    if c.revision == 0
        || c.revision > 9_007_199_254_740_991
        || c.projects.len() > 5000
        || c.repositories.len() > 5000
        || serde_json::to_vec(&serde_json::json!({"jsonrpc":"2.0","id":format!("catalog:{}", "x".repeat(100)),"method":"catalog.push","params":c}))?.len() > tower_protocol::MAX_FRAME_BYTES
    {
        return Err(Error::Conflict("catalog exceeds limits"));
    }
    let mut projects = BTreeSet::new();
    let mut repos = BTreeSet::new();
    for p in &c.projects {
        if !text(&p.id, 160)
            || p.name.is_empty()
            || p.name.chars().count() > 300
            || !projects.insert(p.id.as_str())
        {
            return Err(Error::Conflict("invalid catalog project"));
        }
    }
    for r in &c.repositories {
        if !text(&r.id, 160)
            || r.name.is_empty()
            || r.name.chars().count() > 300
            || !repos.insert(r.id.as_str())
            || r.project_ids.len() > 5000
            || r.url_aliases.len() > 100
        {
            return Err(Error::Conflict("invalid catalog repository"));
        }
        let mut seen = BTreeSet::new();
        if r.project_ids
            .iter()
            .any(|id| !projects.contains(id.as_str()) || !seen.insert(id))
        {
            return Err(Error::Conflict("invalid repository project links"));
        }
        for url in r.upstream_url.iter().chain(&r.url_aliases) {
            if !text(url, 2048)
                || url.chars().any(char::is_whitespace)
                || url.contains('?')
                || url.contains('#')
            {
                return Err(Error::Conflict("invalid catalog remote"));
            }
            let scp = !url.contains("://")
                && url.split_once('@').is_some_and(|(user, rest)| {
                    !user.is_empty()
                        && !user.contains(['/', ':', '@'])
                        && rest.split_once(':').is_some_and(|(host, path)| {
                            !host.is_empty() && !host.contains('/') && !path.is_empty()
                        })
                });
            if !scp {
                let parsed =
                    url::Url::parse(url).map_err(|_| Error::Conflict("invalid catalog remote"))?;
                if !matches!(parsed.scheme(), "ssh" | "https")
                    || parsed.host_str().is_none()
                    || parsed.password().is_some()
                    || parsed.scheme() == "https" && !parsed.username().is_empty()
                {
                    return Err(Error::Conflict(
                        "catalog contains invalid remote or credentials",
                    ));
                }
            }
        }
    }
    Ok(())
}
fn catalog_write(
    tx: &Transaction<'_>,
    c: &RepositoryCatalog,
    delta: Option<Vec<u8>>,
) -> Result<bool> {
    validate_catalog(c)?;
    let mut canonical = c.clone();
    canonical.projects.sort_by(|a, b| a.id.cmp(&b.id));
    canonical.repositories.sort_by(|a, b| a.id.cmp(&b.id));
    for r in &mut canonical.repositories {
        r.project_ids.sort();
        r.url_aliases.sort();
    }
    let c = &canonical;
    let payload = serde_json::to_string(c)?;
    let old: Option<(Option<i64>, Option<String>)> = tx
        .query_row(
            "SELECT revision,payload FROM repository_catalog WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((revision, previous)) = old else {
        return Err(Error::Conflict("catalog has no active pairing"));
    };
    if let Some(revision) = revision {
        if revision < 0 || revision as u64 > c.revision {
            return Err(Error::Conflict("stale catalog revision"));
        }
        if revision as u64 == c.revision {
            return if previous.as_deref() == Some(&payload) {
                Ok(false)
            } else {
                Err(Error::Conflict("conflicting catalog revision"))
            };
        }
    }
    tx.execute(
        "UPDATE repository_catalog SET revision=?1,payload=?2,delta_hash=?3 WHERE singleton=1",
        params![c.revision as i64, payload, delta],
    )?;
    let valid = c.repositories.iter().map(|r| r.id.as_str()).collect();
    detach_catalog(tx, Some(&valid))?;
    Ok(true)
}
impl Store {
    pub fn catalog_pairing(&self, pairing: &str) -> Result<bool> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let old: Option<String> = tx
            .query_row(
                "SELECT pairing FROM repository_catalog WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        if old.as_deref() == Some(pairing) {
            return Ok(false);
        }
        detach_catalog(&tx, None)?;
        tx.execute("INSERT INTO repository_catalog(singleton,pairing) VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET pairing=excluded.pairing,revision=NULL,payload=NULL,delta_hash=NULL",[pairing])?;
        tx.commit()?;
        Ok(true)
    }
    pub fn catalog(&self) -> Result<Option<RepositoryCatalog>> {
        let c = self.lock()?;
        let payload: Option<String> = c
            .query_row(
                "SELECT payload FROM repository_catalog WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        payload
            .map(|s| serde_json::from_str(&s).map_err(Error::from))
            .transpose()
    }
    pub fn apply_catalog(&self, c: &RepositoryCatalog) -> Result<bool> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let changed = catalog_write(&tx, c, None)?;
        tx.commit()?;
        Ok(changed)
    }
    pub fn apply_catalog_delta(&self, d: &CatalogDelta) -> Result<bool> {
        let hash = Sha256::digest(serde_json::to_vec(d)?).to_vec();
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let (payload, old_hash): (Option<String>, Option<Vec<u8>>) = tx.query_row(
            "SELECT payload,delta_hash FROM repository_catalog WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut catalog: RepositoryCatalog =
            serde_json::from_str(&payload.ok_or(Error::Conflict("catalog snapshot required"))?)?;
        if catalog.revision == d.revision && old_hash.as_ref() == Some(&hash) {
            return Ok(false);
        }
        if catalog.revision != d.base_revision || d.revision <= d.base_revision {
            return Err(Error::Conflict("catalog base mismatch"));
        }
        fn apply<T: Clone>(
            rows: Vec<T>,
            ups: &[T],
            removed: &[String],
            id: impl Fn(&T) -> &str,
        ) -> Result<Vec<T>> {
            if ups.len() > 5000 || removed.len() > 5000 {
                return Err(Error::Conflict("catalog delta exceeds limits"));
            }
            let mut seen = BTreeSet::new();
            for key in ups
                .iter()
                .map(&id)
                .chain(removed.iter().map(String::as_str))
            {
                if !seen.insert(key) {
                    return Err(Error::Conflict("duplicate catalog delta identity"));
                }
            }
            let mut map: BTreeMap<String, T> =
                rows.into_iter().map(|v| (id(&v).to_owned(), v)).collect();
            for key in removed {
                map.remove(key);
            }
            for row in ups {
                map.insert(id(row).to_owned(), row.clone());
            }
            Ok(map.into_values().collect())
        }
        catalog.projects = apply(
            catalog.projects,
            &d.projects.upserts,
            &d.projects.removed,
            |p| &p.id,
        )?;
        catalog.repositories = apply(
            catalog.repositories,
            &d.repositories.upserts,
            &d.repositories.removed,
            |r| &r.id,
        )?;
        catalog.revision = d.revision;
        let changed = catalog_write(&tx, &catalog, Some(hash))?;
        tx.commit()?;
        Ok(changed)
    }
    pub fn checkouts(&self, filter: &CheckoutFilter) -> Result<Vec<StoredCheckout>> {
        let conn = self.lock()?;
        let rows = conn
            .prepare_cached(
                "SELECT identity,binding,record,association_manual FROM checkouts ORDER BY id",
            )?
            .query_map([], checkout_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(decode_checkout)
            .filter(|row| {
                row.as_ref().map_or(true, |r| {
                    filter
                        .repository_id
                        .as_ref()
                        .is_none_or(|id| r.record.repository_id.as_ref() == Some(id))
                })
            })
            .collect()
    }
    pub fn checkout(&self, id: &str) -> Result<StoredCheckout> {
        let conn = self.lock()?;
        let row = conn
            .query_row(
                "SELECT identity,binding,record,association_manual FROM checkouts WHERE id=?1",
                [id],
                checkout_row,
            )
            .optional()?
            .ok_or(Error::NotFound)?;
        decode_checkout(row)
    }
    pub fn checkout_identity(&self, identity: &str) -> Result<Option<StoredCheckout>> {
        let conn = self.lock()?;
        conn.query_row(
            "SELECT identity,binding,record,association_manual FROM checkouts WHERE identity=?1",
            [identity],
            checkout_row,
        )
        .optional()?
        .map(decode_checkout)
        .transpose()
    }
    pub fn save_checkout(&self, mut c: StoredCheckout) -> Result<Checkout> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let old = tx
            .query_row(
                "SELECT identity,binding,record,association_manual FROM checkouts WHERE identity=?1",
                [&c.identity],
                checkout_row,
            )
            .optional()?
            .map(decode_checkout)
            .transpose()?;
        if let Some(old) = old {
            if c.record.id != old.record.id || c.record.revision != old.record.revision {
                return Err(Error::Conflict("checkout observation changed"));
            }
            let mut compare = c.record.clone();
            compare.observed_at = old.record.observed_at;
            if compare == old.record
                && c.binding == old.binding
                && c.association_manual == old.association_manual
                && c.record.observed_at.saturating_sub(old.record.observed_at) < 60_000
            {
                tx.commit()?;
                return Ok(old.record);
            }
        } else {
            if c.record.revision != 0
                || tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM checkouts WHERE id=?1)",
                    [&c.record.id],
                    |r| r.get::<_, bool>(0),
                )?
            {
                return Err(Error::Conflict("checkout observation changed"));
            }
            let n: i64 = tx.query_row("SELECT count(*) FROM checkouts", [], |r| r.get(0))?;
            if n >= 5000 {
                return Err(Error::Conflict("checkout capacity reached"));
            }
        }
        if let Some(repository) = &c.record.repository_id {
            let payload: Option<String> = tx
                .query_row(
                    "SELECT payload FROM repository_catalog WHERE singleton=1",
                    [],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            let catalog = payload
                .map(|p| serde_json::from_str::<RepositoryCatalog>(&p))
                .transpose()?;
            if !catalog.is_some_and(|c| c.repositories.iter().any(|r| &r.id == repository)) {
                return Err(Error::Conflict("repository is no longer in the catalog"));
            }
        }
        c.record.revision = tick(&tx)?;
        save(&tx, &c)?;
        tx.commit()?;
        Ok(c.record)
    }
    pub fn forget_checkout(&self, id: &str) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let records = all_records(&tx)?;
        for mut record in records {
            if record.checkout_id.as_deref() == Some(id) {
                record.checkout_id = None;
                record.revision = next_revision(&tx)?;
                save_record(&tx, &record)?;
            }
        }
        if tx.execute("DELETE FROM checkouts WHERE id=?1", [id])? == 0 {
            return Err(Error::NotFound);
        }
        tick(&tx)?;
        tx.commit()?;
        Ok(())
    }
    pub fn checkout_snapshot(&self, since: Option<u64>) -> Result<Option<(u64, Vec<Checkout>)>> {
        let conn = self.lock()?;
        let cursor = conn.query_row(
            "SELECT value FROM counters WHERE name='checkouts'",
            [],
            |r| r.get::<_, i64>(0),
        )? as u64;
        if since == Some(cursor) {
            return Ok(None);
        }
        let records = conn
            .prepare_cached("SELECT record FROM checkouts ORDER BY id")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .map(|record| serde_json::from_str(&record).map_err(Error::from))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some((cursor, records)))
    }
    pub fn link_session_checkout(&self, session: &str, checkout: &str) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        if !tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM checkouts WHERE id=?1)",
            [checkout],
            |r| r.get::<_, bool>(0),
        )? {
            return Err(Error::NotFound);
        }
        let data: String = tx.query_row(
            "SELECT public_json FROM agents WHERE id=?1",
            [session],
            |r| r.get(0),
        )?;
        let mut record: AgentRecord = serde_json::from_str(&data)?;
        if record.checkout_id.as_deref() != Some(checkout) {
            record.checkout_id = Some(checkout.into());
            record.revision = next_revision(&tx)?;
            save_record(&tx, &record)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn save_vcs_plan(&self, op: &StoredVcsOperation) -> Result<()> {
        let conn = self.lock()?;
        let n: i64 = conn.query_row(
            "SELECT count(*) FROM vcs_operations WHERE state IN ('prepared','running')",
            [],
            |r| r.get(0),
        )?;
        if n >= 128 {
            return Err(Error::Conflict("VCS operation capacity reached"));
        }
        conn.execute(
            "INSERT INTO vcs_operations(id,actor,state,prepared,record) VALUES(?1,?2,?3,?4,?5)",
            params![
                op.record.plan_id,
                op.actor,
                op.record.state,
                serde_json::to_string(&op.prepared)?,
                serde_json::to_string(&op.record)?
            ],
        )?;
        Ok(())
    }
    pub fn vcs_operation(&self, id: &str) -> Result<StoredVcsOperation> {
        let conn = self.lock()?;
        decode_operation(
            conn.query_row(
                "SELECT actor,prepared,record FROM vcs_operations WHERE id=?1",
                [id],
                operation_row,
            )
            .optional()?
            .ok_or(Error::NotFound)?,
        )
    }
    pub fn claim_vcs_operation(&self, id: &str, actor: &str, now: u64) -> Result<bool> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let (owner, state, data): (String, String, String) = tx
            .query_row(
                "SELECT actor,state,record FROM vcs_operations WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?
            .ok_or(Error::NotFound)?;
        if owner != actor {
            return Err(Error::Conflict("operation belongs to another caller"));
        }
        if state != "prepared" {
            return Ok(false);
        }
        let mut record: VcsOperation = serde_json::from_str(&data)?;
        if now > record.expires_at {
            return Err(Error::Conflict("VCS plan expired"));
        }
        record.state = "running".into();
        tx.execute(
            "UPDATE vcs_operations SET state='running',record=?2 WHERE id=?1",
            params![id, serde_json::to_string(&record)?],
        )?;
        tx.commit()?;
        Ok(true)
    }
    pub fn complete_vcs_operation(&self, record: &VcsOperation) -> Result<()> {
        self.lock()?.execute("UPDATE vcs_operations SET state=?2,record=?3,prepared=CASE WHEN ?2='unknown' THEN prepared ELSE 'null' END WHERE id=?1 AND (state='running' OR state='completed' AND ?2='completed')",params![record.plan_id,record.state,serde_json::to_string(record)?])?;
        Ok(())
    }
    fn vcs_operations(&self, query: &str) -> Result<Vec<StoredVcsOperation>> {
        let conn = self.lock()?;
        let rows = conn
            .prepare_cached(query)?
            .query_map([], operation_row)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        rows.into_iter().map(decode_operation).collect()
    }
    pub fn reserved_vcs_operations(&self) -> Result<Vec<StoredVcsOperation>> {
        self.vcs_operations("SELECT actor,prepared,record FROM vcs_operations WHERE state IN ('running','unknown') AND released=0")
    }
    pub fn release_vcs_operation(&self, id: &str) -> Result<VcsOperation> {
        let mut conn = self.lock()?;
        let tx = conn.transaction()?;
        let json: String = tx
            .query_row("SELECT record FROM vcs_operations WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .optional()?
            .ok_or(Error::NotFound)?;
        let mut record: VcsOperation = serde_json::from_str(&json)?;
        if record.state != "unknown" {
            return Err(Error::Conflict(
                "only an uncertain operation can be released",
            ));
        }
        record.released = true;
        tx.execute(
            "UPDATE vcs_operations SET released=1,prepared='null',record=?2 WHERE id=?1",
            params![id, serde_json::to_string(&record)?],
        )?;
        tx.commit()?;
        Ok(record)
    }
    pub fn recover_vcs_operations(&self) -> Result<()> {
        for mut op in self.vcs_operations(
            "SELECT actor,prepared,record FROM vcs_operations WHERE state='running'",
        )? {
            op.record.state = "unknown".into();
            op.record.error = Some(tower_protocol::RpcError::application(
                "execution_unknown",
                "Daemon stopped before VCS outcome was recorded; do not retry automatically",
            ));
            self.complete_vcs_operation(&op.record)?;
        }
        Ok(())
    }
    pub fn expire_vcs_plans(&self, now: u64) -> Result<()> {
        let conn = self.lock()?;
        conn.execute("DELETE FROM vcs_operations WHERE state='prepared' AND json_extract(record,'$.expires_at')<?1",[now.min(i64::MAX as u64) as i64])?;
        conn.execute("DELETE FROM vcs_operations WHERE state IN ('completed','failed') AND json_extract(record,'$.created_at')<?1",[now.saturating_sub(30*86400000).min(i64::MAX as u64) as i64])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower_protocol::{CatalogChanges, CatalogProject, CatalogRepository};
    fn catalog() -> RepositoryCatalog {
        RepositoryCatalog {
            revision: 1,
            projects: vec![CatalogProject {
                id: "p".into(),
                name: "Project".into(),
            }],
            repositories: vec![CatalogRepository {
                id: "r".into(),
                name: "Repository".into(),
                project_ids: vec!["p".into()],
                upstream_url: Some("https://example.invalid/repo.git".into()),
                url_aliases: vec![],
            }],
        }
    }
    fn checkout() -> StoredCheckout {
        StoredCheckout {
            identity: "native-key".into(),
            binding: serde_json::json!({"native":"identity"}),
            association_manual: false,
            record: Checkout {
                id: "checkout_one".into(),
                repository_id: Some("r".into()),
                path: "~/work/repo".into(),
                system: "git".into(),
                branch: Some("main".into()),
                head: Some("a".repeat(40)),
                dirty: false,
                conflicts: false,
                locked: false,
                prunable: false,
                available: true,
                tracking_ref: None,
                ahead: None,
                behind: None,
                observed_at: 1,
                revision: 0,
            },
        }
    }
    #[test]
    fn catalog_updates_are_atomic_and_pairing_change_preserves_local_checkout_identity() {
        let s = Store::open(Path::new(":memory:")).unwrap();
        s.catalog_pairing("pairing-a").unwrap();
        let c = catalog();
        assert!(s.apply_catalog(&c).unwrap());
        assert!(!s.apply_catalog(&c).unwrap());
        let mut invalid = c.clone();
        invalid.revision = 2;
        invalid.repositories[0].project_ids = vec!["missing".into()];
        assert!(s.apply_catalog(&invalid).is_err());
        assert_eq!(s.catalog().unwrap(), Some(c));
        let saved = s.save_checkout(checkout()).unwrap();
        s.catalog_pairing("pairing-b").unwrap();
        let after = s.checkout(&saved.id).unwrap();
        assert_eq!(after.record.id, saved.id);
        assert_eq!(after.record.path, saved.path);
        assert!(after.record.repository_id.is_none());
        assert!(s.catalog().unwrap().is_none());
    }
    #[test]
    fn catalog_delta_has_exact_base_duplicate_identity_and_reference_checks() {
        let s = Store::open(Path::new(":memory:")).unwrap();
        s.catalog_pairing("pairing").unwrap();
        s.apply_catalog(&catalog()).unwrap();
        let mut delta = CatalogDelta {
            base_revision: 1,
            revision: 2,
            projects: CatalogChanges {
                upserts: vec![CatalogProject {
                    id: "p".into(),
                    name: "Renamed".into(),
                }],
                removed: vec![],
            },
            repositories: CatalogChanges {
                upserts: vec![],
                removed: vec![],
            },
        };
        assert!(s.apply_catalog_delta(&delta).unwrap());
        assert!(!s.apply_catalog_delta(&delta).unwrap());
        delta.projects.upserts[0].name = "Conflict".into();
        assert!(s.apply_catalog_delta(&delta).is_err());
        delta.base_revision = 2;
        delta.revision = 3;
        delta.projects.removed = vec!["p".into()];
        assert!(s.apply_catalog_delta(&delta).is_err());
        delta.projects.upserts.clear();
        assert!(s.apply_catalog_delta(&delta).is_err());
        assert_eq!(s.catalog().unwrap().unwrap().revision, 2);
    }
    #[test]
    fn checkout_move_preserves_id_and_timestamp_only_probe_does_not_trigger_sync() {
        let s = Store::open(Path::new(":memory:")).unwrap();
        s.catalog_pairing("pairing").unwrap();
        s.apply_catalog(&catalog()).unwrap();
        let a = s.save_checkout(checkout()).unwrap();
        let (cursor, records) = s.checkout_snapshot(None).unwrap().unwrap();
        assert_eq!(records, vec![a.clone()]);
        assert!(s.checkout_snapshot(Some(cursor)).unwrap().is_none());
        let mut next = s.checkout(&a.id).unwrap();
        next.record.observed_at = 20;
        assert_eq!(s.save_checkout(next).unwrap().id, a.id);
        assert_eq!(s.checkout_snapshot(None).unwrap().unwrap().0, cursor);
        let mut moved = s.checkout(&a.id).unwrap();
        moved.record.path = "~/work/moved".into();
        let moved = s.save_checkout(moved).unwrap();
        assert_eq!(moved.id, a.id);
        assert!(moved.revision > a.revision);
        let (changed, records) = s.checkout_snapshot(Some(cursor)).unwrap().unwrap();
        assert!(changed > cursor);
        assert_eq!(records, vec![moved]);
    }
    #[test]
    fn observations_cannot_replace_bindings_or_restore_deleted_catalog_links() {
        let s = Store::open(Path::new(":memory:")).unwrap();
        s.catalog_pairing("pairing").unwrap();
        s.apply_catalog(&catalog()).unwrap();
        let initial = checkout();
        let saved = s.save_checkout(initial.clone()).unwrap();
        let mut competing = initial;
        competing.binding = serde_json::json!({"replacement":true});
        assert!(s.save_checkout(competing).is_err());
        let mut old = s.checkout(&saved.id).unwrap();
        let mut renamed = old.clone();
        renamed.record.path = "~/moved".into();
        s.save_checkout(renamed).unwrap();
        assert!(s.save_checkout(old.clone()).is_err());
        old = s.checkout(&saved.id).unwrap();
        let mut removed = catalog();
        removed.revision = 2;
        removed.repositories.clear();
        s.apply_catalog(&removed).unwrap();
        assert!(s.save_checkout(old).is_err());
        assert!(
            s.checkout(&saved.id)
                .unwrap()
                .record
                .repository_id
                .is_none()
        );
        let mut new = checkout();
        new.identity = "another-key".into();
        new.record.id = "another-id".into();
        assert!(s.save_checkout(new).is_err());
    }
    #[test]
    fn catalog_rejects_credentials_and_local_or_executable_remotes() {
        let s = Store::open(Path::new(":memory:")).unwrap();
        s.catalog_pairing("pairing").unwrap();
        for remote in [
            "https://secret@example.invalid/r",
            "ssh://git:secret@example.invalid/r",
            "https://example.invalid/r?token=secret",
            "file:///tmp/repo",
            "ext::command",
            "../repo",
        ] {
            let mut c = catalog();
            c.repositories[0].upstream_url = Some(remote.into());
            assert!(s.apply_catalog(&c).is_err(), "{remote}");
        }
        for (n, remote) in [
            "git@example.invalid:team/repo.git",
            "ssh://git@example.invalid/team/repo.git",
            "https://example.invalid/team/repo.git",
        ]
        .iter()
        .enumerate()
        {
            let mut c = catalog();
            c.revision = n as u64 + 1;
            c.repositories[0].upstream_url = Some((*remote).into());
            s.apply_catalog(&c).unwrap();
        }
    }
    #[test]
    fn claimed_operations_recover_as_unknown_without_redispatch_and_keep_reservation() {
        let s = Store::open(Path::new(":memory:")).unwrap();
        let record = VcsOperation {
            plan_id: "vcs_one".into(),
            checkout_id: Some("checkout_one".into()),
            operation: "push".into(),
            state: "prepared".into(),
            created_at: 1,
            expires_at: 999,
            review: Value::Null,
            released: false,
            result: None,
            error: None,
        };
        s.save_vcs_plan(&StoredVcsOperation {
            actor: "human".into(),
            prepared: serde_json::json!({"private":"plan"}),
            record,
        })
        .unwrap();
        assert!(s.claim_vcs_operation("vcs_one", "other", 2).is_err());
        assert!(s.claim_vcs_operation("vcs_one", "human", 2).unwrap());
        assert!(!s.claim_vcs_operation("vcs_one", "human", 2).unwrap());
        s.recover_vcs_operations().unwrap();
        let op = s.vcs_operation("vcs_one").unwrap();
        assert_eq!(op.record.state, "unknown");
        assert!(!op.prepared.is_null());
        assert_eq!(s.reserved_vcs_operations().unwrap().len(), 1);
        assert!(!s.claim_vcs_operation("vcs_one", "human", 3).unwrap());
        s.release_vcs_operation("vcs_one").unwrap();
        assert!(s.reserved_vcs_operations().unwrap().is_empty());
        assert_eq!(s.vcs_operation("vcs_one").unwrap().record.state, "unknown");
        let mut done = op;
        done.record.plan_id = "vcs_done".into();
        done.record.state = "prepared".into();
        s.save_vcs_plan(&done).unwrap();
        s.claim_vcs_operation("vcs_done", "human", 2).unwrap();
        done.record.state = "completed".into();
        s.complete_vcs_operation(&done.record).unwrap();
        s.recover_vcs_operations().unwrap();
        assert_eq!(
            s.vcs_operation("vcs_done").unwrap().record.state,
            "completed"
        );
        done.record.result = Some(Value::Bool(true));
        s.complete_vcs_operation(&done.record).unwrap();
        assert_eq!(
            s.vcs_operation("vcs_done").unwrap().record.result,
            Some(Value::Bool(true))
        );
    }
}
