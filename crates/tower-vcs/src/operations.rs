use crate::{
    Binding, CheckoutTarget, Error, Expectation, Intent, Operation, Outcome, Prepared, Result,
    Review, Root, Tracking,
    native::{self, fail},
};
use std::{
    path::{Component, Path, PathBuf},
    time::Duration,
};

async fn lock(binding: &Binding) -> Result<tower_pal::SingletonLock> {
    if native::identity(&binding.common_dir)? != binding.common_identity {
        return Err(fail("identity_changed"));
    }
    let until = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match tower_pal::SingletonLock::acquire(&binding.common_dir.join("tower-vcs.lock")) {
            Ok(lock) => return Ok(lock),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    && tokio::time::Instant::now() < until =>
            {
                tokio::time::sleep(Duration::from_millis(20)).await
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(fail("repository_busy"));
            }
            Err(_) => return Err(fail("repository_lock_failed")),
        }
    }
}
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || name.starts_with('-')
        || name.contains('\\')
        || name.chars().any(char::is_control)
        || !matches!(
            Path::new(name).components().collect::<Vec<_>>().as_slice(),
            [Component::Normal(_)]
        )
    {
        return Err(fail("invalid_worktree_name"));
    }
    Ok(())
}
fn root(path: &Path) -> Result<Root> {
    if !path.is_absolute() {
        return Err(fail("invalid_worktree_root"));
    }
    tower_pal::ensure_user_dir(path).map_err(|_| fail("worktree_root_unavailable"))?;
    let canonical = std::fs::canonicalize(path).map_err(|_| fail("worktree_root_unavailable"))?;
    if !canonical.is_dir() {
        return Err(fail("invalid_worktree_root"));
    }
    Ok(Root {
        configured: path.into(),
        identity: native::identity(&canonical)?,
        canonical,
    })
}
fn destination(root: &Root, name: &str, exists: bool) -> Result<PathBuf> {
    validate_name(name)?;
    let path = root.canonical.join(name);
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if exists && meta.is_dir() => Ok(path),
        Err(e) if !exists && e.kind() == std::io::ErrorKind::NotFound => Ok(path),
        _ => Err(fail(if exists {
            "worktree_unavailable"
        } else {
            "destination_exists"
        })),
    }
}
async fn branch_ref(b: &Binding, name: &str) -> Result<String> {
    let name = name.strip_prefix("refs/heads/").unwrap_or(name);
    if name.len() > 1024 || name.chars().any(char::is_control) {
        return Err(fail("invalid_reference"));
    }
    let (code, output) = native::probe(b, &["check-ref-format", "--branch", name]).await?;
    if code != 0 || native::text(output)? != name {
        return Err(fail("invalid_reference"));
    }
    Ok(format!("refs/heads/{name}"))
}
async fn revision(b: &Binding, value: &str) -> Result<String> {
    if value.is_empty() || value.len() > 4096 || value.chars().any(char::is_control) {
        return Err(fail("invalid_revision"));
    }
    let value = format!("{value}^{{commit}}");
    let bytes = native::git(b, &["rev-parse", "--verify", "--end-of-options", &value])
        .await
        .map_err(|_| fail("revision_not_found"))?;
    let value = native::text(bytes)?;
    if !native::oid(&value) {
        return Err(fail("git_protocol"));
    }
    Ok(value)
}
async fn config_value(b: &Binding, key: &str) -> Result<Option<String>> {
    let (code, bytes) = native::probe(b, &["config", "--null", "--get-all", key]).await?;
    if code == 1 {
        return Ok(None);
    }
    if code != 0 {
        return Err(fail("git_failed"));
    }
    let values = bytes
        .split(|v| *v == 0)
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>();
    if values.len() != 1 {
        return Err(fail("ambiguous_configuration"));
    }
    Ok(Some(
        String::from_utf8(values[0].to_vec()).map_err(|_| fail("unsupported_encoding"))?,
    ))
}
struct Remote {
    url: String,
    reference: String,
    tracking: Option<Tracking>,
}
async fn remote(
    b: &Binding,
    selected: &Option<String>,
    reference: &Option<String>,
    push: bool,
) -> Result<Remote> {
    let (_, branch) = native::head(b).await?;
    let branch = branch
        .as_deref()
        .and_then(|b| b.strip_prefix("refs/heads/"));
    let configured = if let Some(branch) = branch {
        config_value(b, &format!("branch.{branch}.remote")).await?
    } else {
        None
    };
    let name = selected
        .clone()
        .or(configured.clone())
        .ok_or_else(|| fail("remote_required"))?;
    if name.is_empty()
        || name.len() > 1024
        || name.starts_with('-')
        || name.chars().any(char::is_control)
    {
        return Err(fail("invalid_remote"));
    }
    let reference = if let Some(reference) = reference {
        branch_ref(b, reference).await?
    } else if selected.is_none() || configured.as_ref() == Some(&name) {
        let branch = branch.ok_or_else(|| fail("reference_required"))?;
        let upstream = config_value(b, &format!("branch.{branch}.merge")).await?;
        branch_ref(b, upstream.as_deref().unwrap_or(branch)).await?
    } else {
        branch_ref(b, branch.ok_or_else(|| fail("reference_required"))?).await?
    };
    let url = if name == "." {
        native::path_text(&b.common_dir)?.to_owned()
    } else {
        let mut args = vec!["remote", "get-url", "--all"];
        if push {
            args.push("--push");
        }
        args.extend(["--", name.as_str()]);
        let value = native::text(
            native::git(b, &args)
                .await
                .map_err(|_| fail("remote_not_found"))?,
        )?;
        if value.contains('\n') {
            return Err(fail("ambiguous_remote"));
        }
        value
    };
    let url = normalize_url(b, &url)?;
    let tracking = tracking(b, &name, &reference).await?;
    let remote = Remote {
        url,
        reference,
        tracking,
    };
    let (alias, env) = remote_environment(&remote.url);
    let pattern = format!(r"^remote\.{alias}\.");
    if native::probe(b, &["config", "--get-regexp", &pattern])
        .await?
        .0
        == 0
    {
        return Err(fail("ambiguous_remote_configuration"));
    }
    let (code, output) = native::run(
        Some(b),
        cwd(b),
        &["remote".into(), "-v".into()],
        false,
        false,
        &env,
    )
    .await?;
    if code != 0 {
        return Err(fail("remote_unavailable"));
    }
    let text = native::text(output)?;
    let mut matches = 0;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix(&format!("{alias}\t")) {
            let url = value
                .strip_suffix(" (fetch)")
                .or_else(|| value.strip_suffix(" (push)"))
                .ok_or_else(|| fail("git_protocol"))?;
            if url != remote.url {
                return Err(fail("ambiguous_remote_rewrite"));
            }
            matches += 1;
        }
    }
    if matches != 2 {
        return Err(fail("ambiguous_remote_configuration"));
    }
    Ok(remote)
}
async fn tracking(b: &Binding, remote_name: &str, remote_ref: &str) -> Result<Option<Tracking>> {
    let (_, branch) = native::head(b).await?;
    let Some(branch) = branch else {
        return Ok(None);
    };
    let raw = native::text(
        native::git(
            b,
            &[
                "for-each-ref",
                "--format=%(upstream)%00%(upstream:remotename)%00%(upstream:remoteref)",
                &branch,
            ],
        )
        .await?,
    )?;
    let parts = raw.split('\0').collect::<Vec<_>>();
    if parts.len() != 3
        || !parts[0].starts_with("refs/remotes/")
        || parts[1] != remote_name
        || parts[2] != remote_ref
    {
        return Ok(None);
    }
    let reference = parts[0].to_owned();
    let url =
        native::text(native::git(b, &["remote", "get-url", "--all", "--", remote_name]).await?)?;
    if url.contains('\n') {
        return Ok(None);
    }
    let url = normalize_url(b, &url)?;
    let (code, raw) = native::probe(
        b,
        &["rev-parse", "--verify", "--end-of-options", &reference],
    )
    .await?;
    let previous = if code == 0 {
        let value = native::text(raw)?;
        if !native::oid(&value) {
            return Err(fail("git_protocol"));
        }
        Some(value)
    } else {
        None
    };
    Ok(Some(Tracking {
        reference,
        previous,
        url,
    }))
}
fn normalize_url(b: &Binding, value: &str) -> Result<String> {
    if value.is_empty()
        || value.len() > 8192
        || value.chars().any(char::is_control)
        || native::remote_helper(value)
    {
        return Err(fail("unsupported_remote"));
    }
    if value.contains("://") {
        let url = url::Url::parse(value).map_err(|_| fail("invalid_remote"))?;
        if !matches!(url.scheme(), "https" | "http" | "ssh" | "git" | "file") {
            return Err(fail("unsupported_remote"));
        }
        if url.scheme() == "file" {
            let path = url.to_file_path().map_err(|_| fail("invalid_remote"))?;
            return Ok(native::path_text(
                &std::fs::canonicalize(path).map_err(|_| fail("remote_unavailable"))?,
            )?
            .into());
        }
        return Ok(value.into());
    }
    if let Some((host, _)) = value.split_once(':')
        && !host.contains('/')
    {
        return Ok(value.into());
    }
    let path = Path::new(value);
    let path = if path.is_absolute() {
        path.into()
    } else {
        cwd(b).join(path)
    };
    Ok(
        native::path_text(&std::fs::canonicalize(path).map_err(|_| fail("remote_unavailable"))?)?
            .into(),
    )
}
fn remote_environment(url: &str) -> (String, Vec<(String, String)>) {
    let alias = format!("tower-{}", native::hash(url.as_bytes()));
    (
        alias.clone(),
        vec![
            ("GIT_CONFIG_COUNT".into(), "2".into()),
            ("GIT_CONFIG_KEY_0".into(), format!("remote.{alias}.url")),
            ("GIT_CONFIG_VALUE_0".into(), url.into()),
            ("GIT_CONFIG_KEY_1".into(), format!("remote.{alias}.pushurl")),
            ("GIT_CONFIG_VALUE_1".into(), url.into()),
        ],
    )
}
fn cwd(b: &Binding) -> &Path {
    b.directory
        .as_deref()
        .filter(|p| p.is_dir())
        .unwrap_or(&b.common_dir)
}
async fn remote_oid(
    b: &Binding,
    url: &str,
    reference: &str,
    auth: &crate::Authentication,
) -> Result<Option<String>> {
    let (alias, env) = remote_environment(url);
    let (code, bytes) = native::run_with_auth(
        Some(b),
        cwd(b),
        &[
            "ls-remote".into(),
            "--refs".into(),
            "--".into(),
            alias,
            reference.into(),
        ],
        true,
        false,
        &env,
        auth,
    )
    .await?;
    if code != 0 {
        return Err(fail("remote_unavailable"));
    }
    let text = native::text(bytes)?;
    let mut found = None;
    for line in text.lines() {
        let Some((oid, name)) = line.split_once('\t') else {
            return Err(fail("git_protocol"));
        };
        if name == reference {
            if found.is_some() || !native::oid(oid) {
                return Err(fail("git_protocol"));
            }
            found = Some(oid.to_owned());
        }
    }
    Ok(found)
}
async fn fetch(
    b: &Binding,
    url: &str,
    reference: &str,
    oid: &str,
    auth: &crate::Authentication,
) -> Result<()> {
    let (alias, env) = remote_environment(url);
    let (code, _) = native::run_with_auth(
        Some(b),
        cwd(b),
        &[
            "fetch".into(),
            "--no-write-fetch-head".into(),
            "--no-tags".into(),
            "--no-recurse-submodules".into(),
            "--".into(),
            alias,
            reference.into(),
        ],
        true,
        false,
        &env,
        auth,
    )
    .await?;
    if code != 0 || revision(b, oid).await.as_deref() != Ok(oid) {
        return Err(fail("fetch_failed"));
    }
    Ok(())
}
async fn ancestor(b: &Binding, old: &str, new: &str) -> Result<()> {
    let (code, _) = native::probe(b, &["merge-base", "--is-ancestor", old, new]).await?;
    if code != 0 {
        return Err(fail("not_fast_forward"));
    }
    Ok(())
}
fn require_worktree(b: &Binding) -> Result<&Path> {
    b.directory.as_deref().ok_or_else(|| fail("not_checkout"))
}
async fn clean(b: &Binding, remove: bool) -> Result<()> {
    let s = native::status(b).await?;
    if !s.available {
        return Err(fail("stale_checkout"));
    }
    if s.conflicts || s.operation_in_progress {
        return Err(fail("checkout_conflicted"));
    }
    if s.dirty {
        return Err(fail("checkout_dirty"));
    }
    if remove
        && !native::git(
            b,
            &[
                "ls-files",
                "--others",
                "--ignored",
                "--exclude-standard",
                "-z",
            ],
        )
        .await?
        .is_empty()
    {
        return Err(fail("checkout_has_ignored_files"));
    }
    Ok(())
}
fn root_check(root: &Root) -> Result<()> {
    if std::fs::canonicalize(&root.configured).ok().as_ref() != Some(&root.canonical)
        || native::identity(&root.canonical)? != root.identity
    {
        return Err(fail("worktree_root_changed"));
    }
    Ok(())
}
fn repair_binding(b: &Binding, path: &Path) -> Result<Binding> {
    if native::identity(&b.git_dir)? != b.git_identity
        || native::identity(&b.common_dir)? != b.common_identity
        || Some(native::identity(path)?) != b.directory_identity
    {
        return Err(fail("identity_changed"));
    }
    let mut value = b.clone();
    value.directory = Some(path.into());
    Ok(value)
}
pub async fn prepare(
    binding: &Binding,
    operation: Operation,
    worktree_root: &Path,
    auth: &crate::Authentication,
) -> Result<Prepared> {
    let _lock = lock(binding).await?;
    let mut root_value = None;
    let mut tracking = None;
    let mut actual = binding.clone();
    if let Operation::CheckoutRepair { name } = &operation {
        let path = if let Some(name) = name {
            let r = root(worktree_root)?;
            let path = destination(&r, name, true)?;
            root_value = Some(r);
            path
        } else {
            require_worktree(binding)?.into()
        };
        actual = repair_binding(binding, &path)?;
    } else {
        native::check_identity(binding)?;
        if binding.directory.is_some() && !native::status(binding).await?.available {
            return Err(fail("stale_checkout"));
        }
    }
    check_checkout_binding(
        &actual,
        matches!(operation, Operation::CheckoutRepair { .. }),
    )
    .await?;
    let (head, branch) = native::head(&actual).await?;
    let (worktrees, list) = native::worktrees(&actual).await?;
    let entry = binding
        .directory
        .as_ref()
        .and_then(|p| list.iter().find(|w| w.path == *p));
    let mut review = Review {
        authentication: auth.mode(),
        operation: operation.kind().into(),
        system: "git".into(),
        source: binding.directory.clone(),
        destination: None,
        branch: branch.clone(),
        head: head.clone(),
        target: None,
        remote: None,
        warnings: vec!["Tower disables Git hooks for this operation.".into()],
    };
    let intent = match &operation {
        Operation::Push {
            remote: selection,
            reference,
        } => {
            let oid = head.clone().ok_or_else(|| fail("unborn_branch"))?;
            let remote = remote(&actual, selection, reference, true).await?;
            tracking = remote.tracking.clone();
            let old = remote_oid(&actual, &remote.url, &remote.reference, auth).await?;
            if let Some(old) = &old {
                fetch(&actual, &remote.url, &remote.reference, old, auth).await?;
                ancestor(&actual, old, &oid).await?;
            }
            review.target = Some(oid.clone());
            review.branch = Some(remote.reference.clone());
            review.remote = native::public_remote(&remote.url);
            Intent::Push {
                url: remote.url,
                reference: remote.reference,
                oid,
                remote_oid: old,
            }
        }
        Operation::Pull {
            remote: selection,
            reference,
        } => {
            require_worktree(&actual)?;
            clean(&actual, false).await?;
            if branch.is_none() {
                return Err(fail("detached_head"));
            }
            let old = head.as_deref().ok_or_else(|| fail("unborn_branch"))?;
            let remote = remote(&actual, selection, reference, false).await?;
            tracking = remote.tracking.clone();
            let target = remote_oid(&actual, &remote.url, &remote.reference, auth)
                .await?
                .ok_or_else(|| fail("remote_reference_missing"))?;
            fetch(&actual, &remote.url, &remote.reference, &target, auth).await?;
            ancestor(&actual, old, &target).await?;
            review.target = Some(target.clone());
            review.remote = native::public_remote(&remote.url);
            review
                .warnings
                .push("Preparation fetches objects without changing local branches.".into());
            Intent::Pull {
                url: remote.url,
                reference: remote.reference,
                oid: target,
            }
        }
        Operation::CheckoutCreate { name, target } => {
            let r = root(worktree_root)?;
            let path = destination(&r, name, false)?;
            root_value = Some(r);
            if path.starts_with(&binding.common_dir) {
                return Err(fail("invalid_destination"));
            }
            let (oid, branch, new_branch) = match target {
                CheckoutTarget::ExistingBranch { branch } => {
                    let reference = branch_ref(&actual, branch).await?;
                    if list.iter().any(|w| w.branch.as_ref() == Some(&reference)) {
                        return Err(fail("branch_in_use"));
                    }
                    let oid = revision(&actual, &reference).await?;
                    (oid, Some(reference), false)
                }
                CheckoutTarget::NewBranch {
                    branch,
                    start_point,
                } => {
                    let reference = branch_ref(&actual, branch).await?;
                    if native::probe(&actual, &["show-ref", "--verify", "--quiet", &reference])
                        .await?
                        .0
                        == 0
                    {
                        return Err(fail("branch_exists"));
                    }
                    let oid = revision(&actual, start_point.as_deref().unwrap_or("HEAD")).await?;
                    (oid, Some(reference), true)
                }
                CheckoutTarget::Detached { revision: value } => {
                    (revision(&actual, value).await?, None, false)
                }
            };
            review.destination = Some(path.clone());
            review.branch = branch.clone();
            review.target = Some(oid.clone());
            Intent::Create {
                path,
                oid,
                branch,
                new_branch,
            }
        }
        Operation::CheckoutMove { name } => {
            require_worktree(&actual)?;
            if binding.git_dir == binding.common_dir {
                return Err(fail("main_checkout"));
            }
            if entry.is_some_and(|w| w.locked) {
                return Err(fail("checkout_locked"));
            }
            clean(&actual, false).await?;
            let r = root(worktree_root)?;
            let path = destination(&r, name, false)?;
            if path.starts_with(&binding.common_dir) || path.starts_with(require_worktree(&actual)?)
            {
                return Err(fail("invalid_destination"));
            }
            if actual
                .directory_identity
                .as_ref()
                .is_some_and(|d| d.device != r.identity.device)
            {
                return Err(fail("cross_device_move"));
            }
            root_value = Some(r);
            review.destination = Some(path.clone());
            Intent::Move { path }
        }
        Operation::CheckoutRemove => {
            require_worktree(&actual)?;
            if binding.git_dir == binding.common_dir {
                return Err(fail("main_checkout"));
            }
            if entry.is_some_and(|w| w.locked) {
                return Err(fail("checkout_locked"));
            }
            clean(&actual, true).await?;
            Intent::Remove
        }
        Operation::CheckoutRepair { .. } => {
            let path = require_worktree(&actual)?.to_path_buf();
            if binding.git_dir == binding.common_dir {
                return Err(fail("main_checkout"));
            }
            review.destination = Some(path.clone());
            Intent::Repair { path }
        }
    };
    let mut affected_paths = binding.directory.clone().into_iter().collect::<Vec<_>>();
    if let Some(path) = &review.destination
        && !affected_paths.contains(path)
    {
        affected_paths.push(path.clone());
    }
    Ok(Prepared {
        binding: actual.clone(),
        operation,
        review,
        affected_paths,
        expectation: Expectation {
            config: native::config(&actual).await?,
            worktrees: native::hash(&worktrees),
            head,
            branch,
            root: root_value,
            tracking,
        },
        intent,
    })
}
fn absent(path: &Path) -> Result<()> {
    match path.symlink_metadata() {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(fail("destination_exists")),
        Err(_) => Err(fail("destination_unavailable")),
    }
}
async fn check_checkout_binding(b: &Binding, repair: bool) -> Result<()> {
    if let Some(path) = &b.directory {
        match native::binding(path).await {
            Ok(current)
                if current.git_identity == b.git_identity
                    && current.common_identity == b.common_identity => {}
            Err(_) if repair => {
                let link = path.join(".git");
                if let Ok(meta) = link.symlink_metadata()
                    && !meta.is_file()
                {
                    return Err(fail("identity_changed"));
                }
            }
            _ => return Err(fail("stale_checkout")),
        }
    }
    Ok(())
}
pub(crate) async fn refresh_tracking(p: &Prepared) -> bool {
    let Some(tracking) = &p.expectation.tracking else {
        return true;
    };
    let (url, target) = match &p.intent {
        Intent::Push { url, oid, .. } | Intent::Pull { url, oid, .. } => (url, oid),
        _ => return true,
    };
    let refresh = async {
        if url != &tracking.url || native::config(&p.binding).await? != p.expectation.config {
            return Err(fail("tracking_changed"));
        }
        if native::probe(
            &p.binding,
            &["symbolic-ref", "--quiet", &tracking.reference],
        )
        .await?
        .0 == 0
        {
            return Err(fail("tracking_changed"));
        }
        let (code, current) = native::probe(
            &p.binding,
            &[
                "rev-parse",
                "--verify",
                "--end-of-options",
                &tracking.reference,
            ],
        )
        .await?;
        if code == 0 && native::text(current)? == *target {
            return Ok(());
        }
        let expected = tracking
            .previous
            .clone()
            .unwrap_or_else(|| "0".repeat(target.len()));
        let (code, _) = native::probe(
            &p.binding,
            &[
                "update-ref",
                "--no-deref",
                "-m",
                "tower: refresh reviewed remote",
                &tracking.reference,
                target,
                &expected,
            ],
        )
        .await?;
        if code != 0 {
            return Err(fail("tracking_changed"));
        }
        Ok::<_, Error>(())
    };
    tokio::time::timeout(Duration::from_secs(5), refresh)
        .await
        .is_ok_and(|result| result.is_ok())
}
async fn revalidate(p: &Prepared, auth: &crate::Authentication) -> Result<()> {
    if p.review.operation != p.operation.kind()
        || p.review.system != "git"
        || p.review.head != p.expectation.head
    {
        return Err(fail("invalid_plan"));
    }
    native::check_identity(&p.binding)?;
    check_checkout_binding(&p.binding, matches!(p.intent, Intent::Repair { .. })).await?;
    if native::config(&p.binding).await? != p.expectation.config {
        return Err(fail("configuration_changed"));
    }
    let (head, branch) = native::head(&p.binding).await?;
    if head != p.expectation.head || branch != p.expectation.branch {
        return Err(fail("checkout_changed"));
    }
    if native::hash(&native::worktrees(&p.binding).await?.0) != p.expectation.worktrees {
        return Err(fail("worktrees_changed"));
    }
    if let Some(root) = &p.expectation.root {
        root_check(root)?;
    }
    match &p.intent {
        Intent::Push {
            url,
            reference,
            oid,
            remote_oid: expected,
        } => {
            let Operation::Push {
                remote: selected,
                reference: target,
            } = &p.operation
            else {
                return Err(fail("invalid_plan"));
            };
            let configured = remote(&p.binding, selected, target, true).await?;
            if configured.url != *url
                || configured.reference != *reference
                || p.review.target.as_ref() != Some(oid)
                || p.review.branch.as_ref() != Some(reference)
                || p.review.remote != native::public_remote(url)
            {
                return Err(fail("invalid_plan"));
            }
            if remote_oid(&p.binding, url, reference, auth).await? != *expected {
                return Err(fail("remote_changed"));
            }
        }
        Intent::Pull {
            url,
            reference,
            oid,
        } => {
            let Operation::Pull {
                remote: selected,
                reference: target,
            } = &p.operation
            else {
                return Err(fail("invalid_plan"));
            };
            let configured = remote(&p.binding, selected, target, false).await?;
            if configured.url != *url
                || configured.reference != *reference
                || p.review.target.as_ref() != Some(oid)
                || p.review.remote != native::public_remote(url)
            {
                return Err(fail("invalid_plan"));
            }
            clean(&p.binding, false).await?;
            revision(&p.binding, oid).await?;
            ancestor(
                &p.binding,
                p.expectation
                    .head
                    .as_deref()
                    .ok_or_else(|| fail("unborn_branch"))?,
                oid,
            )
            .await?;
        }
        Intent::Create {
            path,
            oid,
            branch,
            new_branch,
        } => {
            let Operation::CheckoutCreate { name, .. } = &p.operation else {
                return Err(fail("invalid_plan"));
            };
            validate_name(name)?;
            if p.expectation
                .root
                .as_ref()
                .map(|r| r.canonical.join(name))
                .as_ref()
                != Some(path)
                || p.review.destination.as_ref() != Some(path)
                || p.review.target.as_ref() != Some(oid)
                || p.review.branch != *branch
            {
                return Err(fail("invalid_plan"));
            }
            absent(path)?;
            revision(&p.binding, oid).await?;
            if let Some(branch) = branch {
                let existing = revision(&p.binding, branch).await;
                if *new_branch {
                    if existing.is_ok() {
                        return Err(fail("branch_exists"));
                    }
                } else if existing.as_ref() != Ok(oid) {
                    return Err(fail("branch_changed"));
                }
            }
        }
        Intent::Move { path } => {
            let Operation::CheckoutMove { name } = &p.operation else {
                return Err(fail("invalid_plan"));
            };
            validate_name(name)?;
            if p.expectation
                .root
                .as_ref()
                .map(|r| r.canonical.join(name))
                .as_ref()
                != Some(path)
                || p.review.destination.as_ref() != Some(path)
            {
                return Err(fail("invalid_plan"));
            }
            absent(path)?;
            clean(&p.binding, false).await?;
        }
        Intent::Remove => {
            if !matches!(p.operation, Operation::CheckoutRemove) {
                return Err(fail("invalid_plan"));
            }
            clean(&p.binding, true).await?;
        }
        Intent::Repair { path } => {
            let Operation::CheckoutRepair { name } = &p.operation else {
                return Err(fail("invalid_plan"));
            };
            if p.binding.directory.as_ref() != Some(path)
                || p.review.destination.as_ref() != Some(path)
            {
                return Err(fail("invalid_plan"));
            }
            if let Some(name) = name {
                validate_name(name)?;
                if p.expectation
                    .root
                    .as_ref()
                    .map(|r| r.canonical.join(name))
                    .as_ref()
                    != Some(path)
                {
                    return Err(fail("invalid_plan"));
                }
            }
        }
    }
    Ok(())
}
pub(crate) struct Applied {
    pub outcome: Outcome,
    pub _guard: tower_pal::SingletonLock,
}
pub async fn execute(
    p: &Prepared,
    auth: &crate::Authentication,
    check: impl Fn(&[PathBuf]) -> Result<()> + Send + Sync,
    dispatched: &std::sync::atomic::AtomicBool,
) -> Result<Applied> {
    let guard = lock(&p.binding).await?;
    revalidate(p, auth).await?;
    check(&p.affected_paths)?;
    let mut env = Vec::new();
    let mut network = false;
    let args = match &p.intent {
        Intent::Push {
            url,
            reference,
            oid,
            ..
        } => {
            let (alias, e) = remote_environment(url);
            env = e;
            network = true;
            vec![
                "push".into(),
                "--porcelain".into(),
                "--no-force".into(),
                "--no-mirror".into(),
                "--no-follow-tags".into(),
                "--recurse-submodules=no".into(),
                "--signed=false".into(),
                "--".into(),
                alias,
                format!("{oid}:{reference}"),
            ]
        }
        Intent::Pull { oid, .. } => vec![
            "merge".into(),
            "--ff-only".into(),
            "--no-edit".into(),
            "--no-autostash".into(),
            "--".into(),
            oid.clone(),
        ],
        Intent::Create {
            path,
            oid,
            branch,
            new_branch,
        } => {
            let mut args = vec!["worktree".into(), "add".into()];
            if *new_branch {
                args.extend([
                    "--no-track".into(),
                    "-b".into(),
                    branch
                        .as_deref()
                        .and_then(|b| b.strip_prefix("refs/heads/"))
                        .ok_or_else(|| fail("invalid_plan"))?
                        .into(),
                ]);
            } else if branch.is_none() {
                args.push("--detach".into());
            }
            args.extend([
                "--".into(),
                native::path_text(path)?.into(),
                if !*new_branch {
                    branch
                        .as_deref()
                        .and_then(|b| b.strip_prefix("refs/heads/"))
                        .unwrap_or(oid)
                        .into()
                } else {
                    oid.clone()
                },
            ]);
            args
        }
        Intent::Move { path } => vec![
            "worktree".into(),
            "move".into(),
            "--".into(),
            native::path_text(require_worktree(&p.binding)?)?.into(),
            native::path_text(path)?.into(),
        ],
        Intent::Remove => vec![
            "worktree".into(),
            "remove".into(),
            "--".into(),
            native::path_text(require_worktree(&p.binding)?)?.into(),
        ],
        Intent::Repair { path } => vec![
            "worktree".into(),
            "repair".into(),
            "--".into(),
            native::path_text(path)?.into(),
        ],
    };
    auth.validate()?;
    dispatched.store(true, std::sync::atomic::Ordering::Release);
    let (code, _) = native::run_with_auth(
        Some(&p.binding),
        cwd(&p.binding),
        &args,
        network,
        true,
        &env,
        auth,
    )
    .await?;
    if code != 0 {
        return Err(Error::new("operation_failed", true));
    }
    let verify = async {
        let result = match &p.intent {
            Intent::Push {
                url,
                reference,
                oid,
                ..
            } => {
                if remote_oid(&p.binding, url, reference, auth).await?.as_ref() != Some(oid) {
                    return Err(fail("verification_failed"));
                }
                Some(p.binding.clone())
            }
            Intent::Pull { oid, .. } => {
                if native::head(&p.binding).await?
                    != (Some(oid.clone()), p.expectation.branch.clone())
                {
                    return Err(fail("verification_failed"));
                }
                Some(p.binding.clone())
            }
            Intent::Create {
                path, oid, branch, ..
            } => {
                let b = native::binding(path).await?;
                if b.common_identity != p.binding.common_identity
                    || native::head(&b).await? != (Some(oid.clone()), branch.clone())
                {
                    return Err(fail("verification_failed"));
                }
                Some(b)
            }
            Intent::Move { path } | Intent::Repair { path } => {
                let b = native::binding(path).await?;
                if b.identity_key() != p.binding.identity_key()
                    || b.directory_identity != p.binding.directory_identity
                {
                    return Err(fail("verification_failed"));
                }
                Some(b)
            }
            Intent::Remove => {
                if require_worktree(&p.binding)?.symlink_metadata().is_ok() {
                    return Err(fail("verification_failed"));
                }
                None
            }
        };
        Ok(result)
    }
    .await
    .map_err(|_: Error| Error::new("operation_unknown", true))?;
    Ok(Applied {
        outcome: Outcome {
            state: "applied".into(),
            binding: verify,
            affected_paths: p.affected_paths.clone(),
            refresh_pending: false,
        },
        _guard: guard,
    })
}
