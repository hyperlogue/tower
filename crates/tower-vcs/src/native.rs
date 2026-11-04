use crate::{Binding, Discovery, Error, Identity, Result, Snapshot};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

const MAX_OUTPUT: usize = 4 * 1024 * 1024;
const MAX_WORKTREES: usize = 4096;
pub(crate) fn fail(code: &str) -> Error {
    Error::new(code, false)
}
pub(crate) fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
pub(crate) fn identity(path: &Path) -> Result<Identity> {
    let (device, inode) = tower_pal::file_identity(path).map_err(|e| {
        fail(if e.kind() == std::io::ErrorKind::NotFound {
            "missing"
        } else {
            "path_unavailable"
        })
    })?;
    Ok(Identity { device, inode })
}
pub(crate) fn text(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes)
        .map(|s| s.strip_suffix('\n').unwrap_or(&s).to_owned())
        .map_err(|_| fail("unsupported_encoding"))
}
pub(crate) fn oid(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|c| c.is_ascii_hexdigit())
}
pub(crate) fn environment() -> tower_pal::Environment {
    clean_environment(tower_pal::environment())
}
fn clean_environment(mut env: tower_pal::Environment) -> tower_pal::Environment {
    env.retain(|k, _| !k.starts_with("GIT_"));
    env.insert("GIT_TERMINAL_PROMPT".into(), "0".into());
    env.insert("GIT_OPTIONAL_LOCKS".into(), "0".into());
    env.insert("LC_ALL".into(), "C".into());
    env.insert("LANG".into(), "C".into());
    env
}
pub(crate) async fn run(
    binding: Option<&Binding>,
    cwd: &Path,
    args: &[String],
    network: bool,
    mutation: bool,
    extra: &[(String, String)],
) -> Result<(i32, Vec<u8>)> {
    let mut argv = vec![
        "git".into(),
        "--no-pager".into(),
        "-c".into(),
        "core.hooksPath=/dev/null".into(),
        "-c".into(),
        "core.fsmonitor=false".into(),
        "-c".into(),
        "submodule.recurse=false".into(),
        "-c".into(),
        "protocol.ext.allow=never".into(),
    ];
    if let Some(b) = binding {
        argv.push(format!("--git-dir={}", path_text(&b.git_dir)?));
        if let Some(path) = &b.directory {
            argv.push(format!("--work-tree={}", path_text(path)?));
        }
    }
    argv.extend_from_slice(args);
    let mut env = environment();
    env.extend(extra.iter().cloned());
    tower_pal::capture(
        &argv,
        &env,
        cwd,
        Duration::from_secs(if network { 60 } else { 15 }),
        MAX_OUTPUT,
    )
    .await
    .map_err(|e| {
        Error::new(
            if e.kind() == std::io::ErrorKind::TimedOut {
                "operation_timed_out"
            } else {
                "git_unavailable"
            },
            mutation,
        )
    })
}
pub(crate) fn path_text(path: &Path) -> Result<&str> {
    path.to_str().ok_or_else(|| fail("unsupported_encoding"))
}
pub(crate) async fn git(b: &Binding, args: &[&str]) -> Result<Vec<u8>> {
    let cwd = b
        .directory
        .as_deref()
        .filter(|p| p.is_dir())
        .unwrap_or(&b.common_dir);
    let (code, bytes) = run(
        Some(b),
        cwd,
        &args.iter().map(|s| (*s).into()).collect::<Vec<_>>(),
        false,
        false,
        &[],
    )
    .await?;
    if code != 0 {
        return Err(fail("git_failed"));
    }
    Ok(bytes)
}
pub(crate) async fn probe(b: &Binding, args: &[&str]) -> Result<(i32, Vec<u8>)> {
    let cwd = b
        .directory
        .as_deref()
        .filter(|p| p.is_dir())
        .unwrap_or(&b.common_dir);
    run(
        Some(b),
        cwd,
        &args.iter().map(|s| (*s).into()).collect::<Vec<_>>(),
        false,
        false,
        &[],
    )
    .await
}
async fn at(path: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let (code, data) = run(
        None,
        path,
        &args.iter().map(|s| (*s).into()).collect::<Vec<_>>(),
        false,
        false,
        &[],
    )
    .await?;
    if code != 0 {
        return Err(fail("not_repository"));
    }
    Ok(data)
}
pub(crate) async fn binding(path: &Path) -> Result<Binding> {
    let path = std::fs::canonicalize(path).map_err(|_| fail("path_unavailable"))?;
    if !path.is_dir() {
        return Err(fail("not_directory"));
    }
    let git_dir = PathBuf::from(text(
        at(&path, &["rev-parse", "--absolute-git-dir"]).await?,
    )?);
    let common_dir = PathBuf::from(text(
        at(
            &path,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )
        .await?,
    )?);
    let git_dir = std::fs::canonicalize(git_dir).map_err(|_| fail("path_unavailable"))?;
    let common_dir = std::fs::canonicalize(common_dir).map_err(|_| fail("path_unavailable"))?;
    let bare = text(at(&path, &["rev-parse", "--is-bare-repository"]).await?)? == "true";
    let inside = text(at(&path, &["rev-parse", "--is-inside-work-tree"]).await?)? == "true";
    let directory = if inside {
        Some(
            std::fs::canonicalize(PathBuf::from(text(
                at(&path, &["rev-parse", "--show-toplevel"]).await?,
            )?))
            .map_err(|_| fail("path_unavailable"))?,
        )
    } else {
        None
    };
    Ok(Binding {
        git_identity: identity(&git_dir)?,
        common_identity: identity(&common_dir)?,
        directory_identity: directory.as_deref().map(identity).transpose()?,
        directory,
        git_dir,
        common_dir,
        bare,
    })
}
fn native_storage_identity(b: &Binding) -> Result<()> {
    if identity(&b.git_dir)? != b.git_identity || identity(&b.common_dir)? != b.common_identity {
        return Err(fail("identity_changed"));
    }
    Ok(())
}
pub(crate) fn check_identity(b: &Binding) -> Result<()> {
    native_storage_identity(b)?;
    if let Some(path) = &b.directory
        && Some(identity(path)?) != b.directory_identity
    {
        return Err(fail("identity_changed"));
    }
    Ok(())
}
#[derive(Default, Clone)]
pub(crate) struct Worktree {
    pub path: PathBuf,
    pub head: Option<String>,
    pub branch: Option<String>,
    pub bare: bool,
    pub locked: bool,
    pub prunable: bool,
}
pub(crate) async fn worktrees(b: &Binding) -> Result<(Vec<u8>, Vec<Worktree>)> {
    let raw = git(b, &["worktree", "list", "--porcelain", "-z"]).await?;
    let mut result = Vec::new();
    let mut current = None;
    for field in raw.split(|c| *c == 0) {
        if field.is_empty() {
            if let Some(entry) = current.take() {
                result.push(entry);
            }
            continue;
        }
        let field = std::str::from_utf8(field).map_err(|_| fail("unsupported_encoding"))?;
        if let Some(path) = field.strip_prefix("worktree ") {
            if current.is_some() {
                return Err(fail("git_protocol"));
            }
            current = Some(Worktree {
                path: path.into(),
                ..Default::default()
            });
        } else if let Some(w) = current.as_mut() {
            if let Some(value) = field.strip_prefix("HEAD ") {
                w.head = Some(value.into());
            } else if let Some(value) = field.strip_prefix("branch ") {
                w.branch = Some(value.into());
            } else if field == "bare" {
                w.bare = true;
            } else if field == "locked" || field.starts_with("locked ") {
                w.locked = true;
            } else if field == "prunable" || field.starts_with("prunable ") {
                w.prunable = true;
            }
        }
        if result.len() > MAX_WORKTREES {
            return Err(fail("too_large"));
        }
    }
    if let Some(entry) = current {
        result.push(entry);
    }
    if result.len() > MAX_WORKTREES {
        return Err(fail("too_large"));
    }
    Ok((raw, result))
}
fn absolute_backlink(base: &Path, target: &Path) -> Result<PathBuf> {
    use std::path::Component;
    let joined = if target.is_absolute() {
        target.to_path_buf()
    } else {
        base.join(target)
    };
    let mut result = PathBuf::new();
    for part in joined.components() {
        match part {
            Component::RootDir => result.push(std::path::MAIN_SEPARATOR_STR),
            Component::Normal(name) => result.push(name),
            Component::CurDir => {}
            Component::ParentDir => {
                if !result.pop() {
                    return Err(fail("git_protocol"));
                }
            }
            Component::Prefix(_) => return Err(fail("unsupported_encoding")),
        }
    }
    Ok(result)
}
pub async fn discover(path: &Path) -> Result<Discovery> {
    let source = binding(path).await?;
    let (_, entries) = worktrees(&source).await?;
    let mut admins = BTreeMap::new();
    if let Ok(paths) = std::fs::read_dir(source.common_dir.join("worktrees")) {
        for (n, entry) in paths.enumerate() {
            if n >= MAX_WORKTREES {
                return Err(fail("too_large"));
            }
            let entry = entry.map_err(|_| fail("path_unavailable"))?;
            if !entry
                .file_type()
                .map_err(|_| fail("path_unavailable"))?
                .is_dir()
            {
                continue;
            }
            if let Ok(file) = std::fs::File::open(entry.path().join("gitdir")) {
                use std::io::Read;
                let mut bytes = Vec::new();
                if file.take(65537).read_to_end(&mut bytes).is_err() || bytes.len() > 65536 {
                    return Err(fail("git_protocol"));
                }
                let target = absolute_backlink(&entry.path(), &PathBuf::from(text(bytes)?))?;
                if let Some(parent) = target.parent() {
                    admins.insert(parent.to_path_buf(), entry.path());
                }
            }
        }
    }
    let mut worktrees = Vec::new();
    for (index, entry) in entries.into_iter().enumerate() {
        if entry.bare {
            continue;
        }
        let found = match binding(&entry.path).await {
            Ok(found) if found.common_identity == source.common_identity => found,
            _ => {
                // Git lists the main worktree first. Linked identities require their own admin directory.
                let git_dir = admins
                    .get(&entry.path)
                    .cloned()
                    .or_else(|| (index == 0).then(|| source.common_dir.clone()))
                    .ok_or_else(|| fail("stale_checkout"))?;
                Binding {
                    directory: Some(entry.path),
                    git_identity: identity(&git_dir)?,
                    common_identity: source.common_identity.clone(),
                    git_dir,
                    common_dir: source.common_dir.clone(),
                    directory_identity: None,
                    bare: false,
                }
            }
        };
        worktrees.push(found);
    }
    Ok(Discovery { source, worktrees })
}
pub(crate) async fn head(b: &Binding) -> Result<(Option<String>, Option<String>)> {
    let (code, bytes) = probe(b, &["rev-parse", "--verify", "HEAD^{commit}"]).await?;
    let head = if code == 0 {
        let h = text(bytes)?;
        if !oid(&h) {
            return Err(fail("git_protocol"));
        }
        Some(h)
    } else {
        None
    };
    let (code, bytes) = probe(b, &["symbolic-ref", "--quiet", "HEAD"]).await?;
    let branch = if code == 0 { Some(text(bytes)?) } else { None };
    Ok((head, branch))
}
pub(crate) async fn config(b: &Binding) -> Result<String> {
    Ok(hash(
        &git(b, &["config", "--null", "--show-origin", "--list"]).await?,
    ))
}
pub(crate) fn remote_helper(raw: &str) -> bool {
    raw.split_once("::").is_some_and(|(prefix, _)| {
        !prefix.is_empty()
            && prefix
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'+' | b'-' | b'.'))
    })
}
pub(crate) fn public_remote(raw: &str) -> Option<String> {
    if raw.is_empty() || remote_helper(raw) || raw.chars().any(char::is_control) {
        return None;
    }
    if let Ok(mut url) = url::Url::parse(raw)
        && url.has_host()
    {
        if url.scheme() != "ssh" {
            let _ = url.set_username("");
        }
        let _ = url.set_password(None);
        url.set_query(None);
        url.set_fragment(None);
        return Some(url.into());
    }
    if raw.contains("://") {
        return None;
    }
    let raw = raw.split(['?', '#']).next()?;
    if let Some((host, path)) = raw.split_once(':')
        && !host.contains('/')
    {
        return Some(format!("{host}:{path}"));
    }
    Some(raw.into())
}
pub async fn status(b: &Binding) -> Result<Snapshot> {
    if native_storage_identity(b).is_err() {
        return Ok(Snapshot {
            system: "git".into(),
            branch: None,
            head: None,
            dirty: false,
            conflicts: false,
            operation_in_progress: false,
            locked: false,
            prunable: false,
            available: false,
            tracking_ref: None,
            ahead: None,
            behind: None,
            tracked_changes: 0,
            untracked_changes: 0,
            remote_urls: Vec::new(),
        });
    }
    let available = check_identity(b).is_ok()
        && if let Some(path) = &b.directory {
            binding(path).await.is_ok_and(|actual| {
                actual.git_identity == b.git_identity && actual.common_identity == b.common_identity
            })
        } else {
            true
        };
    let (_, list) = worktrees(b).await?;
    let entry = b
        .directory
        .as_ref()
        .and_then(|path| list.iter().find(|w| w.path == *path));
    let (head, branch) = if available {
        head(b).await?
    } else {
        (
            entry.and_then(|e| e.head.clone()),
            entry.and_then(|e| e.branch.clone()),
        )
    };
    let mut snapshot = Snapshot {
        system: "git".into(),
        branch: branch.map(|b| b.strip_prefix("refs/heads/").unwrap_or(&b).into()),
        head,
        dirty: false,
        conflicts: false,
        operation_in_progress: false,
        locked: entry.is_some_and(|e| e.locked),
        prunable: entry.is_some_and(|e| e.prunable),
        available,
        tracking_ref: None,
        ahead: None,
        behind: None,
        tracked_changes: 0,
        untracked_changes: 0,
        remote_urls: Vec::new(),
    };
    if !available {
        return Ok(snapshot);
    }
    if b.directory.is_some() {
        let raw = git(
            b,
            &[
                "status",
                "--porcelain=v2",
                "-z",
                "--branch",
                "--untracked-files=all",
                "--ignore-submodules=none",
            ],
        )
        .await?;
        let mut fields = raw.split(|v| *v == 0);
        while let Some(field) = fields.next() {
            match field.first() {
                Some(b'1') => snapshot.tracked_changes += 1,
                Some(b'2') => {
                    snapshot.tracked_changes += 1;
                    fields.next();
                }
                Some(b'u') => {
                    snapshot.conflicts = true;
                    snapshot.tracked_changes += 1;
                }
                Some(b'?') => snapshot.untracked_changes += 1,
                Some(b'#') => {
                    if let Ok(line) = std::str::from_utf8(field) {
                        if let Some(v) = line.strip_prefix("# branch.upstream ") {
                            snapshot.tracking_ref = Some(v.into());
                        }
                        if let Some(v) = line.strip_prefix("# branch.ab ") {
                            let mut parts = v.split_whitespace();
                            snapshot.ahead = parts
                                .next()
                                .and_then(|v| v.strip_prefix('+'))
                                .and_then(|v| v.parse().ok());
                            snapshot.behind = parts
                                .next()
                                .and_then(|v| v.strip_prefix('-'))
                                .and_then(|v| v.parse().ok());
                        }
                    }
                }
                _ => {}
            }
        }
        snapshot.dirty = snapshot.tracked_changes > 0 || snapshot.untracked_changes > 0;
        snapshot.operation_in_progress = [
            "MERGE_HEAD",
            "CHERRY_PICK_HEAD",
            "REVERT_HEAD",
            "BISECT_LOG",
            "rebase-merge",
            "rebase-apply",
        ]
        .iter()
        .any(|p| b.git_dir.join(p).exists());
    }
    let (code, raw) = probe(
        b,
        &[
            "config",
            "--null",
            "--get-regexp",
            r"^remote\..*\.(url|pushurl)$",
        ],
    )
    .await?;
    if code == 0 {
        for entry in raw.split(|v| *v == 0) {
            if let Some(split) = entry.iter().position(|v| *v == b'\n')
                && let Ok(value) = std::str::from_utf8(&entry[split + 1..])
                && let Some(url) = public_remote(value)
                && !snapshot.remote_urls.contains(&url)
            {
                snapshot.remote_urls.push(url);
                if snapshot.remote_urls.len() > 256 {
                    return Err(fail("too_large"));
                }
            }
        }
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn remote_display_omits_credentials_queries_and_helper_commands() {
        assert_eq!(
            public_remote("https://secret:password@example.invalid/repo?token=private#key"),
            Some("https://example.invalid/repo".into())
        );
        assert_eq!(
            public_remote("git@example.invalid:repo"),
            Some("git@example.invalid:repo".into())
        );
        assert!(public_remote("ext::command --secret value").is_none());
        assert_eq!(
            public_remote("ssh://git@[::1]/repo"),
            Some("ssh://git@[::1]/repo".into())
        );
        assert_eq!(
            public_remote("git@[2001:db8::1]:repo"),
            Some("git@[2001:db8::1]:repo".into())
        );
    }
    #[test]
    fn inherited_git_selectors_and_config_injection_are_removed() {
        let env = clean_environment(
            [
                ("PATH".into(), "/bin".into()),
                ("GIT_DIR".into(), "wrong".into()),
                ("GIT_WORK_TREE".into(), "wrong".into()),
                ("GIT_INDEX_FILE".into(), "wrong".into()),
                ("GIT_CONFIG_COUNT".into(), "1".into()),
                ("GIT_CONFIG_KEY_0".into(), "core.worktree".into()),
                ("GIT_CONFIG_VALUE_0".into(), "wrong".into()),
                ("GIT_CONFIG_PARAMETERS".into(), "injected".into()),
            ]
            .into(),
        );
        assert_eq!(env["PATH"], "/bin");
        assert!(
            env.keys()
                .filter(|k| k.starts_with("GIT_"))
                .all(|k| matches!(k.as_str(), "GIT_TERMINAL_PROMPT" | "GIT_OPTIONAL_LOCKS"))
        );
    }
}
