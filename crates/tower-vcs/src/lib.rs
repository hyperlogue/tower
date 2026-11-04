use serde::{Deserialize, Serialize};
use std::{fmt, path::PathBuf};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Error {
    pub code: String,
    pub may_have_effect: bool,
}
impl Error {
    pub fn new(code: impl Into<String>, may_have_effect: bool) -> Self {
        Self {
            code: code.into(),
            may_have_effect,
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.code)
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Identity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Binding {
    pub directory: Option<PathBuf>,
    pub git_dir: PathBuf,
    pub common_dir: PathBuf,
    pub bare: bool,
    git_identity: Identity,
    common_identity: Identity,
    directory_identity: Option<Identity>,
}
impl Binding {
    pub fn can_reconcile(&self, previous: &Self) -> bool {
        self.identity_key() == previous.identity_key()
            && match (&self.directory_identity, &previous.directory_identity) {
                (Some(current), Some(old)) => current == old,
                (None, None) => self.directory.is_none() && previous.directory.is_none(),
                _ => false,
            }
    }
    pub fn repository_key(&self) -> String {
        format!(
            "git:{}:{}",
            self.common_identity.device, self.common_identity.inode
        )
    }
    pub fn identity_key(&self) -> String {
        format!(
            "git:{}:{}:{}:{}",
            self.common_identity.device,
            self.common_identity.inode,
            self.git_identity.device,
            self.git_identity.inode
        )
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Discovery {
    pub source: Binding,
    pub worktrees: Vec<Binding>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub system: String,
    pub branch: Option<String>,
    pub head: Option<String>,
    pub dirty: bool,
    pub conflicts: bool,
    pub operation_in_progress: bool,
    pub locked: bool,
    pub prunable: bool,
    pub available: bool,
    pub tracking_ref: Option<String>,
    pub ahead: Option<u64>,
    pub behind: Option<u64>,
    pub tracked_changes: u64,
    pub untracked_changes: u64,
    pub remote_urls: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckoutTarget {
    ExistingBranch {
        branch: String,
    },
    NewBranch {
        branch: String,
        start_point: Option<String>,
    },
    Detached {
        revision: String,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Operation {
    Push {
        remote: Option<String>,
        reference: Option<String>,
    },
    Pull {
        remote: Option<String>,
        reference: Option<String>,
    },
    CheckoutCreate {
        name: String,
        target: CheckoutTarget,
    },
    CheckoutMove {
        name: String,
    },
    CheckoutRemove,
    CheckoutRepair {
        name: Option<String>,
    },
}
impl Operation {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Push { .. } => "push",
            Self::Pull { .. } => "pull",
            Self::CheckoutCreate { .. } => "checkout.create",
            Self::CheckoutMove { .. } => "checkout.move",
            Self::CheckoutRemove => "checkout.remove",
            Self::CheckoutRepair { .. } => "checkout.repair",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Review {
    pub operation: String,
    pub system: String,
    pub source: Option<PathBuf>,
    pub destination: Option<PathBuf>,
    pub branch: Option<String>,
    pub head: Option<String>,
    pub target: Option<String>,
    pub remote: Option<String>,
    pub warnings: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Prepared {
    binding: Binding,
    operation: Operation,
    review: Review,
    affected_paths: Vec<PathBuf>,
    expectation: Expectation,
    intent: Intent,
}
impl fmt::Debug for Prepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Prepared")
            .field("review", &self.review)
            .finish_non_exhaustive()
    }
}
impl Prepared {
    pub fn repository_key(&self) -> String {
        self.binding.repository_key()
    }
    pub fn review(&self) -> &Review {
        &self.review
    }
    pub fn affected_paths(&self) -> &[PathBuf] {
        &self.affected_paths
    }
    pub fn operation_kind(&self) -> &'static str {
        self.operation.kind()
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct Expectation {
    config: String,
    worktrees: String,
    head: Option<String>,
    branch: Option<String>,
    root: Option<Root>,
    tracking: Option<Tracking>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Tracking {
    reference: String,
    previous: Option<String>,
    url: String,
}
#[derive(Clone, Serialize, Deserialize)]
struct Root {
    configured: PathBuf,
    canonical: PathBuf,
    identity: Identity,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Intent {
    Push {
        url: String,
        reference: String,
        oid: String,
        remote_oid: Option<String>,
    },
    Pull {
        url: String,
        reference: String,
        oid: String,
    },
    Create {
        path: PathBuf,
        oid: String,
        branch: Option<String>,
        new_branch: bool,
    },
    Move {
        path: PathBuf,
    },
    Remove,
    Repair {
        path: PathBuf,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Outcome {
    pub state: String,
    pub binding: Option<Binding>,
    pub affected_paths: Vec<PathBuf>,
    pub refresh_pending: bool,
}

mod native;
mod operations;
pub fn canonical_remote(value: &str) -> Option<String> {
    native::public_remote(value)
}
/// Check pinned filesystem identities without running Git.
/// Resolving `.git` pointers and checking refs/configuration still require discovery or inspection.
pub fn validate_binding(binding: &Binding) -> Result<()> {
    native::check_identity(binding)
}
pub async fn discover(path: &std::path::Path) -> Result<Discovery> {
    tokio::time::timeout(std::time::Duration::from_secs(30), native::discover(path))
        .await
        .map_err(|_| Error::new("inspection_timed_out", false))?
}
pub async fn status(binding: &Binding) -> Result<Snapshot> {
    tokio::time::timeout(std::time::Duration::from_secs(30), native::status(binding))
        .await
        .map_err(|_| Error::new("inspection_timed_out", false))?
}
pub async fn prepare(
    binding: &Binding,
    operation: Operation,
    worktree_root: &std::path::Path,
) -> Result<Prepared> {
    tokio::time::timeout(
        std::time::Duration::from_secs(180),
        operations::prepare(binding, operation, worktree_root),
    )
    .await
    .map_err(|_| Error::new("preparation_timed_out", false))?
}
pub async fn execute(
    prepared: &Prepared,
    check: impl Fn(&[PathBuf]) -> Result<()> + Send + Sync,
) -> Result<Outcome> {
    let dispatched = std::sync::atomic::AtomicBool::new(false);
    let mut applied = tokio::time::timeout(
        std::time::Duration::from_secs(180),
        operations::execute(prepared, check, &dispatched),
    )
    .await
    .map_err(|_| {
        Error::new(
            "operation_timed_out",
            dispatched.load(std::sync::atomic::Ordering::Acquire),
        )
    })??;
    applied.outcome.refresh_pending = !operations::refresh_tracking(prepared).await;
    Ok(applied.outcome)
}
