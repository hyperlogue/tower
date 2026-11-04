use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogProject {
    pub id: String,
    pub name: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogRepository {
    pub id: String,
    pub name: String,
    pub project_ids: Vec<String>,
    pub upstream_url: Option<String>,
    #[serde(default)]
    pub url_aliases: Vec<String>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryCatalog {
    pub revision: u64,
    pub projects: Vec<CatalogProject>,
    pub repositories: Vec<CatalogRepository>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogChanges<T> {
    pub upserts: Vec<T>,
    pub removed: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogDelta {
    pub base_revision: u64,
    pub revision: u64,
    pub projects: CatalogChanges<CatalogProject>,
    pub repositories: CatalogChanges<CatalogRepository>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkout {
    pub id: String,
    pub repository_id: Option<String>,
    pub path: String,
    pub system: String,
    pub branch: Option<String>,
    pub head: Option<String>,
    pub dirty: bool,
    pub conflicts: bool,
    pub locked: bool,
    pub prunable: bool,
    pub available: bool,
    pub tracking_ref: Option<String>,
    pub ahead: Option<u64>,
    pub behind: Option<u64>,
    pub observed_at: u64,
    pub revision: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckoutSnapshot {
    pub seq: u64,
    pub full: bool,
    pub records: Vec<Checkout>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckoutDelta {
    pub seq: u64,
    pub base_seq: u64,
    pub upserts: Vec<Checkout>,
    pub removed: Vec<String>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CheckoutFilter {
    pub repository_id: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckoutRegister {
    pub directory: String,
    #[serde(default)]
    pub create: Option<crate::Value>,
    pub repository_id: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckoutParams {
    pub checkout_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckoutBind {
    pub checkout_id: String,
    pub repository_id: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VcsPrepare {
    pub checkout_id: String,
    pub operation: String,
    #[serde(default)]
    pub options: crate::Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VcsPlanParams {
    pub plan_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VcsOperation {
    pub plan_id: String,
    pub checkout_id: Option<String>,
    pub operation: String,
    pub state: String,
    pub created_at: u64,
    pub expires_at: u64,
    pub review: crate::Value,
    #[serde(default)]
    pub released: bool,
    pub result: Option<crate::Value>,
    pub error: Option<crate::RpcError>,
}
