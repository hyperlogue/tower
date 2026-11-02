use crate::Result;
use tower_protocol::{AttachInfo, IdParams};
use towerd::client::Client;

pub async fn run(client: &mut Client, id: &str) -> Result<()> {
    let result = client
        .call("session.attach_info", &IdParams { id: id.into() })
        .await?;
    let info: AttachInfo = serde_json::from_value(serde_json::to_value(result)?)?;
    tower_terminal::attach(info).await
}
