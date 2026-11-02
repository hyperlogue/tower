use crate::Result;
use tower_protocol::{AgentRecord, Capabilities, DeliveryEndpoint, DeliveryReceipt};

pub trait RegisteredDelivery: Send + Sync {
    fn validate(&self, _endpoint: &DeliveryEndpoint) -> Result<()> {
        Ok(())
    }
    fn capabilities(&self, agent: &AgentRecord, endpoint: &DeliveryEndpoint) -> Capabilities;
    fn send(
        &self,
        agent: &AgentRecord,
        endpoint: &DeliveryEndpoint,
        text: &str,
    ) -> Result<DeliveryReceipt>;
}
