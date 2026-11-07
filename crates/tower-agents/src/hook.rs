use crate::{
    events::{self, AgentEvent},
    runtime::RuntimeState,
};

pub trait HookSession: Send {
    fn start(&mut self) {}
    fn changed(&mut self) -> futures_util::future::BoxFuture<'_, ()> {
        Box::pin(std::future::pending())
    }
    fn apply(&mut self, state: &mut RuntimeState, event: AgentEvent, now: u64);
    fn refresh(&mut self, _state: &mut RuntimeState, _now: u64) -> bool {
        false
    }
}
pub struct Stateless;
impl HookSession for Stateless {
    fn apply(&mut self, state: &mut RuntimeState, event: AgentEvent, now: u64) {
        events::apply(state, event, now);
    }
}
