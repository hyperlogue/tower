use crate::Journal;
use std::sync::Arc;
use tokio::sync::mpsc;
use tower_protocol::ExecutionCommand;

pub(crate) enum Control {
    Command(ExecutionCommand),
    Stop(String),
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Start {
    Launch,
    Continue,
}
pub(crate) struct Handle {
    pub normal: mpsc::Sender<ExecutionCommand>,
    pub control: mpsc::Sender<Control>,
}
pub(crate) fn channels() -> (
    Arc<Handle>,
    mpsc::Receiver<ExecutionCommand>,
    mpsc::Receiver<Control>,
) {
    let (normal, n) = mpsc::channel(16);
    let (control, c) = mpsc::channel(16);
    (Arc::new(Handle { normal, control }), n, c)
}
#[derive(Clone)]
pub(crate) struct Context {
    pub paths: tower_pal::Paths,
    pub journal: Arc<Journal>,
    pub authority: crate::runtime::Authority,
}
