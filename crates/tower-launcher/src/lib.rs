//! The supervisor that runs one agent session, and the API a program uses
//! to run such sessions in its own terminal windows without a Tower daemon.

mod agent;
mod app_server;
mod injection;
mod launcher;
pub mod local;
mod session;
pub use launcher::{hook, project_environment, run};
