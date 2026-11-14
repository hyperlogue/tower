//! A minimal embedding program: it runs agents in its own terminal and tracks
//! them through `tower_launcher::local`, with no Tower daemon or pool.

use std::{io, process::ExitCode};
use tower_agents::runtime::{AgentSpec, Embedding};
use tower_launcher::local::{self, Local};

fn local() -> io::Result<Local> {
    Local::new(Embedding {
        helper: std::env::current_exe()?.to_string_lossy().into_owned(),
        state: std::env::var("EMBEDDER_STATE").map_err(io::Error::other)?,
        codex_endpoint: None,
    })
}
fn execute(args: &[String]) -> io::Result<i32> {
    if let Some(result) = local::helper(args) {
        return result.map(|()| 0);
    }
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["launch", id, harness, cwd, ref args @ ..] => {
            let plan = local()?.prepare(
                id,
                AgentSpec {
                    harness: tower_agents::Harness::parse(harness)
                        .ok_or_else(|| io::Error::other("unknown harness"))?,
                    cwd: cwd.into(),
                    args: args.iter().map(|a| (*a).into()).collect(),
                    mode: std::env::var("EMBEDDER_MODE").ok(),
                    resume: None,
                    fork: false,
                    native_home: None,
                },
            )?;
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(local::run(&plan))
        }
        ["list"] => {
            for session in local()?.sessions()? {
                println!(
                    "{} {} {}",
                    session.plan.id,
                    session.state.status,
                    if session.live { "live" } else { "ended" }
                );
            }
            Ok(0)
        }
        ["stop", id] => local()?.stop(id).map(|()| 0),
        _ => Err(io::Error::other(
            "usage: embedder launch <id> <harness> <cwd> [args] | list | stop <id>",
        )),
    }
}
fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match execute(&args) {
        Ok(code) => ExitCode::from(code.clamp(0, 255) as u8),
        Err(error) => {
            eprintln!("embedder: {error}");
            ExitCode::FAILURE
        }
    }
}
