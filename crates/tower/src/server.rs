use clap::{Parser, Subcommand};
use std::{process::ExitCode, sync::Arc};
use tower_pal::Paths;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser)]
#[command(version, about = "Tower local agent server")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}
#[derive(Subcommand)]
enum Command {
    /// Run the server in the foreground, or detach from the terminal.
    Run {
        #[arg(long)]
        detach: bool,
    },
    #[command(name = "__runtime", hide = true)]
    Runtime {
        #[command(subcommand)]
        command: OwnerCommand,
    },
    #[command(name = "__codex", hide = true)]
    Codex {
        #[command(subcommand)]
        command: OwnerCommand,
    },
    #[command(name = "__acp", hide = true)]
    Acp { harness: String },
}
#[derive(Subcommand)]
enum OwnerCommand {
    Run {
        #[arg(long)]
        detach: bool,
    },
    Stop,
}
fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
}
fn execute(command: Command) -> Result<()> {
    let paths = Paths::discover()?;
    match command {
        Command::Run { detach } => {
            if detach {
                tower_pal::detach(&paths)?;
            }
            let runtime = runtime()?;
            let execution = Arc::new(tower_runtime::Client::new(paths.clone()));
            let backends = tower_sessions::Backends::new(
                "terminal",
                vec![
                    Arc::new(tower_runtime::TerminalBackend::new(
                        Arc::new(tower_terminal::TerminalBackend::new(paths.clone())),
                        execution.clone(),
                    )),
                    Arc::new(tower_runtime::Backend::new(
                        execution.clone(),
                        "codex-app-server",
                    )),
                    Arc::new(tower_runtime::Backend::new(execution.clone(), "acp")),
                ],
            )?
            .with_execution(execution)
            .with_delivery(Arc::new(tower_agents::delivery::NativeDelivery));
            runtime.block_on(towerd::run(
                paths,
                if detach {
                    "detached; caller cgroup may apply"
                } else {
                    "foreground"
                },
                backends,
            ))?;
        }
        Command::Runtime {
            command: OwnerCommand::Run { detach },
        } => {
            if detach {
                tower_pal::detach_runtime(&paths)?;
            }
            let terminal = Arc::new(tower_terminal::TerminalBackend::new(paths.clone()));
            runtime()?.block_on(tower_runtime::run(paths, Some(terminal)))?;
        }
        Command::Runtime {
            command: OwnerCommand::Stop,
        } => tower_runtime::Client::new(paths).shutdown()?,
        Command::Codex { command } => {
            if matches!(command, OwnerCommand::Run { detach: true }) {
                tower_pal::detach_codex_host(&paths)?;
            }
            let runtime = runtime()?;
            match command {
                OwnerCommand::Run { .. } => {
                    runtime.block_on(tower_agents::codex_host::run(paths))?
                }
                OwnerCommand::Stop => runtime.block_on(tower_agents::codex_host::stop(&paths))?,
            }
        }
        Command::Acp { harness } => runtime()?.block_on(tower_runtime::bridge::run(&harness))?,
    }
    Ok(())
}
fn main() -> ExitCode {
    match execute(
        Cli::parse()
            .command
            .unwrap_or(Command::Run { detach: false }),
    ) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tower: {error}");
            ExitCode::FAILURE
        }
    }
}
