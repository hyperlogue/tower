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
    #[command(name = "__acp", hide = true)]
    Acp { harness: String },
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
            let startup = if detach {
                "detached; caller cgroup may apply"
            } else {
                "foreground"
            };
            runtime()?.block_on(towerd::run(paths.clone(), startup, |database| async move {
                let terminal = Arc::new(tower_terminal::TerminalBackend::new(paths.clone()));
                let execution =
                    tower_runtime::Runtime::open(paths, database, terminal.clone()).await?;
                Ok(tower_sessions::Backends::new(
                    "terminal",
                    vec![
                        Arc::new(tower_runtime::TerminalBackend::new(
                            terminal,
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
                .with_delivery(Arc::new(tower_agents::delivery::NativeDelivery)))
            }))?;
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
