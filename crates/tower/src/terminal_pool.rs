use clap::{Parser, Subcommand};
use std::process::ExitCode;
use tower_pal::Paths;

#[derive(Parser)]
#[command(version, about = "Tower terminal pool")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}
#[derive(Subcommand)]
enum Command {
    /// Run the pool in the foreground, or detach from the terminal.
    Run {
        #[arg(long)]
        detach: bool,
    },
    /// Stop the pool and all its terminal sessions.
    Stop,
}
fn execute(command: Command) -> std::io::Result<()> {
    let paths = Paths::discover()?;
    match command {
        Command::Run { detach } => {
            if detach {
                tower_pal::detach_pool(&paths)?;
            }
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(tower_pool::run(paths))
        }
        Command::Stop => {
            let mut stream = tower_pal::connect_blocking(&paths.pool_socket())?;
            tower_pool_proto::hello_sync(&mut stream)?;
            tower_pool_proto::write_sync(&mut stream, &tower_pool_proto::Request::Shutdown)?;
            tower_pool_proto::read_sync(&mut stream)?
                .json::<tower_pool_proto::Reply>()?
                .checked()?;
            Ok(())
        }
    }
}
fn main() -> ExitCode {
    match execute(
        Cli::parse()
            .command
            .unwrap_or(Command::Run { detach: false }),
    ) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("tower-terminal-pool: {error}");
            ExitCode::FAILURE
        }
    }
}
