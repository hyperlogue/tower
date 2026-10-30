//! Independent host for tower's persistent terminal sessions.

fn main() -> std::process::ExitCode {
    match std::env::args().nth(1).as_deref() {
        Some("--version" | "-V") => {
            println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
            std::process::ExitCode::SUCCESS
        }
        Some("--help" | "-h") => {
            println!(
                "Independent host for tower's persistent terminal sessions.\n\nBootstrap only; runtime commands are not implemented yet.\n\nOptions:\n  -h, --help       Show help\n  -V, --version    Show version"
            );
            std::process::ExitCode::SUCCESS
        }
        _ => {
            eprintln!("tower-pool: runtime commands are not implemented yet; use --help");
            std::process::ExitCode::FAILURE
        }
    }
}
