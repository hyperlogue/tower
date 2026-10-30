//! Human and program entry points. The daemon runtime is hosted by this binary.
use clap::{Args, Parser, Subcommand};
use std::process::ExitCode;
use tower_pal::Paths;
use tower_protocol::*;
use towerd::client::Client;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Parser)]
#[command(
    version,
    about = "Local agent registry and daemon",
    long_about = "Local agent registry and daemon. This build provides the UDS core, registration, local rules, and tokens. Owned terminals and message delivery are not available yet."
)]
struct Cli {
    #[arg(long, global = true)]
    json: bool,
    #[arg(long, global = true, help = "Do not start the daemon if it is absent")]
    no_start: bool,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    Ls(ListArgs),
    Get {
        id: String,
    },
    Register {
        #[arg(long)]
        harness: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        pid: Option<u32>,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        cwd: Option<String>,
    },
    Status {
        #[command(flatten)]
        credential: CredentialArgs,
        #[arg(long)]
        state: String,
        #[arg(long)]
        seq: u64,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, help = "JSON object with last_tool, model, or context_tokens")]
        detail: Option<String>,
    },
    Heartbeat(CredentialArgs),
    Unregister(CredentialArgs),
    Whoami,
    Rules {
        #[command(subcommand)]
        command: RulesCommand,
    },
    Token {
        #[command(subcommand)]
        command: TokenCommand,
    },
    Agents {
        #[command(subcommand)]
        command: AgentsCommand,
    },
    /// Invoke an implemented JSON-RPC method. Prints the result as JSON.
    Rpc {
        method: String,
        #[arg(default_value = "{}")]
        params: String,
    },
}
#[derive(Subcommand)]
enum DaemonCommand {
    /// Run the daemon in the foreground (for a service manager), or detach.
    Run {
        #[arg(long)]
        detach: bool,
    },
    Status,
    Stop,
}
#[derive(Args, Default)]
struct ListArgs {
    #[arg(long)]
    all: bool,
    #[arg(long)]
    harness: Option<String>,
    #[arg(long)]
    status: Option<String>,
    #[arg(long)]
    cwd_prefix: Option<String>,
    #[arg(long)]
    ownership: Option<String>,
}
#[derive(Args)]
struct CredentialArgs {
    #[arg(long, env = "TOWER_AGENT_ID")]
    id: String,
    #[arg(long, env = "TOWER_AGENT_TOKEN", hide_env_values = true)]
    token: String,
}
#[derive(Subcommand)]
enum AgentsCommand {
    Ls(ListArgs),
    Status { id: String },
}
#[derive(Subcommand)]
enum RulesCommand {
    Ls,
    Add {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long, default_value = "allow")]
        action: String,
        #[arg(long)]
        id: Option<String>,
    },
    Rm {
        id: String,
    },
}
#[derive(Subcommand)]
enum TokenCommand {
    Issue {
        name: String,
        #[arg(long, value_delimiter = ',', required = true)]
        scope: Vec<String>,
        #[arg(long)]
        expires: Option<String>,
    },
    Revoke {
        name: String,
    },
    Ls,
}
fn context() -> Result<CallerContext> {
    context_with(|key| std::env::var(key))
}
fn context_with(
    read: impl Fn(&str) -> std::result::Result<String, std::env::VarError>,
) -> Result<CallerContext> {
    let var = |key| match read(key) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{key} must contain valid UTF-8")),
    };
    // Read every marker, even one with lower precedence. Malformed identity
    // must never disappear into the human fallback.
    let agent_token = var("TOWER_AGENT_TOKEN")?.map(Secret);
    let bearer_token = var("TOWER_BEARER_TOKEN")?.map(Secret);
    let agent_id = var("TOWER_AGENT_ID")?;
    let claude = var("CLAUDE_CODE_SESSION_ID")?;
    let codex_thread = var("CODEX_THREAD_ID")?;
    let codex_session = var("CODEX_SESSION_ID")?;
    let (harness, native) = if let Some(n) = claude {
        (Some("claude-code".into()), Some(n))
    } else if let Some(n) = codex_thread.or(codex_session) {
        (Some("codex".into()), Some(n))
    } else {
        (None, None)
    };
    Ok(CallerContext {
        agent_token,
        bearer_token,
        agent_id,
        harness,
        native_session_id: native,
    })
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let json = cli.json;
    match execute(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if let Some(towerd::Error::Rpc(error)) = e.downcast_ref::<towerd::Error>() {
                if json {
                    println!("{}", serde_json::json!({"error":error}));
                } else {
                    eprintln!("tower: {error}");
                    if let Some(Value::Array(ids)) = error.data.details.get("candidates") {
                        for id in ids {
                            if let Value::String(id) = id {
                                eprintln!("  {}", visible(id));
                            }
                        }
                    }
                }
            } else {
                eprintln!("tower: {e}");
            }
            ExitCode::FAILURE
        }
    }
}
fn execute(cli: Cli) -> Result<()> {
    let paths = Paths::discover()?;
    if let Command::Daemon {
        command: DaemonCommand::Run { detach },
    } = &cli.command
    {
        if *detach {
            tower_pal::detach(&paths)?;
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(towerd::run(
            paths,
            if *detach {
                "detached; caller cgroup may apply"
            } else {
                "foreground"
            },
        ))?;
        return Ok(());
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(cli, paths))
}
async fn run(cli: Cli, paths: Paths) -> Result<()> {
    let stopping = matches!(
        cli.command,
        Command::Daemon {
            command: DaemonCommand::Stop
        }
    );
    let mut client = match Client::connect(&paths, context()?, !cli.no_start && !stopping).await {
        Ok(c) => c,
        Err(towerd::Error::Io(e))
            if stopping
                && matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
        {
            println!("not running");
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    match cli.command {
        Command::Daemon {
            command: DaemonCommand::Status,
        } => {
            let r = client.call("daemon.info", &serde_json::json!({})).await?;
            if cli.json {
                print_json(&r)?;
            } else {
                let info: DaemonInfo = serde_json::from_value(serde_json::to_value(r)?)?;
                println!("towerd {} · pid {} · {}", info.version, info.pid, info.mode);
                println!("node: {}", info.node_name);
                println!("startup: {}", info.startup);
                println!(
                    "pool: {}",
                    if info.pool.running {
                        "running"
                    } else {
                        "not running"
                    }
                );
            }
        }
        Command::Daemon {
            command: DaemonCommand::Stop,
        } => {
            print_json(
                &client
                    .call("daemon.shutdown", &serde_json::json!({}))
                    .await?,
            )?;
        }
        Command::Daemon {
            command: DaemonCommand::Run { .. },
        } => unreachable!(),
        Command::Ls(args)
        | Command::Agents {
            command: AgentsCommand::Ls(args),
        } => {
            let r = client
                .call(
                    "agent.list",
                    &ListParams {
                        filter: AgentFilter {
                            harness: args.harness,
                            status: args.status,
                            cwd_prefix: args.cwd_prefix,
                            ownership: args.ownership,
                            include_exited: args.all,
                        },
                    },
                )
                .await?;
            if cli.json {
                print_json(&r)?;
            } else {
                let records: Vec<AgentRecord> = serde_json::from_value(serde_json::to_value(r)?)?;
                println!("ID\tNAME\tHARNESS\tMODE\tSTATUS\tMESSAGE\tTRUST\tCWD\tATTACH");
                for r in records {
                    let message = if r.caps.message == "none" {
                        format!("none: {}", r.caps.message_reason.unwrap_or_default())
                    } else {
                        r.caps.message
                    };
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                        visible(&r.id),
                        visible(r.display_name.as_deref().unwrap_or("-")),
                        visible(&r.harness),
                        visible(r.mode.as_deref().unwrap_or("-")),
                        visible(&r.status),
                        visible(&message),
                        if r.trust == "weak" { "WEAK" } else { "strong" },
                        visible(&r.cwd),
                        r.attach_count
                    );
                }
            }
        }
        Command::Get { id }
        | Command::Agents {
            command: AgentsCommand::Status { id },
        } => print_json(&client.call("agent.get", &IdParams { id }).await?)?,
        Command::Register {
            harness,
            session_id,
            pid,
            name,
            cwd,
        } => {
            let cwd = cwd.unwrap_or(tower_pal::cwd()?.to_string_lossy().into_owned());
            let r = client
                .call(
                    "agent.register",
                    &RegisterParams {
                        harness,
                        cwd,
                        native_session_id: session_id,
                        pid,
                        display_name: name,
                    },
                )
                .await?;
            if cli.json {
                print_json(&r)?;
            } else {
                let r: RegisterResult = serde_json::from_value(serde_json::to_value(r)?)?;
                println!("TOWER_AGENT_ID={}\nTOWER_AGENT_TOKEN={}", r.id, r.token.0);
            }
        }
        Command::Status {
            credential,
            state,
            seq,
            name,
            detail,
        } => {
            let detail = match detail {
                Some(s) => Patch::Set(serde_json::from_str::<Value>(&s)?),
                None => Patch::Missing,
            };
            let p = UpdateParams {
                id: credential.id,
                token: Secret(credential.token),
                seq,
                status: Some(state),
                display_name: name.map_or(Patch::Missing, Patch::Set),
                detail,
            };
            print_json(&client.call("agent.update", &p).await?)?;
        }
        Command::Heartbeat(c) => print_json(
            &client
                .call(
                    "agent.heartbeat",
                    &AgentCredential {
                        id: c.id,
                        token: Secret(c.token),
                    },
                )
                .await?,
        )?,
        Command::Unregister(c) => print_json(
            &client
                .call(
                    "agent.unregister",
                    &AgentCredential {
                        id: c.id,
                        token: Secret(c.token),
                    },
                )
                .await?,
        )?,
        Command::Whoami => print_json(
            &client
                .call("identity.whoami", &serde_json::json!({}))
                .await?,
        )?,
        Command::Rules { command } => match command {
            RulesCommand::Ls => {
                print_json(&client.call("rules.list", &serde_json::json!({})).await?)?
            }
            RulesCommand::Add {
                from,
                to,
                action,
                id,
            } => print_json(
                &client
                    .call(
                        "rules.put",
                        &RulePutParams {
                            rule: Rule {
                                id: id.unwrap_or_default(),
                                from,
                                to,
                                action,
                                created_by: String::new(),
                                created_at: 0,
                            },
                        },
                    )
                    .await?,
            )?,
            RulesCommand::Rm { id } => {
                print_json(&client.call("rules.remove", &IdParams { id }).await?)?
            }
        },
        Command::Token { command } => match command {
            TokenCommand::Issue {
                name,
                scope,
                expires,
            } => {
                let r = client
                    .call(
                        "token.issue",
                        &TokenIssueParams {
                            name,
                            scope,
                            expires,
                            expires_at: None,
                        },
                    )
                    .await?;
                if cli.json {
                    print_json(&r)?;
                } else {
                    let token: TokenResult = serde_json::from_value(serde_json::to_value(r)?)?;
                    println!("{}", token.token.0);
                }
            }
            TokenCommand::Revoke { name } => {
                print_json(&client.call("token.revoke", &NameParams { name }).await?)?
            }
            TokenCommand::Ls => {
                print_json(&client.call("token.list", &serde_json::json!({})).await?)?
            }
        },
        Command::Rpc { method, params } => {
            let params: serde_json::Value = serde_json::from_str(&params)?;
            print_json(&client.call(&method, &params).await?)?;
        }
    }
    Ok(())
}
fn print_json(value: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
fn visible(s: &str) -> String {
    s.chars()
        .flat_map(|c| {
            if c.is_control() {
                c.escape_default().collect::<Vec<_>>()
            } else {
                vec![c]
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_identity_environment_never_disappears_into_human_context() {
        for bad in [
            "TOWER_AGENT_TOKEN",
            "TOWER_BEARER_TOKEN",
            "TOWER_AGENT_ID",
            "CLAUDE_CODE_SESSION_ID",
            "CODEX_THREAD_ID",
            "CODEX_SESSION_ID",
        ] {
            let error = context_with(|key| {
                if key == bad {
                    Err(std::env::VarError::NotUnicode(
                        "secret-must-not-appear".into(),
                    ))
                } else {
                    Err(std::env::VarError::NotPresent)
                }
            })
            .unwrap_err()
            .to_string();
            assert!(error.contains(bad));
            assert!(!error.contains("secret-must-not-appear"));
        }
        assert!(
            context_with(|_| Err(std::env::VarError::NotPresent))
                .unwrap()
                .agent_token
                .is_none()
        );
    }
}
