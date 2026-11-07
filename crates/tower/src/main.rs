//! CLI, MCP, hook, and per-session launcher entry points.
use clap::{Args, Parser, Subcommand};
use std::process::ExitCode;
use tower_pal::Paths;
use tower_protocol::*;
use tower_server::client::Client;
mod attach;
mod clipboard;
mod hooks;
mod mcp;
mod vcs;
mod wrapper;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[derive(Parser)]
#[command(version, about = "Local agent sessions and registry")]
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
    #[command(name = "__session-mcp", hide = true)]
    SessionMcp {
        context: std::path::PathBuf,
    },
    #[command(name = "__agent", hide = true)]
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
    /// Receive a native agent lifecycle report. Always exits successfully.
    Hook {
        harness: String,
        event: String,
    },
    Run {
        harness: String,
        #[arg(last = true)]
        args: Vec<String>,
    },
    Launch {
        harness: String,
        #[arg(long)]
        checkout: Option<String>,
        #[arg(long)]
        protocol: Option<String>,
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long)]
        headless: bool,
        #[arg(long, help = "Reserve the session until an explicit attach")]
        defer: bool,
        #[arg(long, help = "TERM for the new terminal session")]
        term: Option<String>,
        #[arg(long, requires = "cols")]
        rows: Option<u16>,
        #[arg(long, requires = "rows")]
        cols: Option<u16>,
        #[arg(long)]
        resume: Option<String>,
        #[arg(long, requires = "resume")]
        fork: bool,
        #[arg(last = true)]
        args: Vec<String>,
    },
    Attach {
        id: String,
        #[arg(
            long,
            conflicts_with = "clipboard_stdin",
            help = "Private JSON file with clipboard client credentials"
        )]
        clipboard: Option<std::path::PathBuf>,
        #[arg(
            long,
            help = "Read one JSON clipboard credential or attach ticket from stdin"
        )]
        clipboard_stdin: bool,
    },
    /// Offer this computer's image clipboard, or inspect a session's source.
    Clipboard {
        #[command(subcommand)]
        command: clipboard::Command,
    },
    Kill {
        id: String,
        #[arg(long, help = "Confirm stopping an externally managed process")]
        yes: bool,
    },
    Restart {
        id: String,
        #[arg(last = true)]
        args: Option<Vec<String>>,
        #[arg(
            long,
            conflicts_with = "args",
            help = "Discard saved launch options when resuming"
        )]
        clear_args: bool,
    },
    Resume {
        id: String,
        #[arg(last = true)]
        args: Option<Vec<String>>,
        #[arg(
            long,
            conflicts_with = "args",
            help = "Discard saved launch options when resuming"
        )]
        clear_args: bool,
    },
    Snapshot {
        id: String,
    },
    /// Manage the terminal pool.
    Pool {
        #[command(subcommand)]
        command: PoolCommand,
    },
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    Ls(ListArgs),
    /// Read or atomically change a session's tags.
    Tags {
        id: String,
        #[arg(long, value_delimiter = ',')]
        add: Vec<String>,
        #[arg(long, value_delimiter = ',')]
        remove: Vec<String>,
    },
    Attention {
        #[command(subcommand)]
        command: AttentionCommand,
    },
    Host {
        #[command(subcommand)]
        command: HostCommand,
    },
    Get {
        id: String,
    },
    Register {
        #[arg(long)]
        protocol: Option<String>,
        #[arg(long)]
        harness: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        pid: Option<u32>,
        #[arg(
            long,
            help = "This process hosts several sessions; never signal it to stop one session"
        )]
        shared_process: bool,
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
    Login {
        #[arg(required_unless_present = "key_stdin")]
        url: Option<String>,
        #[arg(
            conflicts_with = "key_stdin",
            allow_hyphen_values = true,
            help = "Owner-created pairing code; prompts securely when omitted"
        )]
        pairing_code: Option<String>,
        #[arg(long, help = "Read a pre-issued machine access key from stdin")]
        key_stdin: bool,
        #[arg(
            long,
            conflicts_with = "key_stdin",
            help = "Suggested node display name"
        )]
        name: Option<String>,
        #[arg(
            long,
            conflicts_with = "key_stdin",
            help = "Request read-only machine access"
        )]
        read_only: bool,
    },
    Logout,

    Send {
        to: String,
        #[arg(required_unless_present = "stdin", conflicts_with = "stdin")]
        body: Option<String>,
        #[arg(long, help = "Read message text from stdin")]
        stdin: bool,
    },
    Approve {
        id: String,
        #[arg(long)]
        deny: bool,
        #[arg(long, help = "Permit only one resend of this exact message")]
        once: bool,
    },
    Approval {
        #[command(subcommand)]
        command: ApprovalCommand,
    },
    Mcp {
        #[command(subcommand)]
        command: Option<IntegrationCommand>,
    },
    Hooks {
        #[command(subcommand)]
        command: IntegrationCommand,
    },

    Events {
        #[arg(
            long,
            value_delimiter = ',',
            default_value = "agents,sessions,messages,approvals,sync"
        )]
        topics: Vec<String>,
    },
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
    /// Registered local VCS working copies.
    Checkout {
        #[command(subcommand)]
        command: vcs::CheckoutCommand,
    },
    /// Inspect or execute a reviewed version-control operation.
    Vcs {
        #[command(subcommand)]
        command: vcs::VcsCommand,
    },
    /// Show the cached repository catalog and its freshness.
    Catalog,
    /// Invoke an implemented JSON-RPC method. Prints the result as JSON.
    Rpc {
        method: String,
        #[arg(default_value = "{}")]
        params: String,
    },
}
#[derive(Subcommand)]
enum AttentionCommand {
    /// Clear the session's current needs-attention flag.
    Clear { id: String },
}
#[derive(Subcommand)]
enum HostCommand {
    /// Sample host CPU, memory, and free space on the home filesystem.
    Metrics,
}
#[derive(Subcommand)]
enum AgentCommand {
    Run { plan: std::path::PathBuf },
    Env,
}
#[derive(Subcommand)]
enum DaemonCommand {
    Install {
        #[arg(long, help = "Write service files without enabling them")]
        write_only: bool,
    },
    Uninstall {
        #[arg(
            long,
            help = "Remove service files without contacting the service manager"
        )]
        write_only: bool,
    },
    /// Start the server if needed and report its status.
    Start,
    Status,
    Stop,
}
#[derive(Subcommand)]
enum PoolCommand {
    /// Replace the pool with the installed version. Its sessions restart and
    /// resume their conversations.
    Upgrade {
        /// Wait until no terminal session is mid-turn.
        #[arg(long)]
        wait_idle: bool,
    },
}
#[derive(Args, Default)]
struct ListArgs {
    #[arg(long)]
    protocol: Option<String>,
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
    Send {
        to: String,
        body: String,
    },
    Conversations {
        #[arg(long)]
        harness: Option<String>,
        #[arg(long)]
        cwd_prefix: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long)]
        refresh: bool,
    },
    Supported,
    Resumable {
        #[arg(long)]
        harness: Option<String>,
    },
    Ls(ListArgs),
    Status {
        id: String,
    },
}
#[derive(Subcommand)]
enum IntegrationCommand {
    Install {
        harness: String,
        #[arg(long,default_value="user",value_parser=["user","project"])]
        scope: String,
    },
    Uninstall {
        harness: String,
        #[arg(long,default_value="user",value_parser=["user","project"])]
        scope: String,
    },
}
#[derive(Subcommand)]
enum ApprovalCommand {
    Ls,
    Status {
        id: String,
    },
    Watch {
        id: String,
        #[arg(long, default_value_t = 600)]
        timeout_s: u64,
    },
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
        /// `session` governs starting and controlling sessions instead of messages.
        #[arg(long, value_parser = ["message", "session"], default_value = "message")]
        kind: String,
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
    let program = std::env::args_os().next();
    if let Some(program) = program.as_ref().and_then(|p| p.to_str())
        && tower_clipboard::shim::invoked_as(program)
    {
        let Ok(args) = std::env::args_os()
            .skip(1)
            .map(|arg| arg.into_string())
            .collect::<std::result::Result<Vec<_>, _>>()
        else {
            return ExitCode::FAILURE;
        };
        return match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => match runtime.block_on(tower_clipboard::shim::run(program, args)) {
                Ok(0) => ExitCode::SUCCESS,
                _ => ExitCode::FAILURE,
            },
            Err(_) => ExitCode::FAILURE,
        };
    }
    let cli = Cli::parse();
    let json = cli.json;
    match execute(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            if let Some(tower_server::Error::Rpc(error)) = e.downcast_ref::<tower_server::Error>() {
                if json {
                    println!("{}", serde_json::json!({"error":error}));
                } else {
                    eprintln!("tower-cli: {error}");
                    if let Some(Value::Array(ids)) = error.data.details.get("candidates") {
                        for id in ids {
                            if let Value::String(id) = id {
                                eprintln!("  {}", visible(id));
                            }
                        }
                    }
                }
            } else {
                eprintln!("tower-cli: {e}");
            }
            ExitCode::FAILURE
        }
    }
}
fn execute(mut cli: Cli) -> Result<()> {
    if let Command::Login {
        pairing_code,
        key_stdin: false,
        ..
    } = &mut cli.command
        && pairing_code.is_none()
    {
        *pairing_code = Some(tower_pal::prompt_secret("Pairing code: ", 128)?);
    }
    if let Command::Hook { harness, event } = &cli.command {
        if std::env::var_os("TOWER_LAUNCHER_SOCKET").is_some() {
            tower_terminal::launcher::hook(harness, event);
        } else {
            let _ = hooks::report(harness, event);
        }
        return Ok(());
    }
    if let Command::Agent { command } = &cli.command {
        match command {
            AgentCommand::Env => println!("{}", serde_json::to_string(&tower_pal::environment())?),
            AgentCommand::Run { plan } => {
                let code = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?
                    .block_on(tower_terminal::launcher::run(plan))?;
                if code != 0 {
                    return Err(format!("agent exited with status {code}").into());
                }
            }
        }
        return Ok(());
    }
    if let Command::Hooks { command }
    | Command::Mcp {
        command: Some(command),
    } = &cli.command
    {
        let (harness, scope, install) = match command {
            IntegrationCommand::Install { harness, scope } => (harness, scope, true),
            IntegrationCommand::Uninstall { harness, scope } => (harness, scope, false),
        };
        let path = if matches!(cli.command, Command::Hooks { .. }) {
            tower_agents::integration::hooks(harness, scope == "project", install)?
        } else {
            tower_agents::integration::mcp(harness, scope == "project", install)?
        };
        println!("{}", path.display());
        if install && harness == "codex" && matches!(cli.command, Command::Hooks { .. }) {
            eprintln!(
                "Review and trust the Tower hooks in Codex before use. Project configuration also requires native project trust."
            );
        }
        return Ok(());
    }
    let paths = Paths::discover()?;
    if let Command::SessionMcp { context } = &cli.command {
        let credentials: tower_agents::codex_environment::SessionMcpContext =
            serde_json::from_slice(&tower_pal::read_private(context)?)?;
        let mut paths = paths;
        if !credentials.socket.is_absolute() {
            return Err("Invalid session control socket".into());
        }
        paths.socket = credentials.socket.clone();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(mcp::serve(paths, credentials.caller_context(), true))?;
        return Ok(());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run(cli, paths));
    runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    result
}
async fn run(cli: Cli, paths: Paths) -> Result<()> {
    if let Command::Login {
        pairing_code: Some(code),
        ..
    } = &cli.command
    {
        tower_server::allgather::validate_pairing_code(code)?;
    }
    if matches!(cli.command, Command::Mcp { command: None }) {
        return mcp::serve(paths, context()?, !cli.no_start).await;
    }

    let stopping = matches!(
        cli.command,
        Command::Daemon {
            command: DaemonCommand::Stop
        }
    );
    let mut client = match Client::connect(&paths, context()?, !cli.no_start && !stopping).await {
        Ok(c) => c,
        Err(tower_server::Error::Io(e))
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
        Command::Login {
            url,
            pairing_code,
            key_stdin,
            name,
            read_only,
        } => {
            if key_stdin {
                client
                    .call("daemon.service_access", &serde_json::json!({}))
                    .await?;
                let bytes = tokio::task::spawn_blocking(|| {
                    tower_pal::read_stdin_bounded(4096, std::time::Duration::from_secs(30))
                })
                .await??;
                let key = Secret(String::from_utf8(bytes)?.trim_end().into());
                let url = match url {
                    Some(url) => tower_server::allgather::enrollment_url(&url)?,
                    None => tower_server::allgather::configured_url(&paths)?,
                };
                print_json(
                    &client
                        .call("allgather.login", &serde_json::json!({"key":key,"url":url}))
                        .await?,
                )?;
            } else {
                let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(600);
                let params = serde_json::json!({"url":url,"pairing_code":pairing_code,"node_label":name,"read_only":read_only});
                let started = loop {
                    match client.call("allgather.enrollment.begin", &params).await {
                        Ok(result) => break serde_json::to_value(result)?,
                        Err(tower_server::Error::Rpc(e))
                            if matches!(
                                e.data.code.as_str(),
                                "enrollment_unavailable" | "rate_limited"
                            ) => {}
                        Err(tower_server::Error::Io(_)) => {
                            client = Client::connect(&paths, context()?, !cli.no_start).await?;
                        }
                        Err(error) => return Err(error.into()),
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(
                            "Pairing attempt expired; obtain a new code from the owner".into()
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                };
                let id = started["id"]
                    .as_str()
                    .ok_or("Enrollment did not return an identity")?
                    .to_owned();
                eprintln!(
                    "Pairing status: {}",
                    started["authorization_url"]
                        .as_str()
                        .ok_or("Pairing did not return an owner status URL")?
                );
                if cli.json {
                    print_json(&started)?;
                }
                let mut interval = started["interval"].as_u64().unwrap_or(5).clamp(1, 30);
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
                    if tokio::time::Instant::now() >= deadline {
                        return Err(
                            "Pairing attempt expired; obtain a new code from the owner".into()
                        );
                    }
                    let polled = match client
                        .call("allgather.enrollment.poll", &serde_json::json!({"id":id}))
                        .await
                    {
                        Ok(value) => serde_json::to_value(value)?,
                        Err(tower_server::Error::Rpc(e))
                            if matches!(
                                e.data.code.as_str(),
                                "enrollment_unavailable" | "rate_limited"
                            ) =>
                        {
                            continue;
                        }
                        Err(tower_server::Error::Io(_)) => {
                            client = Client::connect(&paths, context()?, !cli.no_start).await?;
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    };
                    interval = polled["interval"].as_u64().unwrap_or(interval).clamp(1, 30);
                    match polled["state"].as_str() {
                        Some("complete") => {
                            print_json(&polled)?;
                            break;
                        }
                        Some("pending" | "authorized") => {}
                        Some("denied") => return Err("Pairing was denied".into()),
                        Some("expired") => {
                            return Err(
                                "Pairing attempt expired; obtain a new code from the owner".into(),
                            );
                        }
                        _ => return Err("Unexpected pairing state".into()),
                    }
                }
            }
        }
        Command::Logout => print_json(
            &client
                .call("allgather.logout", &serde_json::json!({}))
                .await?,
        )?,
        Command::Mcp { .. } | Command::Hooks { .. } => unreachable!("handled before connecting"),
        Command::Agents {
            command: AgentsCommand::Send { to, body },
        } => print_json(
            &client
                .call(
                    "message.send",
                    &MessageSendParams {
                        to,
                        body,
                        ..Default::default()
                    },
                )
                .await?,
        )?,
        Command::Send { to, body, stdin } => {
            let body = if stdin {
                let bytes = tokio::task::spawn_blocking(|| {
                    tower_pal::read_stdin_bounded(32 * 1024, std::time::Duration::from_secs(30))
                })
                .await??;
                String::from_utf8(bytes)?
            } else {
                body.unwrap_or_default()
            };
            print_json(
                &client
                    .call(
                        "message.send",
                        &MessageSendParams {
                            to,
                            body,
                            ..Default::default()
                        },
                    )
                    .await?,
            )?;
        }
        Command::Approve { id, deny, once } => print_json(
            &client
                .call(
                    "approval.resolve",
                    &ApprovalResolveParams {
                        id,
                        allow: !deny,
                        remember: !once,
                    },
                )
                .await?,
        )?,
        Command::Approval { command } => match command {
            ApprovalCommand::Ls => {
                print_json(&client.call("approval.list", &serde_json::json!({})).await?)?
            }
            ApprovalCommand::Status { id } => {
                print_json(&client.call("approval.status", &IdParams { id }).await?)?
            }
            ApprovalCommand::Watch { id, timeout_s } => print_json(
                &client
                    .call("approval.watch", &ApprovalWatchParams { id, timeout_s })
                    .await?,
            )?,
        },
        Command::Run { harness, args } => wrapper::run(paths, &mut client, harness, args).await?,
        Command::Checkout { command } => vcs::checkout(&mut client, command).await?,
        Command::Vcs { command } => vcs::vcs(&mut client, command).await?,
        Command::Catalog => print_json(&client.call("catalog.get", &serde_json::json!({})).await?)?,
        Command::Launch {
            harness,
            checkout,
            protocol,
            cwd,
            headless,
            defer,
            term,
            rows,
            cols,
            resume,
            fork,
            args,
        } => {
            let terminal = (defer || term.is_some() || rows.is_some()).then(|| TerminalOptions {
                term,
                size: rows
                    .zip(cols)
                    .map(|(rows, cols)| TerminalSize { rows, cols }),
                defer,
            });
            let params = SessionOpenParams {
                terminal,
                checkout_id: checkout.clone(),
                harness,
                protocol,
                mode: None,
                headless,
                args,
                resume: resume.map(|native_session_id| ConversationSeed {
                    native_session_id,
                    fork,
                }),
                cwd: cwd.unwrap_or(if checkout.is_some() {
                    String::new()
                } else {
                    tower_pal::cwd()?.to_string_lossy().into_owned()
                }),
            };
            let r = client.call("session.open", &params).await?;
            let opened: SessionOpenResult = serde_json::from_value(serde_json::to_value(&r)?)?;
            if cli.json {
                print_json(&r)?;
            } else if !headless && !defer && tower_pal::is_terminal() {
                if opened.caps.is_some_and(|caps| caps.attach) {
                    attach::run(&mut client, &opened.id).await?;
                } else {
                    println!("{}", opened.id);
                }
            } else {
                println!("{}", opened.id);
            }
        }
        Command::Attach {
            id,
            clipboard,
            clipboard_stdin,
        } => attach::with_clipboard(&mut client, &id, clipboard, clipboard_stdin).await?,
        Command::Clipboard { command } => clipboard::run(command, &paths, &mut client).await?,
        Command::Kill { id, yes } => print_json(
            &client
                .call("session.kill", &serde_json::json!({"id":id,"confirm":yes}))
                .await?,
        )?,
        Command::Restart {
            id,
            args,
            clear_args,
        } => print_json(
            &client
                .call(
                    "session.restart",
                    &SessionRestartParams {
                        id,
                        mode: None,
                        args: if clear_args { Some(vec![]) } else { args },
                    },
                )
                .await?,
        )?,
        Command::Resume {
            id,
            args,
            clear_args,
        } => print_json(
            &client
                .call(
                    "session.resume",
                    &SessionRestartParams {
                        id,
                        mode: None,
                        args: if clear_args { Some(vec![]) } else { args },
                    },
                )
                .await?,
        )?,
        Command::Snapshot { id } => {
            let r = client.call("session.snapshot", &IdParams { id }).await?;
            if cli.json {
                print_json(&r)?;
            } else {
                let s: SnapshotResult = serde_json::from_value(serde_json::to_value(r)?)?;
                use std::io::Write;
                std::io::stdout().write_all(s.buffer.as_bytes())?;
                if s.truncated {
                    eprintln!("tower-cli: oldest terminal history was discarded");
                }
                if s.limited {
                    eprintln!("tower-cli: snapshot contains unsupported terminal sequences");
                }
            }
        }
        Command::Pool {
            command: PoolCommand::Upgrade { wait_idle },
        } => {
            let params = PoolUpgradeParams {
                require_idle: wait_idle,
            };
            let mut waiting = false;
            let r = loop {
                match client.call("pool.upgrade", &params).await {
                    Err(tower_server::Error::Rpc(e))
                        if wait_idle && e.data.code == "sessions_busy" =>
                    {
                        if !std::mem::replace(&mut waiting, true) {
                            eprintln!("tower-cli: waiting for terminal sessions to become idle");
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                    result => break result?,
                }
            };
            print_json(&r)?;
        }
        Command::Daemon {
            command: DaemonCommand::Status | DaemonCommand::Start,
        } => {
            let r = client.call("daemon.info", &serde_json::json!({})).await?;
            if cli.json {
                print_json(&r)?;
            } else {
                let info: DaemonInfo = serde_json::from_value(serde_json::to_value(r)?)?;
                println!("tower {} · pid {} · {}", info.version, info.pid, info.mode);
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
            command: DaemonCommand::Install { write_only },
        } => {
            client
                .call("daemon.service_access", &serde_json::json!({}))
                .await?;
            let installed = tower_pal::install_services(&paths, write_only)?;
            print_json(&serde_json::json!({"files":installed.files,"enabled":installed.enabled}))?;
            if !cli.json {
                if cfg!(target_os = "linux") {
                    eprintln!("For startup before login, run: loginctl enable-linger \"$USER\"");
                }
                if !write_only && !installed.enabled {
                    eprintln!(
                        "Service files were written. Enable them through your user service manager."
                    );
                }
            }
        }
        Command::Daemon {
            command: DaemonCommand::Uninstall { write_only },
        } => {
            client
                .call("daemon.service_access", &serde_json::json!({}))
                .await?;
            print_json(
                &serde_json::json!({"removed":tower_pal::uninstall_services(&paths,write_only)?}),
            )?;
        }
        Command::Daemon {
            command: DaemonCommand::Stop,
        } => {
            let result = client
                .call("daemon.shutdown", &serde_json::json!({}))
                .await?;
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    match tower_pal::SingletonLock::acquire(&paths.run.join("towerd.lock")) {
                        Ok(_lock) => return Ok::<_, std::io::Error>(()),
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        }
                        Err(e) => return Err(e),
                    }
                }
            })
            .await
            .map_err(|_| "daemon shutdown could not be confirmed")??;
            print_json(&result)?;
        }
        Command::Agent { .. } | Command::SessionMcp { .. } | Command::Hook { .. } => unreachable!(),
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
                            protocol: args.protocol,
                            include_exited: args.all,
                        },
                    },
                )
                .await?;
            if cli.json {
                print_json(&r)?;
            } else {
                let records: Vec<AgentRecord> = serde_json::from_value(serde_json::to_value(r)?)?;
                println!(
                    "ID\tNAME\tHARNESS\tPROTOCOL\tMODE\tSTATUS\tATTENTION\tTAGS\tMESSAGE\tTRUST\tCWD\tATTACH"
                );
                for r in records {
                    let message = if r.caps.message == "none" {
                        format!("none: {}", r.caps.message_reason.unwrap_or_default())
                    } else {
                        r.caps.message
                    };
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                        visible(&r.id),
                        visible(r.display_name.as_deref().unwrap_or("-")),
                        visible(&r.harness),
                        visible(r.protocol.as_deref().unwrap_or("-")),
                        visible(r.mode.as_deref().unwrap_or("-")),
                        visible(&r.status),
                        if r.needs_attention { "yes" } else { "-" },
                        serde_json::to_string(&r.tags)?,
                        visible(&message),
                        if r.trust == "weak" { "WEAK" } else { "strong" },
                        visible(&r.cwd),
                        r.pool_session
                            .as_ref()
                            .map_or_else(|| "-".into(), |_| r.attach_count.to_string())
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
            protocol,
            session_id,
            pid,
            shared_process,
            name,
            cwd,
        } => {
            let cwd = cwd.unwrap_or(tower_pal::cwd()?.to_string_lossy().into_owned());
            let r = client
                .call(
                    "agent.register",
                    &RegisterParams {
                        shared_process,
                        delivery: None,
                        harness,
                        protocol,
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
                delivery: Patch::Missing,
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
                kind,
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
                                kind: Some(if kind == "session" {
                                    RuleKind::Session
                                } else {
                                    RuleKind::Message
                                }),
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
        Command::Agents {
            command: AgentsCommand::Supported,
        } => print_json(&client.call("harness.list", &serde_json::json!({})).await?)?,
        Command::Agents {
            command: AgentsCommand::Resumable { harness },
        } => print_json(
            &client
                .call(
                    "session.list_resumable",
                    &serde_json::json!({"harness":harness}),
                )
                .await?,
        )?,
        Command::Events { topics } => {
            let snapshot = client
                .call(
                    "subscribe",
                    &SubscribeParams {
                        topics,
                        approval_id: None,
                    },
                )
                .await?;
            println!(
                "{}",
                serde_json::json!({"method":"snapshot","params":snapshot})
            );
            loop {
                println!("{}", serde_json::to_string(&client.next_event().await?)?);
            }
        }
        Command::Agents {
            command:
                AgentsCommand::Conversations {
                    harness,
                    cwd_prefix,
                    limit,
                    refresh,
                },
        } => {
            print_json(
                &client
                    .call(
                        "conversation.list",
                        &ConversationListParams {
                            harness,
                            cwd_prefix,
                            limit: Some(limit),
                            refresh,
                            protocol: None,
                        },
                    )
                    .await?,
            )?;
        }
        Command::Host {
            command: HostCommand::Metrics,
        } => {
            print_json(&client.call("host.metrics", &serde_json::json!({})).await?)?;
        }
        Command::Tags { id, add, remove } => {
            if add.is_empty() && remove.is_empty() {
                let record: AgentRecord = serde_json::from_value(serde_json::to_value(
                    client.call("agent.get", &IdParams { id }).await?,
                )?)?;
                print_json(&record.tags)?;
            } else {
                print_json(
                    &client
                        .call("session.tags.patch", &SessionTagsPatch { id, add, remove })
                        .await?,
                )?;
            }
        }
        Command::Attention {
            command: AttentionCommand::Clear { id },
        } => {
            print_json(
                &client
                    .call("session.attention.clear", &IdParams { id })
                    .await?,
            )?;
        }
        Command::Rpc { method, params } => {
            let params: serde_json::Value = serde_json::from_str(&params)?;
            print_json(&client.call(&method, &params).await?)?;
        }
    }
    Ok(())
}
fn print_json(value: &impl serde::Serialize) -> Result<()> {
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
    fn login_accepts_an_optional_code_and_keeps_manual_keys_separate() {
        for code in [
            "A".repeat(43),
            format!("-{}", "A".repeat(42)),
            format!("--{}", "A".repeat(41)),
        ] {
            let cli = Cli::try_parse_from([
                "tower-cli",
                "login",
                "https://allgather.example",
                &code,
                "--read-only",
            ])
            .unwrap();
            assert!(
                matches!(cli.command, Command::Login { pairing_code:Some(value), key_stdin:false, read_only:true, .. } if value == code)
            );
            let error = Cli::try_parse_from([
                "tower-cli",
                "login",
                "https://allgather.example",
                &code,
                "--key-stdin",
            ])
            .err()
            .unwrap();
            assert!(!error.to_string().contains(&code));
        }
        let cli = Cli::try_parse_from(["tower-cli", "login", "https://allgather.example"]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Login {
                pairing_code: None,
                key_stdin: false,
                ..
            }
        ));
        for args in [
            vec!["tower-cli", "login", "--key-stdin"],
            vec![
                "tower-cli",
                "login",
                "https://allgather.example",
                "--key-stdin",
            ],
            vec![
                "tower-cli",
                "login",
                "--key-stdin",
                "https://allgather.example",
            ],
        ] {
            let cli = Cli::try_parse_from(args).unwrap();
            assert!(matches!(
                cli.command,
                Command::Login {
                    key_stdin: true,
                    pairing_code: None,
                    ..
                }
            ));
        }
        let old = Cli::try_parse_from([
            "tower-cli",
            "login",
            "https://allgather.example",
            "--browser",
        ])
        .unwrap();
        let Command::Login {
            pairing_code: Some(code),
            ..
        } = old.command
        else {
            panic!()
        };
        assert!(tower_server::allgather::validate_pairing_code(&code).is_err());
    }
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
