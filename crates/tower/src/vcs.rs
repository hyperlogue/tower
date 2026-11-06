use crate::{Result, print_json};
use clap::{Subcommand, ValueEnum};
use serde_json::{Value, json};
use towerd::client::Client;

#[derive(Clone, Copy, ValueEnum)]
pub enum Auth {
    Host,
    SshAgent,
}
async fn authentication(client: &mut Client, auth: Auth) -> Result<Value> {
    if matches!(auth, Auth::Host) {
        return Ok(json!({"kind":"host"}));
    }
    let hello = serde_json::to_value(
        client
            .call(
                "hello",
                &json!({"protocol_version":tower_protocol::PROTOCOL_VERSION}),
            )
            .await?,
    )?;
    if !hello["capabilities"]
        .as_array()
        .is_some_and(|caps| caps.iter().any(|c| c == "vcs.ssh-agent.v1"))
    {
        return Err("The server does not support per-operation SSH agents".into());
    }
    let socket = std::env::var("SSH_AUTH_SOCK")
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or("SSH_AUTH_SOCK is unavailable; open a connection with agent forwarding")?;
    Ok(json!({"kind":"ssh_agent","socket":tower_protocol::Secret(socket)}))
}

#[derive(Subcommand)]
pub enum CheckoutCommand {
    List {
        #[arg(long)]
        repository: Option<String>,
    },
    Get {
        id: String,
    },
    Register {
        directory: String,
        #[arg(long)]
        repository: Option<String>,
        #[arg(
            long,
            help = "JSON options to prepare the first worktree, including for bare storage"
        )]
        create: Option<String>,
    },
    Bind {
        id: String,
        repository: String,
    },
    Unbind {
        id: String,
    },
    Forget {
        id: String,
    },
}
#[derive(Subcommand)]
pub enum VcsCommand {
    Status {
        checkout: String,
    },
    Prepare {
        checkout: String,
        #[arg(value_parser=["push","pull","checkout.create","checkout.move","checkout.remove","checkout.repair"])]
        operation: String,
        #[arg(long, default_value = "{}")]
        options: String,
        #[arg(long, value_enum, default_value = "host")]
        auth: Auth,
    },
    Execute {
        plan: String,
        #[arg(long)]
        wait: bool,
        #[arg(long, value_enum)]
        auth: Option<Auth>,
    },
    Operation {
        plan: String,
    },
    Wait {
        plan: String,
    },
    /// Release an uncertain operation reservation after inspecting native state.
    Release {
        plan: String,
        #[arg(long)]
        yes: bool,
    },
}
pub async fn checkout(client: &mut Client, command: CheckoutCommand) -> Result<()> {
    let (method, params) = match command {
        CheckoutCommand::List { repository } => {
            ("checkout.list", json!({"repository_id":repository}))
        }
        CheckoutCommand::Get { id } => ("checkout.get", json!({"checkout_id":id})),
        CheckoutCommand::Register {
            directory,
            repository,
            create,
        } => (
            "checkout.register",
            json!({"directory":directory,"repository_id":repository,"create":create.map(|c|serde_json::from_str::<Value>(&c)).transpose()?}),
        ),
        CheckoutCommand::Bind { id, repository } => (
            "checkout.bind",
            json!({"checkout_id":id,"repository_id":repository}),
        ),
        CheckoutCommand::Unbind { id } => (
            "checkout.bind",
            json!({"checkout_id":id,"repository_id":null}),
        ),
        CheckoutCommand::Forget { id } => ("checkout.forget", json!({"checkout_id":id})),
    };
    print_json(&client.call(method, &params).await?)
}
pub async fn vcs(client: &mut Client, command: VcsCommand) -> Result<()> {
    let (method, params, wait) = match command {
        VcsCommand::Status { checkout } => ("vcs.status", json!({"checkout_id":checkout}), None),
        VcsCommand::Prepare {
            checkout,
            operation,
            options,
            auth,
        } => (
            "vcs.prepare",
            json!({"checkout_id":checkout,"operation":operation,"options":serde_json::from_str::<Value>(&options)?,"auth":authentication(client, auth).await?}),
            None,
        ),
        VcsCommand::Execute { plan, wait, auth } => {
            let op = serde_json::to_value(
                client
                    .call("vcs.operation", &json!({"plan_id":plan}))
                    .await?,
            )?;
            let selected = auth.unwrap_or(if op["review"]["authentication"] == "ssh_agent" {
                Auth::SshAgent
            } else {
                Auth::Host
            });
            let mut params = json!({"plan_id":plan});
            if op["state"] == "prepared" {
                params["auth"] = authentication(client, selected).await?;
            }
            (
                "vcs.execute",
                params,
                (wait || matches!(selected, Auth::SshAgent)).then_some(plan),
            )
        }
        VcsCommand::Operation { plan } => ("vcs.operation", json!({"plan_id":plan}), None),
        VcsCommand::Release { plan, yes } => {
            ("vcs.release", json!({"plan_id":plan,"confirm":yes}), None)
        }
        VcsCommand::Wait { plan } => ("vcs.operation", json!({"plan_id":plan}), Some(plan)),
    };
    let mut result = serde_json::to_value(client.call(method, &params).await?)?;
    let waiting = wait.is_some();
    if let Some(plan) = wait {
        let until = tokio::time::Instant::now() + std::time::Duration::from_secs(210);
        while result["state"] == "running" && tokio::time::Instant::now() < until {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            result = serde_json::to_value(
                client
                    .call("vcs.operation", &json!({"plan_id":plan}))
                    .await?,
            )?;
        }
    }
    print_json(&result)?;
    if (waiting || method == "vcs.execute")
        && matches!(result["state"].as_str(), Some("failed" | "unknown"))
    {
        return Err("VCS operation did not complete successfully; inspect its receipt".into());
    }
    if waiting && result["state"] == "running" {
        return Err("VCS operation is still running; query its operation receipt".into());
    }
    Ok(())
}
