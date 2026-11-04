use crate::{Result, print_json};
use clap::Subcommand;
use serde_json::{Value, json};
use towerd::client::Client;

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
    },
    Execute {
        plan: String,
        #[arg(long)]
        wait: bool,
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
        } => (
            "vcs.prepare",
            json!({"checkout_id":checkout,"operation":operation,"options":serde_json::from_str::<Value>(&options)?}),
            None,
        ),
        VcsCommand::Execute { plan, wait } => {
            ("vcs.execute", json!({"plan_id":plan}), wait.then_some(plan))
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
        let until = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
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
