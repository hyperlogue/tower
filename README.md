# tower

tower is a local control tower for agents: it hosts sessions, maintains a unified
registry, and sends messages through native harness channels.

The first local runtime is implemented. `tower` can start its daemon, register
external sessions, update and query their state, manage local rules, and issue or
revoke scoped tokens. SQLite preserves identities and metadata across restarts.
Owned terminals, hook installation, scanning, message delivery, and network
synchronization remain to be implemented. Local design documents, reviews, and
runtime notes live in `workspace/docs/`, which is excluded from version control.

## Development

The environment follows captain-miao's pinned Nix inputs and uses Rust **1.98.1**,
edition 2024. Linux uses Clang and mold; macOS uses Nix's default toolchain. The
flake provides x86_64/aarch64 Linux and Apple Silicon macOS environments. CI is
configured for Ubuntu and Apple Silicon macOS runners. CI checks the macOS
runner architecture. Intel macOS is outside the agreed platform contract.

```sh
# Enter the pinned development environment.
nix develop

# Alternatively, enable the shell's direnv hook and authorize this checkout.
direnv allow

cargo build --workspace --all-targets --locked
cargo run -p tower -- --help
```

The .envrc follows captain-miao's parent-environment convention, using direnv's
trust checks. An optional local .env stays out of version control. Nix's Git source
filter requires new source files to be added to Git before it sees them.

Outside Nix, rustup reads rust-toolchain.toml; a system C compiler is also needed.
The declared minimum Rust version matches the tested version. Earlier compilers
are not currently part of the support contract.

## Try the local registry

After `cargo build --workspace --locked`, run:

```sh
./target/debug/tower daemon status
./target/debug/tower register --harness demo --session-id demo --cwd "$PWD"
./target/debug/tower ls
./target/debug/tower rules ls
./target/debug/tower daemon stop
```

Registration prints a session ID and a private token. Set `TOWER_AGENT_ID` and
`TOWER_AGENT_TOKEN` to those values in the reporting process. It can then run
`tower status --state working --seq 1`, `tower heartbeat`, or `tower unregister`.
Increase the sequence for each status update. Without a PID, send a heartbeat
at least once per minute. The daemon marks the session lost after three minutes
without a report. Registered sessions have weak trust; the token authenticates
subsequent reports. `tower ls --all` also shows retained ended sessions.

Commands start the daemon when needed. `--no-start` disables this behavior.
`tower daemon run` runs in the foreground for a service manager. Automatic startup
uses an existing tower systemd or launchd job when present; otherwise it detaches.
Service installation is not implemented. The detached process can still belong
to its caller's service-manager cgroup, so SSH/cgroup survival is not yet verified.

Use `--json` for structured output. `tower rpc METHOD JSON` calls an implemented
method directly. Local runtime notes are in `workspace/docs/RUNTIME.md` when
available in the checkout.

## Checks

Run inside the development shell:

```sh
bash scripts/check-toolchain.sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --workspace --all-targets --locked
cargo test --workspace --locked
alejandra --check .
actionlint
shellcheck scripts/*.sh
bash -n .envrc
```

`nix flake check` also validates package builds, Clippy, and Rust formatting.
The Linux CI lint job runs this check. The development shell places Cargo output
in the repository's `target/` directory, including when entered from a subdirectory.
`nix build` produces tower and tower-pool; the towerd library will be hosted by
tower's daemon entry point. When updating the compiler, keep flake.lock,
rust-toolchain.toml, and the workspace rust-version aligned. CI checks that the
Fenix and rustup pins agree.

## Workspace

| Crate | Responsibility |
| --- | --- |
| `tower-protocol` | Public API and Allgather frames; serde-only, published after the initial contract is ready |
| `tower-pal` | Platform terminal, IPC, process, and lifecycle operations |
| `tower-pool-proto` | Internal pool control and attach protocol |
| `tower-pool` | Independent persistent terminal-session process |
| `tower-store` | Local persistence, enrollment, applied policy snapshots, and synchronization state |
| `tower-agents` | Launchers, hooks, and resume adapters for nine harnesses |
| `towerd` | Registry, sessions, permissions, messaging, events, synchronization |
| `tower` | CLI, daemon entry point, MCP, hooks |

Publication is disabled during bootstrap. The tower workspace package and binary
name do not imply ownership of the existing crates.io name. Project code uses
MIT OR Apache-2.0; extracted upstream code must retain its copyright and licenses.
