# Tower

<p align="center">
  <img src="assets/tower.svg" alt="Tower Icon" width="160" height="160">
</p>

<p align="center">
  <strong>The Headless Session Management & Observability Layer for AI Coding Agents</strong><br>
  Connect web applications, desktop UIs, and workflow engines to local coding agent harnesses and interactive TUIs.
</p>

---

## Overview

AI coding agents thrive in terminal environments (TUIs), but connecting them to modern web applications, dashboards, and orchestrators traditionally requires brittle terminal scraping, orphaned processes, and zero observability.

**Tower** bridges this gap. Built in pure Rust (edition 2024) with a custom zero-dependency terminal emulator and multiplexer, Tower serves as the headless control plane between your frontends and local agent harnesses. It provides real-time session streaming, structured interaction protocols, granular human-in-the-loop approvals, Git worktree isolation, and crash resilience.

```
┌────────────────────────────────────────────────────────┐
│             Web App / UI / Allgather Cloud             │
└───────────┬────────────────────────────────┬───────────┘
            │ JSON-RPC / SSE (Loopback HTTP)  │ Shared PTY Attach
            ▼                                ▼
┌────────────────────────┐      ┌────────────────────────┐
│      tower daemon      │◄────►│  tower-terminal-pool   │
│ (State, Rules, Events) │      │ (PTY Multiplex Engine) │
└───────────┬────────────┘      └───────────┬────────────┘
            │                               │
            ├───────────────┬───────────────┤
            ▼               ▼               ▼
     [ Claude Code ]   [  Codex  ]   [ Other TUIs ]
```

## Highlights

- **Universal Harness Support**: Managed launchers for Claude Code, Codex, Reasonix, Kimi Code, Grok Build, OpenCode, Pi, Antigravity, and omp.
- **Headless & Shared Attachment**: Run agents headless in the background, stream ANSI snapshots over JSON-RPC/HTTP to your webapp, or attach multiple concurrent operators with zero disruption (`Ctrl-Space`, `Ctrl-Q`).
- **Structured Runtimes & ACP Bridges**: Native ACP streaming for Claude Code and Antigravity, plus shared daemon app-server connectivity for Codex, capturing clean events, form requests, and permission prompts.
- **Real-Time Observability**: Loopback HTTP and SSE stream state deltas, tool activity, and instant `needs_attention` flags (waiting for input, approval required, or unexpected exit).
- **Enterprise Governance**: Granular message routing rules, private UDS tokens, and audited approval flows for sensitive tools.
- **Isolated Worktree Lifecycle**: Two-phase (prepare/execute) Git worktree operations prevent repository corruption, with forwarded SSH-agent support.
- **Zero-Downtime Daemon Upgrades**: Hot-upgrade the multiplexer (`tower-cli pool upgrade`) without terminating active sessions or dropping model conversations.
- **Fleet Sync**: Optional Allgather pairing for remote status projection and centralized policy enforcement.

## Architecture

Tower operates via three companion binaries installed on `PATH`:
- **`tower`**: Core background daemon, SQLite state store, governance rules, and JSON-RPC API server.
- **`tower-terminal-pool`**: Dedicated terminal multiplexer managing PTY lifecycles and in-memory scrollback.
- **`tower-cli`**: Command-line management client for session control, inspection, and administration.

## Quick Start

### Build & Install

Requirements: **Rust 1.98.1+** (edition 2024) and a C compiler, or the pinned Nix shell:

```sh
# Enter development shell (or: direnv allow)
nix develop

# Build workspace binaries
cargo build --workspace --all-targets --locked
```

### Persistent Service Setup

Register Tower with your system supervisor (systemd on Linux, launchd on macOS):

```sh
tower-cli daemon install

# Linux: allow background services to run without an active login session
loginctl enable-linger "$USER"
```

State is persisted in `$XDG_STATE_HOME/tower` (`~/.local/state/tower/tower.db`).

## Usage

### 1. Launching and Attaching to Sessions

Ensure target agent CLIs are available on `PATH`.

```sh
# Launch an agent session (attaches interactively by default)
tower-cli launch claude --cwd "$PWD"
tower-cli launch codex --cwd "$PWD"

# Launch headless (ideal for WebApp backends) or in native terminal mode
tower-cli launch claude --headless --cwd "$PWD"
tower-cli launch codex --mode native --cwd "$PWD"

# Attach to an existing session (detach with Ctrl-Space, then Ctrl-Q)
tower-cli attach SESSION_ID
```

### 2. Session Observability & Snapshots

Capture real-time UI states and monitor agent health:

```sh
# List active and unmanaged sessions
tower-cli ls

# Capture an ANSI terminal screen snapshot for web rendering
tower-cli snapshot SESSION_ID --json

# Query host health diagnostics (CPU, memory, disk availability)
tower-cli host metrics

# Clear resolved attention flags
tower-cli attention clear SESSION_ID

# Stream real-time agent lifecycle and transcript events
tower-cli events
```

### 3. Messaging & Approvals

Route messages and resolve permission boundaries safely:

```sh
# Inject prompts or messages into an active agent session
tower-cli send SESSION_ID "Investigate and fix the failing test suite."

# Inspect and resolve pending tool approvals
tower-cli approval status APPROVAL_ID
tower-cli approve APPROVAL_ID
tower-cli approve APPROVAL_ID --deny

# Add access-control rules
tower-cli rules add --from "harness:codex" --to "cwd:~/work" --action allow
```

### 4. Git Worktrees and VCS

Perform safe Git operations using reviewed execution plans:

```sh
# Register an existing repository or working copy
tower-cli checkout register "$PWD"

# Prepare and execute an isolated worktree creation plan
tower-cli vcs prepare CHECKOUT_ID checkout.create \
  --options '{"name":"feature","target":{"kind":"new_branch","branch":"feature"}}'
tower-cli vcs execute PLAN_ID

# Authenticate Git push/pull using a forwarded SSH agent
tower-cli vcs prepare CHECKOUT_ID push --auth ssh-agent
tower-cli vcs execute PLAN_ID
```

### 5. Remote Pairing with Allgather (Optional)

```sh
tower-cli login https://allgather.example
tower-cli rpc allgather.status
tower-cli logout
```

## Configuration

Customize settings in `~/.config/tower/config.toml`:

```toml
[terminal]
scrollback_lines = 3000
scrollback_megabytes = 8

[vcs]
worktree_root = "~/worktrees"

[messaging]
pair_per_minute = 6
outgoing_per_minute = 30

[http]
enabled = true
port = 7878

[[event_sinks]]
kind = "webhook"
target = "http://127.0.0.1:9000/hook"
topics = ["approvals", "sessions"]
```

## Verification

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

## Workspace Crates

- **`tower`**: Executables (`tower`, `tower-cli`, `tower-terminal-pool`).
- **`tower-server`**: Core daemon for registry, governance, approvals, and HTTP/SSE.
- **`tower-pool`** / **`tower-pool-proto`**: Terminal multiplexer and IPC protocol.
- **`tower-terminal`**: Terminal backend adapter and PTY attach client.
- **`tower-launcher`**: Per-session agent supervisor and the API for programs that embed it.
- **`tower-agents`**: Launchers, hooks, and adapters for nine harnesses.
- **`tower-runtime`**: Structured ACP and app-server runtimes and event journals.
- **`tower-vcs`**: Git repository and worktree lifecycle management.
- **`tower-store`**: SQLite state persistence, policy snapshots, and sync state.
- **`tower-pal`**: Platform abstractions for terminal, IPC, and processes.
- **`tower-clipboard`**: Image clipboard transfer broker.
- **`tower-protocol`**: Public local JSON-RPC contract and types.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

Harness integrations adapt portions of Captain Miao; see [LICENSE-CAPTAIN-MIAO](LICENSE-CAPTAIN-MIAO).
