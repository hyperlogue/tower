# tower

Tower manages and monitors agent sessions on one development server. It provides
shared terminal attachment, a local registry, scoped tokens, and live status events.
The pool and terminal parser are implemented in Rust, without libshpool or libghostty.

Managed launchers support Claude Code, Codex, Reasonix, Kimi Code, Grok Build,
opencode, Pi, Antigravity, and omp. Codex supports native and app-server modes.
The public interface is Tower's JSON-RPC API; Miao clients need a Tower adapter.
Native Claude/Codex messages, approvals, process discovery, MCP, loopback HTTP/SSE,
and Allgather status/policy synchronization are available. Allgather supports browser enrollment, incremental state and policy synchronization,
and managed sessions for Codex, Claude Code, and Antigravity. Clipboard sharing
remains separate integration work.

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
cargo run --bin tower -- --help
```

The .envrc follows captain-miao's parent-environment convention, using direnv's
trust checks. An optional local .env stays out of version control. Nix's Git source
filter requires new source files to be added to Git before it sees them.

Outside Nix, rustup reads rust-toolchain.toml; a system C compiler is also needed.
The declared minimum Rust version matches the tested version. Earlier compilers
are not currently part of the support contract.

## Session ownership and protocol

Managed sessions have lifecycles controlled by tower. Unmanaged sessions are
registered and observed. The wire values remain `owned` and `registered`.
The separate `protocol` field identifies terminal, ACP, or another interaction
protocol. Management alone does not imply a PID, terminal attachment, or snapshots.

Backends supply session capabilities. The daemon uses those capabilities and the
selected backend for lifecycle operations. The terminal adapter owns pool startup,
PTY observations, attachment, and snapshots. The daemon can run without terminal
support; registry operations do not start the pool.

The default backend is `terminal`. Structured runtimes use `codex-app-server`
for Codex and `acp` for the Claude Code and Antigravity bridges. They have no
terminal attachment or snapshot. Unknown protocols return `unsupported`.
Unmanaged sessions can also carry protocol metadata:

```sh
./target/debug/tower register --harness external-agent --protocol acp --cwd "$PWD"
./target/debug/tower ls --protocol acp
```

## Managed terminal sessions

Install or copy the single `tower` executable. It starts separate daemon and pool
processes from the same executable. Install the desired agent CLIs on `PATH`:

```sh
./target/debug/tower agents supported
./target/debug/tower open claude --cwd "$PWD"
./target/debug/tower open codex --mode app-server --cwd "$PWD"
./target/debug/tower attach SESSION_ID
./target/debug/tower snapshot SESSION_ID --json
./target/debug/tower restart SESSION_ID
./target/debug/tower kill SESSION_ID
./target/debug/tower agents resumable
./target/debug/tower agents conversations --harness claude
./target/debug/tower open claude --resume NATIVE_SESSION_ID
./target/debug/tower open codex --resume NATIVE_SESSION_ID --fork
./target/debug/tower resume SESSION_ID
./target/debug/tower events
```

Interactive `open` attaches immediately. Use `--headless` to start at 80 columns
and 24 rows without attaching. Without a terminal, `open` reserves the session
until its first attach. Use `--` before arguments intended for the agent. Aliases include `claude`,
`kimi-code`, `grok-build`, `agy`, and `oh-my-pi`.

Press Ctrl-Space, then Ctrl-Q to detach. Kitty keyboard encodings are supported.
Several clients can attach and type; the PTY uses the smallest row and column
counts. A slow reader is disconnected when its output queue fills. Detach and
`tower daemon stop` leave the agent running. For manual cleanup, the internal
`tower __pool stop` command stops the pool and its sessions. Service managers can
run the pool separately with `tower __pool run`. These internal commands are
hidden from normal help.

Snapshots contain reconstructed ANSI text, with a `truncated` flag for discarded
history and a `limited` flag for unsupported terminal behavior. The current bounds
are 10,000 history lines and 16 MiB of retained cells per session. The pool accepts
up to 64 sessions, 16 clients per session, and terminal sizes up to 512 columns by
256 rows. Stopped terminal buffers can be evicted when the pool fills; SQLite
retains ended-session metadata for 30 days by default. Raw OSC 52 clipboard access
is blocked pending the clipboard policy interface.

Each launcher reports native session identity and status through a private socket.
Reports continue while the daemon is stopped. Status stays `unknown` until the
harness supplies evidence. Hook reports retain operational metadata, not prompt text.
Projects with an `.envrc` require direnv approval before their agents start.

Codex defaults to `app-server`. Each managed session has its own app-server process,
with a local relay to its TUI. The server inherits that session's approved project
environment. Stopping one session cannot stop another session's server. Native mode
uses `--no-daemon`. To change modes while preserving the conversation, use
`tower restart SESSION_ID --mode native` or `--mode app-server`.

```toml
[codex]
default_mode = "app-server"
[[codex.override]]
cwd_prefix = "~/work/project"
mode = "native"

[history]
owned_retention_days = 30
```

Put these settings in `~/.config/tower/config.toml` (or `$XDG_CONFIG_HOME/tower/config.toml`).
The longest matching directory prefix selects a Codex override. An explicit
`--mode` takes precedence.

Restart and resume keep the Tower ID. They require a native conversation ID.
Recovery after pool or host loss resumes previously active conversations; it
reports failure instead of starting a different conversation when the native ID
is missing. Kill disables automatic recovery. Manual resume information remains
until the retention period expires. Resume preserves supported runtime options
and does not replay initial prompts, fork requests, or worktree creation.
Restrictive tool and permission options are preserved. Unrecognized options stop
resume before the running agent is killed. Supply replacement options with
`restart SESSION_ID -- OPTIONS` or explicitly discard them with `--clear-args`.
The same options apply to `resume`.

`agents conversations` lists native histories, including conversations created
outside Tower. It does not register or start them. Use `--harness`, `--cwd-prefix`,
`--limit`, and `--refresh` to select results. Reads are bounded and cached briefly;
`truncated` and `errors` report incomplete inventories. Only metadata is returned.
Resume uses the conversation's recorded directory. An existing Tower identity is
returned in an `already_managed` error instead of creating a duplicate. A changed
native storage location after direnv loading prevents launch. Kimi and Antigravity
do not advertise fork support.

Tower preserves native history and credentials. Generated overlays are private.
Codex uses a Tower-owned profile in its native home. Grok uses a Tower-owned hook
file in its native home; the hooks are inactive outside managed sessions.

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
Use `--shared-process` when several registered sessions share a process. Tower
will not infer a sender from that PID or signal the shared process to kill one session.

Commands start the daemon when needed. `--no-start` disables this behavior.
`tower daemon run` runs in the foreground for a service manager. Automatic startup
uses an existing tower systemd or launchd job when present; otherwise it detaches.
Install independent daemon and pool jobs for persistent server operation:

```sh
tower daemon install
# Linux: enable startup without an interactive login.
loginctl enable-linger "$USER"
```

Installation enables future user-session startup without stopping current agents.
On macOS, launchd jobs start at login. Use `--write-only` to generate service files
for manual setup. `tower daemon uninstall` removes future startup configuration;
it does not stop current sessions. Detached fallback processes can still belong
to their caller's service-manager cgroup. Use the installed jobs on an SSH server.

Use `--json` for structured output. `tower rpc METHOD JSON` calls an implemented
method directly. Local runtime notes are in `workspace/docs/RUNTIME.md` when
available in the checkout.

## Messages and external sessions

```sh
tower send SESSION_ID 'Review the failing test.'
printf '%s' 'Review the failing test.' | tower send SESSION_ID --stdin
tower approve APPROVAL_ID
tower approve APPROVAL_ID --once
tower approve APPROVAL_ID --deny
tower approval status APPROVAL_ID
tower approval watch APPROVAL_ID --timeout-s 600
tower rules add --from 'harness:codex' --to 'cwd:~/work/project' --action allow
tower hooks install claude
tower hooks install codex --scope project
tower mcp install claude
tower mcp install codex
tower run claude -- --model sonnet
tower kill REGISTERED_ID --yes
```

Agents require approval for a new messaging relationship unless an allow rule
matches. After approval, resend the message. A remembered approval creates a
directional local allow rule; `--once` permits one resend of the same text.
Local approval cannot remove an Allgather requirement. Rules support `*` and `?`;
these match characters, including `/`. A `cwd:` selector without wildcards matches
that directory and its descendants. Wildcards do not grant administrative scopes.

Claude uses its authenticated inbox. Its receipt says `acknowledgement: transport`:
the socket write completed, but Claude provides no acceptance acknowledgement.
Claude's inbound policy can hold or refuse a message; Tower does not bypass it.
Codex uses its queue or the app-server channel. Managed app-server messages pass
through the TUI relay. Unknown submission outcomes are returned explicitly and
must not be retried automatically. PTY fallback is enabled for managed Claude sessions and can be enabled for other
harnesses with `[inject.HARNESS] pty = true`. It requires an idle supervisor check,
bracketed-paste support, and no pending or recent client input. A receipt says
`acknowledgement: pty_submission`; it does not assert harness acceptance. Native
harness state is observed asynchronously. Codex PTY fallback stays disabled because
its composer state cannot yet be established. Message bodies are limited to 32 KiB and are not
stored in Tower's database or logs. Native harness histories remain harness-owned.

Default rolling-minute limits are 6 messages per agent pair, 30 per human pair,
30 outgoing per sender, and 30 incoming per receiver. Pair limits count both
directions. Identical accepted text is suppressed for 10 seconds. Configure these
in `~/.config/tower/config.toml`:

```toml
[messaging]
pair_per_minute = 6
human_pair_per_minute = 30
outgoing_per_minute = 30
incoming_per_minute = 30

[scan]
enabled = true
interval_s = 10
```

The scanner skips managed-process ownership markers. Scan-only records cannot
receive messages until registration establishes a delivery channel. Installed
hooks report external Claude/Codex state. Codex requires native hook trust review.
`tower run` uses the current terminal and registers the process without starting a
pool. The wrapper uses Codex native mode; use `open` for managed app-server mode.

`tower mcp` serves the seven agent tools over stdio. Managed Claude/Codex sessions
receive its configuration automatically. External installations can be removed
with `tower mcp uninstall HARNESS` and `tower hooks uninstall HARNESS`.

## HTTP and event sinks

```toml
[http]
enabled = true
port = 7878
# Optional exact browser origins. Same-origin requests are accepted by default.
allowed_origins = []

[[event_sinks]]
kind = "webhook"
target = "http://127.0.0.1:9000/hook"
topics = ["approvals", "sessions"]
# token_env = "MY_WEBHOOK_TOKEN"

# Commands receive one metadata event as JSON on stdin; no shell is added.
# [[event_sinks]]
# kind = "command"
# target = "/path/to/event-handler"
# args = []
# topics = ["sessions"]
```

Restart the daemon after changing configuration. HTTP binds only to loopback.
Every `POST /rpc` and `GET /events` request requires an issued bearer token. Host
and Origin are checked. SSE authorization is rechecked while connected; revoking
the token closes the stream. Subscriptions support `agents`, `sessions`, `messages`,
`approvals`, and `sync`; reconnect and query current state after a dropped stream.
Event sinks exclude approval previews. They are transient, bounded, and do not
retry delivery. Webhooks require HTTPS except on loopback.

## Allgather

Start enrollment from the machine that will run the agents:

```sh
tower login https://allgather.example
```

Open the printed URL, authorize the machine, and optionally set its name. The CLI
waits for approval and saves the machine credential. This also works over SSH;
the browser does not need to run on the agent machine. Use `--name NAME` to suggest
a name or `--read-only` to request only the read scope.

```sh
tower rpc allgather.status
tower logout
```

For automated provisioning, an Allgather owner can issue a machine key with
`POST /api/tower/credentials`. Supply it with `tower login https://allgather.example
--key-stdin`. Browser-account and CLI OAuth tokens are separate credentials.

Explicit configuration can restrict the scopes requested during enrollment:

```toml
[allgather]
scopes = ["read", "send", "acp:control"]
```

Credentials are saved with mode 0600. Tower owns session state and local rules.
Allgather owns its policy table. Disconnecting or revoking the server-side key
retains Allgather mode and cached policy until local logout or re-enrollment.
Missing initial policy denies new session starts and messages. An accepted empty
policy removes only remote restrictions. Authorized local rule/token administration
remains available offline. Changed restart arguments require reconnection.

The baseline accepts remote health checks and messages, with configured scopes
and applicable policy. Remote policy changes use full snapshots or negotiated deltas. The connection
cannot administer Tower-local rules, tokens, or approvals. Managed execution uses
its own capability-negotiated interface and requires explicit `acp:control` consent;
an old generic `admin` scope does not grant it. New work requires a current control
connection and applied policy. Running turns may finish while disconnected.

Remote messages still require local approval or an allow rule. For example, an
administrator can permit Allgather messages to Codex sessions with:

```sh
tower rules add --from allgather --to 'harness:codex' --action allow
```

Session synchronization sends changed fields and removals when both peers support
deltas. Reconnect and explicit resync use full snapshots. If a delta is larger
than a full snapshot, Tower sends the smaller snapshot. There is one unacknowledged
status update at a time. Frames are limited to 1 MiB and full projections to 5,000
records; oversized snapshots fail without truncation. Local rules and approvals
are not part of the status projection.

Managed commands have durable receipts and generation-bound execution owners.
Reconnection reads their status and does not repeat uncertain commands. Transcript
events use a separate bounded journal and are released after acknowledgment.
Private permission requests, form requests, URL interactions, and replies remain
in memory. Explicit resume keeps the Tower session ID and creates a fresh execution
generation; an uncertain old execution blocks replacement unless its owner
explicitly approves that overlap through Allgather.

Both sides must advertise `policy-sync.v1` and `session-status.v1`. Policy snapshots
atomically replace only the remote table; stale/conflicting revisions are rejected.
Allgather identifies a Tower record by `(machine_id, tower_id)`. The same opaque
Tower ID on another machine is permitted; IDs are never parsed for routing.
A rejected record remains locally usable with `sync_state: rejected`.
The status command reports redacted policy-application and connection errors.

Launch structured sessions from Allgather after permitting the relevant local
policy. Install the native CLI on the machine and configure its native login first.
Codex always uses app-server for this interface. Claude Code and Antigravity use
Tower's ACP bridges over their native streaming protocols.

| Harness | Permission replies | User questions | Cancellation |
| --- | --- | --- | --- |
| Codex | Yes | Forms and private URL consent | Current turn |
| Claude Code | Yes | Forms | Current turn |
| Antigravity | Native headless restrictions apply | Text prompts | Whole runtime |

The initial adapters accept text input. Unsupported image, audio, and resource
input fails explicitly. Codex configuration supports model, reasoning effort, and
`plan`/`default` collaboration mode. Each adapter reports its supported controls.
Native permissions remain in force. Stopping one runtime does not stop other sessions.

Optional journal limits use these defaults:

```toml
[runtime]
runtime_spool_bytes = 67108864
host_spool_bytes = 268435456
control_reserve_bytes = 1048576
receipt_count = 1000000
receipt_retention_days = 30
```

Receipts for active work remain available. Ended receipts compact after the retention
period; command identities and outcomes remain as enrollment-lifetime tombstones.
A full journal applies backpressure or reports a gap for output lost before sequence
assignment. It does not evict assigned, unacknowledged events.

With an adjacent Allgather checkout and its dependencies installed, use its pinned
environment to run the real integration checks against the local Tower build:

```sh
ALLGATHER_TEST_TOWER="$PWD/target/debug/tower" nix develop ../allgather --no-write-lock-file -c bun --no-env-file ../allgather/scripts/verify-tower.ts
ALLGATHER_TEST_TOWER="$PWD/target/debug/tower" nix develop ../allgather --no-write-lock-file -c bun --no-env-file ../allgather/scripts/verify-tower-managed.ts
```

## Checks

Run inside the development shell:

```sh
bash scripts/check-toolchain.sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo check -p towerd --locked
cargo build --workspace --all-targets --locked
cargo test --workspace --locked
alejandra --check .
actionlint
shellcheck scripts/*.sh
bash -n .envrc
```

`nix flake check` also validates package builds, Clippy, Rust formatting, and a
daemon build with terminal support disabled.
The Linux CI lint job runs this check. The development shell places Cargo output
in the repository's `target/` directory, including when entered from a subdirectory.
`nix build` produces one executable, `tower`; installation checks enforce this.
The towerd and tower-pool libraries run in separate processes through their
respective entry points. When updating the compiler, keep flake.lock,
rust-toolchain.toml, and the workspace rust-version aligned. CI checks that the
Fenix and rustup pins agree.

Optional integration tests run the installed Codex CLI against a local Responses
fixture. They use isolated home directories and no API credentials. They test native and
app-server modes, real TTY attach, Kitty reattachment, simultaneous clients, daemon
restart, switching an app-server conversation to native mode, and native message
delivery. The external app-server test verifies that approval requests reach the
existing TUI. The Claude inbox test checks that delivered text reaches the model:

```sh
cargo build --workspace --locked
cargo test -p tower@0.1.0 --test codex --locked -- --ignored
cargo test -p tower@0.1.0 --test claude --locked -- --ignored
```

The normal suite tests all nine adapters with local process fixtures. Node runs the
generated extension tests and is included in the Nix shell. The optional Claude
test uses a local fake Anthropic API and a synthetic test key. Claude Code and
Codex 0.166.0 have been verified on Linux. Full live-provider coverage for the
other harnesses is pending.

## Capture and replay tests

The optional Linux runner uses the separate Tracetape checkout through its public
library. The checkout must have its dependencies installed. It runs real Claude
and Codex tool calls, captures sanitized provider traffic, replays it in another
workspace, and rejects changed tool output. A response gate verifies `working`
status and daemon restart while the real agent waits for its next response.

```sh
nix develop
cargo build --bin tower --locked
TRACETAPE_ROOT=../tracetape bash scripts/test-e2e.sh
```

The runner uses fresh homes, synthetic keys, and a network namespace with only
loopback. It fails if isolation is unavailable. No real inference service is
contacted. Set `TOWER_TEST_ARTIFACT_DIR=workspace/e2e-traces` to retain sanitized
cassettes for inspection. The regular CI suite does not require installed coding
CLIs or the unpublished Tracetape checkout.

## Workspace

| Crate | Responsibility |
| --- | --- |
| `tower-protocol` | Public local API types; serde-only, published after the initial contract is ready |
| `tower-pal` | Platform terminal, IPC, process, and lifecycle operations |
| `tower-pool-proto` | Internal pool control and attach protocol |
| `tower-pool` | Terminal pool library, hosted in an independent tower process |
| `tower-store` | Local persistence, backend bindings, policy snapshots, and synchronization state |
| `tower-sessions` | Managed backend contract and optional capability interfaces |
| `tower-terminal` | Terminal backend, launcher supervision, Codex relay, and attach client |
| `tower-agents` | Launchers, hooks, and resume adapters for nine harnesses |
| `tower-runtime` | Independent structured runtime owner, native adapters, receipts, and transcript journal |
| `towerd` | Registry, managed backends, authorization, rules, and events |
| `tower` | CLI, daemon/pool/supervisor entry points, and hooks |

Publication is disabled during bootstrap. The tower workspace package and binary
name do not imply ownership of the existing crates.io name. Project code uses
MIT OR Apache-2.0; extracted upstream code must retain its copyright and licenses.
Harness integrations adapt portions of Captain Miao; see `LICENSE-CAPTAIN-MIAO`.
