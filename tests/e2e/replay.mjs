import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { mkdtemp, mkdir, readFile, writeFile, rm, symlink } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { networkInterfaces } from "node:os";
import { pathToFileURL } from "node:url";

assert.equal(process.env.TOWER_TEST_OFFLINE, "1", "run scripts/test-e2e.sh");
assert.deepEqual(Object.keys(networkInterfaces()), ["lo"], "only loopback networking is allowed");
const root = await mkdtemp("/tmp/tower-replay-");
const tapeRoot = resolve(process.env.TRACETAPE_ROOT);
const { startRecorder, startReplay, loadCassette, defaultNormalization } = await import(pathToFileURL(join(tapeRoot, "src/index.ts")));
const tower = resolve(process.env.TOWER_TEST_BIN);
const server = join(dirname(tower), "tower");
const pool = join(dirname(tower), "tower-terminal-pool");
const prompt = "Copy input.txt to artifact.txt, then report done.";
const command = "cat input.txt > artifact.txt; cat artifact.txt";
const models = { claude: "claude-sonnet-4-6", codex: "gpt-5.3-codex" };

function normalizationFor(agent, bindings) {
  const result={...defaultNormalization(),bindings};
  if (agent==="claude") {result.omit=["/metadata/user_id"];return result;}
  const metadata="/client_metadata/x-codex-turn-metadata";
  result.omit=["/prompt_cache_key",metadata+"/turn_started_at_unix_ms"];
  result.jsonStrings=[metadata];
  result.identities=[
    {name:"message",paths:["/input/*/id"]},
    {name:"turn",paths:["/client_metadata/turn_id","/client_metadata/root_turn_id",metadata+"/turn_id",metadata+"/root_turn_id"]},
    {name:"window",paths:["/client_metadata/x-codex-window-id",metadata+"/window_id"]},
    {name:"session",paths:["/client_metadata/session_id","/client_metadata/thread_id",metadata+"/session_id",metadata+"/thread_id"]},
    {name:"installation",paths:["/client_metadata/x-codex-installation-id",metadata+"/installation_id"]},
    {name:"context",paths:[metadata+"/context_window_id"]},
  ];
  result.replace=[
    {path:"/input/*/output",pattern:"Chunk ID: [a-zA-Z0-9_-]+",replacement:"Chunk ID: VOLATILE"},
    {path:"/input/*/output",pattern:"Wall time: [0-9.]+ seconds",replacement:"Wall time: VOLATILE seconds"},
  ];
  return result;
}

async function subprocess(exe, args, env, cwd, timeout = 15000) {
  return await new Promise((resolve, reject) => {
    const child = spawn(exe, args, { env, cwd, stdio: ["ignore", "pipe", "pipe"] });
    let output = "", error = "";
    const timer = setTimeout(() => { child.kill("SIGKILL"); reject(Error("command timed out: " + args[0])); }, timeout);
    child.stdout.on("data", b => { output += b; if (output.length > 2 * 1024 * 1024) child.kill("SIGKILL"); });
    child.stderr.on("data", b => { error += b; if (error.length > 2 * 1024 * 1024) child.kill("SIGKILL"); });
    child.on("error", e => { clearTimeout(timer); reject(e); });
    child.on("close", code => { clearTimeout(timer); resolve({ code, output, error }); });
  });
}
async function fixture(agent, name, content = "fixture-value\n") {
  const dir = join(root, name), home = join(dir, "home"), work = join(dir, "work");
  await mkdir(join(home, "bin"), { recursive: true });
  await mkdir(join(home, ".codex"), { recursive: true });
  await mkdir(work);
  for (const [cli, variable] of [["codex", "TOWER_TEST_CODEX"], ["claude", "TOWER_TEST_CLAUDE"]]) {
    await symlink(process.env[variable], join(home, "bin", cli));
  }
  await writeFile(join(work, "input.txt"), content);
  const env = {
    PATH: join(home, "bin") + ":" + process.env.PATH,
    HOME: home, CODEX_HOME: join(home, ".codex"), CLAUDE_CONFIG_DIR: join(home, ".claude"),
    XDG_STATE_HOME: join(home, ".local/state"), XDG_CONFIG_HOME: join(home, ".config"),
    XDG_DATA_HOME: join(home, ".local/share"), XDG_CACHE_HOME: join(home, ".cache"),
    TMPDIR: dir, TERM: "xterm-256color", LANG: "C.UTF-8", LC_ALL: "C.UTF-8", TZ: "UTC",
    ANTHROPIC_API_KEY: "fixture-key", MAX_THINKING_TOKENS: "0", TOKIO_WORKER_THREADS: "2",
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1", DISABLE_TELEMETRY: "1",
    DISABLE_ERROR_REPORTING: "1", DISABLE_AUTOUPDATER: "1", DISABLE_NON_ESSENTIAL_MODEL_CALLS: "1",
  };
  const bindings = { ROOT: work, HOME: home, CLI_HOME: env[agent === "claude" ? "CLAUDE_CONFIG_DIR" : "CODEX_HOME"], TEMP: dir, TOWER_BINARY: tower };
  const rpc = async (...args) => {
    const result = await subprocess(tower, ["--json", ...args], env, work);
    assert.equal(result.code, 0, args[0] + ": " + result.error + result.output);
    return JSON.parse(result.output);
  };
  return {
    dir, home, work, env, bindings, rpc,
    async close() {
      for (const [binary, args] of [[pool, ["stop"]], ["codex", ["app-server", "daemon", "stop"]], [tower, ["--no-start", "daemon", "stop"]]]) await subprocess(binary, args, env, work).catch(() => {});
    },
    async launch(url) {
      if (agent === "codex") await writeFile(join(home, ".codex/config.toml"), [
        'model = "' + models.codex + '"', 'model_provider = "fixture"', 'approval_policy = "never"',
        'sandbox_mode = "danger-full-access"', 'check_for_update_on_startup = false', 'web_search = "disabled"',
        'include_apps_instructions = false', '[model_providers.fixture]', 'name = "Tower fixture"',
        'base_url = "' + url + '/v1"', 'wire_api = "responses"', 'request_max_retries = 0', 'stream_max_retries = 0',
        '[projects.' + JSON.stringify(work) + ']', 'trust_level = "trusted"', '',
      ].join("\n"));
      else env.ANTHROPIC_BASE_URL = url;
      const args = agent === "claude"
        ? ["--print", "--model", models.claude, "--system-prompt", "Use the requested tool.", "--allowedTools", "Bash", "--tools", "Bash", "--strict-mcp-config", "--setting-sources", "", prompt]
        : ["exec", "--json", "--skip-git-repo-check", "--dangerously-bypass-approvals-and-sandbox", prompt];
      return await rpc("open", agent, "--headless", ...(agent === "codex" ? ["--mode", "native"] : []), "--", ...args);
    },
    async wait(id) {
      const deadline = Date.now() + 45000;
      while (Date.now() < deadline) {
        const row = await rpc("get", id);
        if (row.status === "exited") return row;
        await new Promise(r => setTimeout(r, 40));
      }
      throw Error("session did not finish: " + JSON.stringify(await rpc("get",id)) + " " + JSON.stringify(await rpc("snapshot", id)));
    },
  };
}
function frames(agent, turn, work) {
  if (agent === "codex") {
    const item = turn === 1
      ? { type: "function_call", id: "fc_fixture", call_id: "call_fixture", name: "exec_command", arguments: JSON.stringify({ cmd: command, workdir: work, login: false, max_output_tokens: 1000 }) }
      : { type: "message", id: "msg_fixture", role: "assistant", content: [{ type: "output_text", text: "Done.", annotations: [] }] };
    return [
      { type: "response.created", response: { id: "resp_" + turn, status: "in_progress", output: [] } },
      { type: "response.output_item.done", output_index: 0, item },
      { type: "response.completed", response: { id: "resp_" + turn, status: "completed", output: [], usage: { input_tokens: 1, output_tokens: 1, total_tokens: 2 } } },
    ];
  }
  return [
    { type: "message_start", message: { id: "msg_" + turn, type: "message", role: "assistant", model: models.claude, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 1, output_tokens: 0 } } },
    { type: "content_block_start", index: 0, content_block: turn === 1 ? { type: "tool_use", id: "tool_fixture", name: "Bash", input: {} } : { type: "text", text: "" } },
    { type: "content_block_delta", index: 0, delta: turn === 1 ? { type: "input_json_delta", partial_json: JSON.stringify({ command, description: "Copy test input" }) } : { type: "text_delta", text: "Done." } },
    { type: "content_block_stop", index: 0 },
    { type: "message_delta", delta: { stop_reason: turn === 1 ? "tool_use" : "end_turn", stop_sequence: null }, usage: { output_tokens: 1 } },
    { type: "message_stop" },
  ];
}
try {
  for (const agent of ["claude", "codex"]) {
    const source = await fixture(agent, agent + "-capture");
    let step = 0;
    const observed = [];
    const provider = createServer(async (request, response) => {
      let size = 0; const chunks = [];
      for await (const chunk of request) { size += chunk.length; assert(size < 8 * 1024 * 1024); chunks.push(chunk); }
      observed.push(JSON.parse(Buffer.concat(chunks)));
      response.writeHead(200, { "content-type": "text/event-stream" });
      for (const data of frames(agent, ++step, source.work)) response.write("event: " + data.type + "\ndata: " + JSON.stringify(data) + "\n\n");
      response.end();
    });
    await new Promise(r => provider.listen(0, "127.0.0.1", r));
    const versionResult=await subprocess(process.env[agent==="claude"?"TOWER_TEST_CLAUDE":"TOWER_TEST_CODEX"],["--version"],source.env,source.work);
    const version=versionResult.output.match(/\d+\.\d+\.\d+/)?.[0];assert(version,"CLI version must be recorded");
    const normalization = normalizationFor(agent, Object.keys(source.bindings));
    const recorder = await startRecorder({
      upstream: "http://127.0.0.1:" + provider.address().port,
      manifest: { schema: 1, normalizer: 1, source: "synthetic", cli: { name: agent, version }, model: models[agent], workspace: "tower-copy-v1", normalization },
      expectedSteps: { main: 2 }, bindings: source.bindings, secrets: ["fixture-key"], timeoutMs: 45000,
    });
    const cassettePath = join(root, agent + ".json");

    try {
      const session = await source.launch(recorder.url);
      const row=await Promise.race([source.wait(session.id),recorder.failed.then(error=>{throw error;})]);
      assert.equal(await readFile(join(source.work, "artifact.txt"), "utf8"), "fixture-value\n");
      assert(row.native_session_id, "native hooks must report the conversation identity");
      assert(JSON.stringify(observed[1]).includes("fixture-value"), "provider must receive real tool output");
      await recorder.save(cassettePath);
      const credential=await readFile(join(source.home,".local/state/tower/tokens",session.id),"utf8");
      assert(!(await readFile(cassettePath,"utf8")).includes(credential),"Tower session credentials must not enter traces");
    } finally {
      await source.close(); await recorder.close(); await new Promise(r => provider.close(r));
    }
    const cassette = await loadCassette(cassettePath);
    assert(!JSON.stringify(cassette).includes(source.dir));
    assert(!JSON.stringify(cassette).includes("fixture-key"));
    if (process.env.TOWER_TEST_ARTIFACT_DIR) {
      await mkdir(process.env.TOWER_TEST_ARTIFACT_DIR,{recursive:true});
      await writeFile(join(process.env.TOWER_TEST_ARTIFACT_DIR,agent+".json"),JSON.stringify(cassette,null,2),{mode:0o600});
    }
    cassette.lanes.main[1].response.frames[0].gate="after-tool";
    for (const changed of [false, true]) {
      const destination = await fixture(agent, agent + (changed ? "-mismatch" : "-replay"), changed ? "changed-value\n" : "fixture-value\n");
      const replay = await startReplay({ cassette, bindings: destination.bindings, timeoutMs: 45000 });
      try {
        const session = await destination.launch(replay.url);
        const outcome=await Promise.race([replay.waitForGate("after-tool").then(()=>({ready:true})),replay.failed.then(error=>({error}))]);
        if (outcome.error) {
          assert(changed, String(outcome.error));assert.match(String(outcome.error),/mismatch/);
          await destination.rpc("kill",session.id);
          await destination.wait(session.id);
        } else {
          assert(!changed,"changed tool output must be rejected");
          const before=await destination.rpc("get",session.id);
          assert.equal(before.status,"working","real tool turn must be visible while the provider is paused");
          await destination.rpc("daemon","stop");
          const after=await destination.rpc("get",session.id);
          assert.equal(after.pid,before.pid);assert.equal(after.native_session_id,before.native_session_id);
          assert.equal(after.status,"working");
          replay.release("after-tool");
          await destination.wait(session.id);
        }
        assert.equal(await readFile(join(destination.work, "artifact.txt"), "utf8"), changed ? "changed-value\n" : "fixture-value\n");
        if (changed) assert.throws(() => replay.assertComplete(), /mismatch/);
        else replay.assertComplete();
      } finally { await destination.close(); await replay.close(); }
    }
    console.log(agent + ": real tool execution, sanitized capture, relocated replay, changed-output rejection passed");
  }
} finally { await rm(root, { recursive: true, force: true }); }
