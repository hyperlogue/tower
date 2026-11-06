#!/usr/bin/env node
import fs from 'node:fs';
import net from 'node:net';
import {spawn} from 'node:child_process';
import path from 'node:path';
import {createHash, randomBytes} from 'node:crypto';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {StringDecoder} from 'node:string_decoder';

const MAX_FRAME = 4 * 1024 * 1024;
const MAX_HEADER = 16 * 1024;
const MAX_PENDING = 128;
const decoder = new TextDecoder('utf-8', {fatal: true});
const guid = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function frame(opcode, body, masked) {
  const size = body.length;
  if (size > MAX_FRAME) throw new Error('frame too large');
  const extra = size < 126 ? 0 : size <= 65535 ? 2 : 8;
  const mask = masked ? randomBytes(4) : null;
  const out = Buffer.alloc(2 + extra + (mask ? 4 : 0) + size);
  out[0] = 0x80 | opcode;
  out[1] = (mask ? 0x80 : 0) | (extra === 0 ? size : extra === 2 ? 126 : 127);
  if (extra === 2) out.writeUInt16BE(size, 2);
  if (extra === 8) out.writeBigUInt64BE(BigInt(size), 2);
  let at = 2 + extra;
  if (mask) { mask.copy(out, at); at += 4; }
  for (let index = 0; index < size; index++) out[at + index] = body[index] ^ (mask ? mask[index % 4] : 0);
  return out;
}

export class Peer {
  constructor(socket, client, rest = Buffer.alloc(0)) {
    this.socket = socket;
    this.client = client;
    this.buffer = Buffer.alloc(0);
    this.fragments = [];
    this.fragmentBytes = 0;
    this.fragmented = false;
    this.receiver = null;
    this.waiting = [];
    this.pending = new Map();
    this.next = 0;
    this.closed = false;
    this.onclose = () => {};
    socket.on('data', bytes => this.feed(bytes));
    socket.on('error', () => socket.destroy());
    socket.on('close', () => {
      this.closed = true;
      for (const call of this.pending.values()) { clearTimeout(call.timer); call.reject(new Error('connection closed')); }
      this.pending.clear();
      this.onclose();
    });
    if (rest.length) this.feed(rest);
  }
  listen(receiver) {
    this.receiver = receiver;
    for (const value of this.waiting.splice(0)) receiver(value);
  }
  fail(code = 1002) {
    if (!this.closed && !this.socket.destroyed) {
      const data = Buffer.alloc(2); data.writeUInt16BE(code);
      this.socket.end(frame(8, data, this.client));
    }
    this.closed = true;
    this.socket.destroySoon();
  }
  write(opcode, bytes) {
    if (this.closed || this.socket.destroyed) return false;
    if (this.socket.writableLength + bytes.length > MAX_FRAME) { this.socket.destroy(); return false; }
    this.socket.write(frame(opcode, bytes, this.client));
    return true;
  }
  send(value) { return this.write(1, Buffer.from(JSON.stringify(value))); }
  reply(id, result) { return this.send({jsonrpc: '2.0', id, result}); }
  error(id, code, message) { return this.send({jsonrpc: '2.0', id, error: {code, message}}); }
  call(method, params = {}) {
    if (this.pending.size >= MAX_PENDING) return Promise.reject(new Error('too many requests'));
    const id = `fixture-client:${++this.next}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`request timed out: ${method}`)); this.socket.destroy(); }, 10000);
      this.pending.set(id, {resolve, reject, timer});
      if (!this.send({jsonrpc: '2.0', id, method, params})) { clearTimeout(timer); this.pending.delete(id); reject(new Error('connection unavailable')); }
    });
  }
  message(bytes) {
    let value;
    try { value = JSON.parse(decoder.decode(bytes)); }
    catch { this.fail(1007); return; }
    if (!value || typeof value !== 'object' || Array.isArray(value)) { this.fail(); return; }
    if (value.method === undefined && this.pending.has(value.id)) {
      const call = this.pending.get(value.id);
      this.pending.delete(value.id); clearTimeout(call.timer);
      if (value.error) call.reject(Object.assign(new Error(value.error.message), {code: value.error.code}));
      else call.resolve(value.result);
    } else if (this.receiver) this.receiver(value);
    else if (this.waiting.length < MAX_PENDING) this.waiting.push(value);
    else this.fail(1009);
  }
  feed(bytes) {
    if (this.closed) return;
    if (this.buffer.length + bytes.length > MAX_FRAME + MAX_HEADER) { this.fail(1009); return; }
    this.buffer = Buffer.concat([this.buffer, bytes]);
    while (this.buffer.length >= 2 && !this.closed) {
      const first = this.buffer[0], second = this.buffer[1];
      const opcode = first & 15, final = Boolean(first & 128), masked = Boolean(second & 128);
      if ((first & 0x70) || masked === this.client) { this.fail(); return; }
      let size = second & 127, offset = 2;
      if (size === 126) {
        if (this.buffer.length < 4) return;
        size = this.buffer.readUInt16BE(2); offset = 4;
        if (size < 126) { this.fail(); return; }
      } else if (size === 127) {
        if (this.buffer.length < 10) return;
        const large = this.buffer.readBigUInt64BE(2);
        if (large > BigInt(MAX_FRAME)) { this.fail(1009); return; }
        size = Number(large); offset = 10;
        if (size <= 65535) { this.fail(); return; }
      }
      if (size > MAX_FRAME || (opcode >= 8 && (!final || size > 125))) { this.fail(1009); return; }
      const bodyAt = offset + (masked ? 4 : 0);
      if (this.buffer.length < bodyAt + size) return;
      const body = Buffer.from(this.buffer.subarray(bodyAt, bodyAt + size));
      if (masked) for (let index = 0; index < size; index++) body[index] ^= this.buffer[offset + index % 4];
      this.buffer = this.buffer.subarray(bodyAt + size);
      if (opcode === 8) { if (size === 1) this.fail(); else { this.write(8, body); this.socket.end(); this.closed = true; } return; }
      if (opcode === 9) { this.write(10, body); continue; }
      if (opcode === 10) continue;
      if (opcode === 1 && !this.fragmented) {
        if (final) this.message(body);
        else { this.fragmented = true; this.fragments = [body]; this.fragmentBytes = size; }
      } else if (opcode === 0 && this.fragmented) {
        this.fragmentBytes += size;
        if (this.fragmentBytes > MAX_FRAME) { this.fail(1009); return; }
        this.fragments.push(body);
        if (final) {
          const complete = Buffer.concat(this.fragments, this.fragmentBytes);
          this.fragments = []; this.fragmentBytes = 0; this.fragmented = false;
          this.message(complete);
        }
      } else { this.fail(opcode === 2 ? 1003 : 1002); return; }
    }
  }
}

function headers(bytes) {
  const lines = bytes.toString('ascii').split('\r\n');
  const first = lines.shift();
  const fields = new Map();
  for (const line of lines.filter(Boolean)) {
    const colon = line.indexOf(':');
    if (colon < 1) throw new Error('invalid handshake');
    const key = line.slice(0, colon).toLowerCase();
    if (fields.has(key)) throw new Error('duplicate header');
    fields.set(key, line.slice(colon + 1).trim());
  }
  return {first, fields};
}

function handshake(socket, validate) {
  return new Promise((resolve, reject) => {
    let buffer = Buffer.alloc(0);
    const timer = setTimeout(() => fail(new Error('handshake timed out')), 5000);
    function cleanup() { clearTimeout(timer); socket.off('data', data); socket.off('error', fail); socket.off('close', closed); }
    function fail(error) { cleanup(); socket.destroy(); reject(error); }
    function closed() { fail(new Error('closed during handshake')); }
    function data(bytes) {
      buffer = Buffer.concat([buffer, bytes]);
      const end = buffer.indexOf('\r\n\r\n');
      if (end === -1) { if (buffer.length > MAX_HEADER) fail(new Error('header too large')); return; }
      if (end > MAX_HEADER) { fail(new Error('header too large')); return; }
      try {
        validate(headers(buffer.subarray(0, end + 4)));
        cleanup();
        resolve(buffer.subarray(end + 4));
      } catch (error) { fail(error); }
    }
    socket.on('data', data); socket.on('error', fail); socket.on('close', closed);
  });
}

export async function connectUnix(endpoint) {
  const socket = net.createConnection(endpoint);
  const key = randomBytes(16).toString('base64');
  const expected = createHash('sha1').update(key + guid).digest('base64');
  const reading = handshake(socket, ({first, fields}) => {
    if (!/^HTTP\/1\.1 101(?: |$)/.test(first) || fields.get('sec-websocket-accept') !== expected || fields.get('upgrade')?.toLowerCase() !== 'websocket') throw new Error('invalid upgrade');
  });
  socket.on('connect', () => socket.write(`GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n\r\n`));
  return new Peer(socket, true, await reading);
}

class RpcFailure extends Error {
  constructor(code, message) { super(message); this.code = code; }
}
const invalid = message => { throw new RpcFailure(-32602, message); };
const identifier = value => typeof value === 'string' && value.length > 0 && value.length <= 256 && !/[\x00-\x1f\x7f]/.test(value);
const clone = value => JSON.parse(JSON.stringify(value));

export async function serve(endpoint) {
  const home = process.env.HOME;
  if (!home || !path.isAbsolute(home) || !path.isAbsolute(endpoint)) throw new Error('absolute fixture paths required');
  const historyRoot = process.env.CODEX_HOME || path.join(home, '.codex');
  fs.mkdirSync(historyRoot, {recursive: true, mode: 0o700});
  const historyFile = path.join(historyRoot, 'tower-fixture-threads.json');
  const threads = new Map();
  let nextThread = 0, nextTurn = 0;
  if (fs.existsSync(historyFile)) {
    if (fs.statSync(historyFile).size > MAX_FRAME) throw new Error('history too large');
    const saved = JSON.parse(fs.readFileSync(historyFile, 'utf8'));
    nextThread = saved.nextThread; nextTurn = saved.nextTurn;
    for (const savedThread of saved.threads) threads.set(savedThread.id, {...savedThread, loaded: false, subscribers: new Set(), config: {}, terminals: new Map(), active: null, flags: [], turns: savedThread.turns.map(turn => ({...turn, status: turn.status === 'inProgress' ? 'failed' : turn.status}))});
  }
  const peers = new Set(), connections = new Set();
  function persist() {
    const payload = JSON.stringify({nextThread, nextTurn, threads: [...threads.values()].map(thread => ({id: thread.id, cwd: thread.cwd, name: thread.name, model: thread.model, turns: thread.turns, goal: thread.goal, createdAt: thread.createdAt, updatedAt: thread.updatedAt, parentThreadId: thread.parentThreadId}))});
    if (Buffer.byteLength(payload) > MAX_FRAME) throw new RpcFailure(-32000, 'fixture history capacity');
    const temporary = `${historyFile}.${process.pid}.tmp`;
    fs.writeFileSync(temporary, payload, {mode: 0o600}); fs.renameSync(temporary, historyFile);
  }
  function status(thread) { return {type: !thread.loaded ? 'notLoaded' : thread.active ? 'active' : 'idle', ...(thread.active ? {activeFlags: thread.flags} : {})}; }
  function view(thread, turns = false) {
    return {id: thread.id, cwd: thread.cwd, name: thread.name, path: thread.turns.length ? historyFile : null, status: status(thread), turns: turns ? clone(thread.turns) : [], modelProvider: 'fixture', parentThreadId: thread.parentThreadId, source: 'cli', threadSource: 'user', ephemeral: false, createdAt: thread.createdAt, updatedAt: thread.updatedAt};
  }
  function notify(thread, method, params = {}, broadcast = false) {
    const message = {jsonrpc: '2.0', method, params: {threadId: thread.id, ...params}};
    for (const peer of broadcast ? peers : thread.subscribers) peer.send(message);
  }
  function statusChanged(thread) { notify(thread, 'thread/status/changed', {status: status(thread)}, true); }
  function lookup(id, loaded = false) {
    if (!identifier(id)) invalid('threadId required');
    const thread = threads.get(id);
    if (!thread || (loaded && !thread.loaded)) throw new RpcFailure(-32600, `thread not loaded: ${id}`);
    return thread;
  }
  function subscribe(peer, thread) {
    if (peer.subscriptions.size >= 64 && !peer.subscriptions.has(thread.id)) throw new RpcFailure(-32000, 'fixture subscription capacity');
    thread.subscribers.add(peer); peer.subscriptions.add(thread.id);
  }
  function retireRequests(thread) {
    for (const peer of peers) for (const [id, request] of peer.requests) if (request.thread === thread) peer.requests.delete(id);
  }
  function complete(thread, interrupted = false) {
    const turn = thread.active;
    if (!turn) return;
    retireRequests(thread);
    if (!interrupted) notify(thread, 'item/agentMessage/delta', {turnId: turn.id, itemId: `message-${turn.id}`, delta: 'Fixture answer'});
    turn.status = interrupted ? 'interrupted' : 'completed';
    thread.active = null; thread.flags = []; thread.updatedAt = Math.floor(Date.now() / 1000);
    notify(thread, 'turn/completed', {turn: clone(turn)}); statusChanged(thread); persist();
  }
  function unsubscribe(peer, thread) {
    const had = peer.subscriptions.delete(thread.id); thread.subscribers.delete(peer);
    for (const [id, request] of peer.requests) if (request.thread === thread) peer.requests.delete(id);
    if (!thread.subscribers.size && thread.loaded) {
      complete(thread, true); thread.loaded = false; statusChanged(thread); persist();
    }
    return had;
  }
  function startThread(peer, params, original = null, fork = false) {
    if (params.cwd !== undefined && (typeof params.cwd !== 'string' || !path.isAbsolute(params.cwd) || !fs.statSync(params.cwd).isDirectory())) invalid('cwd must be an existing absolute directory');
    if (original && !fork) {
      if (!original.loaded) {
        original.loaded = true;
        original.cwd = params.cwd ?? original.cwd;
        original.config = clone(params.config ?? {});
        original.model = params.model ?? original.config.model ?? original.model;
      }
      subscribe(peer, original); persist(); return original;
    }
    if (threads.size >= 256) throw new RpcFailure(-32000, 'fixture thread capacity');
    const config = clone(params.config ?? {});
    const thread = {id: `native-codex-${++nextThread}`, cwd: params.cwd ?? original?.cwd ?? home, name: null, model: params.model ?? config.model ?? original?.model ?? 'fixture-model', loaded: true, subscribers: new Set(), config, terminals: new Map(), active: null, flags: [], turns: [], goal: null, createdAt: Math.floor(Date.now() / 1000), updatedAt: Math.floor(Date.now() / 1000), parentThreadId: params.threadSource && params.threadSource !== 'user' ? original?.id ?? null : null};
    threads.set(thread.id, thread); subscribe(peer, thread); persist(); return thread;
  }
  function requestInput(thread, peer, kind) {
    if (!thread.active || !peer || peer.closed) return;
    const id = kind === 'question' ? 'question' : kind === 'url' ? 'url' : 'permission';
    if (peer.requests.has(id)) throw new RpcFailure(-32000, 'fixture request ID already live on this connection');
    peer.requests.set(id, {thread, kind, turn: thread.active.id});
    let method, params;
    if (kind === 'question') {
      method = 'item/tool/requestUserInput';
      params = {threadId: thread.id, turnId: thread.active.id, itemId: 'tool', isBlocking: true, questions: [{id: 'decision', header: 'Decision', question: 'PRIVATE-FORM-MARKER', multiSelect: false, options: [{label: 'yes', description: 'Continue'}, {label: 'no', description: 'Stop'}]}]};
      thread.flags = ['waitingOnUserInput'];
    } else if (kind === 'url') {
      method = 'mcpServer/elicitation/request';
      params = {threadId: thread.id, turnId: thread.active.id, serverName: 'fixture', mode: 'url', message: 'Authorize', elicitationId: 'url-id', url: 'https://example.invalid/connect?nonce=PRIVATE-URL-MARKER'};
      thread.flags = ['waitingOnUserInput'];
    } else {
      method = 'item/commandExecution/requestApproval';
      params = {threadId: thread.id, turnId: thread.active.id, itemId: 'tool', command: 'PRIVATE-REQUEST-MARKER'};
      thread.flags = ['waitingOnApproval'];
    }
    peer.send({jsonrpc: '2.0', id, method, params}); statusChanged(thread);
  }
  function dispatchPrompt(thread, peer, params, rpcId) {
    if (!Array.isArray(params.input) || !params.input.length) invalid('input required');
    if (params.input.some(block => !block || !['text', 'localImage', 'image'].includes(block.type) || (block.type === 'text' && typeof block.text !== 'string'))) invalid('unsupported input');
    const text = params.input.filter(block => block.type === 'text').map(block => block.text).join('');
    if (Buffer.byteLength(text) > 32 * 1024) invalid('fixture prompt too large');
    if (thread.active) throw new RpcFailure(-32000, 'thread is busy');
    if (thread.turns.length >= 512) throw new RpcFailure(-32000, 'fixture turn capacity');
    const dispatchFile = path.join(home, 'dispatches');
    if (fs.existsSync(dispatchFile) && fs.statSync(dispatchFile).size > 16 * 1024 * 1024) throw new RpcFailure(-32000, 'fixture dispatch capacity');
    fs.appendFileSync(dispatchFile, `codex:${text}\n`, {mode: 0o600});
    const turn = {id: `turn-${++nextTurn}`, status: 'inProgress', items: []};
    thread.turns.push(turn); thread.active = turn; thread.flags = []; thread.updatedAt = Math.floor(Date.now() / 1000);
    if (params.model !== undefined) thread.model = params.model;
    peer.reply(rpcId, {turn: clone(turn)});
    notify(thread, 'turn/started', {turn: clone(turn)}); statusChanged(thread); persist();
    if (text === 'approve-backlog') {
      for (let index = 0; index < 450; index++) notify(thread, 'item/started', {turnId: turn.id, item: {id: `tool-${index}`, type: 'commandExecution', command: 'fixture'}});
    }
    if (['approve', 'approve-backlog', 'question', 'url'].includes(text)) requestInput(thread, peer, text === 'approve-backlog' ? 'approve' : text);
    else if (text === 'background') { thread.terminals.set(`background-${turn.id}`, {processId: `background-${turn.id}`}); complete(thread); }
    else if (text !== 'wait') complete(thread);
  }
  function page(values, params) {
    const start = params.cursor === undefined || params.cursor === null ? 0 : Number(params.cursor);
    const limit = params.limit ?? 100;
    if (!Number.isInteger(start) || start < 0 || !Number.isInteger(limit) || limit < 1 || limit > 1000) invalid('invalid pagination');
    return {data: values.slice(start, start + limit), nextCursor: start + limit < values.length ? String(start + limit) : null};
  }
  function receive(peer, value) {
    if (value.method === undefined) {
      const request = peer.requests.get(value.id);
      if (!request) return;
      peer.requests.delete(value.id);
      if (request.thread.active?.id !== request.turn) return;
      if (value.error) { complete(request.thread, true); return; }
      if (!Object.hasOwn(value, 'result')) { peer.fail(); return; }
      if (request.kind === 'question' && value.result?.answers?.decision?.answers?.[0] !== 'yes') { complete(request.thread, true); return; }
      if (request.kind === 'url' && (value.result?.action !== 'accept' || value.result?.content !== undefined)) { complete(request.thread, true); return; }
      if (request.kind === 'approve' && !['accept', 'acceptForSession', 'decline', 'cancel'].includes(value.result?.decision)) { peer.fail(); return; }
      complete(request.thread, request.kind === 'approve' && ['decline', 'cancel'].includes(value.result.decision));
      return;
    }
    const id = value.id;
    const params = value.params ?? {};
    try {
      if (typeof value.method !== 'string' || value.method.length > 128 || !params || typeof params !== 'object' || Array.isArray(params)) invalid('invalid request');
      if (value.method === 'initialize') {
        if (peer.initialized) throw new RpcFailure(-32600, 'already initialized');
        peer.initialized = true;
        peer.reply(id, {userAgent: 'codex-app-server/0.166.0', platformFamily: 'unix', platformOs: process.platform});
        return;
      }
      if (!peer.initialized) throw new RpcFailure(-32600, 'initialize required');
      if (value.method === 'initialized') return;
      if (id === undefined) throw new RpcFailure(-32600, 'request ID required');
      let result;
      switch (value.method) {
        case 'environment/info': {
          if (params.environmentId !== 'local') invalid('only local environment is supported');
          const shell = process.env.SHELL;
          if (!shell || !path.isAbsolute(shell)) throw new RpcFailure(-32000, 'fixture host SHELL is required');
          result = {environment: {id: 'local', cwd: home, shell: {name: path.basename(shell), path: shell}}, shell: {name: path.basename(shell), path: shell}};
          break;
        }
        case 'config/read':
          result = {config: {allow_login_shell: true, shell_environment_policy: {inherit: null, ignore_default_excludes: null, exclude: null, include_only: null, filters: null, set: null, experimental_use_profile: null}, features: {}, mcp_servers: {}}, origins: {}, layers: null};
          break;
        case 'thread/start': case 'thread/resume': case 'thread/fork': {
          const original = value.method === 'thread/start' ? null : lookup(params.threadId);
          const thread = startThread(peer, params, original, value.method === 'thread/fork');
          result = {thread: view(thread), model: thread.model, modelProvider: 'fixture', reasoningEffort: 'medium', cwd: thread.cwd, approvalPolicy: params.approvalPolicy ?? 'on-request', sandbox: {type: 'workspaceWrite'}};
          peer.reply(id, result);
          for (const other of peers) if (other !== peer) other.send({jsonrpc: '2.0', method: 'thread/started', params: {thread: view(thread)}});
          peer.send({jsonrpc: '2.0', method: 'thread/status/changed', params: {threadId: 'unrelated-native-thread', status: {type: 'active', activeFlags: ['waitingOnApproval']}}});
          statusChanged(thread);
          return;
        }
        case 'thread/read': result = {thread: view(lookup(params.threadId), params.includeTurns === true)}; break;
        case 'thread/list': result = page([...threads.values()].map(thread => view(thread)), params); break;
        case 'thread/loaded/list': result = page([...threads.values()].filter(thread => thread.loaded).map(thread => thread.id), params); break;
        case 'thread/unsubscribe': result = {status: unsubscribe(peer, lookup(params.threadId)) ? 'unsubscribed' : 'notSubscribed'}; break;
        case 'thread/goal/get': result = {goal: lookup(params.threadId).goal}; break;
        case 'thread/goal/set': {
          const thread = lookup(params.threadId, true);
          if (!['active', 'paused', 'complete', 'blocked', 'usageLimited', 'budgetLimited'].includes(params.status)) invalid('unsupported goal status');
          thread.goal = {threadId: thread.id, status: params.status}; result = {goal: thread.goal}; persist(); break;
        }
        case 'thread/backgroundTerminals/list': result = page([...lookup(params.threadId).terminals.values()], params); break;
        case 'thread/backgroundTerminals/terminate': result = {terminated: lookup(params.threadId).terminals.delete(params.processId)}; break;
        case 'thread/turns/list': {
          const thread = lookup(params.threadId);
          if (!thread.turns.length) throw new RpcFailure(-32600, `thread ${thread.id} is not materialized yet; thread/turns/list is unavailable before first user message`);
          result = page(params.sortDirection === 'asc' ? thread.turns : [...thread.turns].reverse(), params); break;
        }
        case 'turn/start': dispatchPrompt(lookup(params.threadId, true), peer, params, id); return;
        case 'turn/interrupt': {
          const thread = lookup(params.threadId, true);
          if (!thread.active || thread.active.id !== params.turnId) throw new RpcFailure(-32600, 'turn is not active');
          peer.reply(id, {}); complete(thread, true); return;
        }
        default: throw new RpcFailure(-32601, `unsupported fixture method: ${value.method}`);
      }
      peer.reply(id, result);
    } catch (error) {
      if (id === undefined) peer.fail();
      else peer.error(id, error instanceof RpcFailure ? error.code : -32603, error instanceof RpcFailure ? error.message : 'fixture operation failed');
    }
  }
  const server = net.createServer(socket => {
    if (connections.size >= 64) { socket.destroy(); return; }
    connections.add(socket);
    socket.once('close', () => connections.delete(socket));
    handshake(socket, ({first, fields}) => {
      const key = fields.get('sec-websocket-key');
      if (first !== 'GET / HTTP/1.1' || fields.get('upgrade')?.toLowerCase() !== 'websocket' || !fields.get('connection')?.toLowerCase().split(/\s*,\s*/).includes('upgrade') || fields.get('sec-websocket-version') !== '13' || !key || !/^[A-Za-z0-9+/]{22}==$/.test(key) || Buffer.from(key, 'base64').length !== 16) throw new Error('invalid upgrade');
      socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${createHash('sha1').update(key + guid).digest('base64')}\r\n\r\n`);
    }).then(rest => {
      const peer = new Peer(socket, false, rest);
      peer.subscriptions = new Set(); peer.requests = new Map(); peer.initialized = false;
      peers.add(peer); peer.listen(value => receive(peer, value));
      peer.onclose = () => { peers.delete(peer); for (const id of [...peer.subscriptions]) unsubscribe(peer, threads.get(id)); };
    }).catch(() => socket.destroy());
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(endpoint, resolve); });
  fs.chmodSync(endpoint, 0o600);
  const inode = fs.statSync(endpoint);
  let phase;
  const timer = setInterval(() => {
    let next;
    try { next = fs.readFileSync(path.join(home, 'phase'), 'utf8').trim(); } catch { return; }
    if (next === phase) return;
    phase = next;
    for (const thread of threads.values()) {
      if (!thread.loaded) continue;
      if (next === 'approval') requestInput(thread, thread.subscribers.values().next().value, 'approve');
      else if (next === 'idle') complete(thread);
      else if (next === 'quit') notify(thread, 'fixture/quit');
    }
    persist();
  }, 30);
  let stopped = false;
  function stop() {
    if (stopped) return; stopped = true; clearInterval(timer);
    for (const socket of connections) socket.destroy();
    server.close();
    try { const now = fs.statSync(endpoint); if (now.dev === inode.dev && now.ino === inode.ino) fs.unlinkSync(endpoint); } catch {}
  }
  process.once('SIGTERM', stop); process.once('SIGINT', stop);
  return {server, stop};
}

function cliOptions(args) {
  const config = {}, params = {}, prompts = [];
  let mode = 'thread/start', nativeId;
  for (let index = 0; index < args.length; index++) {
    const arg = args[index];
    if (arg === '--remote') { index++; continue; }
    if (arg === 'resume' || arg === 'fork') { mode = arg === 'resume' ? 'thread/resume' : 'thread/fork'; nativeId = args[++index]; continue; }
    if (arg === '--model' || arg === '-m') { params.model = args[++index]; continue; }
    if (arg === '--sandbox' || arg === '-s') { params.sandbox = args[++index]; continue; }
    if (arg === '--ask-for-approval' || arg === '-a') { params.approvalPolicy = args[++index]; continue; }
    if (arg === '--config' || arg === '-c') {
      const entry = args[++index] ?? '', equals = entry.indexOf('=');
      if (equals <= 0) throw new Error('invalid fixture config option');
      const key = entry.slice(0, equals), raw = entry.slice(equals + 1);
      let value;
      try { value = JSON.parse(raw); }
      catch { if (raw.startsWith("'") && raw.endsWith("'")) value = raw.slice(1, -1); else throw new Error('fixture config supports JSON-compatible TOML values only'); }
      config[key] = value; continue;
    }
    if (arg === '--enable' || arg === '--disable') { config[`features.${args[++index]}`] = arg === '--enable'; continue; }
    if (arg === '--add-dir' || arg === '--profile' || arg === '--model-provider') { index++; continue; }
    if (arg === '--') { prompts.push(...args.slice(index + 1)); break; }
    if (arg.startsWith('-')) {
      if (!['--no-alt-screen', '--verbose', '--debug', '--full-auto', '--dangerously-bypass-approvals-and-sandbox'].includes(arg)) throw new Error(`unsupported fixture option: ${arg}`);
    } else prompts.push(arg);
  }
  if (mode !== 'thread/start' && !identifier(nativeId)) throw new Error('resume/fork identity required');
  return {mode, params: {...params, config, ...(nativeId ? {threadId: nativeId} : {})}, prompt: prompts.join(' ')};
}

export async function tui(endpoint, args) {
  const peer = await connectUnix(endpoint);
  try {
  const options = cliOptions(args);
  let threadId, turnId, composer = '', escape = '', paste = false;
  const images = [], textDecoder = new StringDecoder('utf8');
  let pendingRequest;
  peer.listen(value => {
    if (value.method && value.id !== undefined) {
      pendingRequest = value;
      process.stdout.write(`REQUEST:${value.method}\r\n`);
    } else if (value.params?.threadId === threadId) {
      if (value.method === 'turn/started') turnId = value.params.turn.id;
      if (value.method === 'turn/completed') { turnId = undefined; pendingRequest = undefined; }
      if (value.method === 'item/agentMessage/delta') process.stdout.write(`${value.params.delta}\r\n`);
      if (value.method === 'fixture/quit') { peer.socket.end(); process.exitCode = 0; process.stdin.destroy(); }
    }
  });
  await peer.call('initialize', {clientInfo: {name: 'fixture_tui', version: '1'}, capabilities: {experimentalApi: true}});
  peer.send({method: 'initialized'});
  const result = await peer.call(options.mode, {cwd: process.cwd(), ...options.params});
  threadId = result.thread.id;
  fs.writeFileSync(path.join(process.env.HOME, 'codex.args'), `${args.join('\n')}\n`, {mode: 0o600});
  fs.appendFileSync(path.join(process.env.HOME, 'codex.starts'), `${process.env.TOWER_AGENT_ID ?? ''}\n`, {mode: 0o600});
  if (process.stdin.isTTY) process.stdin.setRawMode(true);
  process.stdout.write('READY\r\n\x1b[?2004h');
  async function submit(text, attachments = []) {
    await peer.call('turn/start', {threadId, input: [{type: 'text', text, text_elements: []}, ...attachments.map(image => ({type: 'localImage', path: image}))]});
  }
  if (options.prompt) await submit(options.prompt);
  let input = Promise.resolve();
  function pasted(bytesText) {
    const text = decoder.decode(Buffer.from(bytesText, 'latin1'));
    if (text.startsWith('file://')) {
      const filename = fileURLToPath(text), metadata = fs.statSync(filename);
      if (!metadata.isFile() || metadata.size > 16 * 1024 * 1024 || images.length >= 16) throw new Error('fixture image limit');
      const bytes = fs.readFileSync(filename);
      images.push(filename);
      process.stdout.write(`IMAGE:${createHash('sha256').update(bytes).digest('hex')}\r\n`);
    } else { composer += text; if (Buffer.byteLength(composer) > 32 * 1024) throw new Error('fixture composer too large'); process.stdout.write(`TEXT:${text}\r\n`); }
  }
  process.stdin.on('data', bytes => {
    input = input.then(async () => {
      for (const byte of bytes) {
        const character = String.fromCharCode(byte);
        if (escape || byte === 27) {
          escape += character;
          if (paste) {
            if (escape.endsWith('\x1b[201~')) { pasted(escape.slice(6, -6)); escape = ''; paste = false; }
          } else if (escape === '\x1b[200~') paste = true;
          else if (escape.length > 2 && /[A-Za-z~]$/.test(escape)) escape = '';
          if (escape.length > MAX_FRAME) throw new Error('fixture input too large');
          continue;
        }
        if (byte === 3) { if (turnId) await peer.call('turn/interrupt', {threadId, turnId}); continue; }
        if (byte === 13 || byte === 10) {
          process.stdout.write('SUBMIT\r\n');
          if (pendingRequest) {
            const request = pendingRequest; pendingRequest = undefined;
            const result = request.method === 'item/commandExecution/requestApproval' ? {decision: composer.toLowerCase().startsWith('n') ? 'decline' : 'accept'} : request.method === 'item/tool/requestUserInput' ? {answers: {decision: {answers: ['yes']}}} : {action: 'accept'};
            peer.reply(request.id, result); composer = '';
          } else if (composer || images.length) { const text = composer; composer = ''; await submit(text, images.splice(0)); }
        } else { const text = textDecoder.write(Buffer.from([byte])); composer += text; if (Buffer.byteLength(composer) > 32 * 1024) throw new Error('fixture composer too large'); if (text) process.stdout.write(`KEY:${text}\r\n`); }
      }
    }).catch(error => { process.stderr.write(`fixture TUI: ${error.message}\n`); peer.socket.destroy(); process.exitCode = 1; process.stdin.destroy(); });
  });
  let phase;
  const phaseTimer = setInterval(() => {
    let next;
    try { next = fs.readFileSync(path.join(process.env.HOME, 'phase'), 'utf8').trim(); } catch { return; }
    if (next === phase) return;
    phase = next;
    if (next === 'work' && !turnId) {
      input = input.then(() => submit('wait')).catch(error => {
        process.stderr.write(`fixture TUI phase: ${error.message}\n`);
        peer.socket.destroy(); process.exitCode = 1; process.stdin.destroy();
      });
    }
  }, 30);
  process.stdin.on('end', () => peer.socket.end());
  peer.onclose = () => { clearInterval(phaseTimer); process.stdin.destroy(); };
  } catch (error) { peer.socket.destroy(); throw error; }
}

const reachable = socket => new Promise(resolve => {
  const probe = net.connect(socket, () => { probe.destroy(); resolve(true); });
  probe.on('error', () => resolve(false));
});

// Mirrors `codex app-server daemon start|stop` with its default socket.
async function daemon(command) {
  const root = path.join(process.env.CODEX_HOME || path.join(process.env.HOME, '.codex'), 'app-server-control');
  const socket = path.join(root, 'app-server-control.sock');
  const pidFile = path.join(root, 'fixture-daemon.pid');
  if (command === 'start') {
    fs.mkdirSync(root, {recursive: true, mode: 0o700});
    if (!await reachable(socket)) {
      const child = spawn(process.execPath, [fileURLToPath(import.meta.url), 'app-server', '--listen', `unix://${socket}`], {detached: true, stdio: 'ignore'});
      fs.writeFileSync(pidFile, String(child.pid));
      child.unref();
      for (let tries = 0; tries < 400 && !await reachable(socket); tries++) await new Promise(resolve => setTimeout(resolve, 25));
    }
    process.stdout.write(`${JSON.stringify({status: 'started', socketPath: socket})}\n`);
  } else if (command === 'stop') {
    try { process.kill(Number(fs.readFileSync(pidFile, 'utf8'))); } catch {}
    fs.rmSync(pidFile, {force: true});
  } else {
    throw new Error(`unsupported fixture daemon command ${command}`);
  }
}

async function main() {
  const args = process.argv.slice(2);
  if (args[0] === '--version' || args[0] === '-V') { process.stdout.write('codex-cli 0.166.0\n'); return; }
  if (args[0] === 'queue') { process.exitCode = 2; return; }
  if (args[0] === 'app-server' && args[1] === 'daemon') { await daemon(args[2]); return; }
  if (args[0] === 'app-server') {
    const endpoint = args[args.indexOf('--listen') + 1];
    if (!endpoint?.startsWith('unix://')) throw new Error('fixture app-server requires --listen unix://PATH');
    await serve(endpoint.slice(7));
  } else {
    const endpoint = args[args.indexOf('--remote') + 1];
    if (!endpoint?.startsWith('unix://')) throw new Error('fixture TUI requires --remote unix://PATH');
    await tui(endpoint.slice(7), args);
  }
}
if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  main().catch(error => { process.stderr.write(`codex fixture: ${error.message}\n`); process.exitCode = 1; });
}
