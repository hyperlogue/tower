import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
const { default: install } = await import(pathToFileURL(process.argv[2]));
const handlers = new Map();
install({ on: (name, fn) => handlers.set(name, fn), getSessionName() { throw Error('unavailable'); } });
let id = 'root', calls = 0;
const pending = [];
const target = {
  select(...args) { calls++; assert.equal(this, ui); assert.equal(args[0], 'SECRET-question'); return new Promise((resolve, reject) => pending.push({ resolve, reject })); },
  confirm() { return true; },
  input() { throw Error('SECRET-native-error'); },
};
// Model OMP's get-only proxy: its adapters look up the target at call time.
const adapters = Object.fromEntries(Object.keys(target).map(name => [name, function (...args) { return target[name].apply(this, args); }]));
const ui = new Proxy(target, { get(object, name) { return adapters[name] ?? object[name]; } });
const ctx = { ui, agent: { kind: 'main' }, sessionManager: { getSessionId: () => id }, get model() { throw Error('unavailable'); } };
const event = (name, extra = {}) => handlers.get(name)({ type: name, ...extra }, ctx);
await event('session_start');
await event('session_start');
const first = ui.select('SECRET-question');
assert.equal(calls, 1);
await event('plan_review_requested', { reviewId: 'review' });
await event('tool_approval_requested', { toolCallId: 'tool' });
pending.shift().resolve('SECRET-answer');
assert.equal(await first, 'SECRET-answer');
await event('tool_approval_resolved', { toolCallId: 'tool' });
await event('plan_review_resolved', { reviewId: 'review' });
assert.equal(ui.confirm(), true);
assert.throws(() => ui.input(), /SECRET-native-error/);
const rejected = ui.select('SECRET-question');
pending.shift().reject(Error('SECRET-rejection'));
await assert.rejects(rejected, /SECRET-rejection/);
const old = ui.select('SECRET-question');
id = 'new'; await event('session_switch');
const current = ui.select('SECRET-question');
pending.shift().resolve('old'); await old;
await event('tool_execution_end', {toolName:'after-old-settles'});
pending.shift().resolve('current'); await current;
const childHandlers = new Map();
install({ on: (name, fn) => childHandlers.set(name, fn) });
const wrapped = Object.getOwnPropertyDescriptor(ui, 'select').value;
const child = { ui, agent: { kind: 'sub' }, sessionManager: { getSessionId: () => 'child' } };
await childHandlers.get('session_start')({}, child);
assert.equal(Object.getOwnPropertyDescriptor(ui, 'select').value, wrapped);
await childHandlers.get('plan_review_requested')({ reviewId: 'child-review' }, child);
await childHandlers.get('plan_review_resolved')({ reviewId: 'child-review' }, child);
await event('tool_execution_end');
