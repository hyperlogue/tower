import fs from 'node:fs';
import net from 'node:net';
import { spawn } from 'node:child_process';

if (process.argv.includes('-G')) process.exit(0);
const config = JSON.parse(fs.readFileSync(process.argv[1] + '.json', 'utf8'));
const command = process.argv.at(-1)?.match(/^git-(upload|receive)-pack /)?.[1];
const socket = process.env.SSH_AUTH_SOCK;
if (!command || !socket) process.exit(71);
const agent = await new Promise((resolve, reject) => {
  const client = net.createConnection(socket);
  client.once('data', bytes => { client.destroy(); resolve(bytes.toString()); });
  client.once('error', reject);
  client.setTimeout(2000, () => client.destroy(new Error('agent timeout')));
}).catch(() => null);
if (!agent) process.exit(72);
fs.appendFileSync(config.log, JSON.stringify({command, agent, cwd:process.cwd()}) + '\n');
if (command === 'receive') {
  while (fs.existsSync(config.hold)) await new Promise(resolve => setTimeout(resolve, 10));
}
const child = spawn(`git-${command}-pack`, [config.remote], {stdio:'inherit'});
const code = await new Promise(resolve => child.on('exit', (code) => resolve(code ?? 128)));
if (command === 'receive' && fs.existsSync(config.lose)) fs.unlinkSync(socket);
process.exit(code);
