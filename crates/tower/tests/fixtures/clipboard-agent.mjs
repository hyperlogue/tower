#!/usr/bin/env node
import fs from 'node:fs';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
if (process.argv[2] === 'queue') process.exit(1);
process.stdin.setRawMode(true);
spawnSync(process.env.TOWER_TEST_BIN, ['hook', process.argv[1].includes('claude') ? 'claude-code' : 'codex', 'SessionStart'], {input: JSON.stringify({session_id:'fixture-native', cwd:process.cwd()})});
process.stdout.write('READY\r\n\x1b[?2004h');
let pending = '';
process.stdin.on('data', bytes => {
  for (const byte of bytes) {
    pending += String.fromCharCode(byte);
    let image;
    if (pending === '\x16') {
      const read = spawnSync('xclip', ['-selection','clipboard','-t','image/png','-o']);
      image = read.status === 0 ? read.stdout : Buffer.alloc(0);
      pending = '';
    } else if (pending.startsWith('\x1b[200~') && pending.endsWith('\x1b[201~')) {
      const text = pending.slice(6, -6);
      if (text.startsWith('file://')) image = fs.readFileSync(fileURLToPath(text));
      else process.stdout.write(`TEXT:${pending}`);
      pending = '';
    } else if (!pending.startsWith('\x1b')) {
      process.stdout.write(`KEY:${String.fromCharCode(byte)}\r\n`);
      pending = '';
    }
    if (image !== undefined) process.stdout.write(`IMAGE:${createHash('sha256').update(image).digest('hex')}\r\n`);
  }
});
