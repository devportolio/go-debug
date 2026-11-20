#!/usr/bin/env node
// Runs a command with GoDebug loaded:  go-debug yarn start:dev wishlists
// With no command, serves the page on its own:  go-debug
const { spawn } = require('node:child_process');
const path = require('node:path');

const register = path.join(__dirname, '..', 'register.cjs');
const [command, ...args] = process.argv.slice(2);
if (command === '--help' || command === '-h') {
  console.log(
    'Usage:\n  go-debug                           serve the page on its own\n' +
      '  go-debug <command> [arguments...]  run a service with GoDebug loaded\n' +
      'Example: go-debug yarn start:dev wishlists',
  );
  process.exit(0);
}
if (!command) {
  process.env.GO_DEBUG_HUB = '1';
  require(register);
  return;
}

const flag = `--require ${JSON.stringify(register)}`;
const child = spawn(command, args, {
  stdio: 'inherit',
  env: { ...process.env, NODE_OPTIONS: [process.env.NODE_OPTIONS, flag].filter(Boolean).join(' ') },
});
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => child.kill(signal));
}
child.on('exit', (code, signal) => process.exit(code ?? (signal ? 1 : 0)));
