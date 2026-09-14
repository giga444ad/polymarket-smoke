const { spawn } = require('child_process');

function run(name, cmd, args) {
  const proc = spawn(cmd, args, { stdio: 'inherit' });
  proc.on('exit', (code) => {
    console.error(`[${name}] упал с кодом ${code}, перезапуск через 3с`);
    setTimeout(() => run(name, cmd, args), 3000);
  });
  return proc;
}

const main = run('main', 'node', ['dist/main.js']);
const recorder = run('recorder', 'node', ['dist/scripts/tick-recorder.js']);

process.on('SIGTERM', () => { main.kill('SIGTERM'); recorder.kill('SIGTERM'); process.exit(0); });
process.on('SIGINT', () => { main.kill('SIGINT'); recorder.kill('SIGINT'); process.exit(0); });