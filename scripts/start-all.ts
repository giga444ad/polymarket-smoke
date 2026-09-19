const { spawn } = require('child_process');

function run(name, cmd, args) {
  const proc = spawn(cmd, args, { stdio: 'inherit' });
  proc.on('exit', (code) => {
    console.error(`[${name}] упал с кодом ${code}, перезапуск через 3с`);
    setTimeout(() => run(name, cmd, args), 3000);
  });
  return proc;
}

const main = run('main', 'node', ['dist/src/main.js']);

// Тик-рекордер (price_ticks/polymarket_price_ticks) нужен ТОЛЬКО бэктесту —
// живой бот его не читает (PriceFeedService держит тики в памяти). Он же
// главный источник распухания БД (по трейду на каждый тик). Выключается
// RUN_TICK_RECORDER=false, чтобы на чисто-лайв прогонах не жечь квоту БД.
// По умолчанию включён — прежнее поведение не меняется.
const recorderEnabled = (process.env.RUN_TICK_RECORDER ?? 'true') !== 'false';
const recorder = recorderEnabled ? run('recorder', 'node', ['dist/scripts/tick-recorder.js']) : null;
if (!recorderEnabled) {
  console.log('[start-all] tick-recorder выключен (RUN_TICK_RECORDER=false) — price_ticks писаться не будут.');
}

process.on('SIGTERM', () => { main.kill('SIGTERM'); recorder?.kill('SIGTERM'); process.exit(0); });
process.on('SIGINT', () => { main.kill('SIGINT'); recorder?.kill('SIGINT'); process.exit(0); });