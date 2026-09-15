/**
 * Драйвер поверх /backtest/run (BacktestModule, Сессия 16). Сам /backtest/run
 * НИЧЕГО не подбирает — это чистый "что если?" на ОДНОЙ переданной комбинации
 * envOverrides. Этот скрипт перебирает сетку значений параметров (декартово
 * произведение), гоняет её по КАЖДОМУ бэкенду/потоку из конфига и сводит всё
 * в одну таблицу skip/win/loss/profit — чтобы видеть компромисс "порог X ->
 * сколько отсеялось, сколько выиграло" разом по всем 4 сервисам, а не дёргать
 * ручку вручную под каждую комбинацию.
 *
 * ВАЖНО (см. лимитации в самом ответе /backtest/run, CHANGES-session-16.md):
 * это оценка "прошёл бы фильтр или нет" на приближённом исходе окна, а не
 * точный P&L — нет полной глубины стакана, нет сохранённого официального
 * исхода Gamma, нет реинвест-прогрессии между окнами. Используй для сравнения
 * ОТНОСИТЕЛЬНЫХ эффектов порогов (стало лучше/хуже), а не абсолютных цифр.
 *
 * Запуск:
 *   npx tsx scripts/backtest-sweep.ts scripts/backtest-sweep.config.json
 * (по умолчанию, если путь не передан, ищет scripts/backtest-sweep.config.json —
 *  сам файл в .gitignore, т.к. содержит логин/пароль; см. .example рядом).
 *
 * Требует роль admin (POST /backtest/run защищён @Roles(Role.ADMIN), см.
 * src/auth) — скрипт логинится один раз через /auth/login на authBaseUrl
 * (главный, IS_MAIN=true, сервис) и использует один и тот же токен для всех
 * 4 бэкендов (не-main сервисы сами сходят на него для проверки, см.
 * README-AUTH.md).
 */
import 'dotenv/config';
import * as fs from 'fs';
import axios from 'axios';

interface BackendCfg {
  label: string;
  baseUrl: string;
  streamKeys: string[];
}

interface SweepConfig {
  authBaseUrl: string;
  username: string;
  password: string;
  from: string;
  to: string;
  backends: BackendCfg[];
  baseEnvOverrides?: Record<string, string>;
  paramGrid: Record<string, string[]>;
  // Не бомбить бэкенд параллельно десятками реплеев сразу — по умолчанию 2.
  concurrency?: number;
}

interface ResultRow {
  backend: string;
  streamKey: string;
  combo: string;
  windows: number;
  wins: number;
  losses: number;
  skipped: number;
  unfilled: number;
  winRate: string;
  totalProfit: number;
  avgEntryPrice: string;
}

function cartesianProduct(grid: Record<string, string[]>): Record<string, string>[] {
  const keys = Object.keys(grid);
  if (keys.length === 0) return [{}];
  return keys.reduce<Record<string, string>[]>(
    (acc, key) => acc.flatMap((combo) => grid[key].map((value) => ({ ...combo, [key]: value }))),
    [{}],
  );
}

async function login(cfg: SweepConfig): Promise<string> {
  const { data } = await axios.post(`${cfg.authBaseUrl.replace(/\/+$/, '')}/auth/login`, {
    username: cfg.username,
    password: cfg.password,
  });
  return data.accessToken as string;
}

async function runOne(
  baseUrl: string,
  token: string,
  streamKey: string,
  from: string,
  to: string,
  envOverrides: Record<string, string>,
): Promise<any> {
  const { data } = await axios.post(
    `${baseUrl.replace(/\/+$/, '')}/backtest/run`,
    { streamKey, from, to, envOverrides },
    { headers: { Authorization: `Bearer ${token}` }, timeout: 120_000 },
  );
  return data;
}

// Простой ограничитель параллелизма без внешних зависимостей.
async function mapWithConcurrency<T, R>(items: T[], limit: number, fn: (item: T, idx: number) => Promise<R>): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let cursor = 0;
  async function worker() {
    while (cursor < items.length) {
      const idx = cursor++;
      results[idx] = await fn(items[idx], idx);
    }
  }
  await Promise.all(new Array(Math.min(limit, items.length)).fill(0).map(worker));
  return results;
}

function comboLabel(combo: Record<string, string>): string {
  const keys = Object.keys(combo);
  if (keys.length === 0) return '(база, без override)';
  return keys.map((k) => `${k}=${combo[k]}`).join(', ');
}

async function main() {
  const configPath = process.argv[2] || 'scripts/backtest-sweep.config.json';
  if (!fs.existsSync(configPath)) {
    console.error(
      `Конфиг не найден: ${configPath}\n` +
        `Скопируй scripts/backtest-sweep.config.example.json -> scripts/backtest-sweep.config.json и заполни логин/пароль/бэкенды.`,
    );
    process.exit(1);
  }
  const cfg: SweepConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  const concurrency = cfg.concurrency ?? 2;

  console.log(`Логин на ${cfg.authBaseUrl}...`);
  const token = await login(cfg);
  console.log('OK, токен получен.\n');

  const combos = cartesianProduct(cfg.paramGrid);
  console.log(`Комбинаций параметров: ${combos.length}`);

  // Задачи = (бэкенд × его потоки × комбинации) — плоский список, чтобы
  // раздать в общий пул воркеров (не по одному бэкенду последовательно).
  interface Task {
    backend: BackendCfg;
    streamKey: string;
    combo: Record<string, string>;
  }
  const tasks: Task[] = [];
  for (const backend of cfg.backends) {
    for (const streamKey of backend.streamKeys) {
      for (const combo of combos) {
        tasks.push({ backend, streamKey, combo });
      }
    }
  }
  console.log(`Всего запусков /backtest/run: ${tasks.length} (concurrency=${concurrency})\n`);

  let done = 0;
  const rows = await mapWithConcurrency(tasks, concurrency, async (task) => {
    const envOverrides = { ...(cfg.baseEnvOverrides ?? {}), ...task.combo };
    try {
      const summary = await runOne(task.backend.baseUrl, token, task.streamKey, cfg.from, cfg.to, envOverrides);
      done += 1;
      process.stdout.write(`\r[${done}/${tasks.length}] ${task.backend.label}/${task.streamKey} — ${comboLabel(task.combo)}          `);
      const decided = summary.wins + summary.losses;
      const row: ResultRow = {
        backend: task.backend.label,
        streamKey: task.streamKey,
        combo: comboLabel(task.combo),
        windows: summary.windowsTotal,
        wins: summary.wins,
        losses: summary.losses,
        skipped: summary.skipped,
        unfilled: summary.unfilled,
        winRate: decided > 0 ? `${((summary.wins / decided) * 100).toFixed(1)}%` : '—',
        totalProfit: Number(summary.totalProfit.toFixed(2)),
        avgEntryPrice: summary.avgEntryPrice != null ? summary.avgEntryPrice.toFixed(4) : '—',
      };
      return row;
    } catch (err: any) {
      done += 1;
      const message = err?.response?.data?.message || err.message;
      console.error(`\n[ОШИБКА] ${task.backend.label}/${task.streamKey} — ${comboLabel(task.combo)}: ${message}`);
      return {
        backend: task.backend.label,
        streamKey: task.streamKey,
        combo: comboLabel(task.combo),
        windows: 0,
        wins: 0,
        losses: 0,
        skipped: 0,
        unfilled: 0,
        winRate: 'ERROR',
        totalProfit: 0,
        avgEntryPrice: '—',
      } as ResultRow;
    }
  });

  console.log('\n\n=== Сводная таблица ===');
  console.table(rows);

  const csvHeader = 'backend,streamKey,combo,windows,wins,losses,skipped,unfilled,winRate,totalProfit,avgEntryPrice';
  const csvLines = rows.map((r) =>
    [r.backend, r.streamKey, `"${r.combo}"`, r.windows, r.wins, r.losses, r.skipped, r.unfilled, r.winRate, r.totalProfit, r.avgEntryPrice].join(','),
  );
  const outPath = `scripts/backtest-sweep-result-${Date.now()}.csv`;
  fs.writeFileSync(outPath, [csvHeader, ...csvLines].join('\n'), 'utf8');
  console.log(`\nCSV сохранён: ${outPath}`);
}

main().catch((err) => {
  console.error('Ошибка:', err?.response?.data ?? err);
  process.exit(1);
});
