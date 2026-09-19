/**
 * Сессия 20 — CLI для Монте-Карло симулятора стратегии (тот же движок, что и
 * /api/simulate/montecarlo, см. src/simulator/montecarlo.util.ts). Удобно
 * прогнать сценарий из терминала без фронта.
 *
 * Пример:
 *   WIN=0.985 C=0.9679 T=10 BASE=5 BETS_DAY=100 DAYS=30 RUNS=5000 \
 *     npx tsx scripts/sim-strategy.ts
 * или несколько винрейтов сразу:
 *   WINS=0.965,0.975,0.985,0.995 C=0.9679 npx tsx scripts/sim-strategy.ts
 */
import { runMonteCarlo, MonteCarloParams } from '../src/simulator/montecarlo.util';

const n = (v: string | undefined, def: number) => (v != null && v !== '' ? Number(v) : def);

const base = n(process.env.BASE, 5);
const c = n(process.env.C, 0.9679);
const cMax = process.env.CMAX ? Number(process.env.CMAX) : undefined;
const closeMode = process.env.MODE === 'steps' ? 'steps' : 'profit';
const T = n(process.env.T, 10);
const steps = n(process.env.STEPS, 60);
const bankroll = n(process.env.BANKROLL, 500);
const betsPerDay = n(process.env.BETS_DAY, 100);
const days = n(process.env.DAYS, 30);
const runs = n(process.env.RUNS, 5000);

const wins = (process.env.WINS ?? process.env.WIN ?? '0.985')
  .split(',').map((s) => Number(s.trim())).filter((x) => Number.isFinite(x));

console.log(
  `Сценарий: base=$${base}, c=${c}${cMax ? `..${cMax}` : ''}, closeMode=${closeMode}, ` +
    `${closeMode === 'profit' ? `target=$${T}` : `targetSteps=${steps}`}, bankroll=$${bankroll}, ` +
    `${betsPerDay} ставок/день × ${days}дн, runs=${runs}`,
);
console.log(`Точка безубытка (винрейт) = цена входа = ${cMax ? ((c + cMax) / 2).toFixed(4) : c}\n`);

const f = (x: number) => (x >= 0 ? '+' : '') + x.toFixed(2);
for (const w of wins) {
  const p: MonteCarloParams = {
    baseStake: base, winRate: w, entryPrice: c, entryPriceMax: cMax, closeMode,
    targetProfitUsd: T, targetSteps: steps, startingBankroll: bankroll,
    betsPerDay, horizonDays: days, runs, seed: 12345,
  };
  const r = runMonteCarlo(p);
  console.log(
    `w=${(w * 100).toFixed(1)}% (edge ${f(r.edge * 100)}пп): ` +
      `$/день медиана ${f(r.perDay.medianProfitUsd)} (сред ${f(r.perDay.meanProfitUsd)}, ` +
      `p5 ${f(r.perDay.p5)} / p95 ${f(r.perDay.p95)}), ` +
      `итог за ${days}дн медиана ${f(r.finalPnl.median)}, ` +
      `руин ${(r.ruinProbability * 100).toFixed(1)}%, ` +
      `просадка(сред) $${r.maxDrawdownUsd.mean.toFixed(0)}, ` +
      `банк/фейл ${r.attempts.completedMean.toFixed(1)}/${r.attempts.failedMean.toFixed(1)}`,
  );
}
