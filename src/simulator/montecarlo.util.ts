/**
 * Сессия 20 — Монте-Карло симулятор доходности стратегии (реинвест +
 * фиксация). Чистая функция, без DI/БД — чтобы её можно было гонять и из
 * контроллера (/api/simulate/montecarlo для фронта polyguru), и из скрипта.
 *
 * МОДЕЛЬ (ровно механика бота, см. TradingService.resolvePendingMarkets):
 *  - Ставка стартует с baseStake. На ВЫИГРЫШЕ капитал реинвестируется:
 *    следующая ставка = выплата = stake / c (где c = цена входа), профит шага
 *    = stake·(1-c)/c. На ПРОИГРЫШЕ теряем текущую ставку и сбрасываемся на
 *    baseStake (новая попытка).
 *  - Фиксация (middle-fix): при closeMode='profit' банкуем, когда накопленный
 *    профит попытки ≥ targetProfitUsd; при 'steps' — когда число выигрышных
 *    шагов ≥ targetSteps. После банка — сброс на baseStake.
 *
 * МАТЕМАТИЧЕСКИЙ ЯКОРЬ (доказано аналитически, симулятор его лишь
 * подтверждает и добавляет дисперсию):
 *  - Проигрышная попытка стоит РОВНО baseStake, независимо от длины серии
 *    (реинвест катит только выигрыши; исходный base — единственное, чем
 *    реально рискуешь).
 *  - Точка безубытка по винрейту = РОВНО цена входа c, независимо от
 *    targetProfit/baseStake/шагов. w>c → +EV, w<c → −EV. Ставочная схема
 *    edge НЕ создаёт — она меняет только дисперсию и скорость роста.
 */

export interface MonteCarloParams {
  baseStake: number; // напр. 5
  winRate: number; // w, истинная вероятность выигрыша на взятой сделке (0..1)
  entryPrice: number; // c, средняя цена входа (0..1). Точка безубытка = c.
  entryPriceMax?: number; // если задано — c берётся равномерно из [entryPrice, entryPriceMax] на каждую ставку
  closeMode: 'profit' | 'steps';
  targetProfitUsd: number; // для closeMode='profit'
  targetSteps: number; // для closeMode='steps'
  startingBankroll: number; // для оценки риска руина (руин = банкролл ≤ 0)
  betsPerDay: number; // сколько РЕАЛЬНЫХ входов в день (placement rate × окон/день) — для перевода в $/день
  horizonDays: number; // сколько дней симулируем
  runs: number; // число прогонов Монте-Карло
  seed?: number; // для воспроизводимости
}

export interface MonteCarloResult {
  inputs: MonteCarloParams;
  breakEvenWinRate: number; // = средняя entryPrice (аналитический якорь)
  edge: number; // winRate − breakEvenWinRate (>0 = +EV)
  betsPerRun: number;
  perDay: { meanProfitUsd: number; medianProfitUsd: number; p5: number; p25: number; p75: number; p95: number };
  finalPnl: { mean: number; median: number; p5: number; p95: number };
  ruinProbability: number; // доля прогонов, где банкролл коснулся ≤ 0
  maxDrawdownUsd: { mean: number; p95: number }; // просадка от пика внутри прогона
  attempts: { completedMean: number; failedMean: number; completionRate: number };
  evPerBetUsd: number; // средний прирост банкролла на одну ставку
}

// Детерминированный ГПСЧ (mulberry32) — воспроизводимость прогонов по seed.
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function percentile(sortedAsc: number[], p: number): number {
  if (sortedAsc.length === 0) return 0;
  const idx = Math.min(sortedAsc.length - 1, Math.max(0, Math.round((p / 100) * (sortedAsc.length - 1))));
  return sortedAsc[idx];
}

export function runMonteCarlo(params: MonteCarloParams): MonteCarloResult {
  const {
    baseStake, winRate, entryPrice, entryPriceMax, closeMode, targetProfitUsd,
    targetSteps, startingBankroll, betsPerDay, horizonDays, runs, seed = 12345,
  } = params;

  const rng = mulberry32(seed);
  const betsPerRun = Math.max(1, Math.round(betsPerDay * horizonDays));
  const avgC = entryPriceMax != null ? (entryPrice + entryPriceMax) / 2 : entryPrice;

  const pnlPerRun: number[] = [];
  const finalBankrolls: number[] = [];
  const maxDDs: number[] = [];
  let ruinCount = 0;
  let totalCompleted = 0;
  let totalFailed = 0;

  for (let r = 0; r < runs; r++) {
    let bankroll = startingBankroll;
    let peak = startingBankroll;
    let maxDD = 0;
    let stake = baseStake;
    let attemptProfit = 0;
    let attemptWins = 0;
    let ruined = false;

    for (let b = 0; b < betsPerRun; b++) {
      // Не можем поставить базовую ставку — фактический руин.
      if (bankroll < baseStake) { ruined = true; break; }
      const c = entryPriceMax != null ? entryPrice + (entryPriceMax - entryPrice) * rng() : entryPrice;

      if (rng() < winRate) {
        const profit = (stake * (1 - c)) / c;
        bankroll += profit;
        attemptProfit += profit;
        attemptWins += 1;
        stake = stake / c; // реинвест полной выплаты
        const banked = closeMode === 'profit' ? attemptProfit >= targetProfitUsd : attemptWins >= targetSteps;
        if (banked) { totalCompleted += 1; stake = baseStake; attemptProfit = 0; attemptWins = 0; }
      } else {
        bankroll -= stake; // теряем всю текущую ставку
        totalFailed += 1;
        stake = baseStake; attemptProfit = 0; attemptWins = 0; // сброс попытки
      }

      if (bankroll > peak) peak = bankroll;
      const dd = peak - bankroll;
      if (dd > maxDD) maxDD = dd;
      if (bankroll <= 0) { ruined = true; break; }
    }

    if (ruined) ruinCount += 1;
    const pnl = bankroll - startingBankroll;
    pnlPerRun.push(pnl);
    finalBankrolls.push(bankroll);
    maxDDs.push(maxDD);
  }

  const sortedPnl = [...pnlPerRun].sort((a, b) => a - b);
  const sortedDD = [...maxDDs].sort((a, b) => a - b);
  const mean = (xs: number[]) => xs.reduce((s, x) => s + x, 0) / Math.max(1, xs.length);
  const meanPnl = mean(pnlPerRun);

  return {
    inputs: params,
    breakEvenWinRate: avgC,
    edge: winRate - avgC,
    betsPerRun,
    perDay: {
      meanProfitUsd: meanPnl / horizonDays,
      medianProfitUsd: percentile(sortedPnl, 50) / horizonDays,
      p5: percentile(sortedPnl, 5) / horizonDays,
      p25: percentile(sortedPnl, 25) / horizonDays,
      p75: percentile(sortedPnl, 75) / horizonDays,
      p95: percentile(sortedPnl, 95) / horizonDays,
    },
    finalPnl: { mean: meanPnl, median: percentile(sortedPnl, 50), p5: percentile(sortedPnl, 5), p95: percentile(sortedPnl, 95) },
    ruinProbability: ruinCount / Math.max(1, runs),
    maxDrawdownUsd: { mean: mean(maxDDs), p95: percentile(sortedDD, 95) },
    attempts: {
      completedMean: totalCompleted / Math.max(1, runs),
      failedMean: totalFailed / Math.max(1, runs),
      completionRate: totalCompleted / Math.max(1, totalCompleted + totalFailed),
    },
    evPerBetUsd: meanPnl / betsPerRun,
  };
}
