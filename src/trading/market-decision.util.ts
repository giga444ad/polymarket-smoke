import { Outcome } from '../polymarket/market-ws-stream';

/**
 * Сессия 16 (модуль бэктеста, см. BACKTEST-PLAN.md).
 *
 * Чистые функции без какой-либо зависимости от NestJS DI, MarketState или
 * времени "снаружи" (там, где нужна текущая метка времени, она передаётся
 * явным аргументом, а не берётся через Date.now()) — вынесены из
 * TradingService один-в-один (логика не переписана, только перемещена),
 * чтобы BacktestRunnerService мог их переиспользовать буквально, а не
 * реализовывать "по мотивам" (см. требование пользователя в BACKTEST-PLAN.md
 * — честный повтор существующей логики, а не отдельная модель).
 */

export type LimitTier = 'T1' | 'T2' | 'T3';

export function computeTier(timeLeftSec: number, tier2Seconds: number, tier3Seconds: number): LimitTier {
  if (timeLeftSec > tier2Seconds) return 'T1';
  if (timeLeftSec > tier3Seconds) return 'T2';
  return 'T3';
}

export function pickFavorite(books: Record<Outcome, { bestBid: number | null }>): Outcome | null {
  const yes = books.YES.bestBid;
  const no = books.NO.bestBid;
  if (yes == null && no == null) return null;
  if (yes == null) return 'NO';
  if (no == null) return 'YES';
  return yes >= no ? 'YES' : 'NO';
}

export function roundToTick(price: number, tickSizeStr: string): number {
  const tick = parseFloat(tickSizeStr) || 0.01;
  const decimals = (tickSizeStr.split('.')[1] ?? '').length || 2;
  let rounded = Math.round(price / tick) * tick;
  const max = 1 - tick;
  rounded = Math.min(Math.max(rounded, tick), max);
  return Number(rounded.toFixed(decimals));
}

/** Сколько $ нужно набрать резюм-лимиткой, чтобы удовлетворить минимум биржи
 *  (не капая эту сумму обратно до стейка шага — иначе проверка допустимого
 *  перерасхода в вызывающем коде никогда не сработает). */
export function limitOrderTargetUsd(betAmount: number, minOrderSize: number, price: number): number {
  const minUsd = minOrderSize * price;
  return Math.max(betAmount, minUsd);
}
