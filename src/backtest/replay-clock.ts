import { IClock } from '../trading/entry-gate.engine';

/**
 * BACKTEST-PLAN.md, п.2.2 ("Виртуальные часы").
 *
 * Живой путь везде использует `Date.now()` (напрямую или через
 * `SYSTEM_CLOCK`, см. entry-gate.engine.ts). Бэктест не может использовать
 * системное время — вся логика гейта/тиров должна думать, что "сейчас"
 * это конкретный исторический момент, который мы реплеим. `ReplayClock` —
 * простейшая реализация `IClock`: держит один изменяемый `currentMs`,
 * которым явно управляет BacktestRunnerService по мере прохода по
 * сохранённым тикам.
 */
export class ReplayClock implements IClock {
  private currentMs: number;

  constructor(initialMs: number) {
    this.currentMs = initialMs;
  }

  now(): number {
    return this.currentMs;
  }

  /** Продвинуть виртуальные часы вперёд (реплей никогда не идёт назад). */
  advanceTo(ms: number): void {
    if (ms > this.currentMs) this.currentMs = ms;
  }
}
