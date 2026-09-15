import { Outcome } from '../polymarket/market-ws-stream';
import { FeedSnapshot, PriceAtResult } from '../polymarket/price-feed.service';
import { IPriceSource } from '../trading/entry-gate.engine';

export interface RawTick {
  ts: number; // unix ms
  price: number;
  source: string;
}

interface Candle {
  start: number;
  open: number;
  high: number;
  low: number;
  close: number;
}

/**
 * BACKTEST-PLAN.md, п.2.1 ("Абстракция источника данных" — самое рискованное
 * место плана).
 *
 * Реплеит массив исторических тиков (из `price_ticks`, записанных
 * `scripts/tick-recorder.ts`) ЧЕРЕЗ ТОЧНО ТУ ЖЕ бухгалтерию свечей/ATR/
 * буфера сырых тиков, что и `PriceFeedService` в проде (см. комментарии в
 * price-feed.service.ts — bucketStart = floor(ts/candleMs)*candleMs, ATR =
 * средний размах последних N ЦЕЛЫХ окон потока, recentTicks — скользящий
 * буфер для точечного поиска цены на момент времени X). Реализация здесь
 * умышленно дублирует эти несколько строк (а не наследуется от
 * PriceFeedService — тот класс держит живой WS-коннект и Nest DI, тянуть
 * его в реплей было бы более хрупко, чем повторить 15 строк чистой
 * бухгалтерии), но математика идентична 1-в-1 — любое расхождение здесь
 * с price-feed.service.ts является багом бэктеста, а не "другой моделью".
 *
 * ВАЖНОЕ ОГРАНИЧЕНИЕ (задокументировано, как и просил план — "явно
 * задокументировать как ограничение бэктеста", см. BACKTEST-PLAN.md 2.3):
 * `price_ticks` пишет ВСЕ три источника (chainlink/binance/bybit) НЕЗАВИСИМО
 * и ПАРАЛЛЕЛЬНО (recorder не воспроизводит "один активный провайдер с
 * авто-фолбэком", как это делает живой `PriceFeedService` — см. Сессию 9,
 * зомби-соединение/ротация). Бэктест поэтому реплеит ОДИН источник целиком
 * (по приоритету providerPriority, по умолчанию chainlink -> binance ->
 * bybit — берётся первый, у которого вообще есть тики в запрошенном
 * диапазоне) — переключения провайдера внутри live-бота (Сессия 9) в
 * бэктесте не воспроизводятся. Если и это недоступно — см. `pickSeries`.
 */
export class ReplayPriceSource implements IPriceSource {
  private readonly candleMs: number;
  private readonly atrCandles: number;
  private readonly staleMs: number;
  private readonly recentTicksRetentionMs: number;

  private readonly ticks: RawTick[]; // отсортированы по ts, один выбранный источник
  private cursor = 0;
  private nowMs = -Infinity;

  private lastPrice: number | null = null;
  private lastPriceAt: number | null = null;
  private lastPriceSource: string | null = null;
  private current: Candle | null = null;
  private closed: Candle[] = [];
  private recentTicks: { ts: number; price: number }[] = [];

  /** Тот streamKey, для которого построен этот источник — единственный,
   *  на который отвечает getSnapshot/getPriceAt (бэктест гоняет по одному
   *  потоку за раз, см. BacktestRunnerService). */
  readonly streamKey: string;
  /** Какой реальный источник (`chainlink`/`binance`/`bybit`) реально был
   *  использован для этого прогона — см. pickSeries. */
  readonly usedSource: string | null;

  constructor(
    streamKey: string,
    allTicks: RawTick[],
    opts: { candleMs: number; atrCandles: number; staleMs: number; recentTicksRetentionMs: number; providerPriority: string[] },
  ) {
    this.streamKey = streamKey;
    this.candleMs = opts.candleMs;
    this.atrCandles = opts.atrCandles;
    this.staleMs = opts.staleMs;
    this.recentTicksRetentionMs = opts.recentTicksRetentionMs;

    const { series, source } = ReplayPriceSource.pickSeries(allTicks, opts.providerPriority);
    this.ticks = series;
    this.usedSource = source;
  }

  /** См. класс-комментарий выше — выбирает ЦЕЛИКОМ один источник по приоритету. */
  private static pickSeries(allTicks: RawTick[], priority: string[]): { series: RawTick[]; source: string | null } {
    for (const name of priority) {
      const series = allTicks.filter((t) => t.source === name).sort((a, b) => a.ts - b.ts);
      if (series.length > 0) return { series, source: name };
    }
    // Ни один приоритетный источник не дал ни одного тика — fail-closed
    // философия проекта: лучше честно "нет данных", чем молча смешать
    // источники или выдумать цену.
    return { series: [], source: null };
  }

  hasAnyData(): boolean {
    return this.ticks.length > 0;
  }

  /** Обрабатывает все ещё не обработанные тики с ts <= targetMs — та же
   *  бухгалтерия, что applyTrade() в PriceFeedService (см. класс-комментарий). */
  advanceTo(targetMs: number): void {
    if (targetMs < this.nowMs) {
      throw new Error(
        `ReplayPriceSource[${this.streamKey}]: попытка отмотать виртуальные часы назад (${targetMs} < ${this.nowMs}) — реплей не поддерживает движение назад во времени.`,
      );
    }
    this.nowMs = targetMs;

    while (this.cursor < this.ticks.length && this.ticks[this.cursor].ts <= targetMs) {
      const trade = this.ticks[this.cursor];
      this.cursor += 1;

      this.lastPrice = trade.price;
      this.lastPriceAt = trade.ts;
      this.lastPriceSource = trade.source;

      this.recentTicks.push({ ts: trade.ts, price: trade.price });
      const cutoff = trade.ts - this.recentTicksRetentionMs;
      while (this.recentTicks.length > 0 && this.recentTicks[0].ts < cutoff) {
        this.recentTicks.shift();
      }

      const bucketStart = Math.floor(trade.ts / this.candleMs) * this.candleMs;
      if (!this.current || this.current.start !== bucketStart) {
        if (this.current) {
          this.closed.push(this.current);
          if (this.closed.length > this.atrCandles * 2) {
            this.closed.splice(0, this.closed.length - this.atrCandles * 2);
          }
        }
        this.current = { start: bucketStart, open: trade.price, high: trade.price, low: trade.price, close: trade.price };
      } else {
        this.current.close = trade.price;
        this.current.high = Math.max(this.current.high, trade.price);
        this.current.low = Math.min(this.current.low, trade.price);
      }
    }
  }

  getSnapshot(_streamKey: string): FeedSnapshot {
    const isStale = this.lastPriceAt != null && this.nowMs - this.lastPriceAt > this.staleMs;
    return {
      price: isStale ? null : this.lastPrice,
      priceAt: isStale ? null : this.lastPriceAt,
      atr: this.computeAtr(),
      candleCount: this.closed.length,
      source: isStale ? null : this.lastPriceSource,
    };
  }

  getPriceAt(_streamKey: string, targetMs: number): PriceAtResult {
    if (this.recentTicks.length === 0) return { price: null, tickAt: null, lagMs: null };
    for (let i = this.recentTicks.length - 1; i >= 0; i--) {
      const tick = this.recentTicks[i];
      if (tick.ts <= targetMs) {
        return { price: tick.price, tickAt: tick.ts, lagMs: targetMs - tick.ts };
      }
    }
    return { price: null, tickAt: null, lagMs: null };
  }

  getTimeInZoneRatio(
    _streamKey: string,
    windowStartMs: number,
    nowMs: number,
    referencePrice: number,
    side: Outcome,
  ): number | null {
    const ticks = this.recentTicks;
    if (ticks.length === 0 || ticks[0].ts > windowStartMs) return null;

    let startIdx = -1;
    for (let i = ticks.length - 1; i >= 0; i--) {
      if (ticks[i].ts <= windowStartMs) {
        startIdx = i;
        break;
      }
    }
    if (startIdx === -1) return null;

    let aboveMs = 0;
    let belowMs = 0;
    let curPrice = ticks[startIdx].price;
    let curTs = windowStartMs;

    for (let i = startIdx + 1; i < ticks.length && curTs < nowMs; i++) {
      const tick = ticks[i];
      if (tick.ts <= windowStartMs) continue;
      const segEnd = Math.min(tick.ts, nowMs);
      const dur = segEnd - curTs;
      if (dur > 0) {
        if (curPrice > referencePrice) aboveMs += dur;
        else belowMs += dur;
      }
      curPrice = tick.price;
      curTs = segEnd;
    }
    if (curTs < nowMs) {
      const dur = nowMs - curTs;
      if (curPrice > referencePrice) aboveMs += dur;
      else belowMs += dur;
    }

    const total = aboveMs + belowMs;
    if (total <= 0) return null;
    const aboveRatio = aboveMs / total;
    return side === 'YES' ? aboveRatio : 1 - aboveRatio;
  }

  private computeAtr(): number | null {
    if (this.closed.length < this.atrCandles) return null;
    const sample = this.closed.slice(-this.atrCandles);
    const sum = sample.reduce((acc, c) => acc + Math.abs(c.high - c.low), 0);
    return sum / sample.length;
  }

  /** Мирроит PriceFeedService.getAtrRobust 1-в-1 (см. class-comment там). */
  getAtrRobust(_streamKey: string): number | null {
    if (this.closed.length < this.atrCandles) return null;
    const sample = this.closed.slice(-this.atrCandles).map((c) => Math.abs(c.high - c.low)).sort((a, b) => a - b);
    const mid = Math.floor(sample.length / 2);
    return sample.length % 2 === 0 ? (sample[mid - 1] + sample[mid]) / 2 : sample[mid];
  }

  /** Мирроит PriceFeedService.getSmoothnessRatio 1-в-1 (см. class-comment там). */
  getSmoothnessRatio(_streamKey: string, sinceMs: number, nowMs: number): number | null {
    const ticks = this.recentTicks.filter((t) => t.ts >= sinceMs && t.ts <= nowMs);
    if (ticks.length < 2) return null;
    const netDisplacement = ticks[ticks.length - 1].price - ticks[0].price;
    let sumAbsDelta = 0;
    for (let i = 1; i < ticks.length; i++) {
      sumAbsDelta += Math.abs(ticks[i].price - ticks[i - 1].price);
    }
    if (sumAbsDelta <= 0) return null;
    return Math.abs(netDisplacement) / sumAbsDelta;
  }
}
