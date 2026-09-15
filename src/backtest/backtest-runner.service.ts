import { BadRequestException, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { PriceTick } from '../entities/price-tick.entity';
import { PolymarketPriceTick } from '../entities/polymarket-price-tick.entity';
import { parseStreamsConfig, StreamDefinition } from '../trading/stream-config';
import { EntryGateEngine, EntryGateConfig } from '../trading/entry-gate.engine';
import { DEFAULT_EDGE_WEIGHTS } from '../trading/edge-score.util';
import {
  computeTier,
  pickFavorite,
  roundToTick,
  limitOrderTargetUsd,
  LimitTier,
} from '../trading/market-decision.util';
import { walkAsksForFill } from '../polymarket/book-fill.util';
import { ReplayPriceSource, RawTick } from './replay-price-source';
import { ReplayClock } from './replay-clock';
import { BacktestRunRequest, BacktestSummary, BacktestTradeResult } from './backtest.types';

type Outcome = 'YES' | 'NO';

interface PmTickRow {
  ts: number;
  slug: string;
  yesBestBid: number | null;
  yesBestAsk: number | null;
  noBestBid: number | null;
  noBestAsk: number | null;
}

interface RestingOrderSim {
  tier: LimitTier;
  outcome: Outcome;
  price: number;
}

/**
 * BACKTEST-PLAN.md — реализация Части 2.
 *
 * ЧТО ЭТО. Не отдельная "модель", реализующая ту же идею заново, а честный
 * повтор существующей логики принятия решений (см. явное требование
 * пользователя в BACKTEST-PLAN.md): и live-путь (TradingService), и этот
 * класс используют РОВНО ОДИН И ТОТ ЖЕ `EntryGateEngine` (см.
 * entry-gate.engine.ts) и РОВНО ТЕ ЖЕ чистые функции принятия решений
 * (см. market-decision.util.ts) и ТУ ЖЕ функцию исполнения по стакану
 * (book-fill.util.ts). Разница только в том, ЧТО подставляется как
 * источник цены/времени/книги: здесь — реплей сохранённых
 * `price_ticks`/`polymarket_price_ticks` вместо живых WS-потоков.
 *
 * ЧЕСТНО ЗАДОКУМЕНТИРОВАННЫЕ ОГРАНИЧЕНИЯ (см. также BacktestSummary.limitations
 * в каждом ответе — план явно требовал не подменять недостаток данных тихой
 * интерполяцией, а показывать это явно):
 *
 *  1. `polymarket_price_ticks` хранит только ЛУЧШИЕ bid/ask, БЕЗ полной
 *     глубины книги — честный VWAP-проход по нескольким уровням (как в
 *     живом `book-fill.util.walkAsksForFill`) здесь физически невозможен
 *     воспроизвести с реальной глубиной. Мы всё равно используем ТУ ЖЕ
 *     функцию `walkAsksForFill`, но с ОДНИМ синтетическим уровнем
 *     (лучшая цена, условно неограниченный объём) — то есть бэктест
 *     систематически ОПТИМИСТИЧНЕЕ реальности в части глубины/частичных
 *     филлов (`fillRatio`/`MIN_FILL_RATIO` в бэктесте не может сработать
 *     содержательно). Это прямое следствие того, что записывалось в
 *     Сессии 15 — если понадобится честная симуляция глубины, `price
 *     recorder` нужно доработать до записи полного стакана, а не только
 *     top-of-book.
 *  2. Минимальный размер ордера биржи (`minOrderSize`, в live приходит от
 *     CLOB API) исторически не сохранялся — используется предположение
 *     `assumedMinOrderSize` (по умолчанию 5), а не реальное значение на тот
 *     момент.
 *  3. `price_ticks` пишет chainlink/binance/bybit НЕЗАВИСИМО и ПАРАЛЛЕЛЬНО
 *     (не "один активный с фолбэком", как живой `PriceFeedService`) — см.
 *     подробный комментарий в `ReplayPriceSource`. Переключения провайдера
 *     из-за "зомби-соединения" (Сессия 9) в бэктесте не реплеятся.
 *  4. Официальный исход (YES/NO won) от Gamma API исторически НЕ хранится —
 *     исход окна в бэктесте определяется приближённо (цена по нашему фиду
 *     на момент официального закрытия окна относительно референса), той же
 *     ценой, что и сам ATR-гейт. Это тот же методологический зазор, что уже
 *     задокументирован в CONTEXT.md (Сессии 10-11) как источник части
 *     дисперсии на live — в бэктесте он не устранён, а прозрачно перенесён
 *     в `impliedWinnerSide` каждой сделки.
 *  5. Реинвест-прогрессия (компаундинг стейка между окнами) НЕ реализована
 *     в этой версии — каждое окно бэктестится с фиксированным `baseStake`
 *     потока (см. BACKTEST-PLAN.md 2.5 — API/UI сознательно отделены от
 *     полноценной symulation прогрессии, это следующий шаг).
 */
@Injectable()
export class BacktestRunnerService {
  private readonly logger = new Logger(BacktestRunnerService.name);

  constructor(
    private readonly config: ConfigService,
    @InjectRepository(PriceTick) private readonly priceTickRepo: Repository<PriceTick>,
    @InjectRepository(PolymarketPriceTick) private readonly pmTickRepo: Repository<PolymarketPriceTick>,
  ) {}

  async run(req: BacktestRunRequest): Promise<BacktestSummary> {
    const streams = parseStreamsConfig(this.config.get<string>('STREAMS_CONFIG'));
    const stream = streams.find((s) => s.streamKey === req.streamKey);
    if (!stream) {
      throw new BadRequestException(
        `Неизвестный streamKey "${req.streamKey}". Настроенные потоки: ${streams.map((s) => s.streamKey).join(', ')}.`,
      );
    }

    const fromMs = this.toMs(req.from);
    const toMs = this.toMs(req.to);
    if (!(toMs > fromMs)) {
      throw new BadRequestException('`to` должен быть строго больше `from`.');
    }

    const cfg = this.resolveConfig(stream, req);
    const providerPriority = req.providerPriority ?? ['chainlink', 'binance', 'bybit'];
    const assumedMinOrderSize = req.assumedMinOrderSize ?? 5;

    const ticker = this.deriveTicker(req.streamKey, this.config.get<string>('FEED_SYMBOL_OVERRIDES', ''));
    const candleMs = stream.intervalSec * 1000;
    const atrCandles = stream.atrCandles ?? cfg.feedAtrCandles;
    const recentTicksRetentionMs = stream.intervalSec * 1000 + cfg.feedRecentTicksRetentionMs;
    // Прогрев ATR — тянем историю тиков на atrCandles*candleMs ДО начала
    // запрошенного диапазона, иначе первые окна бэктеста были бы fail-closed
    // по ATR ровно так же, как свежезапущенный live-поток (см. CONTEXT.md,
    // Сессия 5) — это корректное поведение fail-closed, а не баг, но для
    // содержательного бэктеста нужно дать буферу шанс прогреться заранее.
    const warmupMs = atrCandles * candleMs + recentTicksRetentionMs;

    const rawTicks = await this.loadPriceTicks(ticker, fromMs - warmupMs, toMs);
    const priceSource = new ReplayPriceSource(req.streamKey, rawTicks, {
      candleMs,
      atrCandles,
      staleMs: cfg.feedStaleMs,
      recentTicksRetentionMs,
      providerPriority,
    });

    const clock = new ReplayClock(fromMs - warmupMs);
    const gateEngine = new EntryGateEngine(cfg.gate, priceSource, clock);

    const pmRows = await this.loadPmTicks(req.streamKey, fromMs, toMs);
    const windows = this.groupIntoWindows(pmRows, stream.intervalSec * 1000);

    const trades: BacktestTradeResult[] = [];
    let windowsWithoutPmTicks = 0;
    let windowsWithoutReferencePrice = 0;

    // Синтетические "пустые" окна, у которых вообще нет PM-тиков в этом
    // диапазоне, посчитать честно нельзя (нет книги — нет решения) — они
    // не попадают в `windows` вообще (см. groupIntoWindows), поэтому здесь
    // просто фиксируем факт в статистике данных для UI/ответа.
    for (const win of windows) {
      // Прогреваем виртуальные часы/буфер ДО начала окна, чтобы referencePrice
      // и ATR были посчитаны с той же семантикой, что и в openMarket/onModuleInit
      // (см. entry-gate.engine.ts / ReplayPriceSource).
      clock.advanceTo(win.windowStartMs);
      priceSource.advanceTo(win.windowStartMs);
      const ref = priceSource.getPriceAt(req.streamKey, win.windowStartMs);
      const referencePrice = ref.price;
      if (referencePrice == null) windowsWithoutReferencePrice += 1;

      const trade = this.simulateWindow(win, referencePrice, stream, cfg, gateEngine, priceSource, clock, assumedMinOrderSize);
      trades.push(trade);
    }

    const totalPmWindows = windows.length;
    const wins = trades.filter((t) => t.status === 'win').length;
    const losses = trades.filter((t) => t.status === 'loss').length;
    const skipped = trades.filter((t) => t.status === 'skipped').length;
    const unfilled = trades.filter((t) => t.status === 'unfilled').length;
    const totalProfit = trades.reduce((acc, t) => acc + (t.profit ?? 0), 0);
    const entryPrices = trades.map((t) => t.entryPrice).filter((p): p is number => p != null);
    const avgEntryPrice = entryPrices.length > 0 ? entryPrices.reduce((a, b) => a + b, 0) / entryPrices.length : null;

    return {
      streamKey: req.streamKey,
      fromMs,
      toMs,
      windowsTotal: totalPmWindows,
      wins,
      losses,
      skipped,
      unfilled,
      totalProfit,
      avgEntryPrice,
      usedPriceSource: priceSource.usedSource,
      dataQuality: {
        priceTicksLoaded: rawTicks.length,
        polymarketTicksLoaded: pmRows.length,
        windowsWithoutPmTicks, // всегда 0 по построению (см. комментарий выше) — оставлено для будущего расширения (напр. сверка с ActiveWindow-историей)
        windowsWithoutReferencePrice,
      },
      limitations: [
        'Исполнение симулируется по одному ценовому уровню (лучший bid/ask) — реальная глубина книги исторически не сохранялась, поэтому частичные филлы/MIN_FILL_RATIO не воспроизводятся содержательно.',
        `minOrderSize исторически не сохранялся — использовано предположение ${assumedMinOrderSize}.`,
        `Цена реплеится ИЗ ОДНОГО источника (${priceSource.usedSource ?? 'нет данных'}), выбранного по приоритету [${providerPriority.join(', ')}] — переключения провайдера (Сессия 9) не воспроизводятся.`,
        'Официальный исход Gamma исторически не сохранялся — win/loss определяется приближённо по цене фида на момент официального закрытия окна относительно референса (тот же методологический зазор, что описан в CONTEXT.md, Сессии 10-11).',
        'Реинвест-прогрессия (компаундинг стейка между окнами) не реализована — каждое окно считается с фиксированным baseStake потока.',
      ],
      trades,
    };
  }

  // ---------------------------------------------------------------------

  private simulateWindow(
    win: { slug: string; windowStartMs: number; closesAtMs: number; ticks: PmTickRow[] },
    referencePrice: number | null,
    stream: StreamDefinition,
    cfg: ResolvedBacktestConfig,
    gateEngine: EntryGateEngine,
    priceSource: ReplayPriceSource,
    clock: ReplayClock,
    assumedMinOrderSize: number,
  ): BacktestTradeResult {
    const lastEntryWindowSec = stream.lastEntryWindowSec ?? cfg.lastEntryWindowSec;
    const tier2Seconds = stream.tier2Seconds ?? cfg.tier2Seconds;
    const tier3Seconds = stream.tier3Seconds ?? cfg.tier3Seconds;
    const betAmount = stream.baseStake;

    const entryTicks = win.ticks.filter(
      (t) => (win.closesAtMs - t.ts) / 1000 <= lastEntryWindowSec && t.ts <= win.closesAtMs,
    );

    let restingOrder: RestingOrderSim | null = null;
    let skippedLimitTier: LimitTier | null = null;

    const base: Omit<BacktestTradeResult, 'status' | 'profit' | 'priceAtClose' | 'impliedWinnerSide' | 'skipReason'> = {
      slug: win.slug,
      windowStartMs: win.windowStartMs,
      closesAtMs: win.closesAtMs,
      betAmount,
      chosenOutcome: null,
      entryPrice: null,
      filledAmount: null,
      fillRatio: null,
      orderType: null,
      limitTier: null,
      referencePrice,
      priceAtEntry: null,
      atrRatioAtEntry: null,
    };

    let entry: BacktestTradeResult | null = null;

    for (const tick of entryTicks) {
      clock.advanceTo(tick.ts);
      priceSource.advanceTo(tick.ts);
      const timeLeftSec = Math.max(0, (win.closesAtMs - tick.ts) / 1000);
      const books: Record<Outcome, { bestBid: number | null; bestAsk: number | null }> = {
        YES: { bestBid: tick.yesBestBid, bestAsk: tick.yesBestAsk },
        NO: { bestBid: tick.noBestBid, bestAsk: tick.noBestAsk },
      };

      // 0) Резюм-лимитка уже стоит — проверяем, "накопился" ли встречный
      //    объём (в бэктесте, при отсутствии глубины: просто bestAsk <=
      //    нашей цены, см. class-comment ограничение №1) и ПЕРЕПРОВЕРЯЕМ
      //    гейт заново на момент исполнения (тот же фикс, что и в
      //    live onBookUpdate, см. Сессию 10 в CONTEXT.md).
      if (restingOrder) {
        const ask = books[restingOrder.outcome].bestAsk;
        if (ask != null && ask <= restingOrder.price) {
          const gate = gateEngine.evaluateEntryGate({
            streamKey: stream.streamKey,
            outcome: restingOrder.outcome,
            referencePrice,
            intervalSec: stream.intervalSec,
            windowStartMs: win.windowStartMs,
            timeLeftSec,
            checkPrice: ask,
          });
          if (!gate.allow) {
            restingOrder = null;
            skippedLimitTier = null;
          } else {
            const targetUsd = limitOrderTargetUsd(betAmount, assumedMinOrderSize, restingOrder.price);
            entry = {
              ...base,
              chosenOutcome: restingOrder.outcome,
              entryPrice: restingOrder.price,
              filledAmount: targetUsd,
              fillRatio: targetUsd / betAmount,
              orderType: 'SIMULATED_LIMIT',
              limitTier: restingOrder.tier,
              priceAtEntry: gate.diagnostics.priceAtEntry,
              atrRatioAtEntry: gate.diagnostics.atrRatioAtEntry,
              status: 'unfilled', // placeholder, перезаписывается ниже
              profit: null,
              priceAtClose: null,
              impliedWinnerSide: null,
              skipReason: null,
            };
            break;
          }
        }
      }

      // 1) Правило A — маркет-тейк по лучшей цене (см. class-comment
      //    ограничение №1 — единственный уровень вместо полной глубины,
      //    но та же функция walkAsksForFill, что и в live).
      let ruleAConsumedTick = false;
      for (const oc of ['YES', 'NO'] as const) {
        const ask = books[oc].bestAsk;
        if (ask == null || ask < cfg.minMarketPrice || ask > cfg.maxMarketPrice) continue;
        ruleAConsumedTick = true;

        const fill = walkAsksForFill([{ price: ask, size: Number.POSITIVE_INFINITY }], betAmount, cfg.maxMarketPrice);
        if (fill.filledShares <= 0 || fill.filledShares < assumedMinOrderSize) break;

        const gate = gateEngine.evaluateEntryGate({
          streamKey: stream.streamKey,
          outcome: oc,
          referencePrice,
          intervalSec: stream.intervalSec,
          windowStartMs: win.windowStartMs,
          checkPrice: ask,
          timeLeftSec,
        });
        if (!gate.allow) break; // тик "потрачен" на эту проверку — как и в live (return без Rule B в этот же тик)

        entry = {
          ...base,
          chosenOutcome: oc,
          entryPrice: fill.vwapPrice,
          filledAmount: fill.filledUsd,
          fillRatio: fill.filledRatio,
          orderType: 'SIMULATED_MARKET',
          limitTier: null,
          priceAtEntry: gate.diagnostics.priceAtEntry,
          atrRatioAtEntry: gate.diagnostics.atrRatioAtEntry,
          status: 'unfilled',
          profit: null,
          priceAtClose: null,
          impliedWinnerSide: null,
          skipReason: null,
        };
        break;
      }
      if (entry) break;
      if (ruleAConsumedTick) continue; // как в live: если был валидный ask в диапазоне, Rule B в этот же тик не пробуем

      // 2) Правило B — лимитка-фолбэк.
      const favorite = pickFavorite({ YES: { bestBid: books.YES.bestBid }, NO: { bestBid: books.NO.bestBid } });
      if (!favorite) continue;
      const fb = books[favorite];
      if (fb.bestBid == null || fb.bestBid < cfg.favoriteBidThreshold) continue;
      if (fb.bestAsk != null && fb.bestAsk <= cfg.maxMarketPrice) continue; // предложение есть — Правило A им уже занялось бы

      const tier = computeTier(timeLeftSec, tier2Seconds, tier3Seconds);
      if (skippedLimitTier === tier) continue;
      const desiredPrice = roundToTick(cfg.tierPrices[tier], '0.01');
      if (restingOrder && restingOrder.tier === tier && restingOrder.outcome === favorite) continue;

      const targetUsd = limitOrderTargetUsd(betAmount, assumedMinOrderSize, desiredPrice);
      if (targetUsd > betAmount * cfg.maxOverspendMultiplier) {
        skippedLimitTier = tier;
        continue;
      }
      const gate = gateEngine.evaluateEntryGate({
        streamKey: stream.streamKey,
        outcome: favorite,
        referencePrice,
        intervalSec: stream.intervalSec,
        windowStartMs: win.windowStartMs,
        checkPrice: desiredPrice,
        timeLeftSec,
      });
      if (!gate.allow) {
        skippedLimitTier = tier;
        continue;
      }
      restingOrder = { tier, outcome: favorite, price: desiredPrice };
    }

    // Закрытие окна: считаем цену на закрытии и приближённый исход.
    clock.advanceTo(win.closesAtMs);
    priceSource.advanceTo(win.closesAtMs);
    const closeSnap = priceSource.getPriceAt(win.slug, win.closesAtMs);
    const priceAtClose = closeSnap.price;
    const impliedWinnerSide: Outcome | null =
      priceAtClose != null && referencePrice != null ? (priceAtClose >= referencePrice ? 'YES' : 'NO') : null;

    if (!entry) {
      if (restingOrder) {
        return {
          ...base,
          chosenOutcome: restingOrder.outcome,
          orderType: 'SIMULATED_LIMIT',
          limitTier: restingOrder.tier,
          status: 'unfilled',
          profit: null,
          priceAtClose,
          impliedWinnerSide,
          skipReason: 'Резюм-лимитка не была перекрыта достаточным (по данным бэктеста) встречным объёмом до конца окна.',
        };
      }
      return {
        ...base,
        status: 'skipped',
        profit: null,
        priceAtClose,
        impliedWinnerSide,
        skipReason: 'Ни один исход не вошёл в диапазон маркет-тейка/фаворит не определился, либо гейт блокировал вход на всех тиках окна.',
      };
    }

    const won = impliedWinnerSide != null && entry.chosenOutcome === impliedWinnerSide;
    const spentUsd = entry.filledAmount ?? betAmount;
    const entryPrice = entry.entryPrice ?? cfg.maxMarketPrice;
    const profit = impliedWinnerSide == null ? null : won ? (spentUsd / entryPrice) * (1 - entryPrice) : -spentUsd;

    return {
      ...entry,
      status: impliedWinnerSide == null ? 'unfilled' : won ? 'win' : 'loss',
      profit,
      priceAtClose,
      impliedWinnerSide,
      skipReason: impliedWinnerSide == null ? 'Цена на закрытии по фиду недоступна — исход не определён.' : null,
    };
  }

  // ---------------------------------------------------------------------

  private groupIntoWindows(
    rows: PmTickRow[],
    intervalMs: number,
  ): { slug: string; windowStartMs: number; closesAtMs: number; ticks: PmTickRow[] }[] {
    const bySlug = new Map<string, PmTickRow[]>();
    for (const row of rows) {
      const list = bySlug.get(row.slug) ?? [];
      list.push(row);
      bySlug.set(row.slug, list);
    }

    const windows: { slug: string; windowStartMs: number; closesAtMs: number; ticks: PmTickRow[] }[] = [];
    for (const [slug, ticks] of bySlug) {
      ticks.sort((a, b) => a.ts - b.ts);
      // Приближение официального closesAt (см. class-comment ограничение
      // №4 у win/loss, аналогичная логика тут для границы окна): recorder
      // слушает ровно до тех пор, пока ActiveWindow указывает на это окно,
      // т.е. последний записанный тик очень близок к реальному закрытию
      // (в пределах TICK_RECORDER_PM_SNAPSHOT_MS/WINDOW_POLL_MS, по
      // умолчанию <=2с) — недостаточно точно для секундной точности, но
      // достаточно для честного бэктеста фильтров входа.
      const closesAtMs = ticks[ticks.length - 1].ts;
      const windowStartMs = closesAtMs - intervalMs;
      windows.push({ slug, windowStartMs, closesAtMs, ticks });
    }
    windows.sort((a, b) => a.closesAtMs - b.closesAtMs);
    return windows;
  }

  private async loadPriceTicks(ticker: string, fromMs: number, toMs: number): Promise<RawTick[]> {
    const rows = await this.priceTickRepo
      .createQueryBuilder('t')
      .where('t.ticker = :ticker', { ticker })
      .andWhere('t.ts BETWEEN :from AND :to', { from: fromMs, to: toMs })
      .orderBy('t.ts', 'ASC')
      .getMany();
    return rows.map((r) => ({ ts: Number(r.ts), price: r.price, source: r.source }));
  }

  private async loadPmTicks(streamKey: string, fromMs: number, toMs: number): Promise<PmTickRow[]> {
    const rows = await this.pmTickRepo
      .createQueryBuilder('t')
      .where('t.streamKey = :streamKey', { streamKey })
      .andWhere('t.ts BETWEEN :from AND :to', { from: fromMs, to: toMs })
      .orderBy('t.ts', 'ASC')
      .getMany();
    return rows.map((r) => ({
      ts: Number(r.ts),
      slug: r.slug,
      yesBestBid: r.yesBestBid,
      yesBestAsk: r.yesBestAsk,
      noBestBid: r.noBestBid,
      noBestAsk: r.noBestAsk,
    }));
  }

  private toMs(v: string | number): number {
    if (typeof v === 'number') return v;
    const ms = Date.parse(v);
    if (!Number.isFinite(ms)) throw new BadRequestException(`Не удалось разобрать дату "${v}".`);
    return ms;
  }

  /** Тот же алгоритм, что и PriceFeedService.deriveTicker/parseOverrides —
   *  сознательно продублирован (2 маленьких чистых функции), чтобы не
   *  тянуть сюда весь PriceFeedService ради одного метода. */
  private deriveTicker(streamKey: string, overridesRaw: string): string {
    const overrides = new Map<string, string>();
    for (const pair of (overridesRaw ?? '').split(',')) {
      const [key, ticker] = pair.split(':').map((s) => s.trim());
      if (key && ticker) overrides.set(key, ticker.toLowerCase());
    }
    return overrides.get(streamKey) ?? streamKey.split('-')[0]?.toLowerCase() ?? streamKey.toLowerCase();
  }

  private resolveConfig(stream: StreamDefinition, req: BacktestRunRequest): ResolvedBacktestConfig {
    const ov = req.envOverrides ?? {};
    const get = (key: string, def: string): string => ov[key] ?? this.config.get<string>(key, def);

    const blackoutHoursUtc = new Set(
      get('BLACKOUT_HOURS_UTC', '')
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
        .map((s) => parseInt(s, 10))
        .filter((n) => Number.isFinite(n)),
    );

    const gate: EntryGateConfig = {
      entryFilterEnabled: get('ENTRY_FILTER_ENABLED', 'true') === 'true',
      minDistanceAtrRatio: parseFloat(get('MIN_DISTANCE_ATR_RATIO', '1.5')),
      blackoutHoursFilterEnabled: get('BLACKOUT_HOURS_FILTER_ENABLED', 'false') === 'true',
      blackoutHoursUtc,
      expectedMoveFilterEnabled: get('EXPECTED_MOVE_FILTER_ENABLED', 'false') === 'true',
      safetyKFactor: parseFloat(get('SAFETY_K_FACTOR', '1.5')),
      directionalDriftFilterEnabled: get('DIRECTIONAL_DRIFT_FILTER_ENABLED', 'false') === 'true',
      driftLookbackSec: parseInt(get('DRIFT_LOOKBACK_SEC', '10'), 10),
      // Сессия 18 — см. edge-score.util.ts. Бэктест читает те же ENV, что и
      // live, поэтому прогон на истории воспроизводит ровно ту конфигурацию
      // edge-модели, с которой бот торгует (включая edgeGateEnabled).
      edgeGateEnabled: get('EDGE_GATE_ENABLED', 'false') === 'true',
      edgeMargin: parseFloat(get('EDGE_MARGIN', '0.02')),
      edgeSmoothnessLookbackSec: parseInt(get('EDGE_SMOOTHNESS_LOOKBACK_SEC', '30'), 10),
      edgeWeights: {
        bias: parseFloat(get('EDGE_W_BIAS', String(DEFAULT_EDGE_WEIGHTS.bias))),
        z: parseFloat(get('EDGE_W_Z', String(DEFAULT_EDGE_WEIGHTS.z))),
        drift: parseFloat(get('EDGE_W_DRIFT', String(DEFAULT_EDGE_WEIGHTS.drift))),
        zone: parseFloat(get('EDGE_W_ZONE', String(DEFAULT_EDGE_WEIGHTS.zone))),
        smoothSigned: parseFloat(get('EDGE_W_SMOOTH', String(DEFAULT_EDGE_WEIGHTS.smoothSigned))),
      },
      timeInZoneFilterEnabled: get('TIME_IN_ZONE_FILTER_ENABLED', 'false') === 'true',
      minZoneRatio: parseFloat(get('MIN_ZONE_RATIO', '0.65')),
    };

    return {
      gate,
      minMarketPrice: parseFloat(get('MIN_MARKET_PRICE', '0.99')),
      maxMarketPrice: parseFloat(get('MAX_MARKET_PRICE', '0.999')),
      favoriteBidThreshold: parseFloat(get('FAVORITE_BID_THRESHOLD', '0.90')),
      tierPrices: {
        T1: parseFloat(get('LIMIT_TIER1_PRICE', '0.99')),
        T2: parseFloat(get('LIMIT_TIER2_PRICE', '0.995')),
        T3: parseFloat(get('LIMIT_TIER3_PRICE', '0.999')),
      },
      tier2Seconds: parseInt(get('LIMIT_TIER2_SECONDS', '150'), 10),
      tier3Seconds: parseInt(get('LIMIT_TIER3_SECONDS', '60'), 10),
      lastEntryWindowSec: parseInt(get('LAST_ENTRY_WINDOW_SEC', '60'), 10),
      maxOverspendMultiplier: parseFloat(get('MAX_OVERSPEND_MULTIPLIER', '1.5')),
      minFillRatio: parseFloat(get('MIN_FILL_RATIO', '0.5')),
      feedAtrCandles: parseInt(get('FEED_ATR_CANDLES', '20'), 10),
      feedStaleMs: parseInt(get('FEED_STALE_MS', '5000'), 10),
      feedRecentTicksRetentionMs: parseInt(get('FEED_RECENT_TICKS_RETENTION_MS', '15000'), 10),
    };
  }
}

interface ResolvedBacktestConfig {
  gate: EntryGateConfig;
  minMarketPrice: number;
  maxMarketPrice: number;
  favoriteBidThreshold: number;
  tierPrices: Record<LimitTier, number>;
  tier2Seconds: number;
  tier3Seconds: number;
  lastEntryWindowSec: number;
  maxOverspendMultiplier: number;
  minFillRatio: number;
  feedAtrCandles: number;
  feedStaleMs: number;
  feedRecentTicksRetentionMs: number;
}
