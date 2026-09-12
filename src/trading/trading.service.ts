import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { LessThan, Repository } from 'typeorm';
import { Attempt } from '../entities/attempt.entity';
import { ChosenOutcome, MarketLog, MarketLogStatus, OrderKind } from '../entities/market-log.entity';
import { GammaMarketService } from '../polymarket/gamma-market.service';
import { ClobPublicService } from '../polymarket/clob-public.service';
import { PolymarketTraderService } from '../polymarket/polymarket-trader.service';
import { LiveBook, MarketWsStream, Outcome } from '../polymarket/market-ws-stream';
import { cumulativeUsdAtOrBelow, walkAsksForFill } from '../polymarket/book-fill.util';
import { PriceFeedService } from '../polymarket/price-feed.service';
import { parseStreamsConfig, StreamDefinition } from './stream-config';

type LimitTier = 'T1' | 'T2' | 'T3';

interface RestingOrder {
  tier: LimitTier;
  outcome: Outcome;
  price: number;
  // null в смоуке (ничего реального не выставляли)
  orderId: string | null;
  // Момент, когда резюм-лимитка была выставлена (для orderSentAt в логе —
  // фактическое исполнение может случиться намного позже, вплоть до
  // истечения окна, см. BACKLOG "нужно время отправки ордера и его осуществления").
  placedAt: Date;
}

interface EntryDiagnostics {
  referencePrice: number | null;
  priceAtEntry: number | null;
  atrAtEntry: number | null;
  atrRatioAtEntry: number | null;
  // Источник цены ('chainlink' | 'binance' | 'bybit' | null) — Chainlink это
  // буквально то, чем Polymarket резолвит крипто-маркеты; binance/bybit —
  // лишь приближение (см. PriceFeedService и README).
  priceSource: string | null;
}

interface MarketState {
  // = stream.streamKey; хранится в колонке assetPrefix (переиспользуем
  // существующую схему — она и раньше кодировала "актив+таймфрейм", просто
  // Attempt раньше её игнорировал, см. п.3 бэклога).
  assetPrefix: string;
  slug: string;
  closesAt: Date;
  yesTokenId: string;
  noTokenId: string;
  negRisk: boolean;
  minOrderSize: number;
  stream: MarketWsStream;
  books: Record<Outcome, LiveBook>;
  positioned: boolean;
  finalized: boolean;
  logWritten: boolean;
  restingOrder: RestingOrder | null;
  // Тир, на котором мы уже один раз убедились, что глубины/бюджета не хватает —
  // чтобы не долбить лог на каждый WS-тик одним и тем же выводом (это и был баг со спамом).
  skippedLimitTier: LimitTier | null;
  lastMarketAttemptAt: number;
  closeTimer: NodeJS.Timeout;
  // Цена по внешнему ценовому фиду (Binance, proxy) на момент открытия окна —
  // наш локальный ориентир "точки старта" для UP/DOWN. null, если фид ещё не готов.
  referencePrice: number | null;
  // id уже записанного MarketLog — нужен, чтобы дописать close-диагностику
  // (priceAtClose/atrAtClose) в finalizeMarket, не создавая второй лог.
  marketLogId: string | null;
  // Стейк реинвест-прогрессии ЭТОГО потока, зафиксированный в момент открытия
  // окна (снимок Attempt.currentStake на момент старта шага) — п.1 бэклога.
  // Снимаем один раз при открытии, а не читаем на каждый тик, чтобы ставка
  // внутри уже открытого окна не "поехала", если резолвер параллельно
  // подвинет currentStake по другому, ещё не закрытому шагу того же потока
  // (при последовательных окнах такого не бывает, но так честнее и проще
  // рассуждать про инвариант "ставка шага фиксируется на его открытии").
  betAmount: number;
  // Уже залогировали переход в "окно входа" (последние LAST_ENTRY_WINDOW_SEC
  // секунд) для этого маркета? Чтобы не спамить лог на каждый WS-тик до
  // наступления этого момента — см. onBookUpdate.
  lastMinuteAnnounced: boolean;
}

const EMPTY_BOOK = (outcome: Outcome, tickSize: string): LiveBook => ({
  outcome,
  tickSize,
  asks: [],
  bids: [],
  bestAsk: null,
  bestBid: null,
});

@Injectable()
export class TradingService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TradingService.name);

  private isSmoke: boolean;
  private minMarketPrice: number;
  private maxMarketPrice: number;
  private favoriteBidThreshold: number;
  private tierPrices: Record<LimitTier, number>;
  private tier2Seconds: number;
  private tier3Seconds: number;
  // Не пытаемся входить (ни маркетом, ни лимиткой) раньше, чем останется
  // это количество секунд до закрытия окна. Чем раньше пытаться войти, тем
  // менее уверенно рынок ещё определился с направлением — по факту оба
  // недавних слива случились именно на ранних, "неуверенных" входах, когда
  // маркетмейкер уже давал ¢99, а цена потом успевала развернуться. Лучше
  // пропустить шаг, чем рисковать капиталом на неопределившемся рынке.
  private lastEntryWindowSec: number;
  private maxOverspendMultiplier: number;
  private minFillRatio: number;
  private targetSteps: number;
  private discoveryPollMs: number;
  private resolvePollMs: number;

  // Независимые потоки (актив × таймфрейм) — см. п.3/п.4 бэклога и
  // src/trading/stream-config.ts. Раньше был единственный this.betAmount и
  // единственный assetPrefixes[] с общим счётчиком шагов на все активы сразу.
  private readonly streams: StreamDefinition[];
  private readonly streamByKey: Map<string, StreamDefinition>;

  // --- ATR-гейт входа (см. README/обсуждение) ---
  // По умолчанию выключен (SHADOW-режим): диагностика считается и пишется в
  // каждый лог всегда, а блокировка входа включается явно через .env только
  // после того, как накопится статистика по реальным сливам.
  private entryFilterEnabled: boolean;
  private minDistanceAtrRatio: number;
  // Порог "зависшего" резолва — сколько может провисеть pending_resolve лог,
  // прежде чем мы начнём предупреждать в логах/на фронте (не блокирует торговлю).
  private staleResolveWarnMs: number;

  // По одному активному Attempt на каждый streamKey — независимая
  // прогрессия/прогресс для каждого потока (п.3 бэклога).
  private currentAttempts = new Map<string, Attempt>();
  private activeMarkets = new Map<string, MarketState>();
  private stopped = false;

  constructor(
    private readonly config: ConfigService,
    private readonly gamma: GammaMarketService,
    private readonly clobPublic: ClobPublicService,
    private readonly trader: PolymarketTraderService,
    private readonly priceFeed: PriceFeedService,
    @InjectRepository(Attempt) private readonly attemptRepo: Repository<Attempt>,
    @InjectRepository(MarketLog) private readonly marketLogRepo: Repository<MarketLog>,
  ) {
    this.isSmoke = this.config.get<string>('SMOKE_START', 'true') === 'true';
    this.minMarketPrice = parseFloat(this.config.get<string>('MIN_MARKET_PRICE', '0.99'));
    this.maxMarketPrice = parseFloat(this.config.get<string>('MAX_MARKET_PRICE', '0.999'));
    this.favoriteBidThreshold = parseFloat(
      this.config.get<string>('FAVORITE_BID_THRESHOLD', '0.90'),
    );
    this.tierPrices = {
      T1: parseFloat(this.config.get<string>('LIMIT_TIER1_PRICE', '0.99')),
      T2: parseFloat(this.config.get<string>('LIMIT_TIER2_PRICE', '0.995')),
      T3: parseFloat(this.config.get<string>('LIMIT_TIER3_PRICE', '0.999')),
    };
    this.tier2Seconds = parseInt(this.config.get<string>('LIMIT_TIER2_SECONDS', '150'), 10);
    this.tier3Seconds = parseInt(this.config.get<string>('LIMIT_TIER3_SECONDS', '60'), 10);
    this.lastEntryWindowSec = parseInt(this.config.get<string>('LAST_ENTRY_WINDOW_SEC', '60'), 10);
    this.maxOverspendMultiplier = parseFloat(
      this.config.get<string>('MAX_OVERSPEND_MULTIPLIER', '1.5'),
    );
    this.minFillRatio = parseFloat(this.config.get<string>('MIN_FILL_RATIO', '0.5'));
    this.targetSteps = parseInt(this.config.get<string>('TARGET_STEPS', '500'), 10);
    this.discoveryPollMs = parseInt(this.config.get<string>('MARKET_DISCOVERY_POLL_MS', '1500'), 10);
    this.resolvePollMs = parseInt(this.config.get<string>('RESOLVE_POLL_INTERVAL_MS', '10000'), 10);

    this.streams = parseStreamsConfig(this.config.get<string>('STREAMS_CONFIG'));
    this.streamByKey = new Map(this.streams.map((s) => [s.streamKey, s]));

    this.entryFilterEnabled = this.config.get<string>('ENTRY_FILTER_ENABLED', 'true') === 'true';
    this.minDistanceAtrRatio = parseFloat(this.config.get<string>('MIN_DISTANCE_ATR_RATIO', '1.5'));
    this.staleResolveWarnMs = parseInt(this.config.get<string>('STALE_RESOLVE_WARN_MS', '180000'), 10);
  }

  async onModuleInit() {
    if (!this.isSmoke) {
      try {
        await this.trader.ensureClient();
      } catch (err) {
        this.logger.error(
          `Не удалось инициализировать боевой торговый клиент — принудительно ` +
            `переключаюсь в SMOKE-режим. Причина: ${this.errMsg(err)}`,
        );
        this.isSmoke = true;
      }
    }

    for (const stream of this.streams) {
      const attempt = await this.getOrCreateActiveAttempt(stream);
      this.currentAttempts.set(stream.streamKey, attempt);
    }

    this.logger.log(
      `Старт. Режим: ${this.isSmoke ? 'SMOKE (без реальных ордеров)' : 'LIVE (реальные деньги)'}. ` +
        `Потоки (${this.streams.length}): ` +
        this.streams
          .map((s) => {
            const a = this.currentAttempts.get(s.streamKey)!;
            return `${s.streamKey}[попытка #${a.attemptNumber}, шаг ${a.currentStep}/${a.targetSteps}, стейк $${a.currentStake.toFixed(2)}]`;
          })
          .join('; ') +
        `. Маркет-тейк [¢${this.minMarketPrice * 100}-¢${this.maxMarketPrice * 100}] (минимум заполнения ${this.minFillRatio * 100}%), ` +
        `лимитки-фолбэк от ¢${this.favoriteBidThreshold * 100} (тиры ${this.tierPrices.T1 * 100}/${this.tierPrices.T2 * 100}/${this.tierPrices.T3 * 100}). ` +
        `Окно входа: последние ${this.lastEntryWindowSec}с до закрытия (раньше — не пытаемся войти вообще). ` +
        `ATR-гейт входа: ${this.entryFilterEnabled ? `ВКЛЮЧЁН (мин. ${this.minDistanceAtrRatio}x ATR, при недоступной диагностике — пропуск шага, не вход вслепую)` : 'выключен (только диагностика в логах)'}.`,
    );

    this.startDiscoveryLoop();
    this.startResolverLoop();
  }

  onModuleDestroy() {
    this.stopped = true;
    for (const marketState of this.activeMarkets.values()) {
      clearTimeout(marketState.closeTimer);
      marketState.stream.close();
    }
  }

  private async getOrCreateActiveAttempt(stream: StreamDefinition): Promise<Attempt> {
    const active = await this.attemptRepo.findOne({
      where: { status: 'active', isSmoke: this.isSmoke, streamKey: stream.streamKey },
      order: { createdAt: 'DESC' },
    });
    if (active) return active;

    const last = await this.attemptRepo.findOne({
      where: { isSmoke: this.isSmoke, streamKey: stream.streamKey },
      order: { attemptNumber: 'DESC' },
    });
    const attempt = this.attemptRepo.create({
      attemptNumber: (last?.attemptNumber ?? 0) + 1,
      streamKey: stream.streamKey,
      currentStep: 0,
      targetSteps: this.targetSteps,
      baseStake: stream.baseStake,
      currentStake: stream.baseStake,
      status: 'active',
      isSmoke: this.isSmoke,
      finishedAt: null,
    });
    return this.attemptRepo.save(attempt);
  }

  // ---------------------------------------------------------------------
  // Обнаружение маркетов — независимо по каждому настроенному потоку.
  // ---------------------------------------------------------------------
  private async startDiscoveryLoop() {
    while (!this.stopped) {
      try {
        await this.discoveryTick();
      } catch (err) {
        this.logger.error(`Сбой в цикле обнаружения маркета: ${this.errMsg(err)}`);
      }
      await this.sleep(this.discoveryPollMs);
    }
  }

  private async discoveryTick(): Promise<void> {
    for (const stream of this.streams) {
      const startTs = this.gamma.currentIntervalStartTimestampSec(stream.intervalSec);
      const closeTs = this.gamma.currentIntervalCloseTimestampSec(stream.intervalSec);
      const slug = this.gamma.buildSlugForStart(stream, startTs);

      if (this.activeMarkets.get(stream.streamKey)?.slug === slug) continue; // уже отслеживаем

      const market = await this.gamma.fetchMarketBySlug(slug, closeTs);
      if (!market) continue; // ещё не создан на Gamma — попробуем на следующем тике

      await this.openMarket(stream, market.slug, market.closesAt, market.yesTokenId, market.noTokenId, market.negRisk);
    }
  }

  private async openMarket(
    stream: StreamDefinition,
    slug: string,
    closesAt: Date,
    yesTokenId: string,
    noTokenId: string,
    negRisk: boolean,
  ): Promise<void> {
    const streamKey = stream.streamKey;

    // Разовый REST-бутстрап: тик-сайз + официальный минимальный размер ордера биржи.
    const [yesBoot, noBoot] = await Promise.all([
      this.clobPublic.getBestQuote(yesTokenId),
      this.clobPublic.getBestQuote(noTokenId),
    ]);
    const initialTickSize = yesBoot?.tickSize ?? noBoot?.tickSize ?? '0.01';
    const minOrderSize = yesBoot?.minOrderSize ?? noBoot?.minOrderSize ?? 5;

    // Снимок стейка реинвест-прогрессии ЭТОГО потока на момент открытия окна
    // (п.1 бэклога) — фиксируем один раз здесь, дальше в течение всего окна
    // используем именно это число, а не текущее значение Attempt.
    const attempt = this.currentAttempts.get(streamKey);
    if (!attempt) {
      this.logger.error(`[${streamKey}] ${slug}: нет активного Attempt для потока — пропуск окна (не должно происходить).`);
      return;
    }
    const betAmount = attempt.currentStake;

    // Фиксируем ориентир по внешнему фиду в момент открытия окна — от него будем
    // считать дельту/ATR-рацио на входе и на закрытии. Если фид ещё не успел
    // прогреться (нет ни одного тика), просто останется null — вся диагностика
    // и гейт в этом случае молча отключаются для конкретного окна (fail-open).
    const referenceSnapshot = this.priceFeed.getSnapshot(streamKey);
    if (referenceSnapshot.price == null) {
      this.logger.warn(
        `[${streamKey}] ${slug}: внешний ценовой фид ещё не отдал ни одного тика — ` +
          `диагностика/ATR-гейт для этого окна будут недоступны.`,
      );
    }

    const marketState: MarketState = {
      assetPrefix: streamKey,
      slug,
      closesAt,
      yesTokenId,
      noTokenId,
      negRisk,
      minOrderSize,
      stream: null as any,
      books: {
        YES: EMPTY_BOOK('YES', initialTickSize),
        NO: EMPTY_BOOK('NO', initialTickSize),
      },
      positioned: false,
      finalized: false,
      logWritten: false,
      restingOrder: null,
      skippedLimitTier: null,
      lastMarketAttemptAt: 0,
      closeTimer: null as any,
      referencePrice: referenceSnapshot.price,
      marketLogId: null,
      betAmount,
      lastMinuteAnnounced: false,
    };

    const wsStream = new MarketWsStream(
      yesTokenId,
      noTokenId,
      initialTickSize,
      (outcome, book) => this.onBookUpdate(marketState, outcome, book),
    );
    marketState.stream = wsStream;

    const msUntilClose = Math.max(0, closesAt.getTime() - Date.now());
    marketState.closeTimer = setTimeout(() => this.finalizeMarket(marketState), msUntilClose);

    this.activeMarkets.set(streamKey, marketState);
    wsStream.connect();

    this.logger.log(
      `[${streamKey}] ${slug}: открыт WS-поток (закрытие через ${(msUntilClose / 1000).toFixed(0)}с, стейк шага $${betAmount.toFixed(2)} ` +
        `[попытка #${attempt.attemptNumber}, шаг ${attempt.currentStep + 1}/${attempt.targetSteps}], min_order_size=${minOrderSize}, tick=${initialTickSize}, ` +
        `referencePrice=${referenceSnapshot.price ?? 'н/д'})`,
    );
  }

  // ---------------------------------------------------------------------
  // Реакция на каждое обновление стакана (book / price_change / tick_size_change)
  // ---------------------------------------------------------------------
  private onBookUpdate(marketState: MarketState, outcome: Outcome, book: LiveBook): void {
    marketState.books[outcome] = book;
    if (marketState.positioned || marketState.finalized) return;

    const timeLeftSec = (marketState.closesAt.getTime() - Date.now()) / 1000;
    if (timeLeftSec <= 0) return; // finalizeMarket сам разберётся по таймеру

    // 0) Если по этому исходу уже стоит наша (смоук-)лимитка — проверяем, накопилось
    //    ли ДОСТАТОЧНО объёма продавцов по нашей цене или ниже (а не просто "касание").
    //    Гейт по ATR здесь НЕ применяем повторно — он уже был проверен в момент
    //    выставления резюм-лимитки (placeOrReplaceLimit); здесь только фиксируем
    //    диагностику на момент фактического исполнения для лога.
    if (this.isSmoke && marketState.restingOrder?.outcome === outcome) {
      const resting = marketState.restingOrder;
      const targetUsd = this.limitOrderTargetUsd(marketState, resting.price);
      const availableUsd = cumulativeUsdAtOrBelow(book.asks, resting.price);
      if (availableUsd >= targetUsd) {
        marketState.positioned = true;
        marketState.restingOrder = null;
        const filledShares = targetUsd / resting.price;
        const diagnostics = this.captureDiagnostics(marketState);
        void this.writeLog(marketState, {
          chosenOutcome: outcome,
          chosenTokenId: outcome === 'YES' ? marketState.yesTokenId : marketState.noTokenId,
          entryPrice: resting.price,
          filledAmount: targetUsd,
          fillRatio: targetUsd / marketState.betAmount,
          executed: true,
          orderType: 'SIMULATED_LIMIT',
          limitTier: resting.tier,
          status: 'pending_resolve',
          logMessage: `SMOKE: лимитка не отправлялась на биржу — эмуляция; накопленный объём продавцов по ¢${(resting.price * 100).toFixed(2)} и ниже составил $${availableUsd.toFixed(2)}, взяли ${filledShares.toFixed(2)} шт.`,
          orderSentAt: resting.placedAt,
          orderFilledAt: new Date(),
          ...diagnostics,
        });
        this.logger.log(
          `[${marketState.assetPrefix}][SMOKE][Limit fill] ${marketState.slug}: ${outcome} по ¢${(resting.price * 100).toFixed(2)} (тир ${resting.tier})`,
        );
        return;
      }
    }

    // 1) Ждём последней минуты (LAST_ENTRY_WINDOW_SEC) перед тем, как вообще
    //    пытаться войти — ни маркетом, ни лимиткой. Чем раньше вход, тем
    //    менее рынок ещё "определился": именно так случились оба недавних
    //    слива (ask по ¢99 появлялся за 2-3 минуты до закрытия, а потом цена
    //    успевала развернуться). Лучше пропустить шаг целиком, чем рисковать
    //    капиталом на неопределившемся рынке.
    if (timeLeftSec > this.lastEntryWindowSec) return;
    if (!marketState.lastMinuteAnnounced) {
      marketState.lastMinuteAnnounced = true;
      this.logger.log(
        `[${marketState.assetPrefix}] ${marketState.slug}: вошли в окно входа (последние ${this.lastEntryWindowSec}с) — начинаем искать вход.`,
      );
    }

    // 2) Правило A — агрессивный маркет-тейк: реально проходим по уровням стакана
    //    (не делаем вид, что весь объём взяли по единственной лучшей цене).
    for (const oc of ['YES', 'NO'] as const) {
      const b = marketState.books[oc];
      if (b.bestAsk == null || b.bestAsk < this.minMarketPrice || b.bestAsk > this.maxMarketPrice) continue;

      const now = Date.now();
      if (now - marketState.lastMarketAttemptAt < 800) continue; // не долбим биржу на каждом тике подряд
      marketState.lastMarketAttemptAt = now;
      void this.tryMarketBuy(marketState, oc, b);
      return;
    }

    // 3) Правило B — лимитка-фолбэк, если у фаворита реально нет предложений на продажу.
    const favorite = this.pickFavorite(marketState.books);
    if (!favorite) return;
    const fb = marketState.books[favorite];
    if (fb.bestBid == null || fb.bestBid < this.favoriteBidThreshold) return;
    if (fb.bestAsk != null && fb.bestAsk <= this.maxMarketPrice) return; // предложение есть — им займётся Правило A

    const tier = this.computeTier(timeLeftSec);
    if (marketState.skippedLimitTier === tier) return; // уже проверяли этот тир — бюджета/минимума не хватает, ждём смены тира
    const desiredPrice = this.roundToTick(this.tierPrices[tier], fb.tickSize);
    const existing = marketState.restingOrder;
    if (existing && existing.tier === tier && existing.outcome === favorite) return; // уже стоит нужный уровень

    void this.placeOrReplaceLimit(marketState, favorite, tier, desiredPrice, fb.tickSize);
  }

  private pickFavorite(books: Record<Outcome, LiveBook>): Outcome | null {
    const yes = books.YES.bestBid;
    const no = books.NO.bestBid;
    if (yes == null && no == null) return null;
    if (yes == null) return 'NO';
    if (no == null) return 'YES';
    return yes >= no ? 'YES' : 'NO';
  }

  private computeTier(timeLeftSec: number): LimitTier {
    if (timeLeftSec > this.tier2Seconds) return 'T1';
    if (timeLeftSec > this.tier3Seconds) return 'T2';
    return 'T3';
  }

  private roundToTick(price: number, tickSizeStr: string): number {
    const tick = parseFloat(tickSizeStr) || 0.01;
    const decimals = (tickSizeStr.split('.')[1] ?? '').length || 2;
    let rounded = Math.round(price / tick) * tick;
    const max = 1 - tick;
    rounded = Math.min(Math.max(rounded, tick), max);
    return Number(rounded.toFixed(decimals));
  }

  /** Сколько $ нужно набрать нашей резюм-лимиткой, чтобы удовлетворить минимум биржи
   *  (не капая эту сумму обратно до стейка шага — иначе проверка допустимого перерасхода
   *  в вызывающем коде никогда не сработает). */
  private limitOrderTargetUsd(marketState: MarketState, price: number): number {
    const minUsd = marketState.minOrderSize * price;
    return Math.max(marketState.betAmount, minUsd);
  }

  // ---------------------------------------------------------------------
  // ATR-гейт: считает диагностику по внешнему фиду (по умолчанию Chainlink —
  // тот же фид, которым Polymarket резолвит крипто-маркеты, см. README) и
  // (если включено через .env) решает, достаточно ли убедительно цена
  // отошла от точки старта окна относительно недавней волатильности этого
  // же таймфрейма (ATR теперь считается в масштабе окна потока, не в
  // фиксированных 20 секундах — см. PriceFeedService).
  // ---------------------------------------------------------------------
  private captureDiagnostics(marketState: MarketState): EntryDiagnostics {
    const snap = this.priceFeed.getSnapshot(marketState.assetPrefix);
    const referencePrice = marketState.referencePrice;
    const priceAtEntry = snap.price;
    const atrAtEntry = snap.atr;
  
    let atrRatioAtEntry: number | null = null;
  
    if (
      referencePrice != null &&
      priceAtEntry != null &&
      atrAtEntry != null &&
      atrAtEntry > 0
    ) {
      atrRatioAtEntry =
        Math.abs(priceAtEntry - referencePrice) / atrAtEntry;
    }
  
    this.logger.log(
      `[ATR DEBUG] ${marketState.assetPrefix}: ` +
      `reference=${referencePrice} ` +
      `price=${priceAtEntry} ` +
      `atr=${atrAtEntry} ` +
      `candles=${snap.candleCount} ` +
      `source=${snap.source} ` +
      `ratio=${atrRatioAtEntry}`
    );
  
    return {
      referencePrice,
      priceAtEntry,
      atrAtEntry,
      atrRatioAtEntry,
      priceSource: snap.source,
    };
  }
  

  private evaluateEntryGate(marketState: MarketState): { allow: boolean; diagnostics: EntryDiagnostics; reason: string | null } {
    const diagnostics = this.captureDiagnostics(marketState);

    if (!this.entryFilterEnabled) {
      return { allow: true, diagnostics, reason: null };
    }
    if (diagnostics.atrRatioAtEntry == null) {
      // Гейт включён, но диагностика недоступна (фид не отдал ни одного тика
      // до этого момента, либо ATR ещё не прогрелся) — раньше это было
      // fail-open (пропускали не глядя). Теперь фейл-клоуз: лучше упустить
      // шаг, чем войти вслепую без понимания, насколько уверенно движение.
      return {
        allow: false,
        diagnostics,
        reason:
          'ATR-гейт включён, но диагностика недоступна (нет цены/ATR по внешнему фиду на момент входа) — ' +
          'пропускаем шаг: упустить сделку лучше, чем рисковать капиталом вслепую.',
      };
    }
    if (diagnostics.atrRatioAtEntry < this.minDistanceAtrRatio) {
      return {
        allow: false,
        diagnostics,
        reason:
          `ATR-гейт: дистанция до референса ${diagnostics.atrRatioAtEntry.toFixed(2)}x ATR ` +
          `меньше требуемых ${this.minDistanceAtrRatio}x — похоже на болтанку у границы, а не уверенное движение.`,
      };
    }
    return { allow: true, diagnostics, reason: null };
  }

  // ---------------------------------------------------------------------
  // Правило A: маркет-тейк — честно проходим по уровням стакана (VWAP),
  // не выше maxMarketPrice, и не принимаем сделку, если реальной глубины
  // хватает меньше чем на minFillRatio от заявленной ставки (иначе легко
  // словить дребезг тонкой лимитки, а не настоящее направление рынка).
  // ---------------------------------------------------------------------
  private async tryMarketBuy(marketState: MarketState, outcome: Outcome, book: LiveBook): Promise<void> {
    const tokenId = outcome === 'YES' ? marketState.yesTokenId : marketState.noTokenId;
    const fill = walkAsksForFill(book.asks, marketState.betAmount, this.maxMarketPrice);

    if (fill.filledShares <= 0) {
      this.logger.debug(`[${marketState.assetPrefix}][Market] ${marketState.slug}: нет реальной ликвидности по ${outcome} в диапазоне — пропуск`);
      return;
    }
    if (fill.filledRatio < this.minFillRatio) {
      this.logger.debug(
        `[${marketState.assetPrefix}][Market] ${marketState.slug}: ${outcome} — глубины стакана хватает только на ${(fill.filledRatio * 100).toFixed(0)}% ставки ` +
          `(нужно минимум ${(this.minFillRatio * 100).toFixed(0)}%), похоже на дребезг тонкой заявки — пропуск.`,
      );
      return;
    }
    if (fill.filledShares < marketState.minOrderSize) {
      this.logger.debug(
        `[${marketState.assetPrefix}][Market] ${marketState.slug}: ${outcome} — реально исполнимо только ${fill.filledShares.toFixed(2)} шт, ` +
          `меньше минимума биржи (${marketState.minOrderSize}) — пропуск.`,
      );
      return;
    }

    const gate = this.evaluateEntryGate(marketState);
    if (!gate.allow) {
      this.logger.log(`[${marketState.assetPrefix}][Market] ${marketState.slug}: ${outcome} — вход заблокирован. ${gate.reason}`);
      return;
    }

    try {
      if (this.isSmoke) {
        const orderSentAt = new Date();
        marketState.positioned = true;
        await this.cancelRestingIfAny(marketState);
        const savedId = await this.writeLog(marketState, {
          chosenOutcome: outcome,
          chosenTokenId: tokenId,
          entryPrice: fill.vwapPrice,
          filledAmount: fill.filledUsd,
          fillRatio: fill.filledRatio,
          executed: true,
          orderType: 'SIMULATED_MARKET',
          status: 'pending_resolve',
          logMessage:
            fill.filledRatio < 0.999
              ? `SMOKE: частичное исполнение — забрали $${fill.filledUsd.toFixed(2)} из $${marketState.betAmount.toFixed(2)} (${(fill.filledRatio * 100).toFixed(0)}%) по VWAP ¢${(fill.vwapPrice! * 100).toFixed(2)}.`
              : `SMOKE: маркет-ордер не отправлялся, только эмуляция прохода по стакану (VWAP ¢${(fill.vwapPrice! * 100).toFixed(2)}).`,
          orderSentAt,
          orderFilledAt: new Date(),
          ...gate.diagnostics,
        });
        marketState.marketLogId = savedId;
        this.logger.log(
          `[${marketState.assetPrefix}][SMOKE][Market] ${marketState.slug}: ${outcome} по VWAP ¢${(fill.vwapPrice! * 100).toFixed(2)} ` +
            `($${fill.filledUsd.toFixed(2)}${fill.filledRatio < 0.999 ? `, ${(fill.filledRatio * 100).toFixed(0)}% от заявки` : ''})`,
        );
        return;
      }

      const orderSentAt = new Date();
      const result = await this.trader.placeMarketBuy({
        tokenId,
        amountUsd: marketState.betAmount,
        worstPrice: this.maxMarketPrice,
        tickSize: book.tickSize,
        negRisk: marketState.negRisk,
      });
      const orderFilledAt = new Date();

      if (!result.success) {
        this.logger.warn(`[${marketState.assetPrefix}][LIVE][Market] ${marketState.slug}: ордер не исполнился (success=false), пробуем дальше`);
        return;
      }

      marketState.positioned = true;
      await this.cancelRestingIfAny(marketState);
      // Реальный размер исполнения биржа возвращает в takingAmount/makingAmount —
      // если поле есть, используем его; если нет, используем нашу локальную оценку
      // по стакану как честное приближение (и явно это помечаем в логе).
      const raw: any = result.raw;
      const actualUsd = this.parseFloatSafe(raw?.makingAmount) ?? fill.filledUsd;
      const savedId = await this.writeLog(marketState, {
        chosenOutcome: outcome,
        chosenTokenId: tokenId,
        entryPrice: fill.vwapPrice,
        filledAmount: actualUsd,
        fillRatio: actualUsd / marketState.betAmount,
        executed: true,
        orderType: 'FAK',
        orderId: result.orderId,
        status: 'pending_resolve',
        orderSentAt,
        orderFilledAt,
        ...gate.diagnostics,
      });
      marketState.marketLogId = savedId;
      this.logger.log(
        `[${marketState.assetPrefix}][LIVE][Market] ${marketState.slug}: ${outcome} ордер отправлен (потолок ¢${(this.maxMarketPrice * 100).toFixed(1)}), orderId=${result.orderId}`,
      );
    } catch (err) {
      this.logger.error(`[${marketState.assetPrefix}][Error][Market] ${marketState.slug}: ${this.errMsg(err)}`);
    }
  }

  // ---------------------------------------------------------------------
  // Правило B: лимитка-фолбэк, переставляется по тирам T1(¢99)/T2(¢99.5)/T3(¢99.9)
  // ---------------------------------------------------------------------
  private async placeOrReplaceLimit(
    marketState: MarketState,
    outcome: Outcome,
    tier: LimitTier,
    price: number,
    tickSize: string,
  ): Promise<void> {
    try {
      const tokenId = outcome === 'YES' ? marketState.yesTokenId : marketState.noTokenId;
      const targetUsd = this.limitOrderTargetUsd(marketState, price);

      if (targetUsd > marketState.betAmount * this.maxOverspendMultiplier) {
        marketState.skippedLimitTier = tier;
        this.logger.debug(
          `[${marketState.assetPrefix}][Limit] ${marketState.slug}: пропуск тира ${tier} — нужно ~$${targetUsd.toFixed(2)} для минимума биржи, больше допустимого.`,
        );
        return;
      }

      const gate = this.evaluateEntryGate(marketState);
      if (!gate.allow) {
        marketState.skippedLimitTier = tier;
        this.logger.log(`[${marketState.assetPrefix}][Limit] ${marketState.slug}: тир ${tier} — выставление заблокировано. ${gate.reason}`);
        return;
      }

      const size = Number((targetUsd / price).toFixed(2));

      if (this.isSmoke) {
        marketState.restingOrder = { tier, outcome, price, orderId: null, placedAt: new Date() };
        this.logger.log(
          `[${marketState.assetPrefix}][SMOKE][Limit] ${marketState.slug}: тир ${tier} — ${outcome} по ¢${(price * 100).toFixed(2)} (эмуляция, ждём накопления объёма продавцов $${targetUsd.toFixed(2)})`,
        );
        return;
      }

      await this.cancelRestingIfAny(marketState);
      const result = await this.trader.placeLimitBuy({
        tokenId,
        price,
        size,
        tickSize,
        negRisk: marketState.negRisk,
        expirationUnixSec: Math.floor(marketState.closesAt.getTime() / 1000),
      });

      if (!result.success || !result.orderId) {
        this.logger.warn(`[${marketState.assetPrefix}][LIVE][Limit] ${marketState.slug}: не удалось выставить тир ${tier}`);
        return;
      }

      marketState.restingOrder = { tier, outcome, price, orderId: result.orderId, placedAt: new Date() };
      this.logger.log(
        `[${marketState.assetPrefix}][LIVE][Limit] ${marketState.slug}: тир ${tier} — ${outcome} по ¢${(price * 100).toFixed(2)} выставлен, orderId=${result.orderId}`,
      );
    } catch (err) {
      this.logger.error(`[${marketState.assetPrefix}][Error][Limit] ${marketState.slug}: ${this.errMsg(err)}`);
    }
  }

  private async cancelRestingIfAny(marketState: MarketState): Promise<void> {
    if (marketState.restingOrder?.orderId) {
      await this.trader.cancelOrder(marketState.restingOrder.orderId);
    }
    marketState.restingOrder = null;
    marketState.skippedLimitTier = null;
  }

  // ---------------------------------------------------------------------
  // Закрытие окна: ровно один терминальный MarketLog на маркет.
  // ---------------------------------------------------------------------
  private async finalizeMarket(marketState: MarketState): Promise<void> {
    if (marketState.finalized) return;
    marketState.finalized = true;
    marketState.stream.close();
    this.activeMarkets.delete(marketState.assetPrefix);

    try {
      if (marketState.positioned) {
        await this.cancelRestingIfAny(marketState);
        // Лог уже записан в момент входа — дописываем только "фото" цены/ATR
        // на момент закрытия окна, чтобы потом было видно, что произошло с
        // ценой между входом и резолвом (это и есть материал для разбора сливов).
        if (marketState.marketLogId) {
          const closeSnap = this.priceFeed.getSnapshot(marketState.assetPrefix);
          await this.marketLogRepo.update(marketState.marketLogId, {
            priceAtClose: closeSnap.price,
            atrAtClose: closeSnap.atr,
          });
        }
        return;
      }

      if (marketState.restingOrder) {
        const resting = marketState.restingOrder;
        const tokenId = resting.outcome === 'YES' ? marketState.yesTokenId : marketState.noTokenId;

        if (this.isSmoke) {
          await this.writeLog(marketState, {
            chosenOutcome: resting.outcome,
            chosenTokenId: tokenId,
            entryPrice: null,
            executed: false,
            orderType: 'SIMULATED_LIMIT',
            limitTier: resting.tier,
            status: 'unfilled',
            skipReason: 'Симулированная лимитка не была перекрыта достаточным объёмом продавцов до конца окна.',
            orderSentAt: resting.placedAt,
          });
          return;
        }

        const status = resting.orderId ? await this.trader.getOrderStatus(resting.orderId) : null;
        if (status && status.sizeMatched > 0) {
          const filledAmount = status.sizeMatched * resting.price;
          const savedId = await this.writeLog(marketState, {
            chosenOutcome: resting.outcome,
            chosenTokenId: tokenId,
            entryPrice: resting.price,
            filledAmount,
            fillRatio: filledAmount / marketState.betAmount,
            executed: true,
            orderType: 'GTD',
            limitTier: resting.tier,
            orderId: resting.orderId,
            status: 'pending_resolve',
            logMessage: `Исполнено ${status.sizeMatched}/${status.originalSize} шт.`,
            orderSentAt: resting.placedAt,
            // Момент фактического исполнения GTD-лимитки биржа не отдаёт отдельным
            // полем в этом ответе — используем момент проверки статуса как приближение
            // (честно позже реального момента матча, но точнее, чем ничего).
            orderFilledAt: new Date(),
          });
          if (savedId) {
            const closeSnap = this.priceFeed.getSnapshot(marketState.assetPrefix);
            await this.marketLogRepo.update(savedId, {
              priceAtClose: closeSnap.price,
              atrAtClose: closeSnap.atr,
            });
          }
        } else {
          await this.writeLog(marketState, {
            chosenOutcome: resting.outcome,
            chosenTokenId: tokenId,
            entryPrice: resting.price,
            executed: false,
            orderType: 'GTD',
            limitTier: resting.tier,
            orderId: resting.orderId,
            status: 'unfilled',
            skipReason: 'Лимитка не исполнилась до истечения (GTD).',
            orderSentAt: resting.placedAt,
          });
        }
        await this.cancelRestingIfAny(marketState);
        return;
      }

      await this.writeLog(marketState, {
        chosenOutcome: null,
        executed: false,
        status: 'skipped',
        skipReason: 'Ни один исход не вошёл в диапазон маркет-тейка, фаворит не определился.',
      });
    } catch (err) {
      this.logger.error(`Ошибка финализации ${marketState.slug}: ${this.errMsg(err)}`);
    }
  }

  private async writeLog(
    marketState: MarketState,
    fields: {
      chosenOutcome: ChosenOutcome;
      chosenTokenId?: string | null;
      entryPrice?: number | null;
      filledAmount?: number | null;
      fillRatio?: number | null;
      executed: boolean;
      orderType?: OrderKind | null;
      limitTier?: LimitTier | null;
      orderId?: string | null;
      status: MarketLogStatus;
      skipReason?: string | null;
      logMessage?: string | null;
      referencePrice?: number | null;
      priceAtEntry?: number | null;
      atrAtEntry?: number | null;
      atrRatioAtEntry?: number | null;
      priceSource?: string | null;
      orderSentAt?: Date | null;
      orderFilledAt?: Date | null;
    },
  ): Promise<string | null> {
    if (marketState.logWritten) return null;
    marketState.logWritten = true;

    const attempt = this.currentAttempts.get(marketState.assetPrefix);
    if (!attempt) {
      this.logger.error(`[${marketState.assetPrefix}] ${marketState.slug}: нет активного Attempt на момент записи лога — не должно происходить.`);
      return null;
    }

    const saved = await this.marketLogRepo.save(
      this.marketLogRepo.create({
        attemptId: attempt.id,
        stepNumber: attempt.currentStep + 1,
        assetPrefix: marketState.assetPrefix,
        slug: marketState.slug,
        closesAt: marketState.closesAt,
        betAmount: marketState.betAmount,
        isSmoke: this.isSmoke,
        chosenOutcome: fields.chosenOutcome,
        chosenTokenId: fields.chosenTokenId ?? null,
        entryPrice: fields.entryPrice ?? null,
        filledAmount: fields.filledAmount ?? null,
        fillRatio: fields.fillRatio ?? null,
        executed: fields.executed,
        orderType: fields.orderType ?? null,
        limitTier: fields.limitTier ?? null,
        orderId: fields.orderId ?? null,
        status: fields.status,
        skipReason: fields.skipReason ?? null,
        logMessage: fields.logMessage ?? null,
        referencePrice: fields.referencePrice ?? null,
        priceAtEntry: fields.priceAtEntry ?? null,
        atrAtEntry: fields.atrAtEntry ?? null,
        atrRatioAtEntry: fields.atrRatioAtEntry ?? null,
        priceSource: fields.priceSource ?? null,
        orderSentAt: fields.orderSentAt ?? null,
        orderFilledAt: fields.orderFilledAt ?? null,
      }),
    );
    return saved.id;
  }

  // ---------------------------------------------------------------------
  // Резолвер: REST-опрос Gamma API по закрытым маркетам, продвигает шаги
  // и считает профит по факту исхода. Общий для всех потоков (сами логи уже
  // несут attemptId/assetPrefix=streamKey, поэтому один цикл резолва работает
  // одинаково независимо от того, сколько потоков сконфигурировано).
  // ---------------------------------------------------------------------
  private async startResolverLoop() {
    while (!this.stopped) {
      try {
        await this.resolvePendingMarkets();
        await this.warnStaleUnresolved();
      } catch (err) {
        this.logger.error(`Сбой резолвера: ${this.errMsg(err)}`);
      }
      await this.sleep(this.resolvePollMs);
    }
  }

  /**
   * Отдельная, независимая от "горячего пути" входа проверка: не завис ли
   * какой-то маркет в pending_resolve дольше разумного. НЕ блокирует открытие
   * новых окон (см. discoveryTick) — только предупреждает в логах и попадает
   * в /analytics/summary, чтобы это было видно на фронте, а не только "по ощущениям".
   */
  private async warnStaleUnresolved(): Promise<void> {
    const staleBefore = new Date(Date.now() - this.staleResolveWarnMs);
    const stale = await this.marketLogRepo.find({
      where: { status: 'pending_resolve', createdAt: LessThan(staleBefore) },
      order: { createdAt: 'ASC' },
      take: 20,
    });
    if (stale.length === 0) return;

    for (const log of stale) {
      const ageSec = Math.round((Date.now() - log.createdAt.getTime()) / 1000);
      this.logger.warn(
        `[STALE] ${log.assetPrefix} ${log.slug}: висит в pending_resolve уже ${ageSec}с — ` +
          `резолв Gamma задерживается сильнее обычного (текущее окно закрытия ~30с). Проверь вручную.`,
      );
    }
  }

  private async resolvePendingMarkets(): Promise<void> {
    const pending = await this.marketLogRepo.find({
      where: { status: 'pending_resolve' },
      order: { createdAt: 'ASC' },
      take: 20,
    });

    for (const log of pending) {
      const outcome = await this.gamma.fetchOutcome(log.slug);
      if (!outcome || !outcome.closed) continue;

      const won =
        (log.chosenOutcome === 'YES' && outcome.yesWon === true) ||
        (log.chosenOutcome === 'NO' && outcome.noWon === true);

      const spentUsd = log.filledAmount ?? log.betAmount;
      const entryPrice = log.entryPrice ?? this.maxMarketPrice;
      log.status = won ? 'win' : 'loss';
      log.resolvedAt = new Date();
      log.profit = won ? (spentUsd / entryPrice) * (1 - entryPrice) : -spentUsd;
      if (!won) {
        log.failReason = this.buildFailReason(log);
      }
      await this.marketLogRepo.save(log);

      const attempt = await this.attemptRepo.findOneOrFail({ where: { id: log.attemptId } });
      if (attempt.status !== 'active') continue; // попытка уже закрыта ранее

      const streamKey = log.assetPrefix;
      const stream = this.streamByKey.get(streamKey);
      const baseStake = stream?.baseStake ?? attempt.baseStake;

      if (won) {
        attempt.currentStep += 1;
        // Реинвест-прогрессия (п.1 бэклога): следующий стейк = реально
        // полученные деньги за этот шаг = spentUsd/entryPrice (то, что даёт
        // выплата $1/акцию победителя) — считаем по ФАКТИЧЕСКОЙ цене
        // исполнения (VWAP шага), а не по константе ¢99, потому что VWAP
        // гуляет по тирам лимитки/маркет-тейка.
        attempt.currentStake = spentUsd / entryPrice;
        if (attempt.currentStep >= attempt.targetSteps) {
          attempt.status = 'completed_target';
          attempt.finishedAt = new Date();
          this.logger.log(
            `[GOAL] [${streamKey}] Попытка #${attempt.attemptNumber} (${attempt.isSmoke ? 'smoke' : 'live'}) дошла до ${attempt.targetSteps} шага!`,
          );
        }
        await this.attemptRepo.save(attempt);
        this.currentAttempts.set(streamKey, attempt);
      } else {
        attempt.status = 'failed';
        attempt.finishedAt = new Date();
        await this.attemptRepo.save(attempt);
        this.logger.warn(
          `[LOSS] [${streamKey}] Попытка #${attempt.attemptNumber} (${attempt.isSmoke ? 'smoke' : 'live'}) слита на шаге ${attempt.currentStep} ` +
            `(профит шага $${log.profit.toFixed(2)}, ${log.resolvedAt.toISOString()}). ${log.failReason ?? ''} Открываю новую попытку (стейк сброшен на базовый $${baseStake.toFixed(2)}).`,
        );

        const next = this.attemptRepo.create({
          attemptNumber: attempt.attemptNumber + 1,
          streamKey,
          currentStep: 0,
          targetSteps: this.targetSteps,
          baseStake,
          currentStake: baseStake, // сброс прогрессии на базовый стейк потока
          status: 'active',
          isSmoke: attempt.isSmoke,
          finishedAt: null,
        });
        const saved = await this.attemptRepo.save(next);
        this.currentAttempts.set(streamKey, saved);
      }
    }
  }

  /**
   * Собирает человекочитаемое объяснение слива из уже накопленной по фиду
   * диагностики (референс/цена на входе/ATR/цена на закрытии). Если фида на
   * момент входа или закрытия не было — честно об этом пишет, а не гадает.
   */
  private buildFailReason(log: MarketLog): string {
    const { referencePrice, priceAtEntry, atrAtEntry, atrRatioAtEntry, priceAtClose, atrAtClose, priceSource } = log;

    if (referencePrice == null || priceAtEntry == null) {
      return 'Слив без диагностики фида (referencePrice/priceAtEntry недоступны на момент входа — ' +
        'см. логи PriceFeedService, вероятно ни один из настроенных провайдеров (FEED_PROVIDERS) не был доступен в этот момент).';
    }

    const sourceNote = priceSource
      ? priceSource === 'chainlink'
        ? '(источник: chainlink — тот же фид, которым резолвится сам маркет)'
        : `(источник: ${priceSource} — приближение, не тот фид, которым резолвится маркет)`
      : '(источник неизвестен)';

    const deltaAtEntry = priceAtEntry - referencePrice;
    const entrySide = deltaAtEntry >= 0 ? 'YES (цена была выше референса)' : 'NO (цена была ниже референса)';
    const chosenMatchesEntrySide =
      (log.chosenOutcome === 'YES' && deltaAtEntry >= 0) || (log.chosenOutcome === 'NO' && deltaAtEntry < 0);

    const parts: string[] = [
      `На входе: цена ${priceAtEntry}, референс окна ${referencePrice} (дельта ${deltaAtEntry.toFixed(2)}, сторона ${entrySide}) ${sourceNote}` +
        (atrRatioAtEntry != null ? `, ATR-рацио ${atrRatioAtEntry.toFixed(2)}x` : ', ATR недоступен'),
    ];

    if (!chosenMatchesEntrySide) {
      parts.push('ВНИМАНИЕ: выбранный исход не совпадает со стороной фида на входе — проверить рассинхрон фида/страйка вручную.');
    }

    if (priceAtClose != null) {
      const deltaAtClose = priceAtClose - referencePrice;
      const closeSide = deltaAtClose >= 0 ? 'YES' : 'NO';
      const flipped = (deltaAtEntry >= 0 && deltaAtClose < 0) || (deltaAtEntry < 0 && deltaAtClose >= 0);
      const atrRatioAtClose = atrAtClose && atrAtClose > 0 ? Math.abs(deltaAtClose) / atrAtClose : null;
      parts.push(
        `На закрытии: цена ${priceAtClose} (дельта ${deltaAtClose.toFixed(2)}, сторона ${closeSide}` +
          (atrRatioAtClose != null ? `, ATR-рацио ${atrRatioAtClose.toFixed(2)}x` : '') +
          `). ${flipped ? 'Цена РАЗВЕРНУЛАСЬ относительно момента входа — классический поздний разворот.' : 'Разворота по нашему фиду не зафиксировано (расхождение с резолвом Polymarket — вероятно микро-разница момента фиксации/источника).'}`,
      );
    } else {
      parts.push('Цена на закрытии по фиду недоступна (WS отвалился ближе к концу окна).');
    }

    return parts.join(' ');
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private parseFloatSafe(v: unknown): number | null {
    const n = parseFloat(String(v));
    return Number.isFinite(n) && n > 0 ? n : null;
  }

  private errMsg(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
