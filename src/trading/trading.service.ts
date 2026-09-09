import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Attempt } from '../entities/attempt.entity';
import { ChosenOutcome, MarketLog, MarketLogStatus, OrderKind } from '../entities/market-log.entity';
import { GammaMarketService } from '../polymarket/gamma-market.service';
import { ClobPublicService } from '../polymarket/clob-public.service';
import { PolymarketTraderService } from '../polymarket/polymarket-trader.service';
import { LiveBook, MarketWsStream, Outcome } from '../polymarket/market-ws-stream';
import { cumulativeUsdAtOrBelow, walkAsksForFill } from '../polymarket/book-fill.util';

type LimitTier = 'T1' | 'T2' | 'T3';

interface RestingOrder {
  tier: LimitTier;
  outcome: Outcome;
  price: number;
  // null в смоуке (ничего реального не выставляли)
  orderId: string | null;
}

interface MarketState {
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
  private betAmount: number;
  private minMarketPrice: number;
  private maxMarketPrice: number;
  private favoriteBidThreshold: number;
  private tierPrices: Record<LimitTier, number>;
  private tier2Seconds: number;
  private tier3Seconds: number;
  private maxOverspendMultiplier: number;
  private minFillRatio: number;
  private targetSteps: number;
  private discoveryPollMs: number;
  private resolvePollMs: number;
  private assetPrefixes: string[];

  private currentAttempt: Attempt;
  private activeMarkets = new Map<string, MarketState>();
  private stopped = false;

  constructor(
    private readonly config: ConfigService,
    private readonly gamma: GammaMarketService,
    private readonly clobPublic: ClobPublicService,
    private readonly trader: PolymarketTraderService,
    @InjectRepository(Attempt) private readonly attemptRepo: Repository<Attempt>,
    @InjectRepository(MarketLog) private readonly marketLogRepo: Repository<MarketLog>,
  ) {
    this.isSmoke = this.config.get<string>('SMOKE_START', 'true') === 'true';
    this.betAmount = parseFloat(this.config.get<string>('BET_AMOUNT', '5'));
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
    this.maxOverspendMultiplier = parseFloat(
      this.config.get<string>('MAX_OVERSPEND_MULTIPLIER', '1.5'),
    );
    this.minFillRatio = parseFloat(this.config.get<string>('MIN_FILL_RATIO', '0.5'));
    this.targetSteps = parseInt(this.config.get<string>('TARGET_STEPS', '500'), 10);
    this.discoveryPollMs = parseInt(this.config.get<string>('MARKET_DISCOVERY_POLL_MS', '1500'), 10);
    this.resolvePollMs = parseInt(this.config.get<string>('RESOLVE_POLL_INTERVAL_MS', '10000'), 10);
    this.assetPrefixes = this.config
      .get<string>('MARKET_ASSETS', 'btc-updown-5m')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
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

    this.currentAttempt = await this.getOrCreateActiveAttempt();
    this.logger.log(
      `Старт. Режим: ${this.isSmoke ? 'SMOKE (без реальных ордеров)' : 'LIVE (реальные деньги)'}. ` +
        `Активы: ${this.assetPrefixes.join(', ')}. ` +
        `Попытка #${this.currentAttempt.attemptNumber}, шаг ${this.currentAttempt.currentStep}/${this.currentAttempt.targetSteps}. ` +
        `Маркет-тейк [¢${this.minMarketPrice * 100}-¢${this.maxMarketPrice * 100}] (минимум заполнения ${this.minFillRatio * 100}%), ` +
        `лимитки-фолбэк от ¢${this.favoriteBidThreshold * 100} (тиры ${this.tierPrices.T1 * 100}/${this.tierPrices.T2 * 100}/${this.tierPrices.T3 * 100}).`,
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

  private async getOrCreateActiveAttempt(): Promise<Attempt> {
    const active = await this.attemptRepo.findOne({
      where: { status: 'active', isSmoke: this.isSmoke },
      order: { createdAt: 'DESC' },
    });
    if (active) return active;

    const last = await this.attemptRepo.findOne({
      where: { isSmoke: this.isSmoke },
      order: { attemptNumber: 'DESC' },
    });
    const attempt = this.attemptRepo.create({
      attemptNumber: (last?.attemptNumber ?? 0) + 1,
      currentStep: 0,
      targetSteps: this.targetSteps,
      status: 'active',
      isSmoke: this.isSmoke,
      finishedAt: null,
    });
    return this.attemptRepo.save(attempt);
  }

  // ---------------------------------------------------------------------
  // Обнаружение маркетов по каждому настроенному активу отдельно.
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
    const startTs = this.gamma.currentIntervalStartTimestampSec();
    const closeTs = this.gamma.currentIntervalCloseTimestampSec();

    for (const assetPrefix of this.assetPrefixes) {
      const slug = this.gamma.buildSlugForStart(assetPrefix, startTs);
      if (this.activeMarkets.get(assetPrefix)?.slug === slug) continue; // уже отслеживаем

      const market = await this.gamma.fetchMarketBySlug(slug, closeTs);
      if (!market) continue; // ещё не создан на Gamma — попробуем на следующем тике

      await this.openMarket(assetPrefix, market.slug, market.closesAt, market.yesTokenId, market.noTokenId, market.negRisk);
    }
  }

  private async openMarket(
    assetPrefix: string,
    slug: string,
    closesAt: Date,
    yesTokenId: string,
    noTokenId: string,
    negRisk: boolean,
  ): Promise<void> {
    // Разовый REST-бутстрап: тик-сайз + официальный минимальный размер ордера биржи.
    const [yesBoot, noBoot] = await Promise.all([
      this.clobPublic.getBestQuote(yesTokenId),
      this.clobPublic.getBestQuote(noTokenId),
    ]);
    const initialTickSize = yesBoot?.tickSize ?? noBoot?.tickSize ?? '0.01';
    const minOrderSize = yesBoot?.minOrderSize ?? noBoot?.minOrderSize ?? 5;

    const marketState: MarketState = {
      assetPrefix,
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
    };

    const stream = new MarketWsStream(
      yesTokenId,
      noTokenId,
      initialTickSize,
      (outcome, book) => this.onBookUpdate(marketState, outcome, book),
    );
    marketState.stream = stream;

    const msUntilClose = Math.max(0, closesAt.getTime() - Date.now());
    marketState.closeTimer = setTimeout(() => this.finalizeMarket(marketState), msUntilClose);

    this.activeMarkets.set(assetPrefix, marketState);
    stream.connect();

    this.logger.log(
      `[${assetPrefix}] ${slug}: открыт WS-поток (закрытие через ${(msUntilClose / 1000).toFixed(0)}с, min_order_size=${minOrderSize}, tick=${initialTickSize})`,
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
    if (this.isSmoke && marketState.restingOrder?.outcome === outcome) {
      const resting = marketState.restingOrder;
      const targetUsd = this.limitOrderTargetUsd(marketState, resting.price);
      const availableUsd = cumulativeUsdAtOrBelow(book.asks, resting.price);
      if (availableUsd >= targetUsd) {
        marketState.positioned = true;
        marketState.restingOrder = null;
        const filledShares = targetUsd / resting.price;
        void this.writeLog(marketState, {
          chosenOutcome: outcome,
          chosenTokenId: outcome === 'YES' ? marketState.yesTokenId : marketState.noTokenId,
          entryPrice: resting.price,
          filledAmount: targetUsd,
          fillRatio: targetUsd / this.betAmount,
          executed: true,
          orderType: 'SIMULATED_LIMIT',
          limitTier: resting.tier,
          status: 'pending_resolve',
          logMessage: `SMOKE: лимитка не отправлялась на биржу — эмуляция; накопленный объём продавцов по ¢${(resting.price * 100).toFixed(2)} и ниже составил $${availableUsd.toFixed(2)}, взяли ${filledShares.toFixed(2)} шт.`,
        });
        this.logger.log(
          `[${marketState.assetPrefix}][SMOKE][Limit fill] ${marketState.slug}: ${outcome} по ¢${(resting.price * 100).toFixed(2)} (тир ${resting.tier})`,
        );
        return;
      }
    }

    // 1) Правило A — агрессивный маркет-тейк: реально проходим по уровням стакана
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

    // 2) Правило B — лимитка-фолбэк, если у фаворита реально нет предложений на продажу.
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
   *  (не капая эту сумму обратно до betAmount — иначе проверка допустимого перерасхода
   *  в вызывающем коде никогда не сработает). */
  private limitOrderTargetUsd(marketState: MarketState, price: number): number {
    const minUsd = marketState.minOrderSize * price;
    return Math.max(this.betAmount, minUsd);
  }

  // ---------------------------------------------------------------------
  // Правило A: маркет-тейк — честно проходим по уровням стакана (VWAP),
  // не выше maxMarketPrice, и не принимаем сделку, если реальной глубины
  // хватает меньше чем на minFillRatio от заявленной ставки (иначе легко
  // словить дребезг тонкой лимитки, а не настоящее направление рынка).
  // ---------------------------------------------------------------------
  private async tryMarketBuy(marketState: MarketState, outcome: Outcome, book: LiveBook): Promise<void> {
    const tokenId = outcome === 'YES' ? marketState.yesTokenId : marketState.noTokenId;
    const fill = walkAsksForFill(book.asks, this.betAmount, this.maxMarketPrice);

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

    try {
      if (this.isSmoke) {
        marketState.positioned = true;
        await this.cancelRestingIfAny(marketState);
        await this.writeLog(marketState, {
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
              ? `SMOKE: частичное исполнение — забрали $${fill.filledUsd.toFixed(2)} из $${this.betAmount} (${(fill.filledRatio * 100).toFixed(0)}%) по VWAP ¢${(fill.vwapPrice! * 100).toFixed(2)}.`
              : `SMOKE: маркет-ордер не отправлялся, только эмуляция прохода по стакану (VWAP ¢${(fill.vwapPrice! * 100).toFixed(2)}).`,
        });
        this.logger.log(
          `[${marketState.assetPrefix}][SMOKE][Market] ${marketState.slug}: ${outcome} по VWAP ¢${(fill.vwapPrice! * 100).toFixed(2)} ` +
            `($${fill.filledUsd.toFixed(2)}${fill.filledRatio < 0.999 ? `, ${(fill.filledRatio * 100).toFixed(0)}% от заявки` : ''})`,
        );
        return;
      }

      const result = await this.trader.placeMarketBuy({
        tokenId,
        amountUsd: this.betAmount,
        worstPrice: this.maxMarketPrice,
        tickSize: book.tickSize,
        negRisk: marketState.negRisk,
      });

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
      await this.writeLog(marketState, {
        chosenOutcome: outcome,
        chosenTokenId: tokenId,
        entryPrice: fill.vwapPrice,
        filledAmount: actualUsd,
        fillRatio: actualUsd / this.betAmount,
        executed: true,
        orderType: 'FAK',
        orderId: result.orderId,
        status: 'pending_resolve',
      });
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

      if (targetUsd > this.betAmount * this.maxOverspendMultiplier) {
        marketState.skippedLimitTier = tier;
        this.logger.debug(
          `[${marketState.assetPrefix}][Limit] ${marketState.slug}: пропуск тира ${tier} — нужно ~$${targetUsd.toFixed(2)} для минимума биржи, больше допустимого.`,
        );
        return;
      }
      const size = Number((targetUsd / price).toFixed(2));

      if (this.isSmoke) {
        marketState.restingOrder = { tier, outcome, price, orderId: null };
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

      marketState.restingOrder = { tier, outcome, price, orderId: result.orderId };
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
          });
          return;
        }

        const status = resting.orderId ? await this.trader.getOrderStatus(resting.orderId) : null;
        if (status && status.sizeMatched > 0) {
          const filledAmount = status.sizeMatched * resting.price;
          await this.writeLog(marketState, {
            chosenOutcome: resting.outcome,
            chosenTokenId: tokenId,
            entryPrice: resting.price,
            filledAmount,
            fillRatio: filledAmount / this.betAmount,
            executed: true,
            orderType: 'GTD',
            limitTier: resting.tier,
            orderId: resting.orderId,
            status: 'pending_resolve',
            logMessage: `Исполнено ${status.sizeMatched}/${status.originalSize} шт.`,
          });
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
    },
  ): Promise<void> {
    if (marketState.logWritten) return;
    marketState.logWritten = true;

    await this.marketLogRepo.save(
      this.marketLogRepo.create({
        attemptId: this.currentAttempt.id,
        stepNumber: this.currentAttempt.currentStep + 1,
        assetPrefix: marketState.assetPrefix,
        slug: marketState.slug,
        closesAt: marketState.closesAt,
        betAmount: this.betAmount,
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
      }),
    );
  }

  // ---------------------------------------------------------------------
  // Резолвер: REST-опрос Gamma API по закрытым маркетам, продвигает шаги
  // и считает профит по факту исхода.
  // ---------------------------------------------------------------------
  private async startResolverLoop() {
    while (!this.stopped) {
      try {
        await this.resolvePendingMarkets();
      } catch (err) {
        this.logger.error(`Сбой резолвера: ${this.errMsg(err)}`);
      }
      await this.sleep(this.resolvePollMs);
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
      await this.marketLogRepo.save(log);

      const attempt = await this.attemptRepo.findOneOrFail({ where: { id: log.attemptId } });
      if (attempt.status !== 'active') continue; // попытка уже закрыта ранее

      if (won) {
        attempt.currentStep += 1;
        if (attempt.currentStep >= attempt.targetSteps) {
          attempt.status = 'completed_target';
          attempt.finishedAt = new Date();
          this.logger.log(
            `[GOAL] Попытка #${attempt.attemptNumber} (${attempt.isSmoke ? 'smoke' : 'live'}) дошла до ${attempt.targetSteps} шага!`,
          );
        }
        await this.attemptRepo.save(attempt);
      } else {
        attempt.status = 'failed';
        attempt.finishedAt = new Date();
        await this.attemptRepo.save(attempt);
        this.logger.warn(
          `[LOSS] Попытка #${attempt.attemptNumber} (${attempt.isSmoke ? 'smoke' : 'live'}) слита на шаге ${attempt.currentStep} (${log.assetPrefix}, профит шага $${log.profit.toFixed(2)}). Открываю новую попытку.`,
        );

        const next = this.attemptRepo.create({
          attemptNumber: attempt.attemptNumber + 1,
          currentStep: 0,
          targetSteps: this.targetSteps,
          status: 'active',
          isSmoke: attempt.isSmoke,
          finishedAt: null,
        });
        this.currentAttempt = await this.attemptRepo.save(next);
      }
    }
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
