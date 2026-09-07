import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Attempt } from '../entities/attempt.entity';
import { MarketLog } from '../entities/market-log.entity';
import { GammaMarketService } from '../polymarket/gamma-market.service';
import { ClobPublicService } from '../polymarket/clob-public.service';
import { PolymarketTraderService } from '../polymarket/polymarket-trader.service';

@Injectable()
export class TradingService implements OnModuleInit {
  private readonly logger = new Logger(TradingService.name);

  private isSmoke: boolean;
  private betAmount: number;
  private minPrice: number;
  private maxPrice: number;
  private secondsBeforeClose: number;
  private targetSteps: number;
  private pollFarMs: number;
  private pollNearMs: number;
  private resolvePollMs: number;
  private nearWindowSec: number;

  private currentAttempt: Attempt;
  private lastProcessedSlug = '';
  private stopped = false;

  constructor(
    private readonly config: ConfigService,
    private readonly gamma: GammaMarketService,
    private readonly clobPublic: ClobPublicService,
    private readonly trader: PolymarketTraderService,
    @InjectRepository(Attempt) private readonly attemptRepo: Repository<Attempt>,
    @InjectRepository(MarketLog)
    private readonly marketLogRepo: Repository<MarketLog>,
  ) {
    this.isSmoke = this.config.get<string>('SMOKE_START', 'true') === 'true';
    this.betAmount = parseFloat(this.config.get<string>('BET_AMOUNT', '1'));
    this.minPrice = parseFloat(
      this.config.get<string>('MIN_ENTRY_PRICE', '0.98'),
    );
    this.maxPrice = parseFloat(
      this.config.get<string>('MAX_ENTRY_PRICE', '0.99'),
    );
    this.secondsBeforeClose = parseInt(
      this.config.get<string>('SECONDS_BEFORE_CLOSE', '5'),
      10,
    );
    this.targetSteps = parseInt(
      this.config.get<string>('TARGET_STEPS', '500'),
      10,
    );
    this.pollFarMs = parseInt(
      this.config.get<string>('POLL_INTERVAL_MS_FAR', '2000'),
      10,
    );
    this.pollNearMs = parseInt(
      this.config.get<string>('POLL_INTERVAL_MS_NEAR', '500'),
      10,
    );
    this.resolvePollMs = parseInt(
      this.config.get<string>('RESOLVE_POLL_INTERVAL_MS', '10000'),
      10,
    );
    // Начинаем следить за рынком чуть раньше, чем реально готовы покупать,
    // чтобы не проспать узкое окно секунд.
    this.nearWindowSec = Math.max(10, this.secondsBeforeClose + 5);
  }

  async onModuleInit() {
    if (!this.isSmoke) {
      try {
        await this.trader.ensureClient();
      } catch (err) {
        this.logger.error(
          `Не удалось инициализировать боевой торговый клиент — принудительно ` +
            `переключаюсь в SMOKE-режим, чтобы не остаться без защиты ночью. Причина: ${this.errMsg(err)}`,
        );
        this.isSmoke = true;
      }
    }

    this.currentAttempt = await this.getOrCreateActiveAttempt();
    this.logger.log(
      `Старт. Режим: ${this.isSmoke ? 'SMOKE (без реальных ордеров)' : 'LIVE (реальные деньги)'}. ` +
        `Попытка #${this.currentAttempt.attemptNumber}, шаг ${this.currentAttempt.currentStep}/${this.currentAttempt.targetSteps}. ` +
        `Диапазон входа [¢${this.minPrice * 100}-¢${this.maxPrice * 100}], вход за ${this.secondsBeforeClose}с до закрытия.`,
    );

    this.startEngineLoop();
    this.startResolverLoop();
  }

  onModuleDestroy() {
    this.stopped = true;
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
  // Основной цикл: находим текущий 5м маркет и ловим последние секунды.
  // ---------------------------------------------------------------------
  private async startEngineLoop() {
    while (!this.stopped) {
      try {
        await this.tick();
      } catch (err) {
        this.logger.error(`Сбой в основном цикле: ${this.errMsg(err)}`);
        await this.sleep(this.pollFarMs);
      }
    }
  }

  private async tick(): Promise<void> {
    const market = await this.gamma.fetchCurrentMarket();
    if (!market) {
      await this.sleep(this.pollFarMs);
      return;
    }

    if (market.slug === this.lastProcessedSlug) {
      await this.sleep(this.pollNearMs);
      return;
    }

    const timeLeftSec = (market.closesAt.getTime() - Date.now()) / 1000;

    if (timeLeftSec > this.nearWindowSec) {
      await this.sleep(this.pollFarMs);
      return;
    }

    if (timeLeftSec <= 0) {
      // Проспали окно (например, после сбоя сети) — фиксируем как пропуск и едем дальше.
      await this.recordSkippedMarket(market.slug, market.closesAt, 'Окно входа пропущено (timeLeftSec <= 0)');
      this.lastProcessedSlug = market.slug;
      return;
    }

    if (timeLeftSec > this.secondsBeforeClose) {
      await this.sleep(this.pollNearMs);
      return;
    }

    // Финальное окно — принимаем решение и больше не возвращаемся к этому слагу.
    this.lastProcessedSlug = market.slug;
    await this.evaluateAndAct(market.slug, market.closesAt, market.yesTokenId, market.noTokenId, market.negRisk);
  }

  private async evaluateAndAct(
    slug: string,
    closesAt: Date,
    yesTokenId: string,
    noTokenId: string,
    negRisk: boolean,
  ): Promise<void> {
    const [yesQuote, noQuote] = await Promise.all([
      this.clobPublic.getBestQuote(yesTokenId),
      this.clobPublic.getBestQuote(noTokenId),
    ]);

    type Candidate = {
      outcome: 'YES' | 'NO';
      tokenId: string;
      price: number;
      tickSize: string;
    };

    const candidates: Candidate[] = [];

    for (const [outcome, tokenId, quote] of [
      ['YES', yesTokenId, yesQuote],
      ['NO', noTokenId, noQuote],
    ] as const) {
      if (!quote?.bestAsk) continue;
      const { price, size } = quote.bestAsk;
      const hasDepth = price * size >= this.betAmount;
      if (price >= this.minPrice && price <= this.maxPrice && hasDepth) {
        candidates.push({
          outcome,
          tokenId,
          price,
          tickSize: quote.tickSize ?? '0.01',
        });
      }
    }

    if (candidates.length === 0) {
      const reason = this.describeSkip(yesQuote?.bestAsk?.price, noQuote?.bestAsk?.price);
      await this.recordSkippedMarket(slug, closesAt, reason);
      this.logger.log(`[Skip] ${slug}: ${reason}`);
      return;
    }

    // Из подходящих исходов берём тот, что ближе к ¢99 — он "увереннее".
    const chosen = candidates.reduce((best, c) => (c.price > best.price ? c : best));

    if (this.isSmoke) {
      await this.recordSimulatedBuy(slug, closesAt, chosen.outcome, chosen.tokenId, chosen.price);
      this.logger.log(
        `[SMOKE] ${slug}: выставлена бы лимитка ${chosen.outcome} по ¢${(chosen.price * 100).toFixed(1)} — возможен выигрыш.`,
      );
      return;
    }

    try {
      const size = Number((this.betAmount / chosen.price).toFixed(2));
      const result = await this.trader.placeFokBuy({
        tokenId: chosen.tokenId,
        price: chosen.price,
        size,
        tickSize: chosen.tickSize,
        negRisk,
      });
      await this.recordRealBuy(slug, closesAt, chosen.outcome, chosen.tokenId, chosen.price, result.orderId);
      this.logger.log(
        `[LIVE] ${slug}: ордер отправлен, ${chosen.outcome} по ¢${(chosen.price * 100).toFixed(1)}, orderId=${result.orderId}`,
      );
    } catch (err) {
      await this.recordErrorMarket(slug, closesAt, chosen.outcome, chosen.price, this.errMsg(err));
      this.logger.error(`[Error] ${slug}: не удалось отправить ордер — ${this.errMsg(err)}`);
    }
  }

  private describeSkip(yesAsk?: number, noAsk?: number): string {
    const fmt = (p?: number) => (p === undefined ? 'нет стакана' : `¢${(p * 100).toFixed(1)}`);
    return `цена вне диапазона [¢${this.minPrice * 100}-¢${this.maxPrice * 100}] или нет глубины (YES ask=${fmt(yesAsk)}, NO ask=${fmt(noAsk)})`;
  }

  // ---------------------------------------------------------------------
  // Запись в БД
  // ---------------------------------------------------------------------
  private async recordSkippedMarket(slug: string, closesAt: Date, reason: string) {
    await this.marketLogRepo.save(
      this.marketLogRepo.create({
        attemptId: this.currentAttempt.id,
        stepNumber: this.currentAttempt.currentStep + 1,
        slug,
        closesAt,
        chosenOutcome: null,
        executed: false,
        isSmoke: this.isSmoke,
        status: 'skipped',
        skipReason: reason,
        betAmount: this.betAmount,
      }),
    );
  }

  private async recordSimulatedBuy(
    slug: string,
    closesAt: Date,
    outcome: 'YES' | 'NO',
    tokenId: string,
    price: number,
  ) {
    await this.marketLogRepo.save(
      this.marketLogRepo.create({
        attemptId: this.currentAttempt.id,
        stepNumber: this.currentAttempt.currentStep + 1,
        slug,
        closesAt,
        chosenOutcome: outcome,
        chosenTokenId: tokenId,
        entryPrice: price,
        betAmount: this.betAmount,
        executed: true,
        isSmoke: true,
        orderType: 'SIMULATED',
        status: 'pending_resolve',
        logMessage: 'SMOKE: лимитка не отправлялась на биржу, только эмуляция.',
      }),
    );
  }

  private async recordRealBuy(
    slug: string,
    closesAt: Date,
    outcome: 'YES' | 'NO',
    tokenId: string,
    price: number,
    orderId: string | null,
  ) {
    await this.marketLogRepo.save(
      this.marketLogRepo.create({
        attemptId: this.currentAttempt.id,
        stepNumber: this.currentAttempt.currentStep + 1,
        slug,
        closesAt,
        chosenOutcome: outcome,
        chosenTokenId: tokenId,
        entryPrice: price,
        betAmount: this.betAmount,
        executed: true,
        isSmoke: false,
        orderType: 'FOK',
        orderId,
        status: 'pending_resolve',
      }),
    );
  }

  private async recordErrorMarket(
    slug: string,
    closesAt: Date,
    outcome: 'YES' | 'NO',
    price: number,
    errorMessage: string,
  ) {
    await this.marketLogRepo.save(
      this.marketLogRepo.create({
        attemptId: this.currentAttempt.id,
        stepNumber: this.currentAttempt.currentStep + 1,
        slug,
        closesAt,
        chosenOutcome: outcome,
        entryPrice: price,
        betAmount: this.betAmount,
        executed: false,
        isSmoke: this.isSmoke,
        status: 'error',
        errorMessage,
      }),
    );
  }

  // ---------------------------------------------------------------------
  // Резолвер: проверяет исход закрытых рынков и продвигает счётчик шагов.
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

      log.status = won ? 'win' : 'loss';
      log.resolvedAt = new Date();
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
          `[LOSS] Попытка #${attempt.attemptNumber} (${attempt.isSmoke ? 'smoke' : 'live'}) слита на шаге ${attempt.currentStep}. Открываю новую попытку.`,
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

  private errMsg(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
