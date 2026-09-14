import { Controller, Get, NotFoundException, Param, Query } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { LessThan, Repository } from 'typeorm';
import { Attempt } from '../entities/attempt.entity';
import { MarketLog } from '../entities/market-log.entity';

// Ссылка на страницу события на Polymarket (п.2 сессии 6, см. CONTEXT.md).
// Слаг маркета Polymarket 1-в-1 совпадает со слагом события для этих
// Up/Down маркетов (проверено вручную на живых данных), поэтому отдельного
// маппинга market-slug -> event-slug не потребовалось.
function eventUrlForSlug(slug: string): string {
  return `https://polymarket.com/event/${slug}`;
}

// Общая, компактная проекция MarketLog для списковых ручек (/trades,
// /streams/:key, /attempts/:id) — специально НЕ тащит всю диагностику
// ATR/фида (она есть в /analytics/losses для разбора сливов), чтобы не
// раздувать полезную нагрузку на фронте (п.1 сессии 6 — "не грузить фронт").
function toTradeDto(log: MarketLog, attemptNumber?: number) {
  return {
    id: log.id,
    attemptId: log.attemptId,
    attemptNumber: attemptNumber ?? null,
    stepNumber: log.stepNumber,
    streamKey: log.assetPrefix,
    slug: log.slug,
    eventUrl: eventUrlForSlug(log.slug),
    isSmoke: log.isSmoke,
    status: log.status,
    chosenOutcome: log.chosenOutcome,
    entryPrice: log.entryPrice,
    betAmount: log.betAmount,
    stakePredicted: log.stakePredicted,
    predictedFromLogId: log.predictedFromLogId,
    filledAmount: log.filledAmount,
    fillRatio: log.fillRatio,
    profit: log.profit,
    profitPct: log.filledAmount ? (log.profit != null ? (log.profit / log.filledAmount) * 100 : null) : null,
    orderType: log.orderType,
    closesAt: log.closesAt,
    createdAt: log.createdAt,
    resolvedAt: log.resolvedAt,
  };
}

@Controller('analytics')
export class AnalyticsController {
  constructor(
    private readonly config: ConfigService,
    @InjectRepository(Attempt) private readonly attemptRepo: Repository<Attempt>,
    @InjectRepository(MarketLog)
    private readonly marketLogRepo: Repository<MarketLog>,
  ) {}

  @Get('summary')
  async getSummary() {
    const attempts = await this.attemptRepo.find({
      order: { isSmoke: 'ASC', attemptNumber: 'ASC' },
    });

    const totalLogs = await this.marketLogRepo.count();
    const totalExecuted = await this.marketLogRepo.count({ where: { executed: true } });
    const totalSkipped = await this.marketLogRepo.count({ where: { status: 'skipped' } });
    const totalUnfilled = await this.marketLogRepo.count({ where: { status: 'unfilled' } });
    const totalWins = await this.marketLogRepo.count({ where: { status: 'win' } });
    const totalLosses = await this.marketLogRepo.count({ where: { status: 'loss' } });
    const totalErrors = await this.marketLogRepo.count({ where: { status: 'error' } });

    // Профит считаем отдельно по смоуку и по боевому режиму — это разные "кошельки".
    const profitRow = await this.marketLogRepo
      .createQueryBuilder('log')
      .select('log.isSmoke', 'isSmoke')
      .addSelect('COALESCE(SUM(log.profit), 0)', 'totalProfit')
      .where('log.profit IS NOT NULL')
      .groupBy('log.isSmoke')
      .getRawMany<{ isSmoke: boolean; totalProfit: string }>();

    const startingBankroll = parseFloat(this.config.get<string>('STARTING_BANKROLL', '1000'));
    const profitByMode = { smoke: 0, live: 0 };
    for (const row of profitRow) {
      const value = parseFloat(row.totalProfit) || 0;
      if (row.isSmoke) profitByMode.smoke = value;
      else profitByMode.live = value;
    }

    // Разбивка по активу — сколько шагов/профита принёс каждый настроенный актив.
    const perAssetRaw = await this.marketLogRepo
      .createQueryBuilder('log')
      .select('log.assetPrefix', 'assetPrefix')
      .addSelect('log.isSmoke', 'isSmoke')
      .addSelect(
        `SUM(CASE WHEN log.status = 'win' THEN 1 ELSE 0 END)`,
        'wins',
      )
      .addSelect(
        `SUM(CASE WHEN log.status = 'loss' THEN 1 ELSE 0 END)`,
        'losses',
      )
      .addSelect('COALESCE(SUM(log.profit), 0)', 'profit')
      .groupBy('log.assetPrefix')
      .addGroupBy('log.isSmoke')
      .getRawMany<{ assetPrefix: string; isSmoke: boolean; wins: string; losses: string; profit: string }>();

    const bestAttempt = attempts.reduce<Attempt | null>(
      (best, a) => (!best || a.currentStep > best.currentStep ? a : best),
      null,
    );

    // Лучшая попытка ОТДЕЛЬНО по каждому потоку (streamKey) — раньше был
    // единственный общий счётчик на все активы, теперь у каждого потока своя
    // независимая прогрессия (см. BACKLOG п.3), так что имеет смысл сравнивать
    // прогресс потоков между собой, а не только глобальный максимум.
    const bestByStream = new Map<string, Attempt>();
    for (const a of attempts) {
      const current = bestByStream.get(a.streamKey);
      if (!current || a.currentStep > current.currentStep) bestByStream.set(a.streamKey, a);
    }

    // Маркеты, зависшие в pending_resolve дольше STALE_RESOLVE_WARN_MS — то же,
    // что предупреждает в логах TradingService.warnStaleUnresolved(), но здесь
    // видно на фронте без необходимости лезть в консоль.
    const staleResolveWarnMs = parseInt(this.config.get<string>('STALE_RESOLVE_WARN_MS', '180000'), 10);
    const staleBefore = new Date(Date.now() - staleResolveWarnMs);
    const staleLogs = await this.marketLogRepo.find({
      where: { status: 'pending_resolve', createdAt: LessThan(staleBefore) },
      order: { createdAt: 'ASC' },
      take: 20,
    });

    return {
      startingBankroll,
      currentBankroll: {
        smoke: Number((startingBankroll + profitByMode.smoke).toFixed(2)),
        live: Number((startingBankroll + profitByMode.live).toFixed(2)),
      },
      profit: {
        smoke: Number(profitByMode.smoke.toFixed(2)),
        live: Number(profitByMode.live.toFixed(2)),
      },
      totals: {
        marketsScanned: totalLogs,
        betsPlaced: totalExecuted,
        skipped: totalSkipped,
        unfilled: totalUnfilled,
        wins: totalWins,
        losses: totalLosses,
        errors: totalErrors,
      },
      byAsset: perAssetRaw.map((r) => ({
        assetPrefix: r.assetPrefix,
        isSmoke: r.isSmoke,
        wins: parseInt(r.wins, 10) || 0,
        losses: parseInt(r.losses, 10) || 0,
        profit: Number((parseFloat(r.profit) || 0).toFixed(2)),
      })),
      bestAttempt: bestAttempt
        ? {
            attemptNumber: bestAttempt.attemptNumber,
            streamKey: bestAttempt.streamKey,
            isSmoke: bestAttempt.isSmoke,
            reachedStep: bestAttempt.currentStep,
            targetSteps: bestAttempt.targetSteps,
            status: bestAttempt.status,
          }
        : null,
      bestByStream: [...bestByStream.values()].map((a) => ({
        id: a.id,
        streamKey: a.streamKey,
        attemptNumber: a.attemptNumber,
        isSmoke: a.isSmoke,
        reachedStep: a.currentStep,
        targetSteps: a.targetSteps,
        status: a.status,
        currentStake: a.currentStake,
      })),
      attempts: attempts.map((a) => ({
        id: a.id,
        attemptNumber: a.attemptNumber,
        streamKey: a.streamKey,
        isSmoke: a.isSmoke,
        status: a.status,
        reachedStep: a.currentStep,
        targetSteps: a.targetSteps,
        currentStake: a.currentStake,
        baseStake: a.baseStake,
        createdAt: a.createdAt,
        finishedAt: a.finishedAt,
      })),
      // Незарезолвленные дольше нормы — сигнал, что резолвер Gamma подвис
      // (см. обсуждение "ставлю следующую ставку, пока прошлая не зарезолвилась").
      staleUnresolved: staleLogs.map((log) => ({
        assetPrefix: log.assetPrefix,
        slug: log.slug,
        createdAt: log.createdAt,
        ageSec: Math.round((Date.now() - log.createdAt.getTime()) / 1000),
      })),
    };
  }

  @Get('logs')
  async getRecentLogs(@Query('limit') limit = '100', @Query('status') status?: string) {
    return this.marketLogRepo.find({
      where: status ? ({ status } as any) : undefined,
      order: { createdAt: 'DESC' },
      take: Math.min(parseInt(limit, 10) || 100, 500),
    });
  }

  // Отдельная урезанная выдача только по сливам — с причиной и диагностикой,
  // чтобы на фронте/скриптом можно было быстро разобрать статистику по факторам.
  @Get('losses')
  async getLosses(@Query('limit') limit = '100') {
    const logs = await this.marketLogRepo.find({
      where: { status: 'loss' },
      order: { resolvedAt: 'DESC' },
      take: Math.min(parseInt(limit, 10) || 100, 500),
    });
    return logs.map((log) => ({
      slug: log.slug,
      assetPrefix: log.assetPrefix,
      chosenOutcome: log.chosenOutcome,
      entryPrice: log.entryPrice,
      profit: log.profit,
      resolvedAt: log.resolvedAt,
      failReason: log.failReason,
      referencePrice: log.referencePrice,
      priceAtEntry: log.priceAtEntry,
      atrAtEntry: log.atrAtEntry,
      atrRatioAtEntry: log.atrRatioAtEntry,
      priceAtClose: log.priceAtClose,
      atrAtClose: log.atrAtClose,
    }));
  }

  // ---------------------------------------------------------------------
  // Сессия 6, п.1: единый список ставок (активные pending_resolve + история)
  // по ВСЕМ потокам сразу, отдельно от /analytics/summary — не нагружает
  // тяжёлую сводку и позволяет фронту дёргать это чаще/пагинированно.
  // ---------------------------------------------------------------------
  @Get('trades')
async getTrades(
  @Query('page') page = '1',
  @Query('limit') limit = '100',
  @Query('streamKey') streamKey?: string,
  @Query('status') status?: string,
) {
  const parsedPage = Math.max(parseInt(page, 10) || 1, 1);
  const requestedLimit = parseInt(limit, 10) || 100;
  
  const finalLimit = Math.min(Math.max(requestedLimit, 1), 1000);
  const skip = (parsedPage - 1) * finalLimit;

  const qb = this.marketLogRepo
    .createQueryBuilder('log')
    .leftJoin(Attempt, 'attempt', 'attempt.id = log.attemptId')
    .addSelect('attempt.attemptNumber', 'attemptNumber')
    .orderBy('log.createdAt', 'DESC')
    .skip(skip)
    .take(finalLimit);

  if (streamKey) qb.andWhere('log.assetPrefix = :streamKey', { streamKey });
  if (status) qb.andWhere('log.status = :status', { status });

  const [{ entities, raw }, total] = await Promise.all([
    qb.getRawAndEntities(),
    qb.getCount(),
  ]);

  const data = entities.map((log, i) =>
    toTradeDto(log, parseInt(raw[i]?.attemptNumber, 10) || undefined),
  );

  return {
    data,
    meta: {
      total,
      page: parsedPage,
      limit: finalLimit,
      totalPages: Math.ceil(total / finalLimit),
    },
  };
}

  // Сессия 6, п.3: "событие" в терминах пользователя = поток (streamKey) —
  // все ставки этого потока по ВСЕМ его попыткам (история + активные).
  @Get('streams/:streamKey')
  async getStreamDetail(@Param('streamKey') streamKey: string, @Query('limit') limit = '200') {
    const attemptsOfStream = await this.attemptRepo.find({
      where: { streamKey },
      order: { attemptNumber: 'ASC' },
    });
    if (attemptsOfStream.length === 0) {
      throw new NotFoundException(`Поток "${streamKey}" не найден (нет ни одной попытки).`);
    }
    const attemptNumberById = new Map(attemptsOfStream.map((a) => [a.id, a.attemptNumber]));

    const logs = await this.marketLogRepo.find({
      where: { assetPrefix: streamKey },
      order: { createdAt: 'DESC' },
      take: Math.min(parseInt(limit, 10) || 200, 500),
    });

    const totalProfit = attemptsOfStream.length
      ? (await this.marketLogRepo
          .createQueryBuilder('log')
          .select('COALESCE(SUM(log.profit), 0)', 'total')
          .where('log.assetPrefix = :streamKey', { streamKey })
          .getRawOne<{ total: string }>())
      : null;

    return {
      streamKey,
      attemptsCount: attemptsOfStream.length,
      totalProfit: Number((parseFloat(totalProfit?.total ?? '0') || 0).toFixed(2)),
      attempts: attemptsOfStream.map((a) => ({
        id: a.id,
        attemptNumber: a.attemptNumber,
        status: a.status,
        isSmoke: a.isSmoke,
        reachedStep: a.currentStep,
        targetSteps: a.targetSteps,
        currentStake: a.currentStake,
        baseStake: a.baseStake,
        createdAt: a.createdAt,
        finishedAt: a.finishedAt,
      })),
      trades: logs.map((log) => toTradeDto(log, attemptNumberById.get(log.attemptId))),
    };
  }

  // Сессия 6, п.4: конкретная попытка целиком — её шаги/история/активная
  // ставка/цена входа и т.д., без остальных потоков и попыток.
  @Get('attempts/:id')
  async getAttemptDetail(@Param('id') id: string) {
    const attempt = await this.attemptRepo.findOne({ where: { id } });
    if (!attempt) {
      throw new NotFoundException(`Попытка ${id} не найдена.`);
    }
    const logs = await this.marketLogRepo.find({
      where: { attemptId: id },
      order: { stepNumber: 'ASC' },
    });

    return {
      attempt: {
        id: attempt.id,
        attemptNumber: attempt.attemptNumber,
        streamKey: attempt.streamKey,
        status: attempt.status,
        isSmoke: attempt.isSmoke,
        reachedStep: attempt.currentStep,
        targetSteps: attempt.targetSteps,
        currentStake: attempt.currentStake,
        baseStake: attempt.baseStake,
        createdAt: attempt.createdAt,
        finishedAt: attempt.finishedAt,
      },
      steps: logs.map((log) => toTradeDto(log, attempt.attemptNumber)),
    };
  }
}
