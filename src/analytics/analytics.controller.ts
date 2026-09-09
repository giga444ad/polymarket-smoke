import { Controller, Get, Query } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Attempt } from '../entities/attempt.entity';
import { MarketLog } from '../entities/market-log.entity';

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
            isSmoke: bestAttempt.isSmoke,
            reachedStep: bestAttempt.currentStep,
            targetSteps: bestAttempt.targetSteps,
            status: bestAttempt.status,
          }
        : null,
      attempts: attempts.map((a) => ({
        attemptNumber: a.attemptNumber,
        isSmoke: a.isSmoke,
        status: a.status,
        reachedStep: a.currentStep,
        targetSteps: a.targetSteps,
        createdAt: a.createdAt,
        finishedAt: a.finishedAt,
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
}
