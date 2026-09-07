import { Controller, Get, Query } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Attempt } from '../entities/attempt.entity';
import { MarketLog } from '../entities/market-log.entity';

@Controller('analytics')
export class AnalyticsController {
  constructor(
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
    const totalWins = await this.marketLogRepo.count({ where: { status: 'win' } });
    const totalLosses = await this.marketLogRepo.count({ where: { status: 'loss' } });
    const totalErrors = await this.marketLogRepo.count({ where: { status: 'error' } });

    const bestAttempt = attempts.reduce<Attempt | null>(
      (best, a) => (!best || a.currentStep > best.currentStep ? a : best),
      null,
    );

    return {
      totals: {
        marketsScanned: totalLogs,
        betsPlaced: totalExecuted,
        skipped: totalSkipped,
        wins: totalWins,
        losses: totalLosses,
        errors: totalErrors,
      },
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
