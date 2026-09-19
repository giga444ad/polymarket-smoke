import { Body, Controller, Post, UseGuards, BadRequestException } from '@nestjs/common';
import { RolesGuard } from '../auth/guards/roles.guard';
import { Roles } from '../auth/decorators/roles.decorator';
import { Role } from '../auth/role.enum';
import { runMonteCarlo, MonteCarloParams, MonteCarloResult } from './montecarlo.util';

/**
 * Сессия 20 — эндпоинт для Монте-Карло симулятора стратегии (вкладка
 * "Монте-Карло" во фронте polyguru). Чистый расчёт, БД не трогает.
 * Admin-only (как backtest): считает тысячи прогонов, не для viewer'ов.
 */
@Controller('simulate')
export class SimulatorController {
  @UseGuards(RolesGuard)
  @Roles(Role.ADMIN)
  @Post('montecarlo')
  montecarlo(@Body() body: Partial<MonteCarloParams>): MonteCarloResult {
    const p = this.validate(body);
    return runMonteCarlo(p);
  }

  private validate(b: Partial<MonteCarloParams>): MonteCarloParams {
    const num = (v: unknown, name: string, min: number, max: number, def?: number): number => {
      const n = v == null && def != null ? def : Number(v);
      if (!Number.isFinite(n) || n < min || n > max) {
        throw new BadRequestException(`Параметр "${name}" должен быть числом в [${min}, ${max}] (получено: ${String(v)}).`);
      }
      return n;
    };

    const closeMode = b.closeMode === 'steps' ? 'steps' : 'profit';
    const entryPrice = num(b.entryPrice, 'entryPrice', 0.5, 0.999);
    const entryPriceMax = b.entryPriceMax != null ? num(b.entryPriceMax, 'entryPriceMax', entryPrice, 0.999) : undefined;
    // Потолок runs×bets, чтобы один запрос не подвесил процесс.
    const runs = num(b.runs, 'runs', 100, 50000, 5000);
    const horizonDays = num(b.horizonDays, 'horizonDays', 1, 365, 30);
    const betsPerDay = num(b.betsPerDay, 'betsPerDay', 1, 2000, 100);
    if (runs * betsPerDay * horizonDays > 2_000_000_000) {
      throw new BadRequestException('Слишком большой объём симуляции (runs × betsPerDay × horizonDays) — уменьшите параметры.');
    }

    return {
      baseStake: num(b.baseStake, 'baseStake', 0.01, 100000, 5),
      winRate: num(b.winRate, 'winRate', 0, 1),
      entryPrice,
      entryPriceMax,
      closeMode,
      targetProfitUsd: num(b.targetProfitUsd, 'targetProfitUsd', 0.01, 1_000_000, 10),
      targetSteps: num(b.targetSteps, 'targetSteps', 1, 100000, 60),
      startingBankroll: num(b.startingBankroll, 'startingBankroll', 0.01, 100_000_000, 500),
      betsPerDay,
      horizonDays,
      runs,
      seed: b.seed != null ? num(b.seed, 'seed', 0, 2 ** 31) : undefined,
    };
  }
}
