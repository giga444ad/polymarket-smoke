import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Attempt } from '../entities/attempt.entity';
import { MarketLog } from '../entities/market-log.entity';
import { AnalyticsController } from './analytics.controller';
import { HealthController } from './health.controller';
import { TradingModule } from '../trading/trading.module';

@Module({
  // TradingModule экспортирует BalanceService — нужен контроллеру для
  // кошелькового equity (fetchPortfolioValue) в /analytics/summary (лайв).
  imports: [TypeOrmModule.forFeature([Attempt, MarketLog]), TradingModule],
  controllers: [AnalyticsController, HealthController],
})
export class AnalyticsModule {}
