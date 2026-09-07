import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Attempt } from '../entities/attempt.entity';
import { MarketLog } from '../entities/market-log.entity';
import { AnalyticsController } from './analytics.controller';
import { HealthController } from './health.controller';

@Module({
  imports: [TypeOrmModule.forFeature([Attempt, MarketLog])],
  controllers: [AnalyticsController, HealthController],
})
export class AnalyticsModule {}
