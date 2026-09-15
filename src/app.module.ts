import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ServeStaticModule } from '@nestjs/serve-static';
import { ThrottlerGuard, ThrottlerModule } from '@nestjs/throttler';
import { APP_GUARD } from '@nestjs/core';
import { join } from 'path';

import { Attempt } from './entities/attempt.entity';
import { MarketLog } from './entities/market-log.entity';
import { PriceCandle } from './entities/price-candle.entity';
import { ActiveWindow } from './entities/active-window.entity';
import { PriceTick } from './entities/price-tick.entity';
import { PolymarketPriceTick } from './entities/polymarket-price-tick.entity';
import { EdgeScoreSample } from './entities/edge-score-sample.entity';
import { User } from './auth/user.entity';
import { AuthModule } from './auth/auth.module';
import { TradingModule } from './trading/trading.module';
import { AnalyticsModule } from './analytics/analytics.module';
import { BacktestModule } from './backtest/backtest.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.POSTGRES_HOST || 'localhost',
      port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
      username: process.env.POSTGRES_USER || 'postgres',
      password: process.env.POSTGRES_PASSWORD || 'postgres',
      database: process.env.POSTGRES_DB || 'polymarket_bot',
      entities: [Attempt, MarketLog, PriceCandle, ActiveWindow, PriceTick, PolymarketPriceTick, EdgeScoreSample, User],
      // Только для смоук/дев-контура: сам создаёт таблицы по сущностям.
      // Для боевого использования лучше завести нормальные миграции.
      synchronize: process.env.TYPEORM_SYNC !== 'false',
      logging: process.env.TYPEORM_LOGGING === 'true',
    }),
    
    ServeStaticModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        const isStatic = configService.get<string>('IS_STATIC') === 'true';

        if (!isStatic) {
          return [];
        }

        return [
          {
            rootPath: join(__dirname, '..', 'public'),
          },
        ];
      },
    }),

    // Глобальный "град"-лимитер поверх Auth-гвардов (см. AuthModule) — общий
    // потолок запросов на IP независимо от того, авторизован он или нет.
    // Более узкий лимит на /auth/login задан отдельно через @Throttle там же.
    ThrottlerModule.forRoot([
      {
        ttl: parseInt(process.env.THROTTLE_TTL_MS || '60000', 10),
        limit: parseInt(process.env.THROTTLE_LIMIT || '120', 10),
      },
    ]),

    AuthModule.register(),
    TradingModule,
    AnalyticsModule,
    BacktestModule,
  ],
  providers: [
    {
      provide: APP_GUARD,
      useClass: ThrottlerGuard,
    },
  ],
})
export class AppModule {}
