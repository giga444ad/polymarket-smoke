import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { ServeStaticModule } from '@nestjs/serve-static';
import { join } from 'path';

import { Attempt } from './entities/attempt.entity';
import { MarketLog } from './entities/market-log.entity';
import { PriceCandle } from './entities/price-candle.entity';
import { TradingModule } from './trading/trading.module';
import { AnalyticsModule } from './analytics/analytics.module';

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
      entities: [Attempt, MarketLog, PriceCandle],
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

    TradingModule,
    AnalyticsModule,
  ],
})
export class AppModule {}
