import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { PriceTick } from '../entities/price-tick.entity';
import { PolymarketPriceTick } from '../entities/polymarket-price-tick.entity';
import { BacktestController } from './backtest.controller';
import { BacktestRunnerService } from './backtest-runner.service';

/**
 * Отдельный Nest-модуль, а не отдельный скрипт (см. запрос пользователя —
 * "у нас уже есть полноценный модульный Nest, который может держать оба
 * сервиса на плаву, а мы делаем скриптом"). Читает те же таблицы, что и
 * `scripts/tick-recorder.ts` писал (`price_ticks`/`polymarket_price_ticks`),
 * через тот же TypeORM-коннект, что и остальное приложение (см.
 * app.module.ts) — отдельного подключения к БД, как у recorder-скрипта, не
 * заводим, оно здесь не нужно (бэктест не пишет тики, только читает).
 *
 * Намеренно НЕ импортирует TradingModule/PriceFeedService/GammaMarketService —
 * бэктест не делает никаких живых сетевых запросов и не должен требовать
 * поднятия WS-соединений, чтобы посчитать историю (см. BacktestRunnerService
 * class-comment). Общий код с live-путём — только через
 * `src/trading/entry-gate.engine.ts` и `market-decision.util.ts`
 * (DI-независимые, чистые модули).
 */
@Module({
  imports: [TypeOrmModule.forFeature([PriceTick, PolymarketPriceTick])],
  controllers: [BacktestController],
  providers: [BacktestRunnerService],
})
export class BacktestModule {}
