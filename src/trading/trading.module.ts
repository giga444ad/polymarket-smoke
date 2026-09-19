import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Attempt } from '../entities/attempt.entity';
import { StreamRuntimeConfig } from '../entities/stream-runtime-config.entity';
import { MarketLog } from '../entities/market-log.entity';
import { PriceCandle } from '../entities/price-candle.entity';
import { ActiveWindow } from '../entities/active-window.entity';
import { EdgeScoreSample } from '../entities/edge-score-sample.entity';
import { TradingService } from './trading.service';
import { TradingController } from './trading.controller';
import { GammaMarketService } from '../polymarket/gamma-market.service';
import { ClobPublicService } from '../polymarket/clob-public.service';
import { PolymarketTraderService } from '../polymarket/polymarket-trader.service';
import { PriceFeedService } from '../polymarket/price-feed.service';
import { CandleHistoryService } from '../polymarket/candle-history.service';
import { EdgeSamplerService } from './edge-sampler.service';
import { BalanceService } from '../polymarket/balance.service';
import { RedeemService } from '../polymarket/redeem.service';
import { PolymarketSecureClientService } from '../polymarket/secure-client.service';

@Module({
  imports: [TypeOrmModule.forFeature([Attempt, MarketLog, PriceCandle, ActiveWindow, EdgeScoreSample, StreamRuntimeConfig])],
  controllers: [TradingController],
  providers: [
    TradingService,
    GammaMarketService,
    ClobPublicService,
    PolymarketTraderService,
    PriceFeedService,
    CandleHistoryService,
    // Сессия 17: чисто диагностический сэмплер (см. edge-sampler.service.ts) —
    // не влияет на реальные ордера, включается флагом EDGE_SAMPLER_ENABLED.
    EdgeSamplerService,
    // Подготовка к лайву (см. CONTEXT.md, "Клейм резолва" и "Реальный баланс"):
    // PolymarketSecureClientService — общий @polymarket/client (canary) с
    // авторизацией по простому Relayer API Key; BalanceService — гейт по
    // реальному балансу перед открытием окна; RedeemService — автоклейм
    // резолвнутых выигрышей.
    PolymarketSecureClientService,
    BalanceService,
    RedeemService,
  ],
  // BalanceService экспортируется для AnalyticsModule (кошельковый equity в
  // /analytics/summary) — единый инстанс с кэшем, без второго secure-клиента.
  exports: [TypeOrmModule, BalanceService],
})
export class TradingModule {}
