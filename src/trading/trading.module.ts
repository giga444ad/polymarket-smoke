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
  ],
  exports: [TypeOrmModule],
})
export class TradingModule {}
