import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Attempt } from '../entities/attempt.entity';
import { MarketLog } from '../entities/market-log.entity';
import { PriceCandle } from '../entities/price-candle.entity';
import { TradingService } from './trading.service';
import { TradingController } from './trading.controller';
import { GammaMarketService } from '../polymarket/gamma-market.service';
import { ClobPublicService } from '../polymarket/clob-public.service';
import { PolymarketTraderService } from '../polymarket/polymarket-trader.service';
import { PriceFeedService } from '../polymarket/price-feed.service';
import { CandleHistoryService } from '../polymarket/candle-history.service';

@Module({
  imports: [TypeOrmModule.forFeature([Attempt, MarketLog, PriceCandle])],
  controllers: [TradingController],
  providers: [
    TradingService,
    GammaMarketService,
    ClobPublicService,
    PolymarketTraderService,
    PriceFeedService,
    CandleHistoryService,
  ],
  exports: [TypeOrmModule],
})
export class TradingModule {}
