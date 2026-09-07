import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Attempt } from '../entities/attempt.entity';
import { MarketLog } from '../entities/market-log.entity';
import { TradingService } from './trading.service';
import { GammaMarketService } from '../polymarket/gamma-market.service';
import { ClobPublicService } from '../polymarket/clob-public.service';
import { PolymarketTraderService } from '../polymarket/polymarket-trader.service';

@Module({
  imports: [TypeOrmModule.forFeature([Attempt, MarketLog])],
  providers: [
    TradingService,
    GammaMarketService,
    ClobPublicService,
    PolymarketTraderService,
  ],
  exports: [TypeOrmModule],
})
export class TradingModule {}
