import { Body, Controller, Post } from '@nestjs/common';
import { BacktestRunnerService } from './backtest-runner.service';
import { BacktestRunRequest } from './backtest.types';

@Controller('backtest')
export class BacktestController {
  constructor(private readonly runner: BacktestRunnerService) {}

  /**
   * BACKTEST-PLAN.md, п.2.4. Тело запроса — см. BacktestRunRequest:
   *   { "streamKey": "btc-updown-5m", "from": "2026-09-01T00:00:00Z", "to": "2026-09-08T00:00:00Z",
   *     "envOverrides": { "MIN_DISTANCE_ATR_RATIO": "2", "EXPECTED_MOVE_FILTER_ENABLED": "true" } }
   *
   * Данные берутся ТОЛЬКО из уже накопленной истории (`price_ticks`/
   * `polymarket_price_ticks`, см. Сессия 15/tick-recorder) — никаких живых
   * сетевых запросов к Gamma/CLOB не выполняется.
   */
  @Post('run')
  async run(@Body() body: BacktestRunRequest) {
    return this.runner.run(body);
  }
}
