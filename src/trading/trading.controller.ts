import { Controller, Post, Param } from '@nestjs/common';
import { TradingService } from './trading.service';

@Controller('trading')
export class TradingController {
  constructor(private readonly trading: TradingService) {}

  // Досрочное закрытие попытки (п.5 сессии 6, см. CONTEXT.md) — эмуляция
  // "фиксации прибыли": попытка помечается closed_early, для того же
  // потока сразу поднимается новая активная попытка со сбросом на baseStake.
  @Post('attempts/:id/close-early')
  async closeEarly(@Param('id') id: string) {
    const attempt = await this.trading.closeAttemptEarly(id);
    return {
      ok: true,
      newAttempt: {
        id: attempt.id,
        attemptNumber: attempt.attemptNumber,
        streamKey: attempt.streamKey,
        status: attempt.status,
        currentStake: attempt.currentStake,
      },
    };
  }
}
