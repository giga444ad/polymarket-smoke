import { Controller, Post, Param, UseGuards } from '@nestjs/common';
import { RolesGuard } from '../auth/guards/roles.guard';
import { Roles } from '../auth/decorators/roles.decorator';
import { Role } from '../auth/role.enum';
import { TradingService } from './trading.service';

@Controller('trading')
export class TradingController {
  constructor(private readonly trading: TradingService) {}

  // Мутирующая ручка (реально закрывает попытку/открывает новую со ставкой
  // капитала) — доступна только роли admin. viewer может только читать
  // /analytics/*.
  @UseGuards(RolesGuard)
  @Roles(Role.ADMIN)
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
