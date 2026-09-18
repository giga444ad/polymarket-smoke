import { Body, Controller, Get, Param, Patch, Post, UseGuards } from '@nestjs/common';
import { RolesGuard } from '../auth/guards/roles.guard';
import { Roles } from '../auth/decorators/roles.decorator';
import { Role } from '../auth/role.enum';
import { TradingService } from './trading.service';
import { UpdateStreamSettingsDto } from './dto/update-stream-settings.dto';
import { UpdateAttemptDto } from './dto/update-attempt.dto';

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

  // Экран настроек (шестерёнка в дашборде, Сессия 18) — режим закрытия
  // попытки на каждый известный поток. Чтение доступно любому
  // аутентифицированному пользователю (viewer тоже может смотреть текущий
  // режим), меняет — только admin (см. PATCH ниже).
  @Get('settings')
  async getSettings() {
    return this.trading.getCloseModes();
  }

  @UseGuards(RolesGuard)
  @Roles(Role.ADMIN)
  @Patch('settings/:streamKey')
  async updateSettings(@Param('streamKey') streamKey: string, @Body() dto: UpdateStreamSettingsDto) {
    if (dto.closeMode !== undefined) {
      await this.trading.setCloseMode(streamKey, dto.closeMode);
    }
    if (dto.enabled !== undefined) {
      await this.trading.setEnabled(streamKey, dto.enabled);
    }
    return { ok: true, streamKey, closeMode: dto.closeMode, enabled: dto.enabled };
  }

  // Ручной патч ТЕКУЩЕГО прогресса конкретной попытки (не дефолтов потока) —
  // модалка попытки в дашборде. Только admin.
  @UseGuards(RolesGuard)
  @Roles(Role.ADMIN)
  @Patch('attempts/:id')
  async updateAttempt(@Param('id') id: string, @Body() dto: UpdateAttemptDto) {
    const attempt = await this.trading.patchAttempt(id, dto);
    return {
      ok: true,
      attempt: {
        id: attempt.id,
        attemptNumber: attempt.attemptNumber,
        streamKey: attempt.streamKey,
        status: attempt.status,
        currentStep: attempt.currentStep,
        targetSteps: attempt.targetSteps,
        realizedProfit: attempt.realizedProfit,
        targetProfitUsd: attempt.targetProfitUsd,
        currentStake: attempt.currentStake,
      },
    };
  }
}
