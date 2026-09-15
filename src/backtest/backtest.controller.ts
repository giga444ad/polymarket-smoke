import { Body, Controller, Post, UseGuards } from '@nestjs/common';
import { RolesGuard } from '../auth/guards/roles.guard';
import { Roles } from '../auth/decorators/roles.decorator';
import { Role } from '../auth/role.enum';
import { BacktestRunnerService } from './backtest-runner.service';
import { BacktestRunRequest } from './backtest.types';

@Controller('backtest')
export class BacktestController {
  constructor(private readonly runner: BacktestRunnerService) {}

  // Бэктест может дёргать envOverrides и гоняет тяжёлые запросы по всей
  // истории тиков — ограничиваем admin, а не открываем viewer'ам.
  @UseGuards(RolesGuard)
  @Roles(Role.ADMIN)
  @Post('run')
  async run(@Body() body: BacktestRunRequest) {
    return this.runner.run(body);
  }
}
