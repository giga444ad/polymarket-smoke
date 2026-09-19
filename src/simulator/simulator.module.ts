import { Module } from '@nestjs/common';
import { SimulatorController } from './simulator.controller';

/**
 * Сессия 20 — модуль Монте-Карло симулятора стратегии. Без провайдеров и БД:
 * чистый расчёт в montecarlo.util.ts, дёргается фронтом polyguru.
 */
@Module({
  controllers: [SimulatorController],
})
export class SimulatorModule {}
