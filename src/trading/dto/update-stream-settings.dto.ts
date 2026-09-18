import { IsBoolean, IsIn, IsOptional } from 'class-validator';
import { CloseMode } from '../../entities/stream-runtime-config.entity';

export class UpdateStreamSettingsDto {
  @IsOptional()
  @IsIn(['steps', 'profit'])
  closeMode?: CloseMode;

  // Пауза/возобновление потока (см. TradingService.setEnabled). Оба поля
  // независимо опциональны — можно менять closeMode и enabled отдельными
  // запросами, как уже привык фронт с closeMode.
  @IsOptional()
  @IsBoolean()
  enabled?: boolean;
}
