import { IsIn } from 'class-validator';
import { CloseMode } from '../../entities/stream-runtime-config.entity';

export class UpdateStreamSettingsDto {
  @IsIn(['steps', 'profit'])
  closeMode: CloseMode;
}
