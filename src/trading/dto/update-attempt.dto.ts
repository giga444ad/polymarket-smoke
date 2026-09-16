import { IsInt, IsNumber, IsOptional, Min } from 'class-validator';

// Ручной патч ТЕКУЩЕГО прогресса конкретной попытки (не дефолтов потока —
// те только из ENV, см. TARGET_STEPS/TARGET_PROFIT_USD). Все поля опциональны
// и независимы: можно поправить только currentStep, только realizedProfit,
// или пер-попыточно переопределить её же цель (targetSteps/targetProfitUsd),
// например если для конкретной "разогнавшейся" попытки хочется временно
// поднять/опустить порог фиксации, не трогая дефолт для будущих попыток.
export class UpdateAttemptDto {
  @IsOptional()
  @IsInt()
  @Min(0)
  currentStep?: number;

  @IsOptional()
  @IsNumber()
  realizedProfit?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  targetSteps?: number;

  @IsOptional()
  @IsNumber()
  @Min(0.01)
  targetProfitUsd?: number;
}
