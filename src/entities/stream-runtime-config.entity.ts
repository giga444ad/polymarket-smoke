import { Entity, PrimaryColumn, Column, UpdateDateColumn } from 'typeorm';

// Динамическая (через БД, не ENV) настройка на поток: какой критерий
// закрывает попытку раньше времени по цели — количество шагов (как было
// всегда, дефолт) или накопленная прибыль попытки (см. TARGET_PROFIT_USD).
// Дефолты (targetSteps/targetProfitUsd) по-прежнему приходят ТОЛЬКО из ENV
// и живут на самом Attempt — здесь хранится только ПЕРЕКЛЮЧАТЕЛЬ режима,
// который можно менять на лету через дашборд (экран настроек), без
// редеплоя. Если строки для потока нет — считаем режим 'steps' (обратная
// совместимость с поведением до этой фичи).
export type CloseMode = 'steps' | 'profit';

@Entity('stream_runtime_config')
export class StreamRuntimeConfig {
  @PrimaryColumn({ type: 'varchar', length: 64 })
  streamKey: string;

  @Column({ type: 'varchar', length: 16, default: 'steps' })
  closeMode: CloseMode;

  // Ручной рубильник потока (Сессия "полуручной лайв-тест"): при false
  // discoveryTick перестаёт открывать НОВЫЕ окна для этого потока, но уже
  // открытая позиция/ещё не зарезолвленный шаг доводится до конца как
  // обычно (резолв, клейм) — пауза не бросает деньги на середине. Дефолт
  // true — обратная совместимость с поведением до этой фичи.
  @Column({ type: 'boolean', default: true })
  enabled: boolean;

  @UpdateDateColumn()
  updatedAt: Date;
}
