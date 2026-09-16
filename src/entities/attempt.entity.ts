import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  OneToMany,
  Index,
} from 'typeorm';
import { MarketLog } from './market-log.entity';

// 'closed_early' — попытка остановлена вручную через
// POST /trading/attempts/:id/close-early (см. CONTEXT.md, п.5 сессии 6),
// не дожидаясь ни проигрыша, ни достижения targetSteps.
export type AttemptStatus = 'active' | 'failed' | 'completed_target' | 'closed_early';

@Entity('attempts')
export class Attempt {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // Сквозной номер попытки (1, 2, 3...), отдельно считается для боевого и смоук
  // режима И отдельно для каждого потока (см. streamKey) — попытки разных
  // потоков больше не делят один общий счётчик шагов.
  @Column({ type: 'int' })
  attemptNumber: number;

  // Независимый поток = актив × таймфрейм (напр. "btc-updown-5m",
  // "btc-updown-15m", "bitcoin-up-or-down" для часового). У каждого потока
  // своя последовательность попыток, свой прогресс и свой текущий стейк —
  // раньше Attempt искался только по isSmoke, без привязки к активу, что
  // было источником гонки stepNumber при нескольких активах в одном инстансе.
  @Index()
  @Column({ type: 'varchar', length: 64, default: 'btc-updown-5m' })
  streamKey: string;

  @Column({ type: 'int', default: 0 })
  currentStep: number;

  @Column({ type: 'int', default: 500 })
  targetSteps: number;

  // Цель попытки в деньгах (см. TARGET_PROFIT_USD) — альтернативный критерий
  // досрочного завершения попытки, наряду с targetSteps. Считается именно
  // ПРИБЫЛЬ этой попытки (см. realizedProfit ниже), а не совокупный баланс/
  // банкролл. Какой из двух критериев (targetSteps vs targetProfitUsd)
  // реально проверяется на каждом шаге — решает StreamRuntimeConfig.closeMode
  // (динамически, через БД/дашборд). Оба поля всегда живут и обновляются на
  // Attempt одновременно, независимо от того, какой режим сейчас активен —
  // если режим переключат на полпути попытки, ни currentStep, ни
  // realizedProfit не сбрасываются и не теряются.
  @Column({ type: 'double precision', default: 20 })
  targetProfitUsd: number;

  // Базовый стейк потока (из конфига потока на момент создания попытки) —
  // сюда сбрасывается currentStake при проигрыше (новая попытка).
  @Column({ type: 'double precision', default: 5 })
  baseStake: number;

  // Текущий стейк реинвест-прогрессии: на старте попытки равен baseStake,
  // после каждого выигрыша становится filledAmount/entryPrice ПРЕДЫДУЩЕГО
  // шага (по факту реальной цены исполнения, а не по константе ¢99 — VWAP
  // гуляет по тирам). Сбрасывается на baseStake при создании новой попытки
  // после проигрыша.
  @Column({ type: 'double precision', default: 5 })
  currentStake: number;

  // Накопленная ПРИБЫЛЬ этой попытки (сумма log.profit по всем win-шагам,
  // проигрыш попытку и так завершает) — не путать с currentStake (это
  // размер следующей ставки) и не путать с общим банкроллом (тот считается
  // по ВСЕМ попыткам сразу, см. /analytics/summary). Именно это поле
  // сравнивается с targetProfitUsd в режиме closeMode='profit'.
  @Column({ type: 'double precision', default: 0 })
  realizedProfit: number;

  @Index()
  @Column({ type: 'varchar', length: 32, default: 'active' })
  status: AttemptStatus;

  @Index()
  @Column({ type: 'boolean', default: false })
  isSmoke: boolean;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;

  @Column({ type: 'timestamptz', nullable: true })
  finishedAt: Date | null;

  @OneToMany(() => MarketLog, (log) => log.attempt)
  logs: MarketLog[];
}
