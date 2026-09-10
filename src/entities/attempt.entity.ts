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

export type AttemptStatus = 'active' | 'failed' | 'completed_target';

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
