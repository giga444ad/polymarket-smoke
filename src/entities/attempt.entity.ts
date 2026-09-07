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

  // Сквозной номер попытки (1, 2, 3...), отдельно считается для боевого и смоук режима.
  @Column({ type: 'int' })
  attemptNumber: number;

  @Column({ type: 'int', default: 0 })
  currentStep: number;

  @Column({ type: 'int', default: 500 })
  targetSteps: number;

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
