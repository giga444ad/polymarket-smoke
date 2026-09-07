import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  ManyToOne,
  Index,
  JoinColumn,
} from 'typeorm';
import { Attempt } from './attempt.entity';

export type MarketLogStatus =
  | 'pending_resolve'
  | 'win'
  | 'loss'
  | 'skipped'
  | 'error';

export type ChosenOutcome = 'YES' | 'NO' | null;

@Entity('market_logs')
export class MarketLog {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @ManyToOne(() => Attempt, (attempt) => attempt.logs, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'attemptId' })
  attempt: Attempt;

  @Column({ type: 'uuid' })
  attemptId: string;

  // Шаг попытки, на который претендует этот маркет (полезно при разборе логов утром).
  @Column({ type: 'int' })
  stepNumber: number;

  @Index()
  @Column({ type: 'varchar', length: 128 })
  slug: string;

  @Column({ type: 'timestamptz' })
  closesAt: Date;

  @Column({ type: 'varchar', length: 8, nullable: true })
  chosenOutcome: ChosenOutcome;

  @Column({ type: 'varchar', length: 128, nullable: true })
  chosenTokenId: string | null;

  @Column({ type: 'double precision', nullable: true })
  entryPrice: number | null;

  @Column({ type: 'double precision', default: 1 })
  betAmount: number;

  // true = ордер (реальный или симулированный) был отправлен; false = шаг пропущен фильтрами
  @Column({ type: 'boolean', default: false })
  executed: boolean;

  @Column({ type: 'boolean' })
  isSmoke: boolean;

  @Column({ type: 'varchar', length: 16, nullable: true })
  orderType: 'FOK' | 'SIMULATED' | null;

  @Column({ type: 'varchar', length: 128, nullable: true })
  orderId: string | null;

  @Index()
  @Column({ type: 'varchar', length: 32, default: 'pending_resolve' })
  status: MarketLogStatus;

  @Column({ type: 'text', nullable: true })
  skipReason: string | null;

  @Column({ type: 'text', nullable: true })
  errorMessage: string | null;

  @Column({ type: 'text', nullable: true })
  logMessage: string | null;

  @CreateDateColumn()
  createdAt: Date;

  @Column({ type: 'timestamptz', nullable: true })
  resolvedAt: Date | null;
}
