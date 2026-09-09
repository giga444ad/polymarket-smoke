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
  | 'unfilled'
  | 'error';

export type ChosenOutcome = 'YES' | 'NO' | null;

export type OrderKind =
  | 'FAK' // реальный маркет-тейк (Правило A)
  | 'GTD' // реальная лимитка (Правило B)
  | 'SIMULATED_MARKET' // смоук-эмуляция Правила A
  | 'SIMULATED_LIMIT'; // смоук-эмуляция Правила B

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

  // Префикс актива (btc-updown-5m, eth-updown-5m, ...) — для мультиассетного режима.
  @Index()
  @Column({ type: 'varchar', length: 64, default: 'btc-updown-5m' })
  assetPrefix: string;

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

  // Настроенная цель ставки (BET_AMOUNT на момент входа).
  @Column({ type: 'double precision', default: 1 })
  betAmount: number;

  // Сколько реально удалось "потратить" при проходе по стакану (может быть
  // меньше betAmount, если глубины не хватило) — это то, что реально стоит
  // на кону для расчёта профита/лосса, а не заявленная цель.
  @Column({ type: 'double precision', nullable: true })
  filledAmount: number | null;

  // Доля betAmount, которую реально удалось исполнить (0..1). Для честности
  // смоука: 1.0 если стакана хватило целиком, меньше — если пришлось резать заявку.
  @Column({ type: 'double precision', nullable: true })
  fillRatio: number | null;

  // Профит в долларах, посчитанный резолвером после исхода: на выигрыше —
  // filledAmount/entryPrice*(1-entryPrice), на проигрыше — минус filledAmount.
  @Column({ type: 'double precision', nullable: true })
  profit: number | null;

  // true = ордер (реальный или симулированный) реально исполнился (хотя бы частично)
  @Column({ type: 'boolean', default: false })
  executed: boolean;

  @Column({ type: 'boolean' })
  isSmoke: boolean;

  @Column({ type: 'varchar', length: 24, nullable: true })
  orderType: OrderKind | null;

  // Для лимиток (Правило B): какой ценовой уровень был выставлен последним (T1/T2/T3).
  @Column({ type: 'varchar', length: 8, nullable: true })
  limitTier: 'T1' | 'T2' | 'T3' | null;

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
