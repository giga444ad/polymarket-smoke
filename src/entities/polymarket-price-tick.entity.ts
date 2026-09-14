import { Entity, PrimaryGeneratedColumn, Column, Index, CreateDateColumn } from 'typeorm';

/**
 * Сессия 15 (см. CONTEXT.md) — снимки лучших bid/ask по книге Polymarket
 * для КОНКРЕТНОГО СЕЙЧАС ОТКРЫТОГО окна каждого потока, раз в ~500мс.
 *
 * Мотивация (сформулирована пользователем): официальные REST-ручки
 * Polymarket отдают цену с точностью ~60с, а бот пытается входить на
 * последних ~45-60с окна — на этом отрезке цена может успевать заметно
 * измениться несколько раз, и REST-снимка попросту недостаточно, чтобы
 * потом честно бэктестить, что "видел" бы бот в конкретный момент.
 *
 * Пишется tick-recorder процессом через read-only MarketWsStream (тот же
 * класс, что и в основном воркере, но отдельный WS-коннект, отдельный
 * процесс) — какой tokenId сейчас слушать, recorder узнаёт из ActiveWindow,
 * а не через собственный дискавери по Gamma API (см. комментарий в
 * active-window.entity.ts, почему так, а не дублированием дискавери).
 */
@Entity('polymarket_price_ticks')
@Index('idx_pm_tick_stream_ts', ['streamKey', 'ts'])
export class PolymarketPriceTick {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ type: 'varchar', length: 64 })
  streamKey: string;

  @Column({ type: 'varchar', length: 255 })
  slug: string;

  // Unix ms момента снимка.
  @Column({ type: 'bigint' })
  ts: number;

  @Column({ type: 'double precision', nullable: true })
  yesBestBid: number | null;

  @Column({ type: 'double precision', nullable: true })
  yesBestAsk: number | null;

  @Column({ type: 'double precision', nullable: true })
  noBestBid: number | null;

  @Column({ type: 'double precision', nullable: true })
  noBestAsk: number | null;

  @CreateDateColumn()
  createdAt: Date;
}
