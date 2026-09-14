import { Entity, PrimaryGeneratedColumn, Column, Index, CreateDateColumn } from 'typeorm';

/**
 * Сессия 15 (см. CONTEXT.md) — персистентная история СЫРЫХ тиков внешнего
 * ценового фида (chainlink/binance/bybit — те же публичные источники, что
 * уже слушает PriceFeedService, см. price-feed.service.ts). В отличие от
 * PriceCandle (одна свеча на ВСЁ окно потока, нужна только для ATR),
 * здесь — каждый отдельный тик, с секундным (а не поминутным) разрешением,
 * специально под будущий бэктест новых фильтров входа (zone-ratio, drift,
 * expected-move — все требуют знать, что было ВНУТРИ окна по секундам, а
 * не только open/high/low/close на весь интервал).
 *
 * Пишется ОТДЕЛЬНЫМ процессом (scripts/tick-recorder.ts), который слушает
 * ровно те же публичные WS-эндпоинты независимо от основного торгового
 * воркера — если этот процесс упадёт или БД временно недоступна, основной
 * воркер вообще не заметит (никакой связи, кроме общей БД на запись).
 */
@Entity('price_ticks')
@Index('idx_price_tick_ticker_ts', ['ticker', 'ts'])
export class PriceTick {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // "btc", "eth", ... — тот же канонический тикер, что и в PriceCandle.
  @Column({ type: 'varchar', length: 32 })
  ticker: string;

  // Unix ms момента тика (время источника, если есть, иначе время приёма).
  @Column({ type: 'bigint' })
  ts: number;

  @Column({ type: 'double precision' })
  price: number;

  // 'chainlink' | 'binance' | 'bybit' — см. price-feed.service.ts адаптеры.
  @Column({ type: 'varchar', length: 24 })
  source: string;

  @CreateDateColumn()
  createdAt: Date;
}
