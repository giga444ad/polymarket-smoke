import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, Index, Unique } from 'typeorm';

/**
 * Персистентная история ЗАКРЫТЫХ свечей ценового фида — кеш под ATR-гейт
 * (см. CONTEXT.md, Сессия 6, п.6). Раньше история жила только в памяти
 * PriceFeedService и терялась при каждом рестарте процесса, из-за чего
 * прогрев ATR (N*intervalSec — до 20 часов для часового потока) начинался
 * заново каждый раз. Теперь: PriceFeedService при старте сначала читает
 * последние нужные свечи отсюда (мгновенно), догружает недостающее через
 * CandleHistoryService (Binance REST klines), и с этого момента каждая
 * новая закрытая живая свеча дописывается сюда же — БД всегда содержит
 * актуальный "хвост" истории на N*2 свечей вперёд от последнего рестарта.
 *
 * Один тикер может использоваться несколькими потоками с РАЗНЫМ размером
 * свечи (напр. хочет кто-то в будущем 4ч/1д поверх того же BTC) — поэтому
 * ключ уникальности включает intervalMs, а не только ticker+startMs.
 */
@Entity('price_candles')
@Unique('uq_price_candle', ['ticker', 'intervalMs', 'startMs'])
export class PriceCandle {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  // "btc", "eth", ... — канонический тикер (см. PriceFeedService.deriveTicker).
  @Index()
  @Column({ type: 'varchar', length: 32 })
  ticker: string;

  // Длительность свечи в мс — равна intervalSec потока (одна свеча ATR =
  // ровно одно окно потока, см. PriceFeedService).
  @Column({ type: 'bigint' })
  intervalMs: number;

  // Начало бакета в мс (Unix ms), выровненное на intervalMs.
  @Column({ type: 'bigint' })
  startMs: number;

  @Column({ type: 'double precision' })
  open: number;

  @Column({ type: 'double precision' })
  high: number;

  @Column({ type: 'double precision' })
  low: number;

  @Column({ type: 'double precision' })
  close: number;

  // Источник, из которого взята свеча ('binance-rest' при бэкафилле,
  // 'chainlink'/'binance'/'bybit' при живой записи из WS-фида) — для отладки
  // расхождений между бэкафиллом и живыми данными.
  @Column({ type: 'varchar', length: 24, default: 'live' })
  source: string;

  @CreateDateColumn()
  createdAt: Date;
}
