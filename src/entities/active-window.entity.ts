import { Entity, PrimaryColumn, Column, UpdateDateColumn } from 'typeorm';

/**
 * Сессия 15 (см. CONTEXT.md) — "указатель" на СЕЙЧАС открытое окно каждого
 * потока, единственная точка связи основного торгового воркера с отдельным
 * (параллельным) tick-recorder процессом.
 *
 * Основной воркер знает, за какими tokenId сейчас следить (это результат
 * его собственного дискавери по Gamma API в openMarket) — вместо того,
 * чтобы recorder-процесс ДУБЛИРОВАЛ этот дискавери сам (лишний трафик на
 * Polymarket API, риск словить рейт-лимит вдвое быстрее, и два независимых
 * источника правды о том, что сейчас открыто), основной воркер просто
 * записывает сюда результат СВОЕГО дискавери — по факту 1 upsert при
 * открытии окна и 1 delete при закрытии, т.е. раз в 5/15/60 минут, а не
 * "постоянное чтение событий", которого просили избежать.
 *
 * recorder читает эту таблицу поллингом раз в 1-2с, для каждого активного
 * потока открывает СВОЙ read-only MarketWsStream (класс уже не завязан на
 * TradingService — переиспользуется как есть) и пишет тики в
 * PolymarketPriceTick. Основной воркер этот класс НЕ читает вообще — только
 * пишет, полная развязка в одну сторону.
 *
 * Запись в эту таблицу НИКОГДА не должна ронять основной торговый флоу —
 * все вызовы обёрнуты в try/catch с логом-предупреждением (см.
 * TradingService.recordActiveWindow/clearActiveWindow).
 */
@Entity('active_windows')
export class ActiveWindow {
  // streamKey как первичный ключ — TypeORM .save() с этим PK делает upsert
  // "из коробки" (перезаписывает старую строку этого потока при новом
  // discoveryTick, без отдельного findOne+update).
  @PrimaryColumn({ type: 'varchar', length: 64 })
  streamKey: string;

  @Column({ type: 'varchar', length: 255 })
  slug: string;

  @Column({ type: 'varchar', length: 128 })
  yesTokenId: string;

  @Column({ type: 'varchar', length: 128 })
  noTokenId: string;

  @Column({ type: 'varchar', length: 16 })
  tickSize: string;

  // windowStartMs может быть null (см. MarketState.windowStartMs — окно,
  // открытое не через штатный discoveryTick, например ручной smoke-тест).
  @Column({ type: 'bigint', nullable: true })
  windowStartMs: number | null;

  @Column({ type: 'bigint' })
  closesAtMs: number;

  @UpdateDateColumn()
  updatedAt: Date;
}
