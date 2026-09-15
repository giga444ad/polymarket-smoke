import { Entity, PrimaryGeneratedColumn, Column, Index, CreateDateColumn } from 'typeorm';
import { Outcome } from '../polymarket/market-ws-stream';

/**
 * Сессия 17 (см. CONTEXT.md, обсуждение "непрерывного поиска входа").
 *
 * ЧИСТО ДИАГНОСТИЧЕСКАЯ таблица — заполняется EdgeSamplerService НЕЗАВИСИМО
 * от TradingService/EntryGateEngine и НИКАК не влияет на реальные ордера
 * (ни живые, ни смоук). Задача: набрать ряд наблюдений "что было видно на
 * рынке в каждый момент второй половины окна -> чем всё закончилось" —
 * материал для калибровки будущей edge-модели (логистическая регрессия по
 * этим строкам + finalOutcome/priceAtClose из связанного MarketLog по
 * slug), вместо подбора коэффициентов на глаз.
 *
 * Сэмплируется по ВСЕМ активным окнам, не только тем, где бот реально
 * пытается войти — иначе выборка будет смещена в сторону уже "удобных"
 * моментов (survivorship bias), а нам нужна полная картина "весь диапазон
 * -> что происходило дальше" (см. идею пользователя про 288 потенциальных
 * свечей в сутки против 42 реальных входов).
 */
@Entity('edge_score_samples')
export class EdgeScoreSample {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Index()
  @Column({ type: 'varchar', length: 64 })
  assetPrefix: string;

  @Index()
  @Column({ type: 'varchar', length: 128 })
  slug: string;

  // Метка времени самого сэмпла (не createdAt — та же логика, что и
  // orderSentAt/orderFilledAt в market-log.entity.ts: нам нужен именно
  // момент наблюдения, а не момент, когда запись физически долетела до БД).
  @Column({ type: 'bigint' })
  sampledAtMs: number;

  @Column({ type: 'double precision' })
  timeLeftSec: number;

  @Column({ type: 'double precision', nullable: true })
  referencePrice: number | null;

  @Column({ type: 'double precision', nullable: true })
  price: number | null;

  // price - referencePrice (знаковая, не модуль — сторона видна по знаку).
  @Column({ type: 'double precision', nullable: true })
  delta: number | null;

  @Column({ type: 'double precision', nullable: true })
  atr: number | null;

  @Column({ type: 'double precision', nullable: true })
  atrRatio: number | null;

  // См. PriceFeedService.getAtrRobust — медианный аналог ATR, устойчивый к
  // единичному спайку в окне последних atrCandles свечей.
  @Column({ type: 'double precision', nullable: true })
  atrRobust: number | null;

  @Column({ type: 'double precision', nullable: true })
  atrRobustRatio: number | null;

  @Column({ type: 'double precision', nullable: true })
  driftRate: number | null;

  @Column({ type: 'double precision', nullable: true })
  zoneRatio: number | null;

  // См. PriceFeedService.getSmoothnessRatio.
  @Column({ type: 'double precision', nullable: true })
  smoothness: number | null;

  // Сторона, которую в этот момент показывал бы фид (price vs reference) —
  // ЧТО БЫЛО БЫ выбрано, если бы вход произошёл прямо сейчас.
  @Column({ type: 'varchar', length: 8, nullable: true })
  impliedSide: Outcome | null;

  // Лучшая цена на продажу фаворитной стороны книги на этот момент (то, что
  // implied-вероятность рынка) — для сравнения с нашей p_model в будущем.
  @Column({ type: 'double precision', nullable: true })
  favoriteBestAsk: number | null;

  @Column({ type: 'varchar', length: 8, nullable: true })
  favoriteOutcome: Outcome | null;

  @CreateDateColumn({ type: 'timestamptz' })
  createdAt: Date;
}
