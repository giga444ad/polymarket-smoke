import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { PriceCandle } from '../entities/price-candle.entity';

export interface HistCandle {
  start: number; // мс
  open: number;
  high: number;
  low: number;
  close: number;
}

// Binance поддерживает эти интервалы ровно (без остатка) — совпадает один в
// один с длительностью наших потоков (5m/15m/1h), поэтому конвертация
// intervalMs -> строка Binance тривиальна, без дробления/агрегации.
const BINANCE_INTERVAL_BY_MS: Record<number, string> = {
  [60_000]: '1m',
  [300_000]: '5m',
  [900_000]: '15m',
  [3_600_000]: '1h',
  [14_400_000]: '4h',
  [86_400_000]: '1d',
};

/**
 * Бэкафилл и персистентность истории свечей ценового фида — см. класс-
 * комментарий PriceCandle и CONTEXT.md (Сессия 6, п.6). Архитектура:
 * PostgreSQL как единственное хранилище (транзакционно, уже есть в стеке —
 * не заводим Redis только ради этого), Binance REST klines как источник
 * бэкафилла ИСТОРИИ (публичный, без ключа, интервалы совпадают с нашими
 * потоками), WS-фид (PriceFeedService) — источник новых свечей вперёд по
 * времени, каждая из которых дописывается сюда же через saveClosedCandle.
 *
 * Из Chainlink/Bybit бэкафилл истории намеренно не делаем: Binance REST
 * klines достаточно как приближение для ПРОШЛОЙ волатильности (ATR — это
 * статистика размаха, а не точный резолвящий источник) — точность источника
 * важна для live-диагностики на входе (там уже стоит Chainlink, см.
 * PriceFeedService), не для прогрева ATR за прошлые часы.
 */
@Injectable()
export class CandleHistoryService {
  private readonly logger = new Logger(CandleHistoryService.name);

  constructor(
    @InjectRepository(PriceCandle) private readonly repo: Repository<PriceCandle>,
  ) {}

  /**
   * Возвращает до `need` последних ЗАКРЫТЫХ свечей для (ticker, intervalMs),
   * самая новая — в конце. Сначала читает из БД; если не хватает — догружает
   * через Binance REST и апсертит недостающее.
   */
  async bootstrap(ticker: string, intervalMs: number, need: number): Promise<HistCandle[]> {
    let fromDb = await this.readFromDb(ticker, intervalMs, need);

    if (fromDb.length < need) {
      const binanceInterval = BINANCE_INTERVAL_BY_MS[intervalMs];
      if (!binanceInterval) {
        this.logger.warn(
          `CandleHistoryService: нет соответствия Binance-интервала для ${ticker}@${intervalMs}мс — ` +
            `бэкафилл истории пропущен, прогрев ATR пойдёт с нуля через живой фид.`,
        );
        return fromDb;
      }
      try {
        const fetched = await this.fetchFromBinance(ticker, binanceInterval, need);
        if (fetched.length > 0) {
          await this.upsertMany(ticker, intervalMs, fetched, 'binance-rest');
          fromDb = await this.readFromDb(ticker, intervalMs, need);
          this.logger.log(
            `CandleHistoryService: ${ticker}@${intervalMs / 1000}с — бэкафилл из Binance REST дал ${fetched.length} свечей ` +
              `(в БД теперь ${fromDb.length}/${need}).`,
          );
        }
      } catch (err) {
        this.logger.warn(
          `CandleHistoryService: бэкафилл из Binance REST для ${ticker}@${intervalMs / 1000}с не удался (${this.errMsg(err)}) — ` +
            `продолжаем с тем, что есть в БД (${fromDb.length}/${need}).`,
        );
      }
    }

    return fromDb;
  }

  /** Апсерт одной живой закрытой свечи из WS-фида (вызывается PriceFeedService при закрытии окна). */
  async saveClosedCandle(ticker: string, intervalMs: number, candle: HistCandle, source: string): Promise<void> {
    try {
      await this.upsertMany(ticker, intervalMs, [candle], source);
    } catch (err) {
      // Персистентность истории — best-effort кеш, не должна ронять горячий
      // путь фида, если БД временно недоступна.
      this.logger.debug(`CandleHistoryService: не удалось сохранить свечу ${ticker}@${intervalMs}: ${this.errMsg(err)}`);
    }
  }

  private async readFromDb(ticker: string, intervalMs: number, need: number): Promise<HistCandle[]> {
    const rows = await this.repo.find({
      where: { ticker, intervalMs: intervalMs as any },
      order: { startMs: 'DESC' },
      take: need,
    });
    return rows
      .map((r) => ({
        start: Number(r.startMs),
        open: r.open,
        high: r.high,
        low: r.low,
        close: r.close,
      }))
      .sort((a, b) => a.start - b.start);
  }

  private async upsertMany(ticker: string, intervalMs: number, candles: HistCandle[], source: string): Promise<void> {
    if (candles.length === 0) return;
    await this.repo.upsert(
      candles.map((c) => ({
        ticker,
        intervalMs: intervalMs as any,
        startMs: c.start as any,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
        source,
      })),
      { conflictPaths: ['ticker', 'intervalMs', 'startMs'], skipUpdateIfNoValuesChanged: true },
    );
  }

  private async fetchFromBinance(ticker: string, interval: string, limit: number): Promise<HistCandle[]> {
    const symbol = `${ticker.toUpperCase()}USDT`;
    // +5 с запасом: последняя свеча Binance может быть ещё не закрыта на момент запроса.
    const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${Math.min(limit + 5, 1000)}`;
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(`Binance klines HTTP ${res.status}`);
    }
    const raw = (await res.json()) as any[];
    // Формат Binance kline: [openTime, open, high, low, close, volume, closeTime, ...]
    // Отбрасываем последний элемент — как правило это ещё текущая, не закрытая свеча.
    const closed = raw.slice(0, Math.max(0, raw.length - 1));
    return closed.map((k) => ({
      start: Number(k[0]),
      open: parseFloat(k[1]),
      high: parseFloat(k[2]),
      low: parseFloat(k[3]),
      close: parseFloat(k[4]),
    }));
  }

  private errMsg(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
