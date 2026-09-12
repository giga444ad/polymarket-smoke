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

/**
 * Наши intervalMs -> Binance interval.
 */
const BINANCE_INTERVAL_BY_MS: Record<number, string> = {
  [60_000]: '1m',
  [300_000]: '5m',
  [900_000]: '15m',
  [3_600_000]: '1h',
  [14_400_000]: '4h',
  [86_400_000]: '1d',
};

/**
 * Наши intervalMs -> Bybit interval.
 *
 * Bybit V5 /v5/market/kline:
 * 1, 3, 5, 15, 30, 60, 120, 240, 480, D, W
 */
const BYBIT_INTERVAL_BY_MS: Record<number, string> = {
  [60_000]: '1',
  [180_000]: '3',
  [300_000]: '5',
  [900_000]: '15',
  [1_800_000]: '30',
  [3_600_000]: '60',
  [7_200_000]: '120',
  [14_400_000]: '240',
  [28_800_000]: '480',
  [86_400_000]: 'D',
  [604_800_000]: 'W',
};

/**
 * Бэкаффил и персистентность истории свечей ценового фида.
 *
 * Архитектура:
 *
 * PostgreSQL — основное хранилище истории.
 *
 * Источник бэкаффила:
 * 1. Binance REST — основной источник исторических klines.
 * 2. Bybit REST — fallback, если Binance недоступен или не вернул
 *    достаточно свечей.
 *
 * Источник новых свечей вперёд по времени:
 * WS-фид (PriceFeedService).
 *
 * Исторические данные используются для прогрева ATR, поэтому Binance/Bybit
 * допустимы как приближённый источник прошлой волатильности.
 */
@Injectable()
export class CandleHistoryService {
  private readonly logger = new Logger(CandleHistoryService.name);

  constructor(
    @InjectRepository(PriceCandle)
    private readonly repo: Repository<PriceCandle>,
  ) {}

  /**
   * Возвращает до `need` последних ЗАКРЫТЫХ свечей для (ticker, intervalMs),
   * самая новая — в конце.
   *
   * Порядок источников:
   *   DB -> Binance REST -> Bybit REST
   *
   * Если внешний источник недоступен, продолжаем с тем, что уже есть в БД.
   */
  async bootstrap(
    ticker: string,
    intervalMs: number,
    need: number,
  ): Promise<HistCandle[]> {
    let fromDb = await this.readFromDb(ticker, intervalMs, need);

    // Истории уже достаточно.
    if (fromDb.length >= need) {
      return fromDb;
    }

    // ------------------------------------------------------------
    // 1. Binance
    // ------------------------------------------------------------

    const binanceInterval = BINANCE_INTERVAL_BY_MS[intervalMs];

    if (binanceInterval) {
      try {
        const fetched = await this.fetchFromBinance(
          ticker,
          binanceInterval,
          need - fromDb.length,
        );

        if (fetched.length > 0) {
          await this.upsertMany(
            ticker,
            intervalMs,
            fetched,
            'binance-rest',
          );

          fromDb = await this.readFromDb(ticker, intervalMs, need);

          this.logger.log(
            `CandleHistoryService: ${ticker}@${intervalMs / 1000}с — ` +
              `бэкаффил из Binance дал ${fetched.length} свечей ` +
              `(в БД теперь ${fromDb.length}/${need}).`,
          );
        }
      } catch (err) {
        this.logger.warn(
          `CandleHistoryService: Binance REST для ` +
            `${ticker}@${intervalMs / 1000}с не удался ` +
            `(${this.errMsg(err)}), пробуем Bybit.`,
        );
      }
    } else {
      this.logger.warn(
        `CandleHistoryService: нет соответствия Binance-интервала ` +
          `для ${ticker}@${intervalMs}мс — Binance бэкаффил пропущен.`,
      );
    }

    // Binance хватило.
    if (fromDb.length >= need) {
      return fromDb;
    }

    // ------------------------------------------------------------
    // 2. Bybit fallback
    // ------------------------------------------------------------

    const bybitInterval = BYBIT_INTERVAL_BY_MS[intervalMs];

    if (!bybitInterval) {
      this.logger.warn(
        `CandleHistoryService: нет соответствия Bybit-интервала ` +
          `для ${ticker}@${intervalMs}мс — ` +
          `бэкаффил через Bybit пропущен.`,
      );

      return fromDb;
    }

    try {
      const fetched = await this.fetchFromBybit(
        ticker,
        bybitInterval,
        need - fromDb.length,
      );

      if (fetched.length > 0) {
        await this.upsertMany(
          ticker,
          intervalMs,
          fetched,
          'bybit-rest',
        );

        fromDb = await this.readFromDb(ticker, intervalMs, need);

        this.logger.log(
          `CandleHistoryService: ${ticker}@${intervalMs / 1000}с — ` +
            `бэкаффил из Bybit дал ${fetched.length} свечей ` +
            `(в БД теперь ${fromDb.length}/${need}).`,
        );
      }
    } catch (err) {
      this.logger.warn(
        `CandleHistoryService: бэкаффил из Bybit REST для ` +
          `${ticker}@${intervalMs / 1000}с не удался ` +
          `(${this.errMsg(err)}) — продолжаем с тем, что есть ` +
          `в БД (${fromDb.length}/${need}).`,
      );
    }

    return fromDb;
  }

  /**
   * Апсерт одной живой закрытой свечи из WS-фида.
   *
   * Вызывается PriceFeedService при закрытии окна.
   */
  async saveClosedCandle(
    ticker: string,
    intervalMs: number,
    candle: HistCandle,
    source: string,
  ): Promise<void> {
    try {
      await this.upsertMany(
        ticker,
        intervalMs,
        [candle],
        source,
      );
    } catch (err) {
      // Персистентность истории — best-effort кеш.
      // Не должна ронять горячий путь фида.
      this.logger.debug(
        `CandleHistoryService: не удалось сохранить свечу ` +
          `${ticker}@${intervalMs}: ${this.errMsg(err)}`,
      );
    }
  }

  /**
   * Читает последние `need` свечей из БД.
   *
   * Наружу всегда возвращаем oldest -> newest.
   */
  private async readFromDb(
    ticker: string,
    intervalMs: number,
    need: number,
  ): Promise<HistCandle[]> {
    const rows = await this.repo.find({
      where: {
        ticker,
        intervalMs: intervalMs as any,
      },
      order: {
        startMs: 'DESC',
      },
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

  /**
   * Массовый upsert свечей.
   */
  private async upsertMany(
    ticker: string,
    intervalMs: number,
    candles: HistCandle[],
    source: string,
  ): Promise<void> {
    if (candles.length === 0) {
      return;
    }

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
      {
        conflictPaths: [
          'ticker',
          'intervalMs',
          'startMs',
        ],
        skipUpdateIfNoValuesChanged: true,
      },
    );
  }

  /**
   * Binance interval -> миллисекунды.
   */
  private getIntervalMsFromBinance(interval: string): number {
    switch (interval) {
      case '1m':
        return 60_000;

      case '5m':
        return 5 * 60_000;

      case '15m':
        return 15 * 60_000;

      case '1h':
        return 60 * 60_000;

      case '4h':
        return 4 * 60 * 60_000;

      case '1d':
        return 24 * 60 * 60_000;

      default:
        throw new Error(
          `Unknown Binance interval: ${interval}`,
        );
    }
  }

  /**
   * Bybit interval -> миллисекунды.
   */
  private getIntervalMsFromBybit(interval: string): number {
    switch (interval) {
      case '1':
        return 60_000;

      case '3':
        return 3 * 60_000;

      case '5':
        return 5 * 60_000;

      case '15':
        return 15 * 60_000;

      case '30':
        return 30 * 60_000;

      case '60':
        return 60 * 60_000;

      case '120':
        return 2 * 60 * 60_000;

      case '240':
        return 4 * 60 * 60_000;

      case '480':
        return 8 * 60 * 60_000;

      case 'D':
        return 24 * 60 * 60_000;

      case 'W':
        return 7 * 24 * 60 * 60_000;

      default:
        throw new Error(
          `Unknown Bybit interval: ${interval}`,
        );
    }
  }

  /**
   * Загружает последние закрытые свечи из Binance REST.
   */
  private async fetchFromBinance(
    ticker: string,
    interval: string,
    limit: number,
  ): Promise<HistCandle[]> {
    const symbol = `${ticker.toUpperCase()}USDT`;

    // Берём запас, потому что последняя свеча может быть текущей
    // и ещё не закрытой.
    const url =
      `https://api.binance.com/api/v3/klines` +
      `?symbol=${symbol}` +
      `&interval=${interval}` +
      `&limit=${Math.min(limit + 5, 1000)}`;

    const res = await fetch(url);

    if (!res.ok) {
      throw new Error(
        `Binance klines HTTP ${res.status}`,
      );
    }

    const raw = (await res.json()) as any[];

    const intervalMs =
      this.getIntervalMsFromBinance(interval);

    const now = Date.now();

    return raw
      .map((k) => ({
        start: Number(k[0]),
        open: Number(k[1]),
        high: Number(k[2]),
        low: Number(k[3]),
        close: Number(k[4]),
      }))
      .filter(
        (candle) =>
          candle.start + intervalMs <= now,
      )
      .sort(
        (a, b) => a.start - b.start,
      )
      .slice(-limit);
  }

  /**
   * Загружает последние закрытые свечи из Bybit REST.
   *
   * category=linear означает USDT perpetual/futures market.
   */
  private async fetchFromBybit(
    ticker: string,
    interval: string,
    limit: number,
  ): Promise<HistCandle[]> {
    const symbol = `${ticker.toUpperCase()}USDT`;

    const url =
      `https://api.bybit.com/v5/market/kline` +
      `?category=linear` +
      `&symbol=${symbol}` +
      `&interval=${interval}` +
      `&limit=${Math.min(limit + 5, 1000)}`;

    const res = await fetch(url);

    if (!res.ok) {
      throw new Error(
        `Bybit klines HTTP ${res.status}`,
      );
    }

    const json = (await res.json()) as {
      retCode: number;
      retMsg: string;
      result?: {
        list?: string[][];
      };
    };

    if (json.retCode !== 0) {
      throw new Error(
        `Bybit klines error ${json.retCode}: ${json.retMsg}`,
      );
    }

    const raw = json.result?.list ?? [];

    const intervalMs =
      this.getIntervalMsFromBybit(interval);

    const now = Date.now();

    return raw
      .map((k) => ({
        start: Number(k[0]),
        open: Number(k[1]),
        high: Number(k[2]),
        low: Number(k[3]),
        close: Number(k[4]),
      }))
      .filter(
        (candle) =>
          candle.start + intervalMs <= now,
      )
      .sort(
        (a, b) => a.start - b.start,
      )
      .slice(-limit);
  }

  private errMsg(err: unknown): string {
    return err instanceof Error
      ? err.message
      : String(err);
  }
}
