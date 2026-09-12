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

// Наши intervalMs -> Binance interval.
const BINANCE_INTERVAL_BY_MS: Record<number, string> = {
  [60_000]: '1m',
  [300_000]: '5m',
  [900_000]: '15m',
  [3_600_000]: '1h',
  [14_400_000]: '4h',
  [86_400_000]: '1d',
};

// Наши intervalMs -> Bybit interval. Bybit V5 /v5/market/kline: 1,3,5,15,30,60,120,240,480,D,W
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
 * Бэкафилл и персистентность истории свечей ценового фида — см. класс-
 * комментарий PriceCandle и CONTEXT.md (Сессия 6 п.6, Сессия 8 п.4).
 *
 * Архитектура: PostgreSQL — постоянное хранилище. Источник бэкафилла (по
 * порядку, продолжаем на следующий если предыдущий недоступен/не хватило):
 *   1. БД (мгновенно, если уже накоплено с прошлого запуска)
 *   2. Binance REST klines (быстро, полная история, НО блокирует запросы
 *      с американских IP — см. ниже)
 *   3. Bybit REST klines (фолбэк — Сессия 8: сервер был в US, Binance
 *      геоблокировал REST оттуда, из-за чего бэкафилл молча проваливался
 *      и ATR-гейт периодически fail-closed видел 0 свечей после каждого
 *      рестарта, см. PriceFeedService.onModuleInit)
 * Источник новых свечей ВПЕРЁД по времени — только WS-фид (PriceFeedService),
 * ни Binance, ни Bybit REST для этого не используются.
 *
 * ВАЖНО про регион хостинга: сам факт, что понадобился Bybit-фолбэк —
 * следствие того, что Binance отдаёт 451 (или похожие ошибки) на REST
 * из США. Порядок "Binance основной, Bybit фолбэк" не гарантирует 100%
 * доступность отовсюду (у бирж свои региональные ограничения) — если оба
 * недоступны, PriceFeedService теперь явно WARN'ит об этом (см. правку в
 * этой же сессии), вместо того чтобы тихо давать 0 свечей без единой строки
 * в логе (именно это и маскировало причину бага №3).
 */
@Injectable()
export class CandleHistoryService {
  private readonly logger = new Logger(CandleHistoryService.name);

  constructor(
    @InjectRepository(PriceCandle) private readonly repo: Repository<PriceCandle>,
  ) {}

  /**
   * Возвращает до `need` последних ЗАКРЫТЫХ свечей для (ticker, intervalMs),
   * самая новая — в конце. Порядок источников: БД -> Binance REST -> Bybit
   * REST. Если внешний источник недоступен — продолжаем с тем, что уже
   * есть (не бросаем исключение наружу, это best-effort кеш).
   */
  async bootstrap(ticker: string, intervalMs: number, need: number): Promise<HistCandle[]> {
    let fromDb = await this.readFromDb(ticker, intervalMs, need);
    if (fromDb.length >= need) return fromDb;

    // --- 1. Binance ---
    const binanceInterval = BINANCE_INTERVAL_BY_MS[intervalMs];
    if (binanceInterval) {
      try {
        const fetched = await this.fetchFromBinance(ticker, binanceInterval, intervalMs, need - fromDb.length);
        if (fetched.length > 0) {
          await this.upsertMany(ticker, intervalMs, fetched, 'binance-rest');
          fromDb = await this.readFromDb(ticker, intervalMs, need);
          this.logger.log(
            `CandleHistoryService: ${ticker}@${intervalMs / 1000}с — бэкафилл из Binance дал ${fetched.length} свечей (в БД теперь ${fromDb.length}/${need}).`,
          );
        }
      } catch (err) {
        this.logger.warn(
          `CandleHistoryService: Binance REST для ${ticker}@${intervalMs / 1000}с не удался (${this.errMsg(err)}) — пробуем Bybit.`,
        );
      }
    } else {
      this.logger.warn(`CandleHistoryService: нет соответствия Binance-интервала для ${ticker}@${intervalMs}мс — Binance бэкафилл пропущен.`);
    }
    if (fromDb.length >= need) return fromDb;

    // --- 2. Bybit (фолбэк, см. класс-комментарий) ---
    const bybitInterval = BYBIT_INTERVAL_BY_MS[intervalMs];
    if (!bybitInterval) {
      this.logger.warn(`CandleHistoryService: нет соответствия Bybit-интервала для ${ticker}@${intervalMs}мс — Bybit бэкафилл пропущен.`);
      return fromDb;
    }
    try {
      const fetched = await this.fetchFromBybit(ticker, bybitInterval, intervalMs, need - fromDb.length);
      if (fetched.length > 0) {
        await this.upsertMany(ticker, intervalMs, fetched, 'bybit-rest');
        fromDb = await this.readFromDb(ticker, intervalMs, need);
        this.logger.log(
          `CandleHistoryService: ${ticker}@${intervalMs / 1000}с — бэкафилл из Bybit дал ${fetched.length} свечей (в БД теперь ${fromDb.length}/${need}).`,
        );
      }
    } catch (err) {
      this.logger.warn(
        `CandleHistoryService: бэкафилл из Bybit REST для ${ticker}@${intervalMs / 1000}с тоже не удался (${this.errMsg(err)}) — ` +
          `продолжаем с тем, что есть в БД (${fromDb.length}/${need}). Оба REST-источника недоступны — см. WARN из PriceFeedService, если это привело к 0 свечам.`,
      );
    }
    return fromDb;
  }

  /** Апсерт одной живой закрытой свечи из WS-фида (вызывается PriceFeedService при закрытии окна). */
  async saveClosedCandle(ticker: string, intervalMs: number, candle: HistCandle, source: string): Promise<void> {
    try {
      await this.upsertMany(ticker, intervalMs, [candle], source);
    } catch (err) {
      // Персистентность истории — best-effort кеш, не должна ронять горячий путь фида.
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
      .map((r) => ({ start: Number(r.startMs), open: r.open, high: r.high, low: r.low, close: r.close }))
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

  /** Загружает последние ЗАКРЫТЫЕ свечи из Binance REST (spot klines, публично, без ключа). */
  private async fetchFromBinance(ticker: string, interval: string, intervalMs: number, limit: number): Promise<HistCandle[]> {
    const symbol = `${ticker.toUpperCase()}USDT`;
    // +5 с запасом: последняя свеча Binance может быть ещё не закрыта на момент запроса.
    const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${Math.min(limit + 5, 1000)}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Binance klines HTTP ${res.status}`);
    const raw = (await res.json()) as any[];
    const now = Date.now();
    return raw
      .map((k) => ({ start: Number(k[0]), open: Number(k[1]), high: Number(k[2]), low: Number(k[3]), close: Number(k[4]) }))
      .filter((c) => c.start + intervalMs <= now) // строго закрытые (а не "отрезать последний элемент")
      .sort((a, b) => a.start - b.start)
      .slice(-limit);
  }

  /**
   * Загружает последние ЗАКРЫТЫЕ свечи из Bybit REST (Сессия 8, фолбэк).
   * category=spot — сознательно, а не linear/perpetual: цена перпетуал-
   * фьючерса может слегка отличаться от спота из-за фандинга/базиса, а ATR
   * тут используется как приближение исторической волатильности спота
   * (того, к чему ближе Chainlink-фид) — spot точнее для этой цели.
   */
  private async fetchFromBybit(ticker: string, interval: string, intervalMs: number, limit: number): Promise<HistCandle[]> {
    const symbol = `${ticker.toUpperCase()}USDT`;
    const url = `https://api.bybit.com/v5/market/kline?category=spot&symbol=${symbol}&interval=${interval}&limit=${Math.min(limit + 5, 1000)}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Bybit klines HTTP ${res.status}`);
    const json = (await res.json()) as { retCode: number; retMsg: string; result?: { list?: string[][] } };
    if (json.retCode !== 0) throw new Error(`Bybit klines error ${json.retCode}: ${json.retMsg}`);
    const raw = json.result?.list ?? [];
    const now = Date.now();
    // Bybit V5 отдаёт list в порядке убывания времени (новые первыми) — сортируем сами, порядок API не важен.
    return raw
      .map((k) => ({ start: Number(k[0]), open: Number(k[1]), high: Number(k[2]), low: Number(k[3]), close: Number(k[4]) }))
      .filter((c) => c.start + intervalMs <= now)
      .sort((a, b) => a.start - b.start)
      .slice(-limit);
  }

  private errMsg(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
