import { Injectable, Logger } from '@nestjs/common';
import axios, { AxiosInstance } from 'axios';

export interface BookLevel {
  price: number;
  size: number;
}

export interface BestQuote {
  tokenId: string;
  bestAsk: BookLevel | null;
  bestBid: BookLevel | null;
  tickSize: string | null;
  negRisk: boolean;
  // Реальный минимум биржи для этого маркета (штук токена), из поля
  // min_order_size REST-ответа. Используется и для маркет-, и для лимит-ордеров.
  minOrderSize: number | null;
}

/**
 * Ходит напрямую в публичный REST CLOB (GET /book?token_id=...).
 * Эндпоинт не требует авторизации, поэтому от него не зависит режим смоука —
 * стакан читаем всегда одинаково, независимо от того, торговый SDK жив или нет.
 *
 * ВАЖНО: в реальном ответе Polymarket порядок уровней bids/asks по разным
 * источникам документирован по-разному (где-то "лучший первый", где-то
 * наоборот). Чтобы не зависеть от этого, лучшую цену всегда вычисляем сами
 * (min по asks, max по bids), а не берём индекс [0] / [-1].
 */
@Injectable()
export class ClobPublicService {
  private readonly logger = new Logger(ClobPublicService.name);
  private readonly http: AxiosInstance;

  constructor() {
    this.http = axios.create({
      baseURL: 'https://clob.polymarket.com',
      timeout: 3000,
    });
  }

  async getBestQuote(tokenId: string): Promise<BestQuote | null> {
    try {
      const { data } = await this.http.get('/book', {
        params: { token_id: tokenId },
      });

      const asks = this.normalizeLevels(data?.asks);
      const bids = this.normalizeLevels(data?.bids);

      const bestAsk = asks.length
        ? asks.reduce((min, l) => (l.price < min.price ? l : min))
        : null;
      const bestBid = bids.length
        ? bids.reduce((max, l) => (l.price > max.price ? l : max))
        : null;

      return {
        tokenId,
        bestAsk,
        bestBid,
        tickSize: data?.tick_size ?? null,
        negRisk: Boolean(data?.neg_risk),
        minOrderSize: Number.isFinite(parseFloat(data?.min_order_size))
          ? parseFloat(data.min_order_size)
          : null,
      };
    } catch (err) {
      // 404 "No orderbook exists" — нормально для только что созданного маркета.
      this.logger.debug(
        `Стакан ${tokenId} недоступен: ${err instanceof Error ? err.message : err}`,
      );
      return null;
    }
  }

  private normalizeLevels(raw: unknown): BookLevel[] {
    if (!Array.isArray(raw)) return [];
    return raw
      .map((l) => ({
        price: parseFloat(l?.price),
        size: parseFloat(l?.size),
      }))
      .filter(
        (l) =>
          Number.isFinite(l.price) &&
          Number.isFinite(l.size) &&
          l.price > 0 &&
          l.size > 0,
      );
  }
}
