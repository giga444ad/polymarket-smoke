import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import axios, { AxiosInstance } from 'axios';

export interface CurrentMarketInfo {
  slug: string;
  question: string | null;
  closesAt: Date;
  yesTokenId: string;
  noTokenId: string;
  conditionId: string | null;
  negRisk: boolean;
}

export interface MarketOutcome {
  slug: string;
  closed: boolean;
  yesWon: boolean | null;
  noWon: boolean | null;
}

/**
 * Читает публичный Gamma API Polymarket (gamma-api.polymarket.com).
 * Только чтение, авторизация не требуется — используется и для поиска
 * текущего 5-минутного маркета, и для резолва исхода после закрытия.
 */
@Injectable()
export class GammaMarketService {
  private readonly logger = new Logger(GammaMarketService.name);
  private readonly http: AxiosInstance;
  private readonly slugPrefix: string;
  private readonly intervalSec = 300;

  constructor(private readonly config: ConfigService) {
    this.http = axios.create({
      baseURL: 'https://gamma-api.polymarket.com',
      timeout: 5000,
    });
    this.slugPrefix = this.config.get<string>(
      'MARKET_SLUG_PREFIX',
      'btc-updown-5m',
    );
  }

  /**
   * Слаг текущего 5-минутного интервала — совпадает с таймстампом его закрытия,
   * например btc-updown-5m-1788724500.
   */
  buildSlugForClose(closeTimestampSec: number): string {
    return `${this.slugPrefix}-${closeTimestampSec}`;
  }

  currentIntervalCloseTimestampSec(nowMs = Date.now()): number {
    const nowSec = Math.floor(nowMs / 1000);
    const slotStart = Math.floor(nowSec / this.intervalSec) * this.intervalSec;
    return slotStart;
  }

  async fetchCurrentMarket(): Promise<CurrentMarketInfo | null> {
    const closeTs = this.currentIntervalCloseTimestampSec();
    const slug = this.buildSlugForClose(closeTs);
    return this.fetchMarketBySlug(slug, closeTs);
  }

  async fetchMarketBySlug(
    slug: string,
    knownCloseTs?: number,
  ): Promise<CurrentMarketInfo | null> {
    try {
      const { data } = await this.http.get('/events', { params: { slug } });
      if (!Array.isArray(data) || data.length === 0) return null;

      const event = data[0];
      const market = event?.markets?.[0];
      if (!market) return null;

      const clobTokenIds = this.parseJsonArray(market.clobTokenIds);
      if (!clobTokenIds || clobTokenIds.length < 2) return null;

      const closesAt = knownCloseTs
        ? new Date(knownCloseTs * 1000)
        : new Date(market.endDate ?? Date.now());

      return {
        slug,
        question: market.question ?? null,
        closesAt,
        yesTokenId: clobTokenIds[0],
        noTokenId: clobTokenIds[1],
        conditionId: market.conditionId ?? null,
        negRisk: Boolean(market.negRisk),
      };
    } catch (err) {
      // Рынок ещё не создан / сеть моргнула — это нормально при опережающем опросе.
      this.logger.debug(
        `Не удалось получить маркет ${slug}: ${this.errMsg(err)}`,
      );
      return null;
    }
  }

  /**
   * Возвращает исход маркета по slug, если он уже закрыт и оракул проставил outcomePrices.
   * Пока маркет не резолвнулся, вернёт closed=false — вызывающий код должен повторить попытку позже.
   */
  async fetchOutcome(slug: string): Promise<MarketOutcome | null> {
    try {
      const { data } = await this.http.get('/events', { params: { slug } });
      if (!Array.isArray(data) || data.length === 0) return null;

      const market = data[0]?.markets?.[0];
      if (!market) return null;

      const outcomePrices = this.parseJsonArray(market.outcomePrices);
      const closed = Boolean(market.closed);

      if (!closed || !outcomePrices || outcomePrices.length < 2) {
        return { slug, closed: false, yesWon: null, noWon: null };
      }

      const yesPrice = parseFloat(outcomePrices[0]);
      const noPrice = parseFloat(outcomePrices[1]);

      return {
        slug,
        closed: true,
        yesWon: yesPrice === 1,
        noWon: noPrice === 1,
      };
    } catch (err) {
      this.logger.debug(
        `Не удалось получить исход ${slug}: ${this.errMsg(err)}`,
      );
      return null;
    }
  }

  private parseJsonArray(value: unknown): string[] | null {
    if (Array.isArray(value)) return value as string[];
    if (typeof value !== 'string') return null;
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : null;
    } catch {
      return null;
    }
  }

  private errMsg(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
