import { Injectable, Logger } from '@nestjs/common';
import axios, { AxiosInstance } from 'axios';
import { StreamDefinition } from '../trading/stream-config';

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
 * Только чтение, авторизация не требуется.
 *
 * Раньше был жёстко завязан на единственный таймфрейм (intervalSec=300,
 * слаг = `${assetPrefix}-${unix_start}`) — предполагалось, что на инстанс
 * приходится ровно один поток. Теперь параметризован по StreamDefinition
 * (см. src/trading/stream-config.ts), чтобы вести несколько независимых
 * потоков (актив × таймфрейм) с разным форматом слага одновременно.
 *
 * Формат слага ПРОВЕРЕН ВРУЧНУЮ на живых данных перед реализацией (см. п.4
 * бэклога — "перед кодированием проверить реальные слаги"):
 *  - 5m:  btc-updown-5m-1788998100   (слаг = таймстамп НАЧАЛА интервала)
 *  - 15m: btc-updown-15m-1788997500  (та же схема, интервал 900с)
 *  - 1h:  bitcoin-up-or-down-september-9-2026-7pm-et — календарная ET-строка,
 *         НЕ unix-таймстамп; месяц словом, день/час без ведущего нуля,
 *         am/pm строчными, суффикс "-et". Числовая арифметика границ часа
 *         (начало/конец) при этом общая с interval-потоками: смещение ET
 *         относительно UTC — целое число часов, поэтому floor(unixSec/3600)
 *         даёт ровно те же моменты, что и границы часа по ET.
 * 4ч/1д не реализованы (сознательно, см. BACKLOG п.5) — если понадобятся,
 * сюда добавляется третий "kind" по тому же принципу.
 */
@Injectable()
export class GammaMarketService {
  private readonly logger = new Logger(GammaMarketService.name);
  private readonly http: AxiosInstance;

  constructor() {
    this.http = axios.create({
      baseURL: 'https://gamma-api.polymarket.com',
      timeout: 5000,
    });
  }

  currentIntervalStartTimestampSec(intervalSec: number, nowMs = Date.now()): number {
    const nowSec = Math.floor(nowMs / 1000);
    return Math.floor(nowSec / intervalSec) * intervalSec;
  }

  currentIntervalCloseTimestampSec(intervalSec: number, nowMs = Date.now()): number {
    return this.currentIntervalStartTimestampSec(intervalSec, nowMs) + intervalSec;
  }

  /** Слаг маркета, который НАЧИНАЕТСЯ в startTimestampSec, для данного потока. */
  buildSlugForStart(stream: StreamDefinition, startTimestampSec: number): string {
    if (stream.kind === 'hourly-et') {
      return this.buildHourlyEtSlug(stream.etSlugBase!, startTimestampSec * 1000);
    }
    return `${stream.slugPrefix}-${startTimestampSec}`;
  }

  /**
   * `bitcoin-up-or-down-september-9-2026-7pm-et` — месяц словом (en-US,
   * строчными), день/год числом без ведущих нулей, час 1-12 без ведущего
   * нуля + am/pm строчными, суффикс -et. Считается по факту календарной
   * даты/часа в America/New_York на момент startMs (не локальной зоне
   * процесса).
   */
  private buildHourlyEtSlug(etSlugBase: string, startMs: number): string {
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/New_York',
      month: 'long',
      day: 'numeric',
      year: 'numeric',
      hour: 'numeric',
      hour12: true,
    }).formatToParts(new Date(startMs));

    const get = (type: string) => parts.find((p) => p.type === type)?.value ?? '';
    const month = get('month').toLowerCase();
    const day = get('day');
    const year = get('year');
    const hour = get('hour');
    const dayPeriod = get('dayPeriod').toLowerCase(); // "am" | "pm"

    return `${etSlugBase}-${month}-${day}-${year}-${hour}${dayPeriod}-et`;
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
