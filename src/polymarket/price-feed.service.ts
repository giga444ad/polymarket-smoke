import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import WebSocket from 'ws';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { SocksProxyAgent } from 'socks-proxy-agent';
import { parseStreamsConfig } from '../trading/stream-config';

interface Candle {
  start: number; // ts начала бакета, мс
  open: number;
  high: number;
  low: number;
  close: number;
}

interface SymbolState {
  symbol: string;
  lastPrice: number | null;
  lastPriceAt: number | null;
  current: Candle | null;
  closed: Candle[]; // последние N ЗАКРЫТЫХ свечей, самая новая — в конце
}

export interface FeedSnapshot {
  price: number | null;
  /** Момент, когда пришёл последний тик (для определения "фид протух"). */
  priceAt: number | null;
  atr: number | null;
  candleCount: number;
}

interface NormalizedTrade {
  symbol: string; // lowercase, напр. "btcusdt"
  price: number;
  ts: number; // мс
}

/**
 * Адаптер конкретной биржи: как собрать URL/подписку и как разобрать сырое
 * сообщение в нормализованный трейд. Единственный источник правды по
 * названиям бирж — FEED_PROVIDERS в .env (см. ниже).
 */
interface ProviderAdapter {
  name: string;
  buildUrl(symbols: string[]): string;
  /** Что отправить сразу после открытия соединения (напр. Bybit требует явный subscribe). */
  onOpenMessage?(symbols: string[]): string | null;
  parseMessage(raw: any): NormalizedTrade | null;
}

const BINANCE_ADAPTER: ProviderAdapter = {
  name: 'binance',
  buildUrl(symbols) {
    const streams = symbols.map((s) => `${s}@trade`).join('/');
    return `wss://stream.binance.com:9443/stream?streams=${streams}`;
  },
  parseMessage(raw) {
    const data = raw?.data;
    if (!data || data.e !== 'trade') return null;
    const symbol = String(data.s ?? '').toLowerCase();
    const price = parseFloat(data.p);
    const ts = Number(data.T) || Date.now();
    if (!symbol || !Number.isFinite(price)) return null;
    return { symbol, price, ts };
  },
};

const BYBIT_ADAPTER: ProviderAdapter = {
  name: 'bybit',
  buildUrl() {
    return 'wss://stream.bybit.com/v5/public/spot';
  },
  onOpenMessage(symbols) {
    return JSON.stringify({ op: 'subscribe', args: symbols.map((s) => `publicTrade.${s.toUpperCase()}`) });
  },
  parseMessage(raw) {
    if (typeof raw?.topic !== 'string' || !raw.topic.startsWith('publicTrade.')) return null;
    const trade = raw?.data?.[0];
    if (!trade) return null;
    const symbol = String(trade.s ?? '').toLowerCase();
    const price = parseFloat(trade.p);
    const ts = Number(trade.T) || Date.now();
    if (!symbol || !Number.isFinite(price)) return null;
    return { symbol, price, ts };
  },
};

const PROVIDERS: Record<string, ProviderAdapter> = {
  binance: BINANCE_ADAPTER,
  bybit: BYBIT_ADAPTER,
};

/**
 * Быстрый WS-фид цены базового актива (proxy-источник для нашей собственной
 * диагностики/ATR-гейта, НЕ тот же источник, что резолвит маркет на
 * Polymarket — резолвер использует Chainlink). Задача этого фида — не
 * повторить точный резолв, а дать дельту "цена сейчас vs цена на старте
 * окна" и локальную волатильность в реальном времени.
 *
 * ВАЖНО (реальный инцидент): Binance по WS периодически отвечает `451
 * Unavailable For Legal Reasons` на handshake — это гео-блокировка по IP
 * хостинга (типично для облачных провайдеров в юрисдикциях, которые Binance
 * не обслуживает), а НЕ временный сбой сети. Бесконечный ретрай на тот же
 * URL в этом случае никогда не восстановится сам — раньше именно так и
 * происходило (лог "ошибка WS: Unexpected server response: 451" каждые
 * ~30с без остановки), и весь фид был мёртв на протяжении ВСЕЙ сессии, а
 * не эпизодически. Из-за этого referencePrice/priceAtEntry были недоступны
 * ВСЕГДА — не потому, что ATR-гейт кого-то не защитил (он по умолчанию и
 * так выключен, см. ENTRY_FILTER_ENABLED), а потому что фид физически не
 * может подключиться с текущего хостинга.
 *
 * Исправлено двумя независимыми механизмами (можно использовать оба сразу):
 *  1) Автоматический фолбэк на другого провайдера (`FEED_PROVIDERS`,
 *     по умолчанию `binance,bybit`) — после `FEED_PROVIDER_FAIL_THRESHOLD`
 *     подряд неудачных попыток подключения к текущему провайдеру бот сам
 *     переключается на следующего в списке (по кругу). Bybit в общем случае
 *     не блокирует те же юрисдикции, что и Binance, и не требует прокси.
 *  2) Опциональный прокси (`FEED_PROXY_URL`, http(s):// или socks5://) —
 *     если геоблок актуален для ВСЕХ настроенных провайдеров сразу.
 */
@Injectable()
export class PriceFeedService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PriceFeedService.name);

  private ws: WebSocket | null = null;
  private stopped = false;
  private reconnectAttempts = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;

  private readonly candleMs: number;
  private readonly atrCandles: number;

  private readonly providers: ProviderAdapter[];
  private readonly failThreshold: number;
  private readonly proxyUrl: string | null;
  private providerIndex = 0;
  // Подряд идущие неудачные попытки подключения ИМЕННО к текущему провайдеру
  // (сбрасывается при успешном 'open' или при переключении на следующего).
  private consecutiveFailuresOnProvider = 0;

  // assetPrefix ("btc-updown-5m") -> binance-style символ ("btcusdt")
  private readonly assetToSymbol = new Map<string, string>();
  // symbol -> состояние (несколько assetPrefix теоретически могут шарить один символ)
  private readonly states = new Map<string, SymbolState>();

  constructor(private readonly config: ConfigService) {
    this.candleMs = parseInt(this.config.get<string>('FEED_CANDLE_MS', '1000'), 10);
    this.atrCandles = parseInt(this.config.get<string>('FEED_ATR_CANDLES', '20'), 10);
    this.failThreshold = parseInt(this.config.get<string>('FEED_PROVIDER_FAIL_THRESHOLD', '3'), 10);
    this.proxyUrl = this.config.get<string>('FEED_PROXY_URL', '').trim() || null;

    const providerNames = this.config
      .get<string>('FEED_PROVIDERS', 'binance,bybit')
      .split(',')
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean);
    this.providers = providerNames.map((name) => {
      const adapter = PROVIDERS[name];
      if (!adapter) {
        throw new Error(
          `FEED_PROVIDERS: неизвестный провайдер "${name}". Доступные: ${Object.keys(PROVIDERS).join(', ')}.`,
        );
      }
      return adapter;
    });
    if (this.providers.length === 0) {
      throw new Error('FEED_PROVIDERS: список провайдеров не должен быть пустым.');
    }

    // Раньше список активов брался из MARKET_ASSETS (общий на все потоки).
    // Теперь единственный источник правды — STREAMS_CONFIG (см. п.3/п.4
    // бэклога, src/trading/stream-config.ts) — каждый streamKey из него сам
    // по себе ключ фида (для потоков без стандартного вывода символа из
    // префикса, напр. "bitcoin-up-or-down", см. FEED_SYMBOL_OVERRIDES ниже).
    const streamKeys = parseStreamsConfig(this.config.get<string>('STREAMS_CONFIG')).map(
      (s) => s.streamKey,
    );

    const overrides = this.parseOverrides(this.config.get<string>('FEED_SYMBOL_OVERRIDES', ''));

    for (const assetPrefix of streamKeys) {
      const symbol = overrides.get(assetPrefix) ?? this.deriveSymbol(assetPrefix);
      this.assetToSymbol.set(assetPrefix, symbol);
      if (!this.states.has(symbol)) {
        this.states.set(symbol, { symbol, lastPrice: null, lastPriceAt: null, current: null, closed: [] });
      }
    }
  }

  onModuleInit(): void {
    this.connect();
  }

  onModuleDestroy(): void {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.ws?.close();
  }

  /** Текущая цена + ATR по фиду для данного actice-префикса (btc-updown-5m и т.п.). */
  getSnapshot(assetPrefix: string): FeedSnapshot {
    const symbol = this.assetToSymbol.get(assetPrefix);
    const state = symbol ? this.states.get(symbol) : undefined;
    if (!state) return { price: null, priceAt: null, atr: null, candleCount: 0 };

    return {
      price: state.lastPrice,
      priceAt: state.lastPriceAt,
      atr: this.computeAtr(state),
      candleCount: state.closed.length,
    };
  }

  private computeAtr(state: SymbolState): number | null {
    if (state.closed.length < this.atrCandles) return null;
    const sample = state.closed.slice(-this.atrCandles);
    const sum = sample.reduce((acc, c) => acc + Math.abs(c.high - c.low), 0);
    return sum / sample.length;
  }

  private deriveSymbol(assetPrefix: string): string {
    // "btc-updown-5m" -> "btc" -> "btcusdt". Для нестандартных активов лучше
    // задать явный маппинг через FEED_SYMBOL_OVERRIDES.
    const base = assetPrefix.split('-')[0]?.toLowerCase() ?? assetPrefix.toLowerCase();
    return `${base}usdt`;
  }

  private parseOverrides(raw: string): Map<string, string> {
    const map = new Map<string, string>();
    for (const pair of raw.split(',')) {
      const [assetPrefix, symbol] = pair.split(':').map((s) => s.trim());
      if (assetPrefix && symbol) map.set(assetPrefix, symbol.toLowerCase());
    }
    return map;
  }

  private buildAgent(): { agent: any } | Record<string, never> {
    if (!this.proxyUrl) return {};
    try {
      const agent = this.proxyUrl.startsWith('socks')
        ? new SocksProxyAgent(this.proxyUrl)
        : new HttpsProxyAgent(this.proxyUrl);
      return { agent };
    } catch (err) {
      this.logger.error(`FEED_PROXY_URL некорректен, подключаюсь напрямую: ${this.errMsg(err)}`);
      return {};
    }
  }

  private connect(): void {
    if (this.stopped) return;
    const symbols = [...this.states.keys()];
    if (symbols.length === 0) {
      this.logger.warn('PriceFeedService: нет активов для подключения — фид не запущен.');
      return;
    }

    const provider = this.providers[this.providerIndex];
    const url = provider.buildUrl(symbols);
    const wsOptions = this.buildAgent();
    this.ws = new WebSocket(url, wsOptions as any);

    this.ws.on('open', () => {
      this.reconnectAttempts = 0;
      this.consecutiveFailuresOnProvider = 0;
      const openMsg = provider.onOpenMessage?.(symbols);
      if (openMsg) this.ws?.send(openMsg);
      this.logger.log(
        `PriceFeedService: подключен к ${provider.name} (${symbols.join(', ')})${this.proxyUrl ? ' через прокси' : ''}.`,
      );
    });

    this.ws.on('message', (raw: WebSocket.RawData) => {
      try {
        const trade = provider.parseMessage(JSON.parse(raw.toString()));
        if (trade) this.applyTrade(trade);
      } catch (err) {
        this.logger.debug(`PriceFeedService: не удалось разобрать сообщение (${provider.name}): ${this.errMsg(err)}`);
      }
    });

    this.ws.on('close', () => this.handleDisconnect(provider));
    this.ws.on('error', (err) => {
      this.logger.warn(`PriceFeedService: ошибка WS (${provider.name}): ${this.errMsg(err)}`);
    });
  }

  /**
   * Единая точка решения "переподключаться к тому же провайдеру или
   * переключиться на следующего" — так же считает подряд идущие сбои и в
   * `error`-ветке (сокет там тоже закрывается почти сразу после ошибки, что
   * триггерит и 'close', так что двойного счёта не происходит — инкремент
   * только здесь, в close).
   */
  private handleDisconnect(provider: ProviderAdapter): void {
    if (this.stopped) return;
    this.consecutiveFailuresOnProvider += 1;

    if (this.consecutiveFailuresOnProvider >= this.failThreshold && this.providers.length > 1) {
      const next = (this.providerIndex + 1) % this.providers.length;
      this.logger.warn(
        `PriceFeedService: ${provider.name} не отвечает уже ${this.consecutiveFailuresOnProvider} подключений подряд ` +
          `(похоже на гео-блокировку по IP хостинга, не на временный сбой сети) — ` +
          `переключаюсь на ${this.providers[next].name}.`,
      );
      this.providerIndex = next;
      this.consecutiveFailuresOnProvider = 0;
      this.reconnectAttempts = 0;
    }

    this.scheduleReconnect();
  }

  private applyTrade(trade: NormalizedTrade): void {
    const state = this.states.get(trade.symbol);
    if (!state) return;

    state.lastPrice = trade.price;
    state.lastPriceAt = trade.ts;

    const bucketStart = Math.floor(trade.ts / this.candleMs) * this.candleMs;
    if (!state.current || state.current.start !== bucketStart) {
      if (state.current) {
        state.closed.push(state.current);
        if (state.closed.length > this.atrCandles * 2) {
          state.closed.splice(0, state.closed.length - this.atrCandles * 2);
        }
      }
      state.current = { start: bucketStart, open: trade.price, high: trade.price, low: trade.price, close: trade.price };
    } else {
      state.current.close = trade.price;
      state.current.high = Math.max(state.current.high, trade.price);
      state.current.low = Math.min(state.current.low, trade.price);
    }
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    this.reconnectAttempts += 1;
    const delay = Math.min(30000, 1000 * this.reconnectAttempts);
    this.logger.warn(`PriceFeedService: соединение закрыто, переподключение через ${delay}мс.`);
    this.reconnectTimer = setTimeout(() => this.connect(), delay);
  }

  private errMsg(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
