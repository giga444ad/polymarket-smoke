import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import WebSocket from 'ws';

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

/**
 * Быстрый WS-фид цены базового актива (proxy, НЕ тот же источник, что резолвит
 * маркет на Polymarket — резолвер использует Chainlink, у которого нет
 * публичного push-WS с нужной частотой обновления). Задача этого фида —
 * не повторить точный резолв, а дать дельту "цена сейчас vs цена на старте
 * окна" и локальную волатильность (ATR-подобную метрику) в реальном времени,
 * этого достаточно, чтобы отличить "уверенное движение" от "болтанки у границы".
 *
 * Один комбинированный WS-коннект на все настроенные активы сразу
 * (Binance combined stream), без API-ключей — читаем публичные трейды.
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
  private readonly baseUrl = 'wss://stream.binance.com:9443/stream';

  // assetPrefix ("btc-updown-5m") -> binance symbol ("btcusdt")
  private readonly assetToSymbol = new Map<string, string>();
  // symbol -> состояние (несколько assetPrefix теоретически могут шарить один символ)
  private readonly states = new Map<string, SymbolState>();

  constructor(private readonly config: ConfigService) {
    this.candleMs = parseInt(this.config.get<string>('FEED_CANDLE_MS', '1000'), 10);
    this.atrCandles = parseInt(this.config.get<string>('FEED_ATR_CANDLES', '20'), 10);

    const assetPrefixes = this.config
      .get<string>('MARKET_ASSETS', 'btc-updown-5m')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);

    const overrides = this.parseOverrides(this.config.get<string>('FEED_SYMBOL_OVERRIDES', ''));

    for (const assetPrefix of assetPrefixes) {
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

  private connect(): void {
    if (this.stopped) return;
    const symbols = [...this.states.keys()];
    if (symbols.length === 0) {
      this.logger.warn('PriceFeedService: нет активов для подключения — фид не запущен.');
      return;
    }

    const streams = symbols.map((s) => `${s}@trade`).join('/');
    const url = `${this.baseUrl}?streams=${streams}`;
    this.ws = new WebSocket(url);

    this.ws.on('open', () => {
      this.reconnectAttempts = 0;
      this.logger.log(`PriceFeedService: подключен к Binance (${symbols.join(', ')}).`);
    });

    this.ws.on('message', (raw: WebSocket.RawData) => {
      try {
        this.handleMessage(JSON.parse(raw.toString()));
      } catch (err) {
        this.logger.debug(`PriceFeedService: не удалось разобрать сообщение: ${this.errMsg(err)}`);
      }
    });

    this.ws.on('close', () => this.scheduleReconnect());
    this.ws.on('error', (err) => {
      this.logger.warn(`PriceFeedService: ошибка WS: ${this.errMsg(err)}`);
    });
  }

  private handleMessage(msg: any): void {
    const data = msg?.data;
    if (!data || data.e !== 'trade') return;

    const symbol = String(data.s ?? '').toLowerCase();
    const state = this.states.get(symbol);
    if (!state) return;

    const price = parseFloat(data.p);
    const tradeTs = Number(data.T) || Date.now();
    if (!Number.isFinite(price)) return;

    state.lastPrice = price;
    state.lastPriceAt = tradeTs;

    const bucketStart = Math.floor(tradeTs / this.candleMs) * this.candleMs;
    if (!state.current || state.current.start !== bucketStart) {
      if (state.current) {
        state.closed.push(state.current);
        if (state.closed.length > this.atrCandles * 2) {
          state.closed.splice(0, state.closed.length - this.atrCandles * 2);
        }
      }
      state.current = { start: bucketStart, open: price, high: price, low: price, close: price };
    } else {
      state.current.close = price;
      state.current.high = Math.max(state.current.high, price);
      state.current.low = Math.min(state.current.low, price);
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
