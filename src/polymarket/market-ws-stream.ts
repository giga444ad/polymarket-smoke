import { Logger } from '@nestjs/common';
import WebSocket from 'ws';

export type Outcome = 'YES' | 'NO';

export interface BookLevel {
  price: number;
  size: number;
}

export interface LiveBook {
  outcome: Outcome;
  tickSize: string;
  /** По возрастанию цены (лучший ask первый). */
  asks: BookLevel[];
  /** По убыванию цены (лучший bid первый). */
  bids: BookLevel[];
  bestAsk: number | null;
  bestBid: number | null;
}

export type BookUpdateHandler = (outcome: Outcome, book: LiveBook) => void;

interface TokenState {
  outcome: Outcome;
  tickSize: string;
  // price -> size, ключи — числа (не строки), чтобы не плодить дубликаты из-за форматирования
  asks: Map<number, number>;
  bids: Map<number, number>;
}

/**
 * Один WS-коннект на один 5-минутный маркет: подписывается на оба токена
 * (YES/NO) через публичный market channel и поддерживает ПОЛНЫЙ локальный
 * стакан по каждому (не только top-of-book) — это нужно, чтобы честно
 * эмулировать исполнение маркет-ордера по нескольким уровням цены, а не
 * делать вид, что весь объём сделки прошёл по единственной лучшей цене.
 *
 * Протокол подтверждён официальной документацией Polymarket
 * (docs.polymarket.com/market-data/websocket/market-channel):
 *   - подписка: {"type":"market","assets_ids":[...],"custom_feature_enabled":true}
 *   - keepalive: клиент шлёт сырую строку "PING" раз в 10с, сервер отвечает "PONG"
 *   - book — полный снапшот при подписке (или ресинке); price_change — точечные
 *     изменения уровня (price/size/side); size=0 означает, что уровень снят.
 *   - tick_size_change прилетает, когда цена уходит выше 0.96 или ниже 0.04.
 */
export class MarketWsStream {
  private readonly logger = new Logger(MarketWsStream.name);
  private ws: WebSocket | null = null;
  private pingTimer: NodeJS.Timeout | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private stopped = false;
  private reconnectAttempts = 0;

  private readonly tokens = new Map<string, TokenState>();

  constructor(
    yesTokenId: string,
    noTokenId: string,
    initialTickSize: string,
    private readonly onUpdate: BookUpdateHandler,
    private readonly wsUrl = 'wss://ws-subscriptions-clob.polymarket.com/ws/market',
  ) {
    this.tokens.set(yesTokenId, {
      outcome: 'YES',
      tickSize: initialTickSize,
      asks: new Map(),
      bids: new Map(),
    });
    this.tokens.set(noTokenId, {
      outcome: 'NO',
      tickSize: initialTickSize,
      asks: new Map(),
      bids: new Map(),
    });
  }

  connect(): void {
    if (this.stopped) return;
    this.ws = new WebSocket(this.wsUrl);

    this.ws.on('open', () => {
      this.reconnectAttempts = 0;
      this.ws?.send(
        JSON.stringify({
          type: 'market',
          assets_ids: Array.from(this.tokens.keys()),
          custom_feature_enabled: true,
        }),
      );
      this.pingTimer = setInterval(() => {
        try {
          this.ws?.send('PING');
        } catch {
          /* переподключение обработает scheduleReconnect через событие close */
        }
      }, 10_000);
    });

    this.ws.on('message', (raw) => this.handleMessage(raw.toString()));
    this.ws.on('close', () => this.scheduleReconnect());
    this.ws.on('error', (err) => {
      this.logger.warn(`WS ошибка: ${err.message}`);
    });
  }

  close(): void {
    this.stopped = true;
    if (this.pingTimer) clearInterval(this.pingTimer);
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.ws?.removeAllListeners();
    this.ws?.close();
    this.ws = null;
  }

  getBook(tokenId: string): LiveBook | null {
    const t = this.tokens.get(tokenId);
    if (!t) return null;
    return this.snapshot(t);
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    if (this.pingTimer) clearInterval(this.pingTimer);
    this.reconnectAttempts += 1;
    const delayMs = Math.min(5000, 500 * this.reconnectAttempts);
    this.logger.warn(
      `WS соединение оборвалось, переподключение через ${delayMs}мс (попытка ${this.reconnectAttempts})`,
    );
    this.reconnectTimer = setTimeout(() => this.connect(), delayMs);
  }

  private handleMessage(raw: string): void {
    if (raw === 'PONG') return;
    let data: any;
    try {
      data = JSON.parse(raw);
    } catch {
      return;
    }
    const events = Array.isArray(data) ? data : [data];
    for (const event of events) this.handleEvent(event);
  }

  private handleEvent(event: any): void {
    switch (event?.event_type) {
      case 'book': {
        const state = this.tokens.get(event.asset_id);
        if (!state) return;
        state.asks = this.levelsToMap(event.asks);
        state.bids = this.levelsToMap(event.bids);
        if (typeof event.tick_size === 'string') state.tickSize = event.tick_size;
        this.emit(state);
        return;
      }
      case 'price_change': {
        const changes = Array.isArray(event.price_changes) ? event.price_changes : [];
        const touched = new Set<TokenState>();
        for (const change of changes) {
          const state = this.tokens.get(change.asset_id);
          if (!state) continue;
          const price = parseFloat(change.price);
          const size = parseFloat(change.size);
          if (!Number.isFinite(price)) continue;
          const side = String(change.side || '').toUpperCase();
          const book = side === 'BUY' ? state.bids : side === 'SELL' ? state.asks : null;
          if (!book) continue;
          if (!Number.isFinite(size) || size <= 0) {
            book.delete(price);
          } else {
            book.set(price, size);
          }
          touched.add(state);
        }
        for (const state of touched) this.emit(state);
        return;
      }
      case 'tick_size_change': {
        const state = this.tokens.get(event.asset_id);
        if (!state) return;
        if (typeof event.new_tick_size === 'string') {
          this.logger.log(
            `tick_size_change: ${event.old_tick_size} -> ${event.new_tick_size} (${state.outcome})`,
          );
          state.tickSize = event.new_tick_size;
        }
        this.emit(state);
        return;
      }
      default:
        return;
    }
  }

  private emit(state: TokenState): void {
    this.onUpdate(state.outcome, this.snapshot(state));
  }

  private snapshot(state: TokenState): LiveBook {
    const asks = Array.from(state.asks.entries())
      .map(([price, size]) => ({ price, size }))
      .filter((l) => l.size > 0)
      .sort((a, b) => a.price - b.price);
    const bids = Array.from(state.bids.entries())
      .map(([price, size]) => ({ price, size }))
      .filter((l) => l.size > 0)
      .sort((a, b) => b.price - a.price);

    return {
      outcome: state.outcome,
      tickSize: state.tickSize,
      asks,
      bids,
      bestAsk: asks.length ? asks[0].price : null,
      bestBid: bids.length ? bids[0].price : null,
    };
  }

  private levelsToMap(raw: unknown): Map<number, number> {
    const map = new Map<number, number>();
    if (!Array.isArray(raw)) return map;
    for (const l of raw) {
      const price = parseFloat(l?.price);
      const size = parseFloat(l?.size);
      if (Number.isFinite(price) && Number.isFinite(size) && size > 0) {
        map.set(price, size);
      }
    }
    return map;
  }
}
