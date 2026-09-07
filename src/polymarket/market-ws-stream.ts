import { Logger } from '@nestjs/common';
import WebSocket from 'ws';

export type Outcome = 'YES' | 'NO';

export interface LiveQuote {
  bestBid: number | null;
  bestAsk: number | null;
  tickSize: string;
}

export type QuoteUpdateHandler = (
  outcome: Outcome,
  quote: LiveQuote,
) => void;

interface TokenState {
  outcome: Outcome;
  bestBid: number | null;
  bestAsk: number | null;
  tickSize: string;
}

/**
 * Один WS-коннект на один 5-минутный маркет: подписывается на оба токена
 * (YES/NO) через публичный market channel и поддерживает live лучшие
 * bid/ask по каждому, обновляясь по событиям book / best_bid_ask /
 * price_change / tick_size_change. Не требует авторизации.
 *
 * Протокол подтверждён официальной документацией Polymarket
 * (docs.polymarket.com/market-data/websocket/market-channel):
 *   - подписка: {"type":"market","assets_ids":[...],"custom_feature_enabled":true}
 *   - keepalive: клиент шлёт сырую строку "PING" раз в 10с, сервер отвечает "PONG"
 *   - tick_size_change прилетает, когда цена уходит выше 0.96 или ниже 0.04 —
 *     именно поэтому в конце свечи возможны цены вида 0.995/0.999.
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
    private readonly onUpdate: QuoteUpdateHandler,
    private readonly wsUrl = 'wss://ws-subscriptions-clob.polymarket.com/ws/market',
  ) {
    this.tokens.set(yesTokenId, {
      outcome: 'YES',
      bestBid: null,
      bestAsk: null,
      tickSize: initialTickSize,
    });
    this.tokens.set(noTokenId, {
      outcome: 'NO',
      bestBid: null,
      bestAsk: null,
      tickSize: initialTickSize,
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
          /* соединение уже умирает — переподключение обработает reconnect-логика */
        }
      }, 10_000);
    });

    this.ws.on('message', (raw) => this.handleMessage(raw.toString()));

    this.ws.on('close', () => this.scheduleReconnect());
    this.ws.on('error', (err) => {
      this.logger.warn(`WS ошибка: ${err.message}`);
      // 'close' сработает следом и запустит reconnect — здесь ничего не делаем.
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

  getQuote(tokenId: string): LiveQuote | null {
    const t = this.tokens.get(tokenId);
    if (!t) return null;
    return { bestBid: t.bestBid, bestAsk: t.bestAsk, tickSize: t.tickSize };
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

    // Сервер иногда шлёт список событий одним сообщением, иногда — по одному.
    const events = Array.isArray(data) ? data : [data];
    for (const event of events) this.handleEvent(event);
  }

  private handleEvent(event: any): void {
    switch (event?.event_type) {
      case 'book': {
        const state = this.tokens.get(event.asset_id);
        if (!state) return;
        const asks = this.normalizeLevels(event.asks);
        const bids = this.normalizeLevels(event.bids);
        state.bestAsk = asks.length ? Math.min(...asks.map((l) => l.price)) : null;
        state.bestBid = bids.length ? Math.max(...bids.map((l) => l.price)) : null;
        if (typeof event.tick_size === 'string') state.tickSize = event.tick_size;
        this.emit(state);
        return;
      }
      case 'best_bid_ask': {
        const state = this.tokens.get(event.asset_id);
        if (!state) return;
        state.bestBid = this.toNumOrNull(event.best_bid);
        state.bestAsk = this.toNumOrNull(event.best_ask);
        this.emit(state);
        return;
      }
      case 'price_change': {
        const changes = Array.isArray(event.price_changes) ? event.price_changes : [];
        for (const change of changes) {
          const state = this.tokens.get(change.asset_id);
          if (!state) continue;
          if (change.best_bid !== undefined) state.bestBid = this.toNumOrNull(change.best_bid);
          if (change.best_ask !== undefined) state.bestAsk = this.toNumOrNull(change.best_ask);
          this.emit(state);
        }
        return;
      }
      case 'tick_size_change': {
        const state = this.tokens.get(event.asset_id);
        if (!state) return;
        if (typeof event.new_tick_size === 'string') {
          state.tickSize = event.new_tick_size;
          this.logger.log(
            `tick_size_change: ${event.old_tick_size} -> ${event.new_tick_size} (${state.outcome})`,
          );
        }
        this.emit(state);
        return;
      }
      default:
        return;
    }
  }

  private emit(state: TokenState): void {
    this.onUpdate(state.outcome, {
      bestBid: state.bestBid,
      bestAsk: state.bestAsk,
      tickSize: state.tickSize,
    });
  }

  private normalizeLevels(raw: unknown): { price: number; size: number }[] {
    if (!Array.isArray(raw)) return [];
    return raw
      .map((l) => ({ price: parseFloat(l?.price), size: parseFloat(l?.size) }))
      .filter(
        (l) => Number.isFinite(l.price) && Number.isFinite(l.size) && l.size > 0,
      );
  }

  private toNumOrNull(v: unknown): number | null {
    const n = parseFloat(v as string);
    return Number.isFinite(n) ? n : null;
  }
}
