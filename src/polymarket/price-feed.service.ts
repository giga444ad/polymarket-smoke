import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import WebSocket from 'ws';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { SocksProxyAgent } from 'socks-proxy-agent';
import { parseStreamsConfig } from '../trading/stream-config';
import { CandleHistoryService } from './candle-history.service';

interface Candle {
  start: number; // ts начала бакета, мс
  open: number;
  high: number;
  low: number;
  close: number;
}

interface StreamFeedState {
  streamKey: string;
  ticker: string; // "btc", "eth", ... — общий тикер для всех провайдеров
  candleMs: number; // размер свечи ATR = ровно длительность окна ЭТОГО потока (см. ниже)
  atrCandles: number; // сколько прошлых окон усредняем (может быть переопределено на поток)
  lastPrice: number | null;
  lastPriceAt: number | null;
  lastPriceSource: string | null;
  current: Candle | null;
  closed: Candle[]; // последние N ЗАКРЫТЫХ свечей, самая новая — в конце
}

export interface FeedSnapshot {
  price: number | null;
  priceAt: number | null;
  atr: number | null;
  candleCount: number;
  source: string | null;
}

interface NormalizedTrade {
  ticker: string; // "btc", "eth", ... (канонический, БЕЗ суффикса типа usdt/usd)
  price: number;
  ts: number; // мс
}

interface ProviderAdapter {
  name: string;
  buildUrl(tickers: string[]): string;
  /** Что отправить сразу после открытия соединения (напр. Bybit/RTDS требуют явный subscribe). */
  onOpenMessage?(tickers: string[]): string | null;
  parseMessage(raw: any): NormalizedTrade | null;
  /** Прикладной heartbeat (RTDS требует текстовый "PING" каждые 5с, помимо протокольного ws ping/pong). */
  heartbeatIntervalMs?: number;
  heartbeatMessage?: string;
}

const BINANCE_ADAPTER: ProviderAdapter = {
  name: 'binance',
  buildUrl(tickers) {
    const streams = tickers.map((t) => `${t}usdt@trade`).join('/');
    return `wss://stream.binance.com:9443/stream?streams=${streams}`;
  },
  parseMessage(raw) {
    const data = raw?.data;
    if (!data || data.e !== 'trade') return null;
    const wireSymbol = String(data.s ?? '').toLowerCase(); // "btcusdt"
    const ticker = wireSymbol.endsWith('usdt') ? wireSymbol.slice(0, -4) : wireSymbol;
    const price = parseFloat(data.p);
    const ts = Number(data.T) || Date.now();
    if (!ticker || !Number.isFinite(price)) return null;
    return { ticker, price, ts };
  },
};

const BYBIT_ADAPTER: ProviderAdapter = {
  name: 'bybit',
  buildUrl() {
    return 'wss://stream.bybit.com/v5/public/spot';
  },
  onOpenMessage(tickers) {
    return JSON.stringify({ op: 'subscribe', args: tickers.map((t) => `publicTrade.${t.toUpperCase()}USDT`) });
  },
  parseMessage(raw) {
    if (typeof raw?.topic !== 'string' || !raw.topic.startsWith('publicTrade.')) return null;
    const trade = raw?.data?.[0];
    if (!trade) return null;
    const wireSymbol = String(trade.s ?? '').toLowerCase(); // "btcusdt"
    const ticker = wireSymbol.endsWith('usdt') ? wireSymbol.slice(0, -4) : wireSymbol;
    const price = parseFloat(trade.p);
    const ts = Number(trade.T) || Date.now();
    if (!ticker || !Number.isFinite(price)) return null;
    return { ticker, price, ts };
  },
};

/**
 * ГЛАВНЫЙ провайдер по умолчанию. Это буквально тот же фид, которым
 * Polymarket резолвит крипто-маркеты (data.chain.link, см. README) — не
 * приближение, а сам источник истины. Официальный публичный Polymarket RTDS
 * (wss://ws-live-data.polymarket.com, топик `crypto_prices_chainlink`), без
 * API-ключа, задокументирован на docs.polymarket.com. Требует прикладной
 * PING каждые 5с (см. heartbeatIntervalMs) — это НЕ протокольный ws-пинг, а
 * отдельный текстовый фрейм, который ждёт именно этот сервис.
 */
const CHAINLINK_RTDS_ADAPTER: ProviderAdapter = {
  name: 'chainlink',
  buildUrl() {
    return 'wss://ws-live-data.polymarket.com';
  },
  onOpenMessage(tickers) {
    return JSON.stringify({
      action: 'subscribe',
      subscriptions: tickers.map((t) => ({
        topic: 'crypto_prices_chainlink',
        type: '*',
        filters: JSON.stringify({ symbol: `${t}/usd` }),
      })),
    });
  },
  heartbeatIntervalMs: 5000,
  heartbeatMessage: 'PING',
  parseMessage(raw) {
    if (raw?.topic !== 'crypto_prices_chainlink') return null;
    const payload = raw?.payload;
    if (!payload) return null;
    const wireSymbol = String(payload.symbol ?? '').toLowerCase(); // "btc/usd"
    const ticker = wireSymbol.split('/')[0];
    const price = parseFloat(payload.value);
    const ts = Number(payload.timestamp) || Date.now();
    if (!ticker || !Number.isFinite(price)) return null;
    return { ticker, price, ts };
  },
};

const PROVIDERS: Record<string, ProviderAdapter> = {
  chainlink: CHAINLINK_RTDS_ADAPTER,
  binance: BINANCE_ADAPTER,
  bybit: BYBIT_ADAPTER,
};

/**
 * Быстрый WS-фид цены базового актива для диагностики/ATR-гейта входа.
 *
 * ИСТОЧНИК (важное изменение после разбора реальных сливов): по умолчанию
 * теперь Chainlink через официальный Polymarket RTDS — это буквально то же,
 * чем Polymarket резолвит крипто Up/Down маркеты (data.chain.link). Раньше
 * дефолтом был Binance/Bybit — приближение, которое в моменты резких
 * движений расходится с Chainlink на 0.3-0.8% на 10-30 секунд (в бинарном
 * маркете у самой границы ¢99 этого достаточно, чтобы диагностика "цена
 * уверенно ушла в одну сторону" оказалась просто неверной — ровно то, что
 * происходило на разобранных инцидентах BTC/SOL). Binance/Bybit остаются
 * доступны как фолбэк-провайдеры (`FEED_PROVIDERS`), но их значения всегда
 * помечаются `source` в FeedSnapshot — не выдаём приближение за истину молча.
 *
 * ОКНО ATR ПРИВЯЗАНО К ДЛИТЕЛЬНОСТИ ОКНА ПОТОКА, А НЕ К ЕГО ДОЛЕ. Важная
 * деталь, из-за которой первая версия этого фикса всё ещё была неверна:
 * если считать ATR по свечам РАЗМЕРОМ intervalSec/N (т.е. дробить каждое
 * окно на N кусков), то мы сравниваем "дрейф цены за ВСЁ прошедшее окно" с
 * "типичным размахом ВНУТРИ 1/N этого окна" — а это не одна и та же
 * величина по масштабу времени. При случайном блуждании размах растёт
 * примерно как √(время), поэтому даже на чистом шуме без всякого
 * направленного движения такое сравнение само по себе даёт коэффициент
 * ≈ √N (для N=20 это ≈4.5), а не ≈1 — то есть гейт всё ещё систематически
 * занижал бы риск, просто не так драматично, как при 20-секундных свечах.
 *
 * Правильное сравнение — не дробить окно, а мерить размах целых ПРОШЛЫХ
 * завершённых окон того же потока и сравнивать с ним: одна свеча ATR = ровно
 * одно окно (`intervalSec`), ATR = средний размах последних N таких окон.
 * Это буквально "насколько обычно двигается цена этого актива за один такой
 * же по длине маркет" — без пересчётных коэффициентов. Платим за это более
 * долгим прогревом: N=20 окон для 5m — это 100 минут, для 1h — 20 часов
 * (поэтому у часового потока в дефолтном `STREAMS_CONFIG` `atrCandles`
 * уменьшен до 8 — см. stream-config.ts). Пока прогрев не набрался,
 * `getSnapshot(...).atr` возвращает null — при включённом
 * `ENTRY_FILTER_ENABLED` это уходит в fail-closed так же, как отсутствие
 * тиков: лучше не торговать этим потоком, чем гадать на нерепрезентативной
 * статистике.
 */
@Injectable()
export class PriceFeedService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(PriceFeedService.name);

  private ws: WebSocket | null = null;
  private stopped = false;
  private reconnectAttempts = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;

  private readonly atrCandles: number;
  private readonly staleMs: number;

  private readonly providers: ProviderAdapter[];
  private readonly failThreshold: number;
  private readonly proxyUrl: string | null;
  private providerIndex = 0;
  private consecutiveFailuresOnProvider = 0;

  // assetPrefix/streamKey ("btc-updown-5m") -> тикер ("btc")
  private readonly streamToTicker = new Map<string, string>();
  // тикер -> какие streamKey на него подписаны (для фан-аута трейдов на
  // несколько потоков с РАЗНЫМ размером свечи ATR, но общим источником трейдов)
  private readonly tickerToStreams = new Map<string, string[]>();
  // streamKey -> собственное состояние свечей/ATR (см. комментарий класса выше)
  private readonly states = new Map<string, StreamFeedState>();

  constructor(
    private readonly config: ConfigService,
    private readonly candleHistory: CandleHistoryService,
  ) {
    this.atrCandles = parseInt(this.config.get<string>('FEED_ATR_CANDLES', '20'), 10);
    this.staleMs = parseInt(this.config.get<string>('FEED_STALE_MS', '5000'), 10);
    this.failThreshold = parseInt(this.config.get<string>('FEED_PROVIDER_FAIL_THRESHOLD', '3'), 10);
    this.proxyUrl = this.config.get<string>('FEED_PROXY_URL', '').trim() || null;

    const providerNames = this.config
      .get<string>('FEED_PROVIDERS', 'chainlink,binance,bybit')
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

    const streams = parseStreamsConfig(this.config.get<string>('STREAMS_CONFIG'));
    const overrides = this.parseOverrides(this.config.get<string>('FEED_SYMBOL_OVERRIDES', ''));
    // Оставлен как защитный нижний предел (на случай экзотически маленького
    // intervalSec) — при обычных 5m/15m/1h НЕ участвует в расчёте, поскольку
    // candleMs теперь равен полной длительности окна, а не её доле.
    const minCandleMs = parseInt(this.config.get<string>('FEED_MIN_CANDLE_MS', '1000'), 10);

    for (const stream of streams) {
      const ticker = overrides.get(stream.streamKey) ?? this.deriveTicker(stream.streamKey);
      this.streamToTicker.set(stream.streamKey, ticker);
      const list = this.tickerToStreams.get(ticker) ?? [];
      list.push(stream.streamKey);
      this.tickerToStreams.set(ticker, list);

      // Одна свеча ATR = ровно одно окно потока (см. класс-комментарий выше —
      // почему НЕ intervalSec/N). Побочный бонус: границы такой свечи
      // (floor(ts/candleMs)*candleMs) совпадают с границами САМИХ маркетов
      // Polymarket для этого потока (см. GammaMarketService.currentIntervalStartTimestampSec
      // — та же формула), так что каждая закрытая свеча ATR — это буквально
      // диапазон цены за одно из прошлых окон этого актива/таймфрейма.
      const candleMs = Math.max(minCandleMs, stream.intervalSec * 1000);
      const atrCandles = stream.atrCandles ?? this.atrCandles;
      this.states.set(stream.streamKey, {
        streamKey: stream.streamKey,
        ticker,
        candleMs,
        atrCandles,
        lastPrice: null,
        lastPriceAt: null,
        lastPriceSource: null,
        current: null,
        closed: [],
      });
    }
  }

  async onModuleInit(): Promise<void> {
    // Бэкафилл истории свечей ПЕРЕД подключением по WS (см. CandleHistoryService
    // и CONTEXT.md, Сессия 6 п.6) — без этого прогрев ATR-гейта после каждого
    // рестарта процесса начинался бы с нуля (до 20 часов для часового потока).
    // Тикеры общие для нескольких потоков (напр. все BTC-потоки), но у каждого
    // потока свой candleMs/atrCandles — бэкафилл делаем на уровне ПОТОКА, не тикера.
    await Promise.all(
      [...this.states.values()].map(async (state) => {
        try {
          const history = await this.candleHistory.bootstrap(state.ticker, state.candleMs, state.atrCandles * 2);
          state.closed = history;
          if (history.length >= state.atrCandles) {
            this.logger.log(
              `PriceFeedService: [${state.streamKey}] ATR прогрет из кеша сразу при старте (${history.length}/${state.atrCandles} свечей).`,
            );
          } else if (history.length > 0) {
            this.logger.log(
              `PriceFeedService: [${state.streamKey}] частичный прогрев из кеша (${history.length}/${state.atrCandles}) — остаток догонит живой фид.`,
            );
          }
        } catch (err) {
          this.logger.warn(`PriceFeedService: бэкафилл истории для [${state.streamKey}] не удался: ${this.errMsg(err)}`);
        }
      }),
    );
    this.connect();
  }

  onModuleDestroy(): void {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.ws?.close();
  }

  /** Текущая цена + ATR по фиду для данного streamKey (btc-updown-5m и т.п.). */
  getSnapshot(streamKey: string): FeedSnapshot {
    const state = this.states.get(streamKey);
    if (!state) return { price: null, priceAt: null, atr: null, candleCount: 0, source: null };

    // Протухший тик (см. дев-разбор "инцидент Chainlink vs Binance": стейл
    // фид у самого резолва — красный флаг, не данные) считаем недоступным —
    // тем самым при включённом ENTRY_FILTER_ENABLED это уйдёт в fail-closed
    // ровно так же, как полное отсутствие тиков.
    const isStale = state.lastPriceAt != null && Date.now() - state.lastPriceAt > this.staleMs;

    return {
      price: isStale ? null : state.lastPrice,
      priceAt: isStale ? null : state.lastPriceAt,
      atr: this.computeAtr(state),
      candleCount: state.closed.length,
      source: isStale ? null : state.lastPriceSource,
    };
  }

  private computeAtr(state: StreamFeedState): number | null {
    if (state.closed.length < state.atrCandles) return null;
    const sample = state.closed.slice(-state.atrCandles);
    const sum = sample.reduce((acc, c) => acc + Math.abs(c.high - c.low), 0);
    return sum / sample.length;
  }

  private deriveTicker(streamKey: string): string {
    // "btc-updown-5m" -> "btc". Для нестандартных активов (напр. часовой
    // "bitcoin-up-or-down") задай явный маппинг через FEED_SYMBOL_OVERRIDES.
    return streamKey.split('-')[0]?.toLowerCase() ?? streamKey.toLowerCase();
  }

  private parseOverrides(raw: string): Map<string, string> {
    const map = new Map<string, string>();
    for (const pair of raw.split(',')) {
      const [streamKey, ticker] = pair.split(':').map((s) => s.trim());
      if (streamKey && ticker) map.set(streamKey, ticker.toLowerCase());
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
    const tickers = [...this.tickerToStreams.keys()];
    if (tickers.length === 0) {
      this.logger.warn('PriceFeedService: нет активов для подключения — фид не запущен.');
      return;
    }

    const provider = this.providers[this.providerIndex];
    const url = provider.buildUrl(tickers);
    const wsOptions = this.buildAgent();
    this.ws = new WebSocket(url, wsOptions as any);

    this.ws.on('open', () => {
      this.reconnectAttempts = 0;
      this.consecutiveFailuresOnProvider = 0;
      const openMsg = provider.onOpenMessage?.(tickers);
      if (openMsg) this.ws?.send(openMsg);

      if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
      if (provider.heartbeatIntervalMs && provider.heartbeatMessage) {
        this.heartbeatTimer = setInterval(() => {
          if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(provider.heartbeatMessage!);
        }, provider.heartbeatIntervalMs);
      }

      this.logger.log(
        `PriceFeedService: подключен к ${provider.name} (${tickers.join(', ')})${this.proxyUrl ? ' через прокси' : ''}.`,
      );
    });

    this.ws.on('message', (raw: WebSocket.RawData) => {
      try {
        // Chainlink RTDS иногда присылает первым фреймом пустую строку
        // (не JSON) — без этой проверки JSON.parse валился на КАЖДОМ
        // подключении и всё уходило в debug-лог как "не удалось разобрать".
        const message = raw.toString();
        if (!message.trim()) return;

        const trade = provider.parseMessage(JSON.parse(message));
        if (trade) this.applyTrade(provider.name, trade);
      } catch (err) {
        this.logger.debug(`PriceFeedService: не удалось разобрать сообщение (${provider.name}): ${this.errMsg(err)}`);
      }
    });

    this.ws.on('close', () => this.handleDisconnect(provider));
    this.ws.on('error', (err) => {
      this.logger.warn(`PriceFeedService: ошибка WS (${provider.name}): ${this.errMsg(err)}`);
    });
  }

  private handleDisconnect(provider: ProviderAdapter): void {
    if (this.stopped) return;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.consecutiveFailuresOnProvider += 1;

    if (this.consecutiveFailuresOnProvider >= this.failThreshold && this.providers.length > 1) {
      const next = (this.providerIndex + 1) % this.providers.length;
      this.logger.warn(
        `PriceFeedService: ${provider.name} не отвечает уже ${this.consecutiveFailuresOnProvider} подключений подряд ` +
        `(похоже на гео-блокировку по IP хостинга или сбой сервиса, не на временный сбой сети) — ` +
        `переключаюсь на ${this.providers[next].name}.`,
      );
      this.providerIndex = next;
      this.consecutiveFailuresOnProvider = 0;
      this.reconnectAttempts = 0;
    }

    this.scheduleReconnect();
  }

  private applyTrade(source: string, trade: NormalizedTrade): void {
    const streamKeys = this.tickerToStreams.get(trade.ticker);
    if (!streamKeys) return;

    for (const streamKey of streamKeys) {
      const state = this.states.get(streamKey);
      if (!state) continue;

      state.lastPrice = trade.price;
      state.lastPriceAt = trade.ts;
      state.lastPriceSource = source;

      const bucketStart = Math.floor(trade.ts / state.candleMs) * state.candleMs;
      if (!state.current || state.current.start !== bucketStart) {
        if (state.current) {
          state.closed.push(state.current);
          if (state.closed.length > state.atrCandles * 2) {
            state.closed.splice(0, state.closed.length - state.atrCandles * 2);
          }
          // Персистим закрытую свечу в БД (кеш под ATR, см. CandleHistoryService) —
          // best-effort, не блокирует горячий путь фида.
          void this.candleHistory.saveClosedCandle(state.ticker, state.candleMs, state.current, source);
        }
        state.current = { start: bucketStart, open: trade.price, high: trade.price, low: trade.price, close: trade.price };
      } else {
        state.current.close = trade.price;
        state.current.high = Math.max(state.current.high, trade.price);
        state.current.low = Math.min(state.current.low, trade.price);
      }
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
