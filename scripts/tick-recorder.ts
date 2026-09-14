/**
 * Сессия 15 (см. CONTEXT.md) — ПАРАЛЛЕЛЬНЫЙ (независимый) от основного
 * торгового воркера процесс: слушает те же публичные источники цены
 * (chainlink/binance/bybit — см. price-feed.service.ts) и книгу Polymarket
 * по активным сейчас окнам, пишет всё в БД с секундной/полусекундной
 * точностью — сырьё для будущего модуля бэктеста (см. BACKTEST-PLAN.md).
 *
 * ПОЧЕМУ ОТДЕЛЬНЫЙ ПРОЦЕСС, А НЕ ЧАСТЬ TradingService:
 *   - основной воркер не должен ничего терять/тормозить из-за лишней
 *     нагрузки на запись — если этот процесс упадёт, зависнет на записи в
 *     БД или получит бан по рейт-лимиту, торговля продолжится как ни в чём
 *     не бывало (единственная связь — общая БД, и то в одну сторону: этот
 *     процесс ЧИТАЕТ ActiveWindow, основной воркер только ПИШЕТ туда);
 *   - не плодим лишнюю нагрузку ВНУТРИ горячего пути onBookUpdate/applyTrade
 *     основного воркера — там как обрабатывалось, так и обрабатывается.
 *
 * ОТКУДА УЗНАЁМ, ЗА КАКИМ ТОКЕНОМ POLYMARKET СЕЙЧАС СЛЕДИТЬ:
 *   Таблица ActiveWindow (см. entities/active-window.entity.ts) — основной
 *   воркер пишет туда результат СВОЕГО дискавери по Gamma API при открытии
 *   окна и чистит при закрытии. Мы НЕ повторяем дискавери сами — это и
 *   дешевле (не удваиваем внешний трафик на Polymarket API), и надёжнее
 *   (один источник правды о том, что сейчас открыто, а не два независимых
 *   которые могут разойтись на границе окна).
 *
 * Использование:
 *   npm run ticks:record
 *   (или npx tsx scripts/tick-recorder.ts)
 *
 * Конфигурация (все опциональны, значения по умолчанию — см. ниже):
 *   TICK_RECORDER_PROVIDERS      — через запятую, какие источники цены слушать
 *                                  (по умолчанию 'chainlink,binance,bybit' —
 *                                  ВСЕ сразу, а не один активный с фолбэком,
 *                                  как в живом боте: для бэктеста выгоднее
 *                                  иметь все три и сравнивать, а не терять
 *                                  историю фолбэк-источников)
 *   TICK_RECORDER_PM_SNAPSHOT_MS — как часто снимать книгу Polymarket (500)
 *   TICK_RECORDER_WINDOW_POLL_MS — как часто перечитывать ActiveWindow (1500)
 *   TICK_RECORDER_FLUSH_MS       — как часто сбрасывать буфер тиков в БД (1000)
 *   FEED_SYMBOL_OVERRIDES, FEED_PROXY_URL — те же смыслы, что и у PriceFeedService
 *
 * Таблицы (TYPEORM_SYNC создаёт их сам, если ещё не существуют):
 *   price_ticks, polymarket_price_ticks, active_windows (последняя уже
 *   создаётся основным приложением — этот процесс её только читает).
 */
import 'dotenv/config';
import { DataSource, Repository } from 'typeorm';
import WebSocket from 'ws';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { SocksProxyAgent } from 'socks-proxy-agent';

import { parseStreamsConfig } from '../src/trading/stream-config';
import { PROVIDERS, ProviderAdapter, NormalizedTrade } from '../src/polymarket/price-feed.service';
import { MarketWsStream } from '../src/polymarket/market-ws-stream';
import { ActiveWindow } from '../src/entities/active-window.entity';
import { PriceTick } from '../src/entities/price-tick.entity';
import { PolymarketPriceTick } from '../src/entities/polymarket-price-tick.entity';

// ---------------------------------------------------------------------
// Конфигурация
// ---------------------------------------------------------------------
const PROVIDER_NAMES = (process.env.TICK_RECORDER_PROVIDERS ?? 'chainlink,binance,bybit')
  .split(',')
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);
const PM_SNAPSHOT_MS = parseInt(process.env.TICK_RECORDER_PM_SNAPSHOT_MS ?? '500', 10);
const WINDOW_POLL_MS = parseInt(process.env.TICK_RECORDER_WINDOW_POLL_MS ?? '1500', 10);
const FLUSH_MS = parseInt(process.env.TICK_RECORDER_FLUSH_MS ?? '1000', 10);
const PROXY_URL = process.env.FEED_PROXY_URL ?? '';

function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function log(msg: string): void {
  // eslint-disable-next-line no-console
  console.log(`[${new Date().toISOString()}] [tick-recorder] ${msg}`);
}

function deriveTicker(streamKey: string, overrides: Map<string, string>): string {
  return overrides.get(streamKey) ?? streamKey.split('-')[0]?.toLowerCase() ?? streamKey.toLowerCase();
}

function parseOverrides(raw: string): Map<string, string> {
  const map = new Map<string, string>();
  for (const pair of raw.split(',')) {
    const [streamKey, ticker] = pair.split(':').map((s) => s.trim());
    if (streamKey && ticker) map.set(streamKey, ticker.toLowerCase());
  }
  return map;
}

function buildAgent(): { agent: any } | Record<string, never> {
  if (!PROXY_URL) return {};
  try {
    const agent = PROXY_URL.startsWith('socks') ? new SocksProxyAgent(PROXY_URL) : new HttpsProxyAgent(PROXY_URL);
    return { agent };
  } catch (err) {
    log(`FEED_PROXY_URL некорректен, подключаюсь напрямую: ${errMsg(err)}`);
    return {};
  }
}

// ---------------------------------------------------------------------
// Буферизованная запись — копим в памяти, сбрасываем пачкой раз в FLUSH_MS,
// чтобы не долбить БД инсертом на КАЖДЫЙ отдельный тик (binance/bybit могут
// слать по несколько трейдов в секунду на активный тикер).
// ---------------------------------------------------------------------
export class BufferedWriter<T extends object> {
  private buffer: T[] = [];
  constructor(
    private readonly repo: Repository<T>,
    private readonly label: string,
  ) {}

  push(row: T): void {
    this.buffer.push(row);
  }

  async flush(): Promise<void> {
    if (this.buffer.length === 0) return;
    const batch = this.buffer;
    this.buffer = [];
    try {
      await this.repo.insert(batch as any);
    } catch (err) {
      // Не теряем данные молча, но и не роняем процесс — просто громко
      // предупреждаем и ВОЗВРАЩАЕМ батч в буфер, чтобы попробовать на
      // следующем flush (напр. БД была недоступна секунду).
      this.buffer.unshift(...batch);
      log(`ОШИБКА записи ${this.label} (${batch.length} строк, вернул в буфер): ${errMsg(err)}`);
    }
  }
}

// ---------------------------------------------------------------------
// Часть A: сырые тики chainlink/binance/bybit — независимо от Polymarket.
// ---------------------------------------------------------------------
function startAssetProviderRecorder(provider: ProviderAdapter, tickers: string[], writer: BufferedWriter<PriceTick>): void {
  let ws: WebSocket | null = null;
  let heartbeatTimer: NodeJS.Timeout | null = null;
  let reconnectAttempts = 0;
  let stopped = false;

  function connect() {
    if (stopped) return;
    const url = provider.buildUrl(tickers);
    ws = new WebSocket(url, buildAgent() as any);

    ws.on('open', () => {
      reconnectAttempts = 0;
      const openMsg = provider.onOpenMessage?.(tickers);
      if (openMsg) ws?.send(openMsg);
      if (heartbeatTimer) clearInterval(heartbeatTimer);
      if (provider.heartbeatIntervalMs && provider.heartbeatMessage) {
        heartbeatTimer = setInterval(() => {
          if (ws?.readyState === WebSocket.OPEN) ws.send(provider.heartbeatMessage!);
        }, provider.heartbeatIntervalMs);
      }
      log(`${provider.name}: подключен (${tickers.join(', ')})${PROXY_URL ? ' через прокси' : ''}.`);
    });

    ws.on('message', (raw: WebSocket.RawData) => {
      try {
        const message = raw.toString();
        if (!message.trim()) return;
        const trade: NormalizedTrade | null = provider.parseMessage(JSON.parse(message));
        if (trade) {
          writer.push({ ticker: trade.ticker, ts: trade.ts, price: trade.price, source: provider.name } as PriceTick);
        }
      } catch {
        // Не JSON / не относящееся к трейду сообщение — молча игнорируем,
        // как и в price-feed.service.ts (это ожидаемо, не ошибка).
      }
    });

    ws.on('close', scheduleReconnect);
    ws.on('error', (err) => log(`${provider.name}: ошибка WS: ${errMsg(err)}`));
  }

  function scheduleReconnect() {
    if (stopped) return;
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    reconnectAttempts += 1;
    const delayMs = Math.min(30_000, 1000 * 2 ** Math.min(reconnectAttempts, 5));
    log(`${provider.name}: соединение закрыто, переподключение через ${delayMs}мс.`);
    setTimeout(connect, delayMs);
  }

  connect();
}

// ---------------------------------------------------------------------
// Часть B: снимки книги Polymarket по активным сейчас окнам (ActiveWindow).
// ---------------------------------------------------------------------
interface TrackedWindow {
  slug: string;
  stream: MarketWsStream;
  snapshotTimer: NodeJS.Timeout;
}

// Вынесено в чистую функцию (без побочных эффектов) специально для
// юнит-тестов (см. scripts/verify-tick-recorder.ts) — вся ветвистая логика
// "что перестать слушать / что начать слушать" проверяется без реальных
// WS-соединений и таймеров.
export function diffActiveWindows(
  trackedStreamKeys: Map<string, string>, // streamKey -> текущий отслеживаемый slug
  rows: ActiveWindow[],
  nowMs: number,
): { toStop: string[]; toStart: ActiveWindow[] } {
  const byStream = new Map(rows.map((r) => [r.streamKey, r]));
  const toStop: string[] = [];
  for (const [streamKey, trackedSlug] of trackedStreamKeys) {
    const row = byStream.get(streamKey);
    // Перестаём слушать, если: указателя больше нет (окно закрылось и
    // основной воркер его вычистил), слаг сменился (новое окно того же
    // потока успело открыться раньше, чем мы опросили) или closesAtMs уже
    // в прошлом (страховка на случай, если clearActiveWindow не дошёл).
    if (!row || row.slug !== trackedSlug || row.closesAtMs <= nowMs) {
      toStop.push(streamKey);
    }
  }
  const toStart: ActiveWindow[] = [];
  for (const row of rows) {
    if (row.closesAtMs <= nowMs) continue; // уже закрылось, не успели вычистить — пропускаем
    if (!trackedStreamKeys.has(row.streamKey) || toStop.includes(row.streamKey)) {
      toStart.push(row);
    }
  }
  return { toStop, toStart };
}

function startPolymarketRecorder(activeWindowRepo: Repository<ActiveWindow>, writer: BufferedWriter<PolymarketPriceTick>): void {
  const tracked = new Map<string, TrackedWindow>(); // streamKey -> ...

  function stopTracking(streamKey: string) {
    const t = tracked.get(streamKey);
    if (!t) return;
    clearInterval(t.snapshotTimer);
    t.stream.close();
    tracked.delete(streamKey);
    log(`${streamKey}: перестал слушать книгу (${t.slug}) — окно закрыто/сменилось.`);
  }

  function startTracking(row: ActiveWindow) {
    const stream = new MarketWsStream(row.yesTokenId, row.noTokenId, row.tickSize, () => {
      // Снимок берём по таймеру ниже через getBook (see below), а не на
      // каждое обновление — так частота записи предсказуема (ровно
      // PM_SNAPSHOT_MS) и не зависит от того, насколько "шумная" книга.
    });
    stream.connect();
    const snapshotTimer = setInterval(() => {
      const yesBook = stream.getBook(row.yesTokenId);
      const noBook = stream.getBook(row.noTokenId);
      writer.push({
        streamKey: row.streamKey,
        slug: row.slug,
        ts: Date.now(),
        yesBestBid: yesBook?.bestBid ?? null,
        yesBestAsk: yesBook?.bestAsk ?? null,
        noBestBid: noBook?.bestBid ?? null,
        noBestAsk: noBook?.bestAsk ?? null,
      } as PolymarketPriceTick);
    }, PM_SNAPSHOT_MS);
    tracked.set(row.streamKey, { slug: row.slug, stream, snapshotTimer });
    log(`${row.streamKey}: начал слушать книгу (${row.slug}, YES=${row.yesTokenId.slice(0, 10)}…, NO=${row.noTokenId.slice(0, 10)}…).`);
  }

  async function poll() {
    let rows: ActiveWindow[] = [];
    try {
      rows = await activeWindowRepo.find();
    } catch (err) {
      log(`Не удалось прочитать ActiveWindow: ${errMsg(err)}`);
      return;
    }
    const trackedStreamKeys = new Map([...tracked].map(([k, v]) => [k, v.slug]));
    const { toStop, toStart } = diffActiveWindows(trackedStreamKeys, rows, Date.now());
    for (const streamKey of toStop) stopTracking(streamKey);
    for (const row of toStart) startTracking(row);
  }

  poll();
  setInterval(poll, WINDOW_POLL_MS);
}

// ---------------------------------------------------------------------
// main
// ---------------------------------------------------------------------
async function main() {
  const dataSource = new DataSource({
    type: 'postgres',
    host: process.env.POSTGRES_HOST || 'localhost',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    username: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
    database: process.env.POSTGRES_DB || 'polymarket_bot',
    entities: [ActiveWindow, PriceTick, PolymarketPriceTick],
    // Тот же дев-режим, что и у основного приложения (см. app.module.ts) —
    // создаёт price_ticks/polymarket_price_ticks сам, если их ещё нет.
    // active_windows создаётся ОСНОВНЫМ приложением; если recorder запущен
    // раньше основного воркера на чистой БД, synchronize здесь создаст и её
    // тоже (структура идентична, конфликта нет).
    synchronize: process.env.TYPEORM_SYNC !== 'false',
    logging: process.env.TYPEORM_LOGGING === 'true',
  });
  await dataSource.initialize();
  log('Подключение к БД установлено.');

  const priceTickRepo = dataSource.getRepository(PriceTick);
  const pmTickRepo = dataSource.getRepository(PolymarketPriceTick);
  const activeWindowRepo = dataSource.getRepository(ActiveWindow);

  const priceTickWriter = new BufferedWriter(priceTickRepo, 'price_ticks');
  const pmTickWriter = new BufferedWriter(pmTickRepo, 'polymarket_price_ticks');
  setInterval(() => {
    void priceTickWriter.flush();
    void pmTickWriter.flush();
  }, FLUSH_MS);

  // --- Часть A: тикеры выводим из STREAMS_CONFIG, как и PriceFeedService ---
  const streams = parseStreamsConfig(process.env.STREAMS_CONFIG);
  const overrides = parseOverrides(process.env.FEED_SYMBOL_OVERRIDES ?? '');
  const tickers = [...new Set(streams.map((s) => deriveTicker(s.streamKey, overrides)))];
  if (tickers.length === 0) {
    log('STREAMS_CONFIG пуст или не задан — нечего слушать по ценовому фиду.');
  }
  for (const name of PROVIDER_NAMES) {
    const provider = PROVIDERS[name];
    if (!provider) {
      log(`TICK_RECORDER_PROVIDERS: неизвестный провайдер "${name}" — пропускаю (доступны: ${Object.keys(PROVIDERS).join(', ')}).`);
      continue;
    }
    if (tickers.length > 0) startAssetProviderRecorder(provider, tickers, priceTickWriter);
  }

  // --- Часть B: Polymarket-книга по активным окнам ---
  startPolymarketRecorder(activeWindowRepo, pmTickWriter);

  log(`Запущен. Провайдеры цены: ${PROVIDER_NAMES.join(', ')}. Тикеры: ${tickers.join(', ') || '(нет)'}. Снимок книги каждые ${PM_SNAPSHOT_MS}мс.`);

  const shutdown = async () => {
    log('Останавливаюсь — сбрасываю буферы...');
    await priceTickWriter.flush();
    await pmTickWriter.flush();
    await dataSource.destroy();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

// Guard: этот файл также импортируется тестами (verify-tick-recorder.ts)
// ради diffActiveWindows/BufferedWriter — не должен пытаться поднять
// реальное соединение с БД/WS при простом импорте функций/классов.
if (require.main === module) {
  main().catch((err) => {
    // eslint-disable-next-line no-console
    console.error('tick-recorder: фатальная ошибка при старте:', err);
    process.exit(1);
  });
}
