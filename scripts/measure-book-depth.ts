/**
 * Разведочный замер реальной глубины стакана на ¢99+ (BACKLOG п.2).
 *
 * Не часть приложения — отдельный долгоживущий процесс для сбора статистики
 * ДО того, как полагаться на цифру "500 подряд" как достижимую: на каждом
 * шаге прогрессии реинвеста (п.1) стейк растёт ~×(1/entryPrice) за шаг,
 * то есть на 500 шагах — на несколько порядков от базового. Этот скрипт не
 * решает, где предел — он просто копит историю $ampout доступного на ¢99+
 * по фавориту каждого настроенного потока, чтобы делать вывод по факту, а
 * не на глаз.
 *
 * Использование:
 *   npx tsx scripts/measure-book-depth.ts
 *
 * Конфигурация — те же STREAMS_CONFIG/интервалы, что и у бота (см. .env),
 * плюс:
 *   DEPTH_POLL_MS       — как часто опрашивать стакан (по умолчанию 5000)
 *   DEPTH_LOG_FILE       — куда писать JSONL (по умолчанию ./book-depth-log.jsonl)
 *   DEPTH_MIN_PRICE      — нижняя граница диапазона глубины (по умолчанию 0.99)
 *
 * Формат строки JSONL:
 *   {"ts":"...","streamKey":"btc-updown-5m","slug":"...","timeLeftSec":123,
 *    "favorite":"YES","favoriteBid":0.99,"depthUsdAtOrAbove99":1234.5,
 *    "levelsCount":7}
 *
 * Разбор накопленного лога — вручную (напр. `jq`) или отдельным ad-hoc
 * скриптом; здесь только сбор, без предположений о том, как именно данные
 * будут анализироваться.
 */
import 'dotenv/config';
import * as fs from 'fs';
import axios from 'axios';
import { parseStreamsConfig, StreamDefinition } from '../src/trading/stream-config';

const POLL_MS = parseInt(process.env.DEPTH_POLL_MS ?? '5000', 10);
const LOG_FILE = process.env.DEPTH_LOG_FILE ?? './book-depth-log.jsonl';
const MIN_PRICE = parseFloat(process.env.DEPTH_MIN_PRICE ?? '0.99');

const gammaHttp = axios.create({ baseURL: 'https://gamma-api.polymarket.com', timeout: 5000 });
const clobHttp = axios.create({ baseURL: 'https://clob.polymarket.com', timeout: 3000 });

interface BookLevel {
  price: number;
  size: number;
}

interface TrackedMarket {
  slug: string;
  closesAt: number; // ms
  yesTokenId: string;
  noTokenId: string;
}

const tracked = new Map<string, TrackedMarket>(); // streamKey -> текущий маркет

function currentIntervalStart(intervalSec: number, nowMs = Date.now()): number {
  return Math.floor(Math.floor(nowMs / 1000) / intervalSec) * intervalSec;
}

function buildHourlyEtSlug(etSlugBase: string, startMs: number): string {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    month: 'long',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    hour12: true,
  }).formatToParts(new Date(startMs));
  const get = (t: string) => parts.find((p) => p.type === t)?.value ?? '';
  return `${etSlugBase}-${get('month').toLowerCase()}-${get('day')}-${get('year')}-${get('hour')}${get('dayPeriod').toLowerCase()}-et`;
}

function buildSlug(stream: StreamDefinition, startTs: number): string {
  return stream.kind === 'hourly-et'
    ? buildHourlyEtSlug(stream.etSlugBase!, startTs * 1000)
    : `${stream.slugPrefix}-${startTs}`;
}

function parseJsonArray(value: unknown): string[] | null {
  if (Array.isArray(value)) return value as string[];
  if (typeof value !== 'string') return null;
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

async function fetchMarketBySlug(slug: string, closeTs: number): Promise<TrackedMarket | null> {
  try {
    const { data } = await gammaHttp.get('/events', { params: { slug } });
    if (!Array.isArray(data) || data.length === 0) return null;
    const market = data[0]?.markets?.[0];
    if (!market) return null;
    const clobTokenIds = parseJsonArray(market.clobTokenIds);
    if (!clobTokenIds || clobTokenIds.length < 2) return null;
    return { slug, closesAt: closeTs * 1000, yesTokenId: clobTokenIds[0], noTokenId: clobTokenIds[1] };
  } catch {
    return null;
  }
}

function normalizeLevels(raw: unknown): BookLevel[] {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((l: any) => ({ price: parseFloat(l?.price), size: parseFloat(l?.size) }))
    .filter((l) => Number.isFinite(l.price) && Number.isFinite(l.size) && l.price > 0 && l.size > 0);
}

async function fetchBook(tokenId: string): Promise<{ asks: BookLevel[]; bids: BookLevel[] } | null> {
  try {
    const { data } = await clobHttp.get('/book', { params: { token_id: tokenId } });
    return {
      asks: normalizeLevels(data?.asks).sort((a, b) => a.price - b.price),
      bids: normalizeLevels(data?.bids).sort((a, b) => b.price - a.price),
    };
  } catch {
    return null;
  }
}

function sumUsdAtOrAbove(levels: BookLevel[], minPrice: number): { usd: number; count: number } {
  let usd = 0;
  let count = 0;
  for (const l of levels) {
    if (l.price >= minPrice) {
      usd += l.price * l.size;
      count += 1;
    }
  }
  return { usd, count };
}

function appendLine(obj: unknown): void {
  fs.appendFileSync(LOG_FILE, JSON.stringify(obj) + '\n', 'utf8');
}

async function tick(streams: StreamDefinition[]): Promise<void> {
  for (const stream of streams) {
    const startTs = currentIntervalStart(stream.intervalSec);
    const closeTs = startTs + stream.intervalSec;
    const slug = buildSlug(stream, startTs);

    let mkt = tracked.get(stream.streamKey);
    if (!mkt || mkt.slug !== slug) {
      const fetched = await fetchMarketBySlug(slug, closeTs);
      if (!fetched) continue; // ещё не создан на Gamma
      mkt = fetched;
      tracked.set(stream.streamKey, mkt);
      console.log(`[${stream.streamKey}] отслеживаю новый маркет: ${slug}`);
    }

    const timeLeftSec = Math.round((mkt.closesAt - Date.now()) / 1000);
    if (timeLeftSec <= 0) continue; // finalizeMarket-эквивалента здесь нет — просто ждём следующего тика на новый слаг

    const [yesBook, noBook] = await Promise.all([fetchBook(mkt.yesTokenId), fetchBook(mkt.noTokenId)]);
    if (!yesBook || !noBook) continue;

    // "Фаворит" — та сторона, у которой выше bid (эвристика, совпадающая с
    // TradingService.pickFavorite) — именно её ask-глубину на ¢99+ мы бы
    // пытались купить в реальной торговле.
    const yesBid = yesBook.bids[0]?.price ?? null;
    const noBid = noBook.bids[0]?.price ?? null;
    let favorite: 'YES' | 'NO' | null = null;
    if (yesBid != null && noBid != null) favorite = yesBid >= noBid ? 'YES' : 'NO';
    else if (yesBid != null) favorite = 'YES';
    else if (noBid != null) favorite = 'NO';
    if (!favorite) continue;

    const favoriteBook = favorite === 'YES' ? yesBook : noBook;
    const { usd, count } = sumUsdAtOrAbove(
      favoriteBook.asks.filter((l) => l.price < 1), // askи ровно по 1.0 — уже резолвнутый рынок, не интересно
      MIN_PRICE,
    );

    appendLine({
      ts: new Date().toISOString(),
      streamKey: stream.streamKey,
      slug: mkt.slug,
      timeLeftSec,
      favorite,
      favoriteBid: favorite === 'YES' ? yesBid : noBid,
      depthUsdAtOrAbove: Number(usd.toFixed(2)),
      minPriceThreshold: MIN_PRICE,
      levelsCount: count,
    });
  }
}

async function main() {
  const streams = parseStreamsConfig(process.env.STREAMS_CONFIG);
  console.log(
    `Замер глубины стакана запущен. Потоки: ${streams.map((s) => s.streamKey).join(', ')}. ` +
      `Порог: ¢${(MIN_PRICE * 100).toFixed(1)}+. Лог: ${LOG_FILE}. Опрос каждые ${POLL_MS}мс. Ctrl+C для остановки.`,
  );

  let stopped = false;
  process.on('SIGINT', () => {
    stopped = true;
    console.log('\nОстановка...');
  });

  while (!stopped) {
    try {
      await tick(streams);
    } catch (err) {
      console.error('Сбой тика замера:', err instanceof Error ? err.message : err);
    }
    await new Promise((r) => setTimeout(r, POLL_MS));
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
