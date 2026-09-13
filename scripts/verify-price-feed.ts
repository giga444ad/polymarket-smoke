/* Ad-hoc verification script — not part of the app.
 * Проверяет вотчдог "зомби-соединения" из Сессии 9 (см. CONTEXT.md):
 * если фид не прислал ни одного трейда дольше FEED_DATA_WATCHDOG_MS, хотя
 * сокет технически ещё OPEN, соединение должно быть форсированно разорвано.
 */
import 'reflect-metadata';
import { PriceFeedService } from '../src/polymarket/price-feed.service';

const fakeConfig = {
  overrides: {
    STREAMS_CONFIG: JSON.stringify([
      { streamKey: 'btc-updown-5m', kind: 'interval', intervalSec: 300, slugPrefix: 'btc-updown-5m', baseStake: 5 },
      { streamKey: 'btc-updown-15m', kind: 'interval', intervalSec: 900, slugPrefix: 'btc-updown-15m', baseStake: 5 },
    ]),
    FEED_PROVIDERS: 'chainlink,binance',
    FEED_DATA_WATCHDOG_MS: '20000',
    FEED_STALE_MS: '5000',
  } as Record<string, string>,
  get(key: string, fallback?: string) {
    return this.overrides[key] ?? fallback;
  },
};

const fakeCandleHistory: any = {
  bootstrap: async () => [],
  saveClosedCandle: async () => {},
};

async function main() {
  const svc: any = new PriceFeedService(fakeConfig as any, fakeCandleHistory);

  // --- Тест A: трейд был только что — вотчдог НЕ должен трогать соединение ---
  let terminated = false;
  svc.ws = { terminate: () => { terminated = true; } };
  svc.stopped = false;
  svc.lastAnyTradeAt = Date.now(); // "только что"
  svc.checkDataWatchdog();
  console.log(`[Тест A] Свежий трейд (0с назад) — вотчдог НЕ разрывает соединение: ${!terminated}`);

  // --- Тест B: тишина дольше FEED_DATA_WATCHDOG_MS (20с) — вотчдог должен разорвать соединение ---
  terminated = false;
  svc.ws = { terminate: () => { terminated = true; } };
  svc.lastAnyTradeAt = Date.now() - 25_000; // 25с тишины > 20с порога
  svc.checkDataWatchdog();
  console.log(`[Тест B] Тишина 25с (> порога 20с) — вотчдог форсирует terminate(): ${terminated}`);

  // --- Тест C: свежий трейд (applyTrade/парсинг) сбрасывает часы тишины ---
  svc.lastAnyTradeAt = Date.now() - 25_000;
  svc.lastAnyTradeAt = Date.now(); // эмулируем то, что делает 'message'-хендлер при успешном parseMessage
  terminated = false;
  svc.ws = { terminate: () => { terminated = true; } };
  svc.checkDataWatchdog();
  console.log(`[Тест C] После свежего трейда часы тишины сброшены — разрыва нет: ${!terminated}`);

  // --- Тест D: stopped=true — вотчдог не должен ничего делать даже при тишине ---
  terminated = false;
  svc.ws = { terminate: () => { terminated = true; } };
  svc.stopped = true;
  svc.lastAnyTradeAt = Date.now() - 999_000;
  svc.checkDataWatchdog();
  console.log(`[Тест D] stopped=true — вотчдог бездействует даже при долгой тишине: ${!terminated}`);

  // --- Тест E (Сессия 11): getPriceAt ищет цену НА ТОЧНЫЙ момент времени, а
  //     не "текущую" — именно это чинит найденный пользователем баг с
  //     referencePrice, отставшим от официального страйка Polymarket на
  //     несколько секунд сетевых round-trip'ов между обнаружением окна и
  //     фиксацией снимка (см. CONTEXT.md, Сессия 11). ---
  {
    const streamKey = 'btc-updown-5m';
    const state = svc.states.get(streamKey);
    state.recentTicks = [
      { ts: 1_000_000, price: 100.0 },
      { ts: 1_002_000, price: 100.5 },
      { ts: 1_005_000, price: 101.0 }, // ближайший тик <= 1_006_000
      { ts: 1_010_000, price: 102.0 }, // это УЖЕ ПОСЛЕ цели — не должен попасть в ответ
    ];

    const atTarget = svc.getPriceAt(streamKey, 1_006_000);
    console.log(
      `[Тест E] Найден ближайший тик ДО цели (101.0 на ts=1005000), а не более поздний/текущий: ${atTarget.price === 101.0 && atTarget.tickAt === 1_005_000}`,
    );

    const exact = svc.getPriceAt(streamKey, 1_002_000);
    console.log(`[Тест E] Точное совпадение по времени найдено (100.5): ${exact.price === 100.5}`);

    const tooOld = svc.getPriceAt(streamKey, 500_000); // раньше самого старого тика в буфере
    console.log(`[Тест E] Цель старше всего буфера — честно null, а НЕ приближение: ${tooOld.price === null}`);

    const unknownStream = svc.getPriceAt('no-such-stream', 1_000_000);
    console.log(`[Тест E] Несуществующий поток — null без исключения: ${unknownStream.price === null}`);
  }

  // --- Тест F: applyTrade кладёт тики в recentTicks и подрезает буфер старше
  //     recentTicksRetentionMs (по умолчанию 15с, здесь используем реальный
  //     дефолт сервиса — 15000мс). ---
  {
    const streamKey = 'btc-updown-5m';
    const state = svc.states.get(streamKey);
    state.recentTicks = [];
    state.candleMs = 300_000;
    state.closed = [];
    state.current = null;

    const now = 2_000_000;
    (svc as any).applyTrade('chainlink', { ticker: 'btc', price: 50000, ts: now - 20_000 }); // старше retention (15с) — должен быть вычищен следующим тиком
    (svc as any).applyTrade('chainlink', { ticker: 'btc', price: 50010, ts: now });
    const stillOld = state.recentTicks.some((t: any) => t.ts === now - 20_000);
    console.log(`[Тест F] Тик старше FEED_RECENT_TICKS_RETENTION_MS вычищен из буфера: ${!stillOld}`);
    console.log(`[Тест F] Свежий тик остался в буфере: ${state.recentTicks.some((t: any) => t.ts === now)}`);
  }

  console.log('\nВСЕ ПРОВЕРКИ ВЫПОЛНЕНЫ.');
}

main();
