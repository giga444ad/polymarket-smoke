/* Ad-hoc verification script — not part of the app. */
import 'reflect-metadata';
import { TradingService } from '../src/trading/trading.service';

const writes: any[] = [];

const fakeConfig = {
  overrides: {
    SMOKE_START: 'true',
    BET_AMOUNT: '1',
    MIN_MARKET_PRICE: '0.99',
    MAX_MARKET_PRICE: '0.999',
    FAVORITE_BID_THRESHOLD: '0.90',
    LIMIT_TIER1_PRICE: '0.99',
    LIMIT_TIER2_PRICE: '0.995',
    LIMIT_TIER3_PRICE: '0.999',
    LIMIT_TIER2_SECONDS: '150',
    LIMIT_TIER3_SECONDS: '60',
    MAX_OVERSPEND_MULTIPLIER: '1.5',
  } as Record<string, string>,
  get(key: string, def?: string) {
    return this.overrides[key] ?? def;
  },
};

const fakeGamma = {
  currentIntervalCloseTimestampSec: () => 1000,
  buildSlugForClose: (ts: number) => `btc-updown-5m-${ts}`,
  fetchMarketBySlug: async () => null,
  fetchOutcome: async () => null,
};

const fakeClobPublic = { getBestQuote: async () => null };

const fakeTrader = {
  ensureClient: async () => {
    throw new Error('boom');
  },
  placeMarketBuy: async () => {
    throw new Error('SMOKE НЕ ДОЛЖЕН звать реальную биржу!');
  },
  placeLimitBuy: async () => {
    throw new Error('SMOKE НЕ ДОЛЖЕН звать реальную биржу!');
  },
  cancelOrder: async () => {},
  getOrderStatus: async () => null,
};

const fakeAttemptRepo = {
  findOne: async () => null,
  create: (x: any) => x,
  save: async (x: any) => ({ ...x, id: 'attempt-1' }),
  findOneOrFail: async () => ({ id: 'attempt-1', status: 'active', currentStep: 0, targetSteps: 500, isSmoke: true }),
};

const fakeMarketLogRepo = {
  create: (x: any) => x,
  save: async (x: any) => {
    writes.push(x);
    return x;
  },
  find: async () => [],
};

function makeMarketState(overrides: any = {}) {
  return {
    slug: 'btc-updown-5m-1000',
    closesAt: new Date(Date.now() + 300_000),
    yesTokenId: 'YES_TOKEN',
    noTokenId: 'NO_TOKEN',
    negRisk: false,
    minOrderSize: 1,
    stream: { close: () => {} },
    quotes: {
      YES: { bestBid: null, bestAsk: null, tickSize: '0.01' },
      NO: { bestBid: null, bestAsk: null, tickSize: '0.01' },
    },
    positioned: false,
    finalized: false,
    logWritten: false,
    restingOrder: null,
    lastMarketAttemptAt: 0,
    closeTimer: setTimeout(() => {}, 999_999),
    ...overrides,
  };
}

async function main() {
  const svc: any = new TradingService(
    fakeConfig as any,
    fakeGamma as any,
    fakeClobPublic as any,
    fakeTrader as any,
    fakeAttemptRepo as any,
    fakeMarketLogRepo as any,
  );
  svc.currentAttempt = { id: 'attempt-1', currentStep: 0, targetSteps: 500 };

  // --- Тест 1: pickFavorite / computeTier / roundToTick — чистая логика ---
  console.log('pickFavorite(YES=0.6,NO=0.3) =', svc.pickFavorite({ YES: { bestBid: 0.6 }, NO: { bestBid: 0.3 } }));
  console.log('computeTier(200s) =', svc.computeTier(200), '(ожидаем T1)');
  console.log('computeTier(100s) =', svc.computeTier(100), '(ожидаем T2)');
  console.log('computeTier(30s)  =', svc.computeTier(30), '(ожидаем T3)');
  console.log('roundToTick(0.995, "0.01") =', svc.roundToTick(0.995, '0.01'), '(ожидаем 0.99 — клэмп на грубую сетку)');
  console.log('roundToTick(0.995, "0.001") =', svc.roundToTick(0.995, '0.001'), '(ожидаем 0.995)');
  console.log('roundToTick(0.999, "0.001") =', svc.roundToTick(0.999, '0.001'), '(ожидаем 0.999, не выше max=1-tick)');

  // --- Тест 2: Правило A — ask входит в [0.99, 0.999] -> должен создаться SIMULATED_MARKET лог ---
  {
    const ms = makeMarketState();
    ms.quotes.YES = { bestBid: 0.97, bestAsk: 0.992, tickSize: '0.01' };
    svc.onQuoteUpdate(ms, 'YES', ms.quotes.YES);
    await sleep(50);
    console.log('\n[Тест 2] Правило A сработало:', writes.length === 1 && writes[0].orderType === 'SIMULATED_MARKET' && writes[0].status === 'pending_resolve');
    console.log('  positioned =', ms.positioned, ' entryPrice =', writes[0]?.entryPrice);
  }
  writes.length = 0;

  // --- Тест 3: ask выше потолка (1.0 невозможна, возьмём 0.9995 > 0.999) -> Правило A НЕ должно сработать ---
  {
    const ms = makeMarketState();
    ms.quotes.YES = { bestBid: 0.999, bestAsk: 0.9998, tickSize: '0.0001' };
    svc.onQuoteUpdate(ms, 'YES', ms.quotes.YES);
    await sleep(50);
    console.log('[Тест 3] Правило A НЕ сработало на цене выше потолка:', writes.length === 0 && !ms.positioned);
  }

  // --- Тест 4: нет предложений (ask=null), но bid фаворита высокий -> должна встать лимитка T1 ---
  {
    const ms = makeMarketState({ closesAt: new Date(Date.now() + 280_000) }); // >150с до конца
    ms.quotes.YES = { bestBid: 0.95, bestAsk: null, tickSize: '0.01' };
    svc.onQuoteUpdate(ms, 'YES', ms.quotes.YES);
    await sleep(50);
    console.log('\n[Тест 4] Лимитка T1 выставлена (смоук, ничего не пишем в БД пока не исполнится):', ms.restingOrder?.tier === 'T1' && ms.restingOrder?.price === 0.99);

    // Теперь имитируем, что чужой ask опустился до нашей цены -> должна "исполниться"
    ms.quotes.YES = { bestBid: 0.95, bestAsk: 0.99, tickSize: '0.01' };
    svc.onQuoteUpdate(ms, 'YES', ms.quotes.YES);
    await sleep(50);
    console.log('[Тест 4] Лимитка T1 "исполнилась" при встречном ask=0.99:', writes.length === 1 && writes[0].orderType === 'SIMULATED_LIMIT' && writes[0].limitTier === 'T1' && ms.positioned);
  }
  writes.length = 0;

  // --- Тест 5: смена тиров по времени (T1 -> T2 -> T3) без исполнения, финализация как 'unfilled' ---
  {
    const ms = makeMarketState({ closesAt: new Date(Date.now() + 200_000) }); // 200с > tier2Seconds(150)
    ms.quotes.NO = { bestBid: 0.93, bestAsk: null, tickSize: '0.001' };
    svc.onQuoteUpdate(ms, 'NO', ms.quotes.NO);
    await sleep(20);
    const tier1ok = ms.restingOrder?.tier === 'T1';

    ms.closesAt = new Date(Date.now() + 100_000); // теперь 100с -> должен перейти в T2
    svc.onQuoteUpdate(ms, 'NO', ms.quotes.NO);
    await sleep(20);
    const tier2ok = ms.restingOrder?.tier === 'T2' && ms.restingOrder?.price === 0.995;

    ms.closesAt = new Date(Date.now() + 30_000); // 30с -> T3
    svc.onQuoteUpdate(ms, 'NO', ms.quotes.NO);
    await sleep(20);
    const tier3ok = ms.restingOrder?.tier === 'T3' && ms.restingOrder?.price === 0.999;

    console.log('\n[Тест 5] Переходы тиров T1->T2->T3:', tier1ok, tier2ok, tier3ok);

    // Финализация без исполнения -> статус 'unfilled', ровно один лог
    await svc.finalizeMarket(ms);
    console.log('[Тест 5] Финализация без исполнения -> unfilled, один лог:', writes.length === 1 && writes[0].status === 'unfilled' && writes[0].limitTier === 'T3');

    // Повторный вызов finalizeMarket не должен писать второй раз
    await svc.finalizeMarket(ms);
    console.log('[Тест 5] Повторный finalizeMarket не дублирует лог:', writes.length === 1);
  }
  writes.length = 0;

  // --- Тест 6: минимальный размер ордера биржи блокирует слишком дешёвую лимитку при маленьком BET_AMOUNT ---
  {
    const ms = makeMarketState({ minOrderSize: 1000, closesAt: new Date(Date.now() + 280_000) });
    ms.quotes.YES = { bestBid: 0.95, bestAsk: null, tickSize: '0.01' };
    svc.onQuoteUpdate(ms, 'YES', ms.quotes.YES);
    await sleep(50);
    console.log('\n[Тест 6] Слишком высокий minOrderSize блокирует лимитку (перерасход выше лимита):', ms.restingOrder === null);
  }

  console.log('\nВСЕ ПРОВЕРКИ ВЫПОЛНЕНЫ.');
  process.exit(0);
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

main().catch((err) => {
  console.error('ОШИБКА ТЕСТА:', err);
  process.exit(1);
});
