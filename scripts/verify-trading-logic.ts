/* Ad-hoc verification script — not part of the app. */
import 'reflect-metadata';
import { TradingService } from '../src/trading/trading.service';

const writes: any[] = [];
const attemptSaves: any[] = [];

const fakeConfig = {
  overrides: {
    SMOKE_START: 'true',
    BET_AMOUNT: '5',
    MIN_MARKET_PRICE: '0.99',
    MAX_MARKET_PRICE: '0.999',
    FAVORITE_BID_THRESHOLD: '0.90',
    LIMIT_TIER1_PRICE: '0.99',
    LIMIT_TIER2_PRICE: '0.995',
    LIMIT_TIER3_PRICE: '0.999',
    LIMIT_TIER2_SECONDS: '150',
    LIMIT_TIER3_SECONDS: '60',
    MAX_OVERSPEND_MULTIPLIER: '1.5',
    MIN_FILL_RATIO: '0.5',
    MARKET_ASSETS: 'btc-updown-5m,eth-updown-5m',
  } as Record<string, string>,
  get(key: string, def?: string) {
    return this.overrides[key] ?? def;
  },
};

const fakeGamma: any = {
  currentIntervalStartTimestampSec: () => 700,
  currentIntervalCloseTimestampSec: () => 1000,
  buildSlugForStart: (assetPrefix: string, ts: number) => `${assetPrefix}-${ts}`,
  fetchMarketBySlug: async () => null,
  fetchOutcome: async () => null as any,
};

const fakeClobPublic = { getBestQuote: async () => null };

const fakePriceFeed: any = {
  getSnapshot: () => ({ price: null, priceAt: null, atr: null, candleCount: 0 }),
};

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
  save: async (x: any) => {
    const saved = { ...x, id: x.id ?? 'attempt-1' };
    attemptSaves.push(saved);
    return saved;
  },
  findOneOrFail: async () => ({ id: 'attempt-1', status: 'active', currentStep: 0, targetSteps: 500, isSmoke: true }),
};

const fakeMarketLogRepo: any = {
  create: (x: any) => x,
  save: async (x: any) => {
    writes.push(x);
    return x;
  },
  find: async () => [] as any[],
};

function makeMarketState(overrides: any = {}) {
  return {
    assetPrefix: 'btc-updown-5m',
    slug: 'btc-updown-5m-1000',
    closesAt: new Date(Date.now() + 300_000),
    yesTokenId: 'YES_TOKEN',
    noTokenId: 'NO_TOKEN',
    negRisk: false,
    minOrderSize: 5,
    stream: { close: () => {} },
    books: {
      YES: { outcome: 'YES', tickSize: '0.01', asks: [], bids: [], bestAsk: null, bestBid: null },
      NO: { outcome: 'NO', tickSize: '0.01', asks: [], bids: [], bestAsk: null, bestBid: null },
    },
    positioned: false,
    finalized: false,
    logWritten: false,
    restingOrder: null,
    skippedLimitTier: null,
    lastMarketAttemptAt: 0,
    closeTimer: setTimeout(() => {}, 999_999),
    referencePrice: null,
    marketLogId: null,
    ...overrides,
  };
}

function book(asks: { price: number; size: number }[], bestBid: number | null, tickSize = '0.01') {
  return {
    outcome: 'YES' as const,
    tickSize,
    asks,
    bids: [],
    bestAsk: asks.length ? asks[0].price : null,
    bestBid,
  };
}

async function main() {
  const svc: any = new TradingService(
    fakeConfig as any,
    fakeGamma as any,
    fakeClobPublic as any,
    fakeTrader as any,
    fakePriceFeed as any,
    fakeAttemptRepo as any,
    fakeMarketLogRepo as any,
  );
  svc.currentAttempt = { id: 'attempt-1', currentStep: 0, targetSteps: 500 };

  console.log('assetPrefixes =', svc.assetPrefixes, '(ожидаем [btc-updown-5m, eth-updown-5m])');

  // --- Тест 1: тонкая заявка (дребезг) — глубина покрывает только 10% ставки -> НЕ покупаем ---
  {
    const ms = makeMarketState();
    const b = book([{ price: 0.99, size: 0.505 }], 0.97); // ~$0.5 из $5
    svc.onBookUpdate(ms, 'YES', b);
    await sleep(50);
    console.log('\n[Тест 1] Дребезг (10% глубины) НЕ куплен:', writes.length === 0 && !ms.positioned);
  }
  writes.length = 0;

  // --- Тест 2: реальная глубина на несколько уровней -> покупаем по VWAP, а не по единственной цене ---
  {
    const ms = makeMarketState();
    const b = book(
      [
        { price: 0.99, size: 3 }, // $2.97
        { price: 0.995, size: 10 }, // остаток $2.03 доберём тут
      ],
      0.97,
    );
    svc.onBookUpdate(ms, 'YES', b);
    await sleep(50);
    const log = writes[0];
    console.log('\n[Тест 2] Куплено по нескольким уровням книги:', writes.length === 1 && ms.positioned);
    console.log('  entryPrice (VWAP, ожидаем строго между 0.99 и 0.995) =', log?.entryPrice);
    console.log('  filledAmount (ожидаем ~5) =', log?.filledAmount, ' fillRatio =', log?.fillRatio);
  }
  writes.length = 0;

  // --- Тест 3: цена выше потолка (0.9998 > 0.999) — Правило A НЕ должно сработать вообще ---
  {
    const ms = makeMarketState();
    const b = book([{ price: 0.9998, size: 100 }], 0.999, '0.0001');
    svc.onBookUpdate(ms, 'YES', b);
    await sleep(50);
    console.log('[Тест 3] Цена выше потолка проигнорирована:', writes.length === 0 && !ms.positioned);
  }

  // --- Тест 4: Правило B — лимитка не исполняется по "касанию", только по накопленному объёму ---
  {
    const ms = makeMarketState({ closesAt: new Date(Date.now() + 280_000) });
    const bNoAsk = { outcome: 'YES' as const, tickSize: '0.01', asks: [], bids: [], bestAsk: null, bestBid: 0.95 };
    svc.onBookUpdate(ms, 'YES', bNoAsk);
    await sleep(30);
    console.log('\n[Тест 4] Лимитка T1 выставлена:', ms.restingOrder?.tier === 'T1' && ms.restingOrder?.price === 0.99);

    // Чуть-чуть предложений появилось, но объёма меньше нашей цели ($5) -> НЕ считаем исполненной
    const thin = book([{ price: 0.99, size: 1 }], 0.95); // $0.99 из $5 нужных
    svc.onBookUpdate(ms, 'YES', thin);
    await sleep(30);
    console.log('[Тест 4] Тонкое касание НЕ считается исполнением лимитки:', writes.length === 0 && !ms.positioned);

    // Теперь объём реально накопился -> считаем исполненной по нашей цене (не по VWAP чужого прохода)
    const deep = book([{ price: 0.98, size: 10 }], 0.95); // $9.8 >= $5 нужных, вся ниже нашей цены 0.99
    svc.onBookUpdate(ms, 'YES', deep);
    await sleep(30);
    console.log('[Тест 4] Достаточный объём -> лимитка исполнена по СВОЕЙ цене 0.99:', writes.length === 1 && writes[0].entryPrice === 0.99 && ms.positioned);
  }
  writes.length = 0;

  // --- Тест 5: антиспам — пропуск тира из-за минимума биржи логируется/проверяется один раз, а не на каждый тик ---
  {
    const ms = makeMarketState({ minOrderSize: 1000, closesAt: new Date(Date.now() + 280_000) });
    const b1 = { outcome: 'YES' as const, tickSize: '0.01', asks: [], bids: [], bestAsk: null, bestBid: 0.95 };
    svc.onBookUpdate(ms, 'YES', b1);
    await sleep(20);
    const skippedAfterFirst = ms.skippedLimitTier;
    // Повторные тики с тем же условием — placeOrReplaceLimit НЕ должен вызываться повторно
    const spy = { calls: 0 };
    const original = svc.placeOrReplaceLimit.bind(svc);
    svc.placeOrReplaceLimit = (...args: any[]) => {
      spy.calls++;
      return original(...args);
    };
    for (let i = 0; i < 20; i++) {
      svc.onBookUpdate(ms, 'YES', { ...b1, bestBid: 0.95 + i * 0.0001 });
    }
    await sleep(50);
    console.log('\n[Тест 5] Антиспам: тир помечен skippedLimitTier =', skippedAfterFirst, '; повторных вызовов placeOrReplaceLimit за 20 тиков =', spy.calls, '(ожидаем 0)');
  }

  // --- Тест 6: резолвер честно считает профит (выигрыш/проигрыш) от filledAmount, а не от заявленной ставки ---
  {
    const log: any = {
      id: 'log-1',
      slug: 'btc-updown-5m-1000',
      chosenOutcome: 'YES',
      entryPrice: 0.99,
      betAmount: 5,
      filledAmount: 4.5, // частичное исполнение
      assetPrefix: 'btc-updown-5m',
      attemptId: 'attempt-1',
    };
    fakeMarketLogRepo.find = async () => [log];
    fakeGamma.fetchOutcome = async () => ({ slug: log.slug, closed: true, yesWon: true, noWon: false });
    await svc.resolvePendingMarkets();
    console.log('\n[Тест 6] Профит на выигрыше считается от filledAmount (4.5/0.99*(1-0.99)=~0.0455):', Math.abs(log.profit - (4.5 / 0.99) * 0.01) < 1e-9);

    const log2: any = { ...log, id: 'log-2', filledAmount: 3 };
    fakeMarketLogRepo.find = async () => [log2];
    fakeGamma.fetchOutcome = async () => ({ slug: log2.slug, closed: true, yesWon: false, noWon: true });
    await svc.resolvePendingMarkets();
    console.log('[Тест 6] Профит на проигрыше = -filledAmount:', log2.profit === -3);
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
