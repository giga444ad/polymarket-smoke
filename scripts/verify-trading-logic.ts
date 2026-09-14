/* Ad-hoc verification script — not part of the app. */
import 'reflect-metadata';
import { TradingService } from '../src/trading/trading.service';

const writes: any[] = [];
const attemptSaves: any[] = [];

const fakeConfig = {
  overrides: {
    SMOKE_START: 'true',
    MIN_MARKET_PRICE: '0.99',
    MAX_MARKET_PRICE: '0.999',
    FAVORITE_BID_THRESHOLD: '0.90',
    LIMIT_TIER1_PRICE: '0.99',
    LIMIT_TIER2_PRICE: '0.995',
    LIMIT_TIER3_PRICE: '0.999',
    LIMIT_TIER2_SECONDS: '40',
    LIMIT_TIER3_SECONDS: '15',
    MAX_OVERSPEND_MULTIPLIER: '1.5',
    MIN_FILL_RATIO: '0.5',
    // Тесты 1-6 проверяют механику стакана/резолва, не новые гейты — держим
    // их отключёнными/широкими здесь и тестируем гейты отдельно ниже
    // (Тесты 9-10), т.к. по умолчанию в проде теперь ENTRY_FILTER_ENABLED=true
    // и LAST_ENTRY_WINDOW_SEC=60 (см. .env.example).
    LAST_ENTRY_WINDOW_SEC: '600',
    ENTRY_FILTER_ENABLED: 'false',
    STREAMS_CONFIG: JSON.stringify([
      { streamKey: 'btc-updown-5m', kind: 'interval', intervalSec: 300, slugPrefix: 'btc-updown-5m', baseStake: 5 },
      { streamKey: 'btc-updown-15m', kind: 'interval', intervalSec: 900, slugPrefix: 'btc-updown-15m', baseStake: 5 },
      { streamKey: 'bitcoin-up-or-down', kind: 'hourly-et', intervalSec: 3600, etSlugBase: 'bitcoin-up-or-down', baseStake: 5 },
    ]),
  } as Record<string, string>,
  get(key: string, def?: string) {
    return this.overrides[key] ?? def;
  },
};

const fakeGamma: any = {
  currentIntervalStartTimestampSec: () => 700,
  currentIntervalCloseTimestampSec: () => 1000,
  buildSlugForStart: (stream: any, ts: number) =>
    stream.kind === 'hourly-et' ? `${stream.etSlugBase}-fake-hourly-slug-${ts}` : `${stream.slugPrefix}-${ts}`,
  fetchMarketBySlug: async () => null,
  fetchOutcome: async () => null as any,
};

const fakeClobPublic = { getBestQuote: async () => null };

const fakePriceFeed: any = {
  getSnapshot: () => ({ price: null, priceAt: null, atr: null, candleCount: 0, source: null }),
  // Сессия 11: реалистичный дефолт для тестов, которые НЕ подставляют свой —
  // "буфер не достаёт так далеко" (соответствует getSnapshot тоже давая null).
  getPriceAt: () => ({ price: null, tickAt: null, lagMs: null }),
  // Сессия 13: Time-in-Zone фильтр — дефолт "недоступно" (буфер не достаёт).
  getTimeInZoneRatio: () => null,
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
  findOneOrFail: async () => ({
    id: 'attempt-1',
    streamKey: 'btc-updown-5m',
    status: 'active',
    currentStep: 0,
    targetSteps: 500,
    baseStake: 5,
    currentStake: 5,
    isSmoke: true,
  }),
};

let fakeLogIdSeq = 0;
const updates: any[] = [];
const fakeMarketLogRepo: any = {
  create: (x: any) => x,
  save: async (x: any) => {
    const saved = { ...x, id: x.id ?? `log-${++fakeLogIdSeq}` };
    writes.push(saved);
    return saved;
  },
  update: async (id: any, patch: any) => {
    updates.push({ id, ...patch });
  },
  find: async () => [] as any[],
  findOne: async () => null as any,
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
    // Стейк шага теперь снимается с Attempt.currentStake в момент открытия
    // окна (реинвест-прогрессия, см. BACKLOG п.1) — в тестах фиксируем $5,
    // как раньше был константный BET_AMOUNT.
    betAmount: 5,
    // Сессия 13: доп. фильтры входа — дефолты нейтральны для тестов, которые
    // их не касаются (intervalSec=300 соответствует дефолтному btc-updown-5m,
    // windowStartMs=null означает "Time-in-Zone недоступен", как и раньше
    // было для окон, открытых не через штатный discoveryTick).
    intervalSec: 300,
    windowStartMs: null,
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
  // currentAttempt раньше был единственным полем — теперь Map<streamKey, Attempt>
  // (независимая прогрессия на поток, см. BACKLOG п.3).
  svc.currentAttempts.set('btc-updown-5m', {
    id: 'attempt-1',
    streamKey: 'btc-updown-5m',
    currentStep: 0,
    targetSteps: 500,
    baseStake: 5,
    currentStake: 5,
  });

  console.log(
    'streams =',
    svc.streams.map((s: any) => s.streamKey),
    '(ожидаем [btc-updown-5m, btc-updown-15m, bitcoin-up-or-down])',
  );

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

  // --- Тест 4b (Сессия 10, микрокейс №1): гейт должен ПЕРЕПРОВЕРЯТЬСЯ на
  //     момент фактического исполнения резюм-лимитки, а не только на момент
  //     её выставления — иначе капитал рискуется по УЖЕ УСТАРЕВШЕМУ разрешению,
  //     если цена успела откатиться обратно к референсу, пока лимитка ждала
  //     накопления объёма продавцов (ровно прод-кейс: гейт пропустил вход при
  //     ATR-рацио ~1.7x на выставлении, к моменту исполнения было уже 0.44x). ---
  {
    // ATR-рацио = |price - referencePrice| / atr. referencePrice берётся из
    // marketState.referencePrice (задаётся при создании через makeMarketState).
    let mockAtr = 0.01;
    let mockPrice = 0.94; // |0.99(референс)-0.94| = 0.05 -> 0.05/0.01 = 5x ATR: уверенно, гейт пропускает
    const dynamicFeed: any = {
      getSnapshot: () => ({ price: mockPrice, priceAt: Date.now(), atr: mockAtr, candleCount: 20, source: 'chainlink' }),
      // Сессия 13: captureDiagnostics теперь всегда зовёт getPriceAt для
      // driftRateAtEntry (SHADOW-диагностика) — фейк должен его реализовывать,
      // как и реальный PriceFeedService. Возвращаем "недоступно", этот тест
      // директиональный дрифт не проверяет (см. отдельный тест ниже).
      getPriceAt: () => ({ price: null, tickAt: null, lagMs: null }),
      getTimeInZoneRatio: () => null,
    };
    const gatedConfig = {
      overrides: { ...fakeConfig.overrides, LAST_ENTRY_WINDOW_SEC: '600', ENTRY_FILTER_ENABLED: 'true', MIN_DISTANCE_ATR_RATIO: '1.5' },
      get(key: string, def?: string) {
        return this.overrides[key] ?? def;
      },
    };
    const gatedSvc: any = new TradingService(
      gatedConfig as any, fakeGamma as any, fakeClobPublic as any, fakeTrader as any,
      dynamicFeed, fakeAttemptRepo as any, fakeMarketLogRepo as any,
    );

    const ms = makeMarketState({ closesAt: new Date(Date.now() + 280_000), referencePrice: 0.99 });
    const bNoAsk = { outcome: 'YES' as const, tickSize: '0.01', asks: [], bids: [], bestAsk: null, bestBid: 0.95 };
    gatedSvc.onBookUpdate(ms, 'YES', bNoAsk);
    await sleep(30);
    console.log('[Тест 4b] Лимитка выставлена при уверенном ATR-рацио (гейт пройден):', ms.restingOrder != null);

    // Пока лимитка "висела" в ожидании объёма продавцов, цена ОТКАТИЛАСЬ
    // почти к референсу — рацио упало намного ниже порога 1.5x.
    mockPrice = 0.988; // |0.99-0.988| = 0.002 -> 0.002/0.01 = 0.2x ATR: гейт больше НЕ пропускает

    const deep = book([{ price: 0.98, size: 10 }], 0.95); // объём накопился достаточный
    gatedSvc.onBookUpdate(ms, 'YES', deep);
    await sleep(30);
    console.log(
      '[Тест 4b] Объём накопился, но цена откатилась (0.2x < 1.5x) — лимитка ОТМЕНЕНА, а НЕ исполнена вслепую:',
      writes.length === 0 && !ms.positioned && ms.restingOrder === null && ms.skippedLimitTier === 'T1',
    );
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

  // --- Тест 7: реинвест-прогрессия — выигрыш поднимает currentStake на следующий шаг,
  //     а не оставляет его константным (BACKLOG п.1). nextStake = filledAmount/entryPrice.
  {
    const log: any = {
      id: 'log-3',
      slug: 'btc-updown-5m-2000',
      chosenOutcome: 'YES',
      entryPrice: 0.99,
      betAmount: 5,
      filledAmount: 5,
      assetPrefix: 'btc-updown-5m',
      attemptId: 'attempt-1',
    };
    fakeMarketLogRepo.find = async () => [log];
    fakeGamma.fetchOutcome = async () => ({ slug: log.slug, closed: true, yesWon: true, noWon: false });
    await svc.resolvePendingMarkets();
    const updatedAttempt = svc.currentAttempts.get('btc-updown-5m');
    const expectedNextStake = 5 / 0.99;
    console.log(
      '\n[Тест 7] currentStake после выигрыша = filledAmount/entryPrice (ожидаем ~5.0505):',
      updatedAttempt?.currentStake,
      Math.abs((updatedAttempt?.currentStake ?? 0) - expectedNextStake) < 1e-9,
    );
  }

  // --- Тест 7b (Сессия 8, баг №1): при ЧАСТИЧНОМ филле неисполненный остаток
  //     заявки не должен теряться — он прибавляется к следующему стейку.
  //     Пример из прода: заявка $6.36, филл $4.95 по ¢99 -> ожидаем
  //     currentStake = 4.95/0.99 + (6.36-4.95) = 5.00 + 1.41 = 6.41,
  //     а НЕ 5.00 (как было бы без фикса — прогрессия "проваливалась" к базе).
  {
    const log: any = {
      id: 'log-3b',
      slug: 'btc-updown-5m-2050',
      chosenOutcome: 'YES',
      entryPrice: 0.99,
      betAmount: 6.36,
      filledAmount: 4.95,
      assetPrefix: 'btc-updown-5m',
      attemptId: 'attempt-1',
    };
    fakeAttemptRepo.findOneOrFail = async () => ({
      id: 'attempt-1',
      attemptNumber: 1,
      streamKey: 'btc-updown-5m',
      status: 'active',
      currentStep: 7,
      targetSteps: 500,
      baseStake: 5,
      currentStake: 6.36,
      isSmoke: true,
    });
    fakeMarketLogRepo.find = async () => [log];
    fakeGamma.fetchOutcome = async () => ({ slug: log.slug, closed: true, yesWon: true, noWon: false });
    await svc.resolvePendingMarkets();
    const updatedAttempt = svc.currentAttempts.get('btc-updown-5m');
    const expected = 4.95 / 0.99 + (6.36 - 4.95);
    console.log(
      '[Тест 7b] Частичный филл: неисполненный остаток прибавлен к следующему стейку (ожидаем ~6.41):',
      updatedAttempt?.currentStake,
      Math.abs((updatedAttempt?.currentStake ?? 0) - expected) < 1e-9,
    );
  }


  {
    const log: any = {
      id: 'log-4',
      slug: 'btc-updown-5m-3000',
      chosenOutcome: 'YES',
      entryPrice: 0.99,
      betAmount: 5.0505,
      filledAmount: 5.0505,
      assetPrefix: 'btc-updown-5m',
      attemptId: 'attempt-1',
    };
    fakeAttemptRepo.findOneOrFail = async () => ({
      id: 'attempt-1',
      attemptNumber: 1,
      streamKey: 'btc-updown-5m',
      status: 'active',
      currentStep: 1,
      targetSteps: 500,
      baseStake: 5,
      currentStake: 5.0505,
      isSmoke: true,
    });
    fakeMarketLogRepo.find = async () => [log];
    fakeGamma.fetchOutcome = async () => ({ slug: log.slug, closed: true, yesWon: false, noWon: true });
    await svc.resolvePendingMarkets();
    const newAttempt = svc.currentAttempts.get('btc-updown-5m');
    console.log(
      '[Тест 8] После проигрыша новая попытка стартует с currentStake=baseStake (5):',
      newAttempt?.currentStake === 5,
    );
  }

  writes.length = 0;
  // --- Тест 9: окно входа — не пытаемся войти раньше LAST_ENTRY_WINDOW_SEC,
  //     даже если стакан даёт отличную цену (см. реальный инцидент — оба
  //     слива случились на ранних, "неопределившихся" входах). ---
  {
    const gatedConfig = {
      overrides: { ...fakeConfig.overrides, LAST_ENTRY_WINDOW_SEC: '60', ENTRY_FILTER_ENABLED: 'false' },
      get(key: string, def?: string) {
        return this.overrides[key] ?? def;
      },
    };
    const gatedSvc: any = new TradingService(
      gatedConfig as any,
      fakeGamma as any,
      fakeClobPublic as any,
      fakeTrader as any,
      fakePriceFeed as any,
      fakeAttemptRepo as any,
      fakeMarketLogRepo as any,
    );
    gatedSvc.currentAttempts.set('btc-updown-5m', {
      id: 'attempt-1',
      streamKey: 'btc-updown-5m',
      currentStep: 0,
      targetSteps: 500,
      baseStake: 5,
      currentStake: 5,
    });

    const early = makeMarketState({ closesAt: new Date(Date.now() + 200_000) }); // 200с до закрытия > окна в 60с
    const b = book([{ price: 0.99, size: 10 }], 0.97); // отличная цена и глубина — но рано
    gatedSvc.onBookUpdate(early, 'YES', b);
    await sleep(30);
    console.log(
      '\n[Тест 9] Слишком рано (200с до закрытия, окно=60с) — вход НЕ предпринят:',
      writes.length === 0 && !early.positioned && !early.restingOrder,
    );

    writes.length = 0;
    const late = makeMarketState({ closesAt: new Date(Date.now() + 50_000) }); // 50с < окна в 60с — уже можно
    gatedSvc.onBookUpdate(late, 'YES', b);
    await sleep(30);
    console.log('[Тест 9] Внутри окна входа (50с < 60с) — вход предпринят:', writes.length === 1 && late.positioned);
  }
  writes.length = 0;

  // --- Тест 10: ATR-гейт теперь fail-closed на отсутствии диагностики, а не
  //     fail-open — упустить шаг лучше, чем рисковать капиталом вслепую. ---
  {
    const gatedConfig = {
      overrides: { ...fakeConfig.overrides, LAST_ENTRY_WINDOW_SEC: '600', ENTRY_FILTER_ENABLED: 'true' },
      get(key: string, def?: string) {
        return this.overrides[key] ?? def;
      },
    };
    const gatedSvc: any = new TradingService(
      gatedConfig as any,
      fakeGamma as any,
      fakeClobPublic as any,
      fakeTrader as any,
      fakePriceFeed as any, // всегда возвращает price:null — "фид не отдал ни одного тика"
      fakeAttemptRepo as any,
      fakeMarketLogRepo as any,
    );
    gatedSvc.currentAttempts.set('btc-updown-5m', {
      id: 'attempt-1',
      streamKey: 'btc-updown-5m',
      currentStep: 0,
      targetSteps: 500,
      baseStake: 5,
      currentStake: 5,
    });

    const ms = makeMarketState();
    const b = book([{ price: 0.99, size: 10 }], 0.97);
    gatedSvc.onBookUpdate(ms, 'YES', b);
    await sleep(30);
    console.log(
      '\n[Тест 10] ATR-гейт включён, диагностики нет -> вход заблокирован (fail-closed):',
      writes.length === 0 && !ms.positioned,
    );
  }
  writes.length = 0;

  // --- Тест 11 (Сессия 6, п.7): BLOCK_ORDERS_IF_PENDING реально блокирует
  // discoveryTick, пока по потоку есть незарезолвленный шаг — это и есть
  // фикс реального race condition по сумме ставки (см. CONTEXT.md). ---
  {
    // Несколько потоков сконфигурировано разом (см. STREAMS_CONFIG в
    // fakeConfig) — проверяем гейт ИМЕННО для btc-updown-5m, не задевая
    // остальные потоки (у них своя, отдельная блокировка по своему streamKey).
    const calledSlugs: string[] = [];
    const gammaSpy: any = {
      ...fakeGamma,
      fetchMarketBySlug: async (slug: string) => {
        calledSlugs.push(slug);
        return null;
      },
    };
    const svcBlocked: any = new TradingService(
      fakeConfig as any,
      gammaSpy,
      fakeClobPublic as any,
      fakeTrader as any,
      fakePriceFeed as any,
      fakeAttemptRepo as any,
      fakeMarketLogRepo as any,
    );
    svcBlocked.pendingGateMode = 'block';
    svcBlocked.pendingByStream.set('btc-updown-5m', new Set(['log-1']));
    await svcBlocked.discoveryTick();
    console.log(
      `[Тест 11] Окно btc-updown-5m НЕ открыто, пока есть pending-шаг (fetchMarketBySlug для него не вызван): ${!calledSlugs.some((s) => s.startsWith('btc-updown-5m-'))}`,
    );

    calledSlugs.length = 0;
    svcBlocked.pendingByStream.set('btc-updown-5m', new Set());
    await svcBlocked.discoveryTick();
    console.log(
      `[Тест 11] После резолва (pending-сет пуст) discoveryTick снова идёт за маркетом btc-updown-5m: ${calledSlugs.some((s) => s.startsWith('btc-updown-5m-'))}`,
    );
  }

  // --- Тест 12 (Сессия 6, п.7): writeLog кладёт id лога в pendingByStream
  // при status='pending_resolve', и марkет-стейт использует СНИМОК
  // attemptId/attemptStepNumber, а не currentAttempts.get(...) "на сейчас". ---
  {
    const ms = makeMarketState({ attemptId: 'attempt-42', attemptStepNumber: 7 });
    const b = book([{ price: 0.99, size: 10 }], 0.97);
    svc.onBookUpdate(ms, 'YES', b);
    await sleep(50);
    const lastWrite = writes[writes.length - 1];
    const pendingSet = svc.pendingByStream.get('btc-updown-5m');
    console.log(
      `[Тест 12] Лог записан со снимком attemptId='attempt-42'/stepNumber=7: ${lastWrite.attemptId === 'attempt-42' && lastWrite.stepNumber === 7}`,
    );
    console.log(`[Тест 12] pendingByStream пополнен id только что записанного лога: ${pendingSet?.has(lastWrite.id) ?? false}`);
  }

  // --- Тест 13 (Сессия 7): pre_resolve — предсказание исхода по Chainlink ---
  {
    const pendingLog = {
      id: 'log-pending-1',
      slug: 'btc-updown-5m-900',
      assetPrefix: 'btc-updown-5m',
      status: 'pending_resolve',
      chosenOutcome: 'YES',
      referencePrice: 0.5, // страйк (условно — "цена BTC" в шкале теста)
      entryPrice: 0.99,
      filledAmount: 9.9,
      betAmount: 10,
      createdAt: new Date(),
    };
    const repoWithPending: any = { ...fakeMarketLogRepo, findOne: async () => pendingLog };

    // 13a: цена ушла далеко вверх (в сторону YES) на 3x ATR — уверенно предсказываем ВЫИГРЫШ.
    const confidentUpFeed: any = { getSnapshot: () => ({ price: 0.53, priceAt: Date.now(), atr: 0.01, candleCount: 20, source: 'chainlink' }) };
    const svcPreWin: any = new TradingService(
      fakeConfig as any, fakeGamma as any, fakeClobPublic as any, fakeTrader as any,
      confidentUpFeed, fakeAttemptRepo as any, repoWithPending,
    );
    svcPreWin.preResolveMinAtrRatio = 2;
    svcPreWin.preResolveMaxChain = 1;
    const winPrediction = await svcPreWin.tryPreResolve('btc-updown-5m');
    // Сессия 8: формула синхронизирована с фиксом бага №1 — неисполненный
    // остаток заявки (betAmount-filledAmount = 10-9.9 = 0.1) тоже переносится.
    const expectedWinStake = 9.9 / 0.99 + (10 - 9.9);
    console.log(
      `[Тест 13a] pre_resolve предсказывает ВЫИГРЫШ и стейк = filledAmount/entryPrice + неисполненный остаток (${expectedWinStake.toFixed(2)}): ` +
        `${winPrediction != null && Math.abs(winPrediction.betAmount - expectedWinStake) < 1e-9}`,
    );

    // 13b: цена ушла в сторону NO (противоположную chosenOutcome) — предсказываем ПРОИГРЫШ, стейк = baseStake.
    const confidentDownFeed: any = { getSnapshot: () => ({ price: 0.47, priceAt: Date.now(), atr: 0.01, candleCount: 20, source: 'chainlink' }) };
    const svcPreLoss: any = new TradingService(
      fakeConfig as any, fakeGamma as any, fakeClobPublic as any, fakeTrader as any,
      confidentDownFeed, fakeAttemptRepo as any, repoWithPending,
    );
    svcPreLoss.preResolveMinAtrRatio = 2;
    svcPreLoss.preResolveMaxChain = 1;
    const lossPrediction = await svcPreLoss.tryPreResolve('btc-updown-5m');
    console.log(`[Тест 13b] pre_resolve предсказывает ПРОИГРЫШ и сбрасывает стейк на baseStake (5): ${lossPrediction?.betAmount === 5}`);

    // 13c: цена почти не сдвинулась (0.2x ATR) — недостаточно уверенно, null (безопасный фолбэк к block).
    const unsureFeed: any = { getSnapshot: () => ({ price: 0.502, priceAt: Date.now(), atr: 0.01, candleCount: 20, source: 'chainlink' }) };
    const svcUnsure: any = new TradingService(
      fakeConfig as any, fakeGamma as any, fakeClobPublic as any, fakeTrader as any,
      unsureFeed, fakeAttemptRepo as any, repoWithPending,
    );
    svcUnsure.preResolveMinAtrRatio = 2;
    svcUnsure.preResolveMaxChain = 1;
    const unsurePrediction = await svcUnsure.tryPreResolve('btc-updown-5m');
    console.log(`[Тест 13c] Слишком близко к страйку (<2x ATR) — предсказание отклонено (null): ${unsurePrediction === null}`);

    // 13d: PRE_RESOLVE_MAX_CHAIN — после достижения потолка новые предсказания не выдаются, пока нет официального резолва.
    svcPreWin.provisionalChainByStream.set('btc-updown-5m', 1); // уже 1 подряд при maxChain=1
    const cappedPrediction = await svcPreWin.tryPreResolve('btc-updown-5m');
    console.log(`[Тест 13d] PRE_RESOLVE_MAX_CHAIN=1 останавливает цепочку предсказаний без подтверждения: ${cappedPrediction === null}`);
  }

  // --- Тест 14 (Сессия 12): priceAtClose тоже должен браться точечно, НА
  //     МОМЕНТ closesAt, а не "текущей" ценой в момент, когда исполнился
  //     код finalizeMarket (симметрично фиксу референса открытия, Сессия 11). ---
  {
    updates.length = 0;
    const closeAtMs = Date.now(); // конкретный "истинный" момент закрытия
    const spyFeed: any = {
      getSnapshot: () => ({ price: 999, priceAt: Date.now(), atr: 0.02, candleCount: 20, source: 'chainlink' }),
      getPriceAt: (streamKey: string, targetMs: number) => {
        console.log(`[Тест 14] getPriceAt вызван с targetMs === closesAt.getTime(): ${targetMs === closeAtMs}`);
        return { price: 101.5, tickAt: targetMs, lagMs: 0 };
      },
    };
    const svcClose: any = new TradingService(
      fakeConfig as any, fakeGamma as any, fakeClobPublic as any, fakeTrader as any,
      spyFeed, fakeAttemptRepo as any, fakeMarketLogRepo as any,
    );
    const ms = makeMarketState({
      closesAt: new Date(closeAtMs),
      positioned: true,
      marketLogId: 'log-close-test',
    });
    await svcClose.finalizeMarket(ms);
    const upd = updates.find((u) => u.id === 'log-close-test');
    console.log(
      '[Тест 14] priceAtClose взят из getPriceAt (101.5), а НЕ из getSnapshot (999):',
      upd?.priceAtClose === 101.5,
    );
    console.log('[Тест 14] atrAtClose по-прежнему берётся из getSnapshot (0.02, агрегат по свечам, лаг тут не при чём):', upd?.atrAtClose === 0.02);
  }

  // --- Тесты 15-18 (Сессия 13): 4 доп. фильтра входа — каждый проверяем
  //     независимо (заблокировано при неблагоприятной диагностике / вход
  //     проходит при благоприятной), с ENTRY_FILTER_ENABLED='false', чтобы
  //     исходный ATR-гейт не мешал изолированно проверить именно новый
  //     фильтр. Общий сценарий стакана/окна — как в Тесте 9 (ask=0.99,
  //     bestBid=0.97, closesAt через 50с < LAST_ENTRY_WINDOW_SEC=60с). ---
  function makeGatedSvc(overrides: Record<string, string>, feed: any) {
    const cfg = {
      overrides: { ...fakeConfig.overrides, LAST_ENTRY_WINDOW_SEC: '60', ENTRY_FILTER_ENABLED: 'false', ...overrides },
      get(key: string, def?: string) {
        return this.overrides[key] ?? def;
      },
    };
    const svc: any = new TradingService(cfg as any, fakeGamma as any, fakeClobPublic as any, fakeTrader as any, feed, fakeAttemptRepo as any, fakeMarketLogRepo as any);
    svc.currentAttempts.set('btc-updown-5m', { id: 'attempt-1', streamKey: 'btc-updown-5m', currentStep: 0, targetSteps: 500, baseStake: 5, currentStake: 5 });
    return svc;
  }
  const gateBook = book([{ price: 0.99, size: 10 }], 0.97);

  // --- Тест 15: Time-of-Day Blackout ---
  {
    const nowHour = new Date().getUTCHours();

    writes.length = 0;
    const blockedSvc = makeGatedSvc({ BLACKOUT_HOURS_FILTER_ENABLED: 'true', BLACKOUT_HOURS_UTC: String(nowHour) }, fakePriceFeed);
    const msBlocked = makeMarketState({ closesAt: new Date(Date.now() + 50_000) });
    blockedSvc.onBookUpdate(msBlocked, 'YES', gateBook);
    await sleep(30);
    console.log('\n[Тест 15] Blackout: текущий час в списке -> вход заблокирован:', writes.length === 0 && !msBlocked.positioned);

    writes.length = 0;
    const otherHour = (nowHour + 12) % 24;
    const allowedSvc = makeGatedSvc({ BLACKOUT_HOURS_FILTER_ENABLED: 'true', BLACKOUT_HOURS_UTC: String(otherHour) }, fakePriceFeed);
    const msAllowed = makeMarketState({ closesAt: new Date(Date.now() + 50_000) });
    allowedSvc.onBookUpdate(msAllowed, 'YES', gateBook);
    await sleep(30);
    console.log('[Тест 15] Blackout: текущий час НЕ в списке -> вход проходит:', writes.length === 1 && msAllowed.positioned);
  }

  // --- Тест 16: Expected Move (динамический запас, тот же временной масштаб, что и ATR) ---
  {
    // intervalSec=300 (btc-updown-5m), timeLeftSec≈50 -> sqrt(50/300)≈0.4082,
    // requiredDelta = atr(0.01) * 0.4082 * SAFETY_K_FACTOR(1.5) ≈ 0.006124.
    writes.length = 0;
    const tooCloseFeed: any = { getSnapshot: () => ({ price: 0.503, priceAt: Date.now(), atr: 0.01, candleCount: 20, source: 'chainlink' }), getPriceAt: () => ({ price: null, tickAt: null, lagMs: null }) };
    const blockedSvc = makeGatedSvc({ EXPECTED_MOVE_FILTER_ENABLED: 'true', SAFETY_K_FACTOR: '1.5' }, tooCloseFeed);
    const msBlocked = makeMarketState({ closesAt: new Date(Date.now() + 50_000), referencePrice: 0.5 });
    blockedSvc.onBookUpdate(msBlocked, 'YES', gateBook);
    await sleep(30);
    console.log('\n[Тест 16] Expected-move: дельта (0.003) меньше требуемого запаса (~0.006) -> заблокировано:', writes.length === 0 && !msBlocked.positioned);

    writes.length = 0;
    const farEnoughFeed: any = { getSnapshot: () => ({ price: 0.51, priceAt: Date.now(), atr: 0.01, candleCount: 20, source: 'chainlink' }), getPriceAt: () => ({ price: null, tickAt: null, lagMs: null }) };
    const allowedSvc = makeGatedSvc({ EXPECTED_MOVE_FILTER_ENABLED: 'true', SAFETY_K_FACTOR: '1.5' }, farEnoughFeed);
    const msAllowed = makeMarketState({ closesAt: new Date(Date.now() + 50_000), referencePrice: 0.5 });
    allowedSvc.onBookUpdate(msAllowed, 'YES', gateBook);
    await sleep(30);
    console.log('[Тест 16] Expected-move: дельта (0.01) больше требуемого запаса (~0.006) -> вход проходит:', writes.length === 1 && msAllowed.positioned);
  }

  // --- Тест 17: Directional Volatility Drift (сигнатурная дельта схлопывается против стороны) ---
  {
    writes.length = 0;
    const fallingFeed: any = {
      getSnapshot: () => ({ price: 0.5, priceAt: Date.now(), atr: 0.01, candleCount: 20, source: 'chainlink' }),
      getPriceAt: () => ({ price: 0.52, tickAt: Date.now(), lagMs: 0 }), // цена 10с назад была ВЫШЕ -> падает -> против YES
    };
    const blockedSvc = makeGatedSvc({ DIRECTIONAL_DRIFT_FILTER_ENABLED: 'true', DRIFT_LOOKBACK_SEC: '10' }, fallingFeed);
    const msBlocked = makeMarketState({ closesAt: new Date(Date.now() + 50_000), referencePrice: 0.5 });
    blockedSvc.onBookUpdate(msBlocked, 'YES', gateBook);
    await sleep(30);
    console.log('\n[Тест 17] Directional-drift: дельта падает против YES -> заблокировано:', writes.length === 0 && !msBlocked.positioned);

    writes.length = 0;
    const risingFeed: any = {
      getSnapshot: () => ({ price: 0.52, priceAt: Date.now(), atr: 0.01, candleCount: 20, source: 'chainlink' }),
      getPriceAt: () => ({ price: 0.5, tickAt: Date.now(), lagMs: 0 }), // цена 10с назад была НИЖЕ -> растёт -> в пользу YES
    };
    const allowedSvc = makeGatedSvc({ DIRECTIONAL_DRIFT_FILTER_ENABLED: 'true', DRIFT_LOOKBACK_SEC: '10' }, risingFeed);
    const msAllowed = makeMarketState({ closesAt: new Date(Date.now() + 50_000), referencePrice: 0.5 });
    allowedSvc.onBookUpdate(msAllowed, 'YES', gateBook);
    await sleep(30);
    console.log('[Тест 17] Directional-drift: дельта растёт в пользу YES -> вход проходит:', writes.length === 1 && msAllowed.positioned);
  }

  // --- Тест 18: Time-in-Zone Ratio (доля прошедшего времени окна на нужной стороне) ---
  {
    writes.length = 0;
    const zoneLowFeed: any = {
      getSnapshot: () => ({ price: 0.52, priceAt: Date.now(), atr: 0.01, candleCount: 20, source: 'chainlink' }),
      getTimeInZoneRatio: () => 0.1, // YES держался только 10% прошедшего времени окна
      getPriceAt: () => ({ price: null, tickAt: null, lagMs: null }),
    };
    const blockedSvc = makeGatedSvc({ TIME_IN_ZONE_FILTER_ENABLED: 'true', MIN_ZONE_RATIO: '0.65' }, zoneLowFeed);
    const msBlocked = makeMarketState({ closesAt: new Date(Date.now() + 50_000), referencePrice: 0.5, windowStartMs: Date.now() - 250_000 });
    blockedSvc.onBookUpdate(msBlocked, 'YES', gateBook);
    await sleep(30);
    console.log('\n[Тест 18] Time-in-Zone: YES держался только 10% окна (< 65%) -> заблокировано:', writes.length === 0 && !msBlocked.positioned);

    writes.length = 0;
    const zoneHighFeed: any = {
      getSnapshot: () => ({ price: 0.52, priceAt: Date.now(), atr: 0.01, candleCount: 20, source: 'chainlink' }),
      getTimeInZoneRatio: () => 0.9, // YES держался 90% прошедшего времени окна
      getPriceAt: () => ({ price: null, tickAt: null, lagMs: null }),
    };
    const allowedSvc = makeGatedSvc({ TIME_IN_ZONE_FILTER_ENABLED: 'true', MIN_ZONE_RATIO: '0.65' }, zoneHighFeed);
    const msAllowed = makeMarketState({ closesAt: new Date(Date.now() + 50_000), referencePrice: 0.5, windowStartMs: Date.now() - 250_000 });
    allowedSvc.onBookUpdate(msAllowed, 'YES', gateBook);
    await sleep(30);
    console.log('[Тест 18] Time-in-Zone: YES держался 90% окна (>= 65%) -> вход проходит:', writes.length === 1 && msAllowed.positioned);

    writes.length = 0;
    const zoneNullFeed: any = {
      getSnapshot: () => ({ price: 0.52, priceAt: Date.now(), atr: 0.01, candleCount: 20, source: 'chainlink' }),
      getTimeInZoneRatio: () => null, // буфер не достаёт до начала окна
      getPriceAt: () => ({ price: null, tickAt: null, lagMs: null }),
    };
    const failClosedSvc = makeGatedSvc({ TIME_IN_ZONE_FILTER_ENABLED: 'true', MIN_ZONE_RATIO: '0.65' }, zoneNullFeed);
    const msFailClosed = makeMarketState({ closesAt: new Date(Date.now() + 50_000), referencePrice: 0.5, windowStartMs: Date.now() - 250_000 });
    failClosedSvc.onBookUpdate(msFailClosed, 'YES', gateBook);
    await sleep(30);
    console.log('[Тест 18] Time-in-Zone: диагностика недоступна (null) -> fail-closed, вход заблокирован:', writes.length === 0 && !msFailClosed.positioned);
  }

  // --- Тест 19 (Сессия 14): per-stream переопределение LAST_ENTRY_WINDOW_SEC —
  //     значение из marketState (снятое в openMarket из STREAMS_CONFIG)
  //     должно ПЕРЕКРЫВАТЬ глобальный ENV-дефолт, а не наоборот. ---
  {
    writes.length = 0;
    // Глобальный дефолт разрешил бы вход за 200с (широкий), но у ЭТОГО
    // конкретного маркета (снимок из STREAMS_CONFIG на момент открытия)
    // окно уже сужено до 30с — 50с до закрытия должно блокировать.
    const wideGlobalSvc = makeGatedSvc({ LAST_ENTRY_WINDOW_SEC: '200' }, fakePriceFeed);
    const msNarrow = makeMarketState({ closesAt: new Date(Date.now() + 50_000), lastEntryWindowSec: 30 });
    wideGlobalSvc.onBookUpdate(msNarrow, 'YES', gateBook);
    await sleep(30);
    console.log(
      '\n[Тест 19] per-stream lastEntryWindowSec=30 < 50с до закрытия -> блокирует, ХОТЯ глобальный ENV=200с разрешил бы:',
      writes.length === 0 && !msNarrow.positioned,
    );

    writes.length = 0;
    // Обратный случай: глобальный дефолт узкий (10с, заблокировал бы), но у
    // потока переопределено на 90с — вход должен пройти.
    const narrowGlobalSvc = makeGatedSvc({ LAST_ENTRY_WINDOW_SEC: '10', LIMIT_TIER2_SECONDS: '5', LIMIT_TIER3_SECONDS: '2' }, fakePriceFeed);
    const msWide = makeMarketState({ closesAt: new Date(Date.now() + 50_000), lastEntryWindowSec: 90 });
    narrowGlobalSvc.onBookUpdate(msWide, 'YES', gateBook);
    await sleep(30);
    console.log(
      '[Тест 19] per-stream lastEntryWindowSec=90 > 50с до закрытия -> вход проходит, ХОТЯ глобальный ENV=10с заблокировал бы:',
      writes.length === 1 && msWide.positioned,
    );
  }

  // --- Тест 20 (Сессия 14): per-stream переопределение computeTier
  //     (tier2Seconds/tier3Seconds) через marketState. ---
  {
    const anySvc: any = makeGatedSvc({ LAST_ENTRY_WINDOW_SEC: '200', LIMIT_TIER2_SECONDS: '150', LIMIT_TIER3_SECONDS: '60' }, fakePriceFeed);
    const msDefault = makeMarketState(); // без override — берёт глобальные 150/60
    console.log(
      '\n[Тест 20] Без override: 100с до закрытия при глобальных T2=150/T3=60 -> тир T2:',
      anySvc.computeTier(100, msDefault) === 'T2',
    );
    const msOverridden = makeMarketState({ tier2Seconds: 40, tier3Seconds: 15 });
    console.log(
      '[Тест 20] С override T2=40/T3=15: те же 100с -> уже тир T1 (за пределами обоих порогов потока):',
      anySvc.computeTier(100, msOverridden) === 'T1',
    );
    console.log(
      '[Тест 20] С override T2=40/T3=15: 20с до закрытия -> тир T2 (между T3=15 и T2=40):',
      anySvc.computeTier(20, msOverridden) === 'T2',
    );
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
