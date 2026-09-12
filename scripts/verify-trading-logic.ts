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
    LIMIT_TIER2_SECONDS: '150',
    LIMIT_TIER3_SECONDS: '60',
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
const fakeMarketLogRepo: any = {
  create: (x: any) => x,
  save: async (x: any) => {
    const saved = { ...x, id: x.id ?? `log-${++fakeLogIdSeq}` };
    writes.push(saved);
    return saved;
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
