import { Injectable, Logger, OnModuleInit, OnModuleDestroy, NotFoundException, BadRequestException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { LessThan, Repository } from 'typeorm';
import { Attempt } from '../entities/attempt.entity';
import { StreamRuntimeConfig, CloseMode } from '../entities/stream-runtime-config.entity';
import { ChosenOutcome, MarketLog, MarketLogStatus, OrderKind } from '../entities/market-log.entity';
import { ActiveWindow } from '../entities/active-window.entity';
import { GammaMarketService } from '../polymarket/gamma-market.service';
import { ClobPublicService } from '../polymarket/clob-public.service';
import { PolymarketTraderService } from '../polymarket/polymarket-trader.service';
import { LiveBook, MarketWsStream, Outcome } from '../polymarket/market-ws-stream';
import { cumulativeUsdAtOrBelow, walkAsksForFill } from '../polymarket/book-fill.util';
import { PriceFeedService } from '../polymarket/price-feed.service';
import { BalanceService } from '../polymarket/balance.service';
import { parseStreamsConfig, StreamDefinition } from './stream-config';
import { EntryGateEngine, EntryDiagnostics, GateContext } from './entry-gate.engine';
import { EdgeWeights, DEFAULT_EDGE_WEIGHTS } from './edge-score.util';
import { LimitTier, computeTier as computeTierPure, pickFavorite as pickFavoritePure, roundToTick as roundToTickPure, limitOrderTargetUsd as limitOrderTargetUsdPure } from './market-decision.util';

type PendingGateMode = 'block' | 'pre_resolve' | 'open';

interface RestingOrder {
  tier: LimitTier;
  outcome: Outcome;
  price: number;
  // null в смоуке (ничего реального не выставляли)
  orderId: string | null;
  // Момент, когда резюм-лимитка была выставлена (для orderSentAt в логе —
  // фактическое исполнение может случиться намного позже, вплоть до
  // истечения окна, см. BACKLOG "нужно время отправки ордера и его осуществления").
  placedAt: Date;
}

interface MarketState {
  // = stream.streamKey; хранится в колонке assetPrefix (переиспользуем
  // существующую схему — она и раньше кодировала "актив+таймфрейм", просто
  // Attempt раньше её игнорировал, см. п.3 бэклога).
  assetPrefix: string;
  slug: string;
  closesAt: Date;
  yesTokenId: string;
  noTokenId: string;
  negRisk: boolean;
  // conditionId маркета из Gamma (для клейма после резолва, см. RedeemService).
  // Может быть null, если Gamma почему-то его не отдала — в этом случае
  // авторедим этого шага просто не сможет случиться (лог/алерт в RedeemService).
  conditionId: string | null;
  minOrderSize: number;
  stream: MarketWsStream;
  books: Record<Outcome, LiveBook>;
  positioned: boolean;
  finalized: boolean;
  logWritten: boolean;
  restingOrder: RestingOrder | null;
  // Тир, на котором мы уже один раз убедились, что глубины/бюджета не хватает —
  // чтобы не долбить лог на каждый WS-тик одним и тем же выводом (это и был баг со спамом).
  skippedLimitTier: LimitTier | null;
  lastMarketAttemptAt: number;
  // Троттл и in-flight-гард для лимит-пути: placeOrReplaceLimit вызывается
  // void'ом на каждый WS-тик стакана, поэтому без этих двух защит при любой
  // ошибке постановки (rate-limit, реджект подписи) он уходил в сотни
  // параллельных запросов/сек к /order (см. боевой лог 09/18 21:18).
  lastLimitAttemptAt: number;
  limitInFlight: boolean;
  closeTimer: NodeJS.Timeout;
  // Цена по внешнему ценовому фиду (Binance, proxy) на момент открытия окна —
  // наш локальный ориентир "точки старта" для UP/DOWN. null, если фид ещё не готов.
  referencePrice: number | null;
  // id уже записанного MarketLog — нужен, чтобы дописать close-диагностику
  // (priceAtClose/atrAtClose) в finalizeMarket, не создавая второй лог.
  marketLogId: string | null;
  // Стейк реинвест-прогрессии ЭТОГО потока, зафиксированный в момент открытия
  // окна (снимок Attempt.currentStake на момент старта шага) — п.1 бэклога.
  // Снимаем один раз при открытии, а не читаем на каждый тик, чтобы ставка
  // внутри уже открытого окна не "поехала", если резолвер параллельно
  // подвинет currentStake по другому, ещё не закрытому шагу того же потока
  // (при последовательных окнах такого не бывает, но так честнее и проще
  // рассуждать про инвариант "ставка шага фиксируется на его открытии").
  betAmount: number;
  // Снимок Attempt.id/currentStep НА МОМЕНТ ОТКРЫТИЯ ЭТОГО ОКНА (см. п.7
  // сессии 6 в CONTEXT.md) — writeLog обязан использовать именно эти
  // значения, а НЕ currentAttempts.get(streamKey) в момент записи лога:
  // между открытием окна и фактическим исполнением проходят десятки секунд,
  // и currentAttempts для потока может успеть смениться (например через
  // досрочное закрытие попытки, см. closeAttemptEarly) — без снимка лог
  // шага ушёл бы не в ту попытку.
  attemptId: string;
  attemptStepNumber: number;
  // Пре-резолв (Сессия 7) — заполняется в openMarket, когда стейк этого окна
  // взят не из подтверждённого Attempt.currentStake, а из предсказания
  // исхода предыдущего ещё не зарезолвленного шага (см. tryPreResolve).
  stakePredicted: boolean;
  predictedFromLogId: string | null;
  // Троттлинг лога "вход заблокирован ATR-гейтом" (Сессия 9, баг из прод-логов:
  // onBookUpdate дёргается на каждый тик стакана во время окна входа — без
  // троттлинга это давало десятки одинаковых строк в секунду, пока гейт
  // держит блокировку). Отдельное поле НА ОКНО (не глобальная карта, как для
  // discoveryTick) — окно и так живёт не дольше нескольких минут, поэтому
  // сбрасывать вручную не нужно, новое окно = новый объект = чистый счётчик.
  lastEntryGateLogAt: number;
  // Уже залогировали переход в "окно входа" (последние LAST_ENTRY_WINDOW_SEC
  // секунд) для этого маркета? Чтобы не спамить лог на каждый WS-тик до
  // наступления этого момента — см. onBookUpdate.
  lastMinuteAnnounced: boolean;
  // Длительность окна этого потока в секундах (снимок stream.intervalSec на
  // момент открытия) — нужна фильтру Expected Move для масштабирования
  // требуемой дельты по доле оставшегося времени ИМЕННО этого окна (Сессия 13).
  intervalSec: number;
  // Официальный момент старта окна в мс (windowStartTs*1000 из openMarket),
  // тот же, что использован для referencePrice — null, если окно открыто не
  // через штатный discoveryTick (см. openMarket). Нужен Time-in-Zone фильтру
  // (Сессия 13) для отсчёта "доли прошедшего времени окна".
  windowStartMs: number | null;
  // Сессия 14: эффективные (уже разрешённые из per-stream override ??
  // глобальный ENV-дефолт) окно входа и границы тиров лимитки — снимок на
  // момент открытия окна, чтобы 5m/15m/1h могли настраиваться независимо
  // (см. StreamDefinition.lastEntryWindowSec/tier2Seconds/tier3Seconds).
  lastEntryWindowSec: number;
  tier2Seconds: number;
  tier3Seconds: number;
}

const EMPTY_BOOK = (outcome: Outcome, tickSize: string): LiveBook => ({
  outcome,
  tickSize,
  asks: [],
  bids: [],
  bestAsk: null,
  bestBid: null,
});

@Injectable()
export class TradingService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TradingService.name);

  private isSmoke: boolean;
  // Запас поверх стейка шага, который должен оставаться свободным на CLOB-
  // балансе, чтобы окно вообще открывалось в лайве (см. BalanceService и
  // обсуждение "клейм не мгновенный — деньги могут быть ещё не заклеймлены").
  private minBalanceBufferUsd: number;
  private minMarketPrice: number;
  private maxMarketPrice: number;
  private favoriteBidThreshold: number;
  private tierPrices: Record<LimitTier, number>;
  private tier2Seconds: number;
  private tier3Seconds: number;
  // Не пытаемся входить (ни маркетом, ни лимиткой) раньше, чем останется
  // это количество секунд до закрытия окна. Чем раньше пытаться войти, тем
  // менее уверенно рынок ещё определился с направлением — по факту оба
  // недавних слива случились именно на ранних, "неуверенных" входах, когда
  // маркетмейкер уже давал ¢99, а цена потом успевала развернуться. Лучше
  // пропустить шаг, чем рисковать капиталом на неопределившемся рынке.
  private lastEntryWindowSec: number;
  private maxOverspendMultiplier: number;
  private minFillRatio: number;
  private targetSteps: number;
  private targetProfitUsd: number;
  private discoveryPollMs: number;
  private resolvePollMs: number;

  // Независимые потоки (актив × таймфрейм) — см. п.3/п.4 бэклога и
  // src/trading/stream-config.ts. Раньше был единственный this.betAmount и
  // единственный assetPrefixes[] с общим счётчиком шагов на все активы сразу.
  private readonly streams: StreamDefinition[];
  private readonly streamByKey: Map<string, StreamDefinition>;

  // --- ATR-гейт входа (см. README/обсуждение) ---
  // По умолчанию выключен (SHADOW-режим): диагностика считается и пишется в
  // каждый лог всегда, а блокировка входа включается явно через .env только
  // после того, как накопится статистика по реальным сливам.
  private entryFilterEnabled: boolean;
  private minDistanceAtrRatio: number;

  // --- Сессия 13: 4 доп. фильтра входа (обсуждение с Gemini по реальным
  // сливам, см. CONTEXT.md) — КАЖДЫЙ включается/выключается своим ENV
  // НЕЗАВИСИМО от остальных и от entryFilterEnabled выше (тот управляет
  // только исходным статическим ATR-рацио-гейтом, как и раньше). Все по
  // умолчанию выключены (false) — поведение бота не меняется, пока каждый
  // не включат явно после калибровки по накопленной в market_logs статистике
  // (диагностика для всех четырёх пишется в лог ВСЕГДА, независимо от флага). ---
  private blackoutHoursFilterEnabled: boolean;
  private blackoutHoursUtc: Set<number>;
  private expectedMoveFilterEnabled: boolean;
  private safetyKFactor: number;
  private directionalDriftFilterEnabled: boolean;
  private driftLookbackSec: number;
  private timeInZoneFilterEnabled: boolean;
  private minZoneRatio: number;
  // Сессия 18 — см. edge-score.util.ts.
  private edgeGateEnabled: boolean;
  private edgeWeights: EdgeWeights;
  private edgeMargin: number;
  private edgeSmoothnessLookbackSec: number;
  // Движок ATR-гейта (Сессия 16) — вынесен в отдельный класс без привязки к
  // NestJS/MarketState (см. entry-gate.engine.ts), чтобы модуль бэктеста мог
  // ЧЕСТНО переиспользовать РОВНО ТУ ЖЕ логику принятия решения о входе, а
  // не переписывать её "по мотивам" (см. BACKTEST-PLAN.md — требование
  // пользователя дословно об этом). Живой путь (TradingService) и бэктест
  // (BacktestRunnerService) отличаются только тем, ЧТО подставляется как
  // IPriceSource/IClock (живой PriceFeedService+Date.now() здесь, реплей
  // исторических тиков там) — сама логика гейта не продублирована.
  private readonly gateEngine: EntryGateEngine;
  // Порог "зависшего" резолва — сколько может провисеть pending_resolve лог,
  // прежде чем мы начнём предупреждать в логах/на фронте (не блокирует торговлю).
  private staleResolveWarnMs: number;

  // Три режима реакции на "предыдущий шаг потока ещё не зарезолвлен
  // официально Gamma" (Сессия 6 п.7 + Сессия 7, см. CONTEXT.md):
  //  - 'block'       — не открываем новое окно, пока не придёт официальный
  //                    резолв. Самый безопасный, изредка пропускает шаг,
  //                    если Gamma отвечает с задержкой. Дефолт.
  //  - 'pre_resolve' — открываем новое окно, но СУММУ стейка берём не из
  //                    Attempt.currentStake (он ещё не обновлён), а из
  //                    предсказания исхода предыдущего шага по живой цене
  //                    Chainlink (см. tryPreResolve) — эта же цена и есть
  //                    источник, которым Gamma резолвит крипто-маркеты, так
  //                    что при уверенном сигнале расхождение с официальным
  //                    резолвом крайне маловероятно. Если предсказание
  //                    недоступно/неуверенное на конкретном тике — на ЭТОМ
  //                    тике ведёт себя как 'block' (безопасный фолбэк), не
  //                    открывает окно вслепую.
  //  - 'open'        — легаси-режим без какой-либо защиты (открывает окно
  //                    с текущим Attempt.currentStake как есть, это и есть
  //                    исходный баг из п.7 сессии 6). Оставлен только для
  //                    явного осознанного выбора, использовать не рекомендуется.
  private pendingGateMode: PendingGateMode;
  private preResolveMinAtrRatio: number;
  // Потолок на число ПОДРЯД идущих окон, открытых через предсказание без
  // хотя бы одного официального подтверждения между ними — не даём риску
  // накапливаться бесконтрольно, если Gamma зависла надолго (см. tryPreResolve).
  private preResolveMaxChain: number;

  // По одному активному Attempt на каждый streamKey — независимая
  // прогрессия/прогресс для каждого потока (п.3 бэклога).
  private currentAttempts = new Map<string, Attempt>();
  private activeMarkets = new Map<string, MarketState>();

  /**
   * Сессия 17 — единственная точка входа для EdgeSamplerService (см.
   * edge-sampler.service.ts). Отдаёт СНИМОК того, что нужно для
   * диагностического сэмплирования, НЕ давая внешнему коду доступа к
   * MarketState целиком (там живут restingOrder/positioned и прочее
   * состояние, трогать которое снаружи TradingService не должен никто).
   * Намеренно не отдаёт сам объект MarketState — только копию нужных полей.
   */
  getActiveMarketsForSampling(): {
    assetPrefix: string;
    slug: string;
    closesAt: Date;
    referencePrice: number | null;
    windowStartMs: number | null;
    intervalSec: number;
    favoriteOutcome: Outcome | null;
    favoriteBestAsk: number | null;
  }[] {
    const out: ReturnType<TradingService['getActiveMarketsForSampling']> = [];
    for (const ms of this.activeMarkets.values()) {
      if (ms.finalized) continue;
      const favoriteOutcome = this.pickFavorite(ms.books);
      out.push({
        assetPrefix: ms.assetPrefix,
        slug: ms.slug,
        closesAt: ms.closesAt,
        referencePrice: ms.referencePrice,
        windowStartMs: ms.windowStartMs,
        intervalSec: ms.intervalSec,
        favoriteOutcome,
        favoriteBestAsk: favoriteOutcome ? (ms.books[favoriteOutcome]?.bestAsk ?? null) : null,
      });
    }
    return out;
  }
  // streamKey -> id логов, ещё не зарезолвленных (status='pending_resolve').
  // Заполняется в writeLog, чистится в resolvePendingMarkets, восстанавливается
  // из БД в onModuleInit (переживает рестарт процесса) — см. pendingGateMode.
  private pendingByStream = new Map<string, Set<string>>();
  // streamKey -> сколько подряд окон открыто через pre_resolve без
  // промежуточного официального подтверждения (см. preResolveMaxChain).
  private provisionalChainByStream = new Map<string, number>();
  // Троттлинг DEBUG-сообщений о пропуске окна в discoveryTick (Сессия 8,
  // баг №2): тик обнаружения гоняется каждые discoveryPollMs (по умолчанию
  // 1.5с) — без троттлинга обычная ситуация "ждём резолва Gamma" превращала
  // логи в сплошной спам (десятки одинаковых строк в минуту на поток).
  // Реальная задержка резолва и так видна по отдельному [STALE]-предупреждению
  // (staleResolveWarnMs) — этот лог нужен только для локальной отладки, не
  // для постоянного потока.
  private lastGateSkipLogAt = new Map<string, number>();
  // Рубильник потока (см. StreamRuntimeConfig.enabled) — кэш в памяти,
  // читается на каждом discoveryTick (раз в ~1.5с), поэтому не ходим в БД
  // на каждый тик: грузим один раз в onModuleInit и обновляем синхронно
  // из setEnabled при PATCH с дашборда.
  private enabledByStream = new Map<string, boolean>();
  private static readonly GATE_SKIP_LOG_THROTTLE_MS = 30_000;
  private static readonly ENTRY_GATE_LOG_THROTTLE_MS = 15_000;
  // Порог "подозрительно большого" лага найденного тика от истинной границы
  // окна (Сессия 12) — чисто информационный, ничего не блокирует.
  private static readonly REFERENCE_LAG_WARN_MS = 3_000;
  private stopped = false;

  constructor(
    private readonly config: ConfigService,
    private readonly gamma: GammaMarketService,
    private readonly clobPublic: ClobPublicService,
    private readonly trader: PolymarketTraderService,
    private readonly priceFeed: PriceFeedService,
    private readonly balanceService: BalanceService,
    @InjectRepository(Attempt) private readonly attemptRepo: Repository<Attempt>,
    @InjectRepository(MarketLog) private readonly marketLogRepo: Repository<MarketLog>,
    @InjectRepository(StreamRuntimeConfig) private readonly streamConfigRepo: Repository<StreamRuntimeConfig>,
    @InjectRepository(ActiveWindow) private readonly activeWindowRepo?: Repository<ActiveWindow>,
  ) {
    this.isSmoke = this.config.get<string>('SMOKE_START', 'true') === 'true';
    this.minBalanceBufferUsd = parseFloat(this.config.get<string>('MIN_BALANCE_BUFFER_USD', '0'));
    this.minMarketPrice = parseFloat(this.config.get<string>('MIN_MARKET_PRICE', '0.99'));
    this.maxMarketPrice = parseFloat(this.config.get<string>('MAX_MARKET_PRICE', '0.999'));
    this.favoriteBidThreshold = parseFloat(
      this.config.get<string>('FAVORITE_BID_THRESHOLD', '0.90'),
    );
    this.tierPrices = {
      T1: parseFloat(this.config.get<string>('LIMIT_TIER1_PRICE', '0.99')),
      T2: parseFloat(this.config.get<string>('LIMIT_TIER2_PRICE', '0.995')),
      T3: parseFloat(this.config.get<string>('LIMIT_TIER3_PRICE', '0.999')),
    };
    this.tier2Seconds = parseInt(this.config.get<string>('LIMIT_TIER2_SECONDS', '150'), 10);
    this.tier3Seconds = parseInt(this.config.get<string>('LIMIT_TIER3_SECONDS', '60'), 10);
    this.lastEntryWindowSec = parseInt(this.config.get<string>('LAST_ENTRY_WINDOW_SEC', '60'), 10);
    this.maxOverspendMultiplier = parseFloat(
      this.config.get<string>('MAX_OVERSPEND_MULTIPLIER', '1.5'),
    );
    this.minFillRatio = parseFloat(this.config.get<string>('MIN_FILL_RATIO', '0.5'));
    this.targetSteps = parseInt(this.config.get<string>('TARGET_STEPS', '500'), 10);
    // Дефолт цели по прибыли (см. "частичный фикс" — Сессия 18): применяется
    // ТОЛЬКО при создании новой попытки, как и targetSteps. Дальнейшее
    // переключение режима (steps/profit) — динамическое, через
    // StreamRuntimeConfig/дашборд, ENV тут не участвует.
    this.targetProfitUsd = parseFloat(this.config.get<string>('TARGET_PROFIT_USD', '20'));
    this.discoveryPollMs = parseInt(this.config.get<string>('MARKET_DISCOVERY_POLL_MS', '1500'), 10);
    this.resolvePollMs = parseInt(this.config.get<string>('RESOLVE_POLL_INTERVAL_MS', '10000'), 10);

    this.streams = parseStreamsConfig(this.config.get<string>('STREAMS_CONFIG'));
    this.streamByKey = new Map(this.streams.map((s) => [s.streamKey, s]));

    // Сессия 14: кросс-валидация per-stream окна входа/тиров С УЧЁТОМ
    // глобальных ENV-дефолтов (parseStreamsConfig проверяет только явно
    // заданные на потоке поля между собой — здесь же нужно проверить и
    // смешанные случаи вроде "поток переопределил только tier2Seconds, а
    // lastEntryWindowSec берёт из глобального ENV").
    for (const s of this.streams) {
      const effWindow = s.lastEntryWindowSec ?? this.lastEntryWindowSec;
      const effT2 = s.tier2Seconds ?? this.tier2Seconds;
      const effT3 = s.tier3Seconds ?? this.tier3Seconds;
      if (effT2 <= effT3) {
        throw new Error(
          `Поток ${s.streamKey}: эффективный tier2Seconds (${effT2}) должен быть больше tier3Seconds (${effT3}) — проверьте STREAMS_CONFIG/LIMIT_TIER2_SECONDS/LIMIT_TIER3_SECONDS.`,
        );
      }
      if (effT2 > effWindow) {
        throw new Error(
          `Поток ${s.streamKey}: эффективный tier2Seconds (${effT2}) не должен превышать lastEntryWindowSec (${effWindow}) — T2 был бы недостижим.`,
        );
      }
    }

    this.entryFilterEnabled = this.config.get<string>('ENTRY_FILTER_ENABLED', 'true') === 'true';
    this.minDistanceAtrRatio = parseFloat(this.config.get<string>('MIN_DISTANCE_ATR_RATIO', '1.5'));
    this.staleResolveWarnMs = parseInt(this.config.get<string>('STALE_RESOLVE_WARN_MS', '180000'), 10);

    const rawMode = (this.config.get<string>('PENDING_GATE_MODE', '') ?? '').trim().toLowerCase();
    if (rawMode === 'block' || rawMode === 'pre_resolve' || rawMode === 'open') {
      this.pendingGateMode = rawMode;
    } else {
      // Обратная совместимость со старым булевым флагом (Сессия 6) — если
      // новый PENDING_GATE_MODE не задан явно, но задан старый, мапим его.
      const legacy = this.config.get<string>('BLOCK_ORDERS_IF_PENDING');
      if (legacy != null && legacy !== '') {
        this.pendingGateMode = legacy === 'true' ? 'block' : 'open';
        this.logger.warn(
          `BLOCK_ORDERS_IF_PENDING устарел (Сессия 7) — используйте PENDING_GATE_MODE=block|pre_resolve|open. ` +
            `Сейчас смаплено в PENDING_GATE_MODE=${this.pendingGateMode}.`,
        );
      } else {
        this.pendingGateMode = 'block'; // безопасный дефолт
      }
    }
    this.preResolveMinAtrRatio = parseFloat(this.config.get<string>('PRE_RESOLVE_MIN_ATR_RATIO', '2'));
    this.preResolveMaxChain = parseInt(this.config.get<string>('PRE_RESOLVE_MAX_CHAIN', '1'), 10);

    // --- Сессия 13: доп. фильтры входа, все по умолчанию выключены ---
    this.blackoutHoursFilterEnabled = this.config.get<string>('BLACKOUT_HOURS_FILTER_ENABLED', 'false') === 'true';
    this.blackoutHoursUtc = new Set(
      (this.config.get<string>('BLACKOUT_HOURS_UTC', '') ?? '')
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
        .map((s) => parseInt(s, 10))
        .filter((n) => Number.isFinite(n)),
    );
    this.expectedMoveFilterEnabled = this.config.get<string>('EXPECTED_MOVE_FILTER_ENABLED', 'false') === 'true';
    this.safetyKFactor = parseFloat(this.config.get<string>('SAFETY_K_FACTOR', '1.5'));
    this.directionalDriftFilterEnabled = this.config.get<string>('DIRECTIONAL_DRIFT_FILTER_ENABLED', 'false') === 'true';
    this.driftLookbackSec = parseInt(this.config.get<string>('DRIFT_LOOKBACK_SEC', '10'), 10);
    this.timeInZoneFilterEnabled = this.config.get<string>('TIME_IN_ZONE_FILTER_ENABLED', 'false') === 'true';
    this.minZoneRatio = parseFloat(this.config.get<string>('MIN_ZONE_RATIO', '0.65'));

    // --- Сессия 18: edge-модель (см. edge-score.util.ts). Веса РУЧНЫЕ, не
    // откалиброванные — поэтому edgeGateEnabled по умолчанию false: p_model
    // считается и логируется всегда (shadow), но реальный вход блокирует
    // только при явном EDGE_GATE_ENABLED=true. ---
    this.edgeGateEnabled = this.config.get<string>('EDGE_GATE_ENABLED', 'false') === 'true';
    this.edgeMargin = parseFloat(this.config.get<string>('EDGE_MARGIN', '0.02'));
    this.edgeSmoothnessLookbackSec = parseInt(this.config.get<string>('EDGE_SMOOTHNESS_LOOKBACK_SEC', '30'), 10);
    this.edgeWeights = {
      bias: parseFloat(this.config.get<string>('EDGE_W_BIAS', String(DEFAULT_EDGE_WEIGHTS.bias))),
      z: parseFloat(this.config.get<string>('EDGE_W_Z', String(DEFAULT_EDGE_WEIGHTS.z))),
      drift: parseFloat(this.config.get<string>('EDGE_W_DRIFT', String(DEFAULT_EDGE_WEIGHTS.drift))),
      zone: parseFloat(this.config.get<string>('EDGE_W_ZONE', String(DEFAULT_EDGE_WEIGHTS.zone))),
      smoothSigned: parseFloat(this.config.get<string>('EDGE_W_SMOOTH', String(DEFAULT_EDGE_WEIGHTS.smoothSigned))),
    };

    this.gateEngine = new EntryGateEngine(
      {
        entryFilterEnabled: this.entryFilterEnabled,
        minDistanceAtrRatio: this.minDistanceAtrRatio,
        blackoutHoursFilterEnabled: this.blackoutHoursFilterEnabled,
        blackoutHoursUtc: this.blackoutHoursUtc,
        expectedMoveFilterEnabled: this.expectedMoveFilterEnabled,
        safetyKFactor: this.safetyKFactor,
        directionalDriftFilterEnabled: this.directionalDriftFilterEnabled,
        driftLookbackSec: this.driftLookbackSec,
        timeInZoneFilterEnabled: this.timeInZoneFilterEnabled,
        minZoneRatio: this.minZoneRatio,
        edgeGateEnabled: this.edgeGateEnabled,
        edgeWeights: this.edgeWeights,
        edgeMargin: this.edgeMargin,
        edgeSmoothnessLookbackSec: this.edgeSmoothnessLookbackSec,
      },
      this.priceFeed,
    );
  }

  async onModuleInit() {
    if (!this.isSmoke) {
      try {
        await this.trader.ensureClient();
      } catch (err) {
        this.logger.error(
          `Не удалось инициализировать боевой торговый клиент — принудительно ` +
            `переключаюсь в SMOKE-режим. Причина: ${this.errMsg(err)}`,
        );
        this.isSmoke = true;
      }
    }

    for (const stream of this.streams) {
      const attempt = await this.getOrCreateActiveAttempt(stream);
      this.currentAttempts.set(stream.streamKey, attempt);
    }

    // Рубильник потока — грузим текущее состояние из БД (переживает рестарт:
    // если поставил на паузу вчера и не включил обратно, после рестарта
    // останется на паузе, а не тихо оживёт).
    const configRows = await this.streamConfigRepo.find();
    const enabledByKey = new Map(configRows.map((r) => [r.streamKey, r.enabled]));
    for (const stream of this.streams) {
      // Пустая БД (после очистки или на первом деплое) = нет строки для
      // этого streamKey -> по умолчанию ПАУЗА, а не автозапуск. Раньше было
      // "?? true" — свежий деплой без единой строки в stream_config сразу
      // начинал реально торговать, что требовало руками сеять enabled=false
      // заранее, чтобы стартовать на паузе. Явно записанное значение (true
      // ИЛИ false) из БД по-прежнему в приоритете — эта смена дефолта не
      // трогает уже настроенные потоки, только те, для которых в БД вообще
      // нет строки.
      this.enabledByStream.set(stream.streamKey, enabledByKey.get(stream.streamKey) ?? false);
    }

    // Восстанавливаем pendingByStream из БД — переживает рестарт процесса.
    // Без этого после рестарта bloqueOrdersIfPending "забыл" бы про шаг,
    // который уже был отправлен до рестарта и всё ещё не зарезолвлен.
    const stillPending = await this.marketLogRepo.find({ where: { status: 'pending_resolve' } });
    for (const log of stillPending) {
      const set = this.pendingByStream.get(log.assetPrefix) ?? new Set<string>();
      set.add(log.id);
      this.pendingByStream.set(log.assetPrefix, set);
    }
    if (stillPending.length > 0) {
      this.logger.log(
        `Восстановлено ${stillPending.length} незарезолвленных шагов из БД после рестарта: ` +
          [...this.pendingByStream.entries()].map(([k, v]) => `${k}=${v.size}`).join(', '),
      );
    }

    this.logger.log(
      `Старт. Режим: ${this.isSmoke ? 'SMOKE (без реальных ордеров)' : 'LIVE (реальные деньги)'}. ` +
        `Потоки (${this.streams.length}): ` +
        this.streams
          .map((s) => {
            const a = this.currentAttempts.get(s.streamKey)!;
            const enabled = this.enabledByStream.get(s.streamKey) ?? false;
            return `${s.streamKey}[попытка #${a.attemptNumber}, шаг ${a.currentStep}/${a.targetSteps}, стейк $${a.currentStake.toFixed(2)}${enabled ? '' : ', ПАУЗА — включить: PATCH /trading/settings/' + s.streamKey}]`;
          })
          .join('; ') +
        `. Маркет-тейк [¢${this.minMarketPrice * 100}-¢${this.maxMarketPrice * 100}] (минимум заполнения ${this.minFillRatio * 100}%), ` +
        `лимитки-фолбэк от ¢${this.favoriteBidThreshold * 100} (тиры ${this.tierPrices.T1 * 100}/${this.tierPrices.T2 * 100}/${this.tierPrices.T3 * 100}). ` +
        `Окно входа (глобальный дефолт): последние ${this.lastEntryWindowSec}с до закрытия (раньше — не пытаемся войти вообще). ` +
        `Сессия 14, per-stream окно/тиры: ` +
        this.streams
          .map((s) => {
            const win = s.lastEntryWindowSec ?? this.lastEntryWindowSec;
            const t2 = s.tier2Seconds ?? this.tier2Seconds;
            const t3 = s.tier3Seconds ?? this.tier3Seconds;
            const overridden = s.lastEntryWindowSec != null || s.tier2Seconds != null || s.tier3Seconds != null;
            return `${s.streamKey}=[окно ${win}с/T2 ${t2}с/T3 ${t3}с${overridden ? ', переопределено' : ', глобальный дефолт'}]`;
          })
          .join('; ') +
        `. ` +
        `ATR-гейт входа: ${this.entryFilterEnabled ? `ВКЛЮЧЁН (мин. ${this.minDistanceAtrRatio}x ATR, при недоступной диагностике — пропуск шага, не вход вслепую)` : 'выключен (только диагностика в логах)'}. ` +
        `Edge-модель (Сессия 18): p_model считается всегда (shadow); как ГЕЙТ ${this.edgeGateEnabled ? `ВКЛЮЧЕНА — маржа ${this.edgeMargin}, веса bias=${this.edgeWeights.bias}/z=${this.edgeWeights.z}/drift=${this.edgeWeights.drift}/zone=${this.edgeWeights.zone}/smooth=${this.edgeWeights.smoothSigned} (ВНИМАНИЕ: веса ручные, не откалиброваны на истории)` : 'выключена (EDGE_GATE_ENABLED=false — реальные входы не блокирует)'}. `,
        `Доп. фильтры (Сессия 13): blackout-hours=${this.blackoutHoursFilterEnabled ? `ВКЛ (${[...this.blackoutHoursUtc].join(',') || 'список пуст'})` : 'выкл'}, ` +
        `expected-move=${this.expectedMoveFilterEnabled ? `ВКЛ (k=${this.safetyKFactor})` : 'выкл'}, ` +
        `directional-drift=${this.directionalDriftFilterEnabled ? `ВКЛ (lookback=${this.driftLookbackSec}с)` : 'выкл'}, ` +
        `time-in-zone=${this.timeInZoneFilterEnabled ? `ВКЛ (мин. ${this.minZoneRatio})` : 'выкл'} — диагностика для всех четырёх пишется в market_logs всегда, независимо от флагов.`,
    );

    this.startDiscoveryLoop();
    this.startResolverLoop();
  }

  onModuleDestroy() {
    this.stopped = true;
    for (const marketState of this.activeMarkets.values()) {
      clearTimeout(marketState.closeTimer);
      marketState.stream.close();
    }
  }

  private async getOrCreateActiveAttempt(stream: StreamDefinition): Promise<Attempt> {
    const active = await this.attemptRepo.findOne({
      where: { status: 'active', isSmoke: this.isSmoke, streamKey: stream.streamKey },
      order: { createdAt: 'DESC' },
    });
    if (active) return active;

    const last = await this.attemptRepo.findOne({
      where: { isSmoke: this.isSmoke, streamKey: stream.streamKey },
      order: { attemptNumber: 'DESC' },
    });
    const attempt = this.attemptRepo.create({
      attemptNumber: (last?.attemptNumber ?? 0) + 1,
      streamKey: stream.streamKey,
      currentStep: 0,
      targetSteps: this.targetSteps,
      targetProfitUsd: this.targetProfitUsd,
      realizedProfit: 0,
      baseStake: stream.baseStake,
      currentStake: stream.baseStake,
      status: 'active',
      isSmoke: this.isSmoke,
      finishedAt: null,
    });
    return this.attemptRepo.save(attempt);
  }

  /**
   * Режим закрытия попытки на поток (Сессия 18, "частичный фикс") — какой
   * критерий из двух (targetSteps vs targetProfitUsd) реально триггерит
   * завершение попытки. Хранится в БД (StreamRuntimeConfig), меняется на
   * лету через PATCH /trading/settings/:streamKey, без редеплоя. Если строки
   * нет — 'steps' (поведение по умолчанию, как было всегда).
   */
  async getCloseMode(streamKey: string): Promise<CloseMode> {
    const row = await this.streamConfigRepo.findOne({ where: { streamKey } });
    return row?.closeMode === 'profit' ? 'profit' : 'steps';
  }

  /** Режимы по ВСЕМ известным потокам сразу — для экрана настроек. */
  async getCloseModes(): Promise<
    Array<{ streamKey: string; closeMode: CloseMode; enabled: boolean; hasActiveWindow: boolean; pendingSteps: number }>
  > {
    const rows = await this.streamConfigRepo.find();
    const byKey = new Map(rows.map((r) => [r.streamKey, r]));
    return this.streams.map((s) => ({
      streamKey: s.streamKey,
      closeMode: byKey.get(s.streamKey)?.closeMode ?? 'steps',
      enabled: this.enabledByStream.get(s.streamKey) ?? true,
      // Чтобы на дашборде было видно "пауза, но ждём завершения текущей
      // сделки" — активное окно и/или ещё не зарезолвленные шаги.
      hasActiveWindow: this.activeMarkets.has(s.streamKey),
      pendingSteps: this.pendingByStream.get(s.streamKey)?.size ?? 0,
    }));
  }

  async setCloseMode(streamKey: string, closeMode: CloseMode): Promise<void> {
    if (!this.streamByKey.has(streamKey)) {
      throw new NotFoundException(`Неизвестный поток "${streamKey}".`);
    }
    await this.streamConfigRepo.upsert({ streamKey, closeMode }, ['streamKey']);
    this.logger.log(`[${streamKey}] Режим закрытия попытки переключён на "${closeMode}".`);
  }

  /**
   * Рубильник потока (пауза/возобновление) — см. StreamRuntimeConfig.enabled
   * и гейт в discoveryTick. Не трогает уже открытую позицию: при выключении
   * discoveryTick просто перестаёт открывать новые окна, а всё, что уже в
   * процессе (WS-стрим текущего окна, резолв, клейм), доводится до конца
   * как обычно — отдельными циклами, которые от этого флага не зависят.
   */
  async setEnabled(streamKey: string, enabled: boolean): Promise<void> {
    if (!this.streamByKey.has(streamKey)) {
      throw new NotFoundException(`Неизвестный поток "${streamKey}".`);
    }
    await this.streamConfigRepo.upsert({ streamKey, enabled }, ['streamKey']);
    this.enabledByStream.set(streamKey, enabled);
    this.logger.log(
      `[${streamKey}] Поток ${enabled ? 'возобновлён' : 'поставлен на паузу'} ` +
        `(активное окно есть: ${this.activeMarkets.has(streamKey)}, незарезолвленных шагов: ${this.pendingByStream.get(streamKey)?.size ?? 0}).`,
    );
  }

  /**
   * Ручной патч ТЕКУЩЕГО прогресса попытки (не дефолтов потока) — см.
   * PATCH /trading/attempts/:id. Каждое поле независимо опционально;
   * обновляем только то, что реально передали, остальное не трогаем.
   * Держим this.currentAttempts в актуальном состоянии, если патчим именно
   * активную попытку потока — иначе следующий resolvePendingMarkets мог бы
   * поработать со старым закэшированным значением.
   */
  async patchAttempt(
    id: string,
    patch: { currentStep?: number; realizedProfit?: number; targetSteps?: number; targetProfitUsd?: number },
  ): Promise<Attempt> {
    const attempt = await this.attemptRepo.findOne({ where: { id } });
    if (!attempt) {
      throw new NotFoundException(`Попытка ${id} не найдена.`);
    }
    if (patch.currentStep !== undefined) attempt.currentStep = patch.currentStep;
    if (patch.realizedProfit !== undefined) attempt.realizedProfit = patch.realizedProfit;
    if (patch.targetSteps !== undefined) attempt.targetSteps = patch.targetSteps;
    if (patch.targetProfitUsd !== undefined) attempt.targetProfitUsd = patch.targetProfitUsd;
    const saved = await this.attemptRepo.save(attempt);

    if (attempt.status === 'active' && this.currentAttempts.get(attempt.streamKey)?.id === attempt.id) {
      this.currentAttempts.set(attempt.streamKey, saved);
    }
    this.logger.log(
      `[${attempt.streamKey}] Попытка #${attempt.attemptNumber} вручную отредактирована: ` +
        `${Object.entries(patch)
          .map(([k, v]) => `${k}=${v}`)
          .join(', ')}.`,
    );
    return saved;
  }

  /**
   * Досрочное закрытие попытки (п.5 сессии 6, см. CONTEXT.md) — вызывается
   * из TradingController по POST /trading/attempts/:id/close-early. Не
   * дожидаемся ни проигрыша, ни достижения targetSteps: фиксируем текущий
   * прогресс/профит попытки как есть и сразу поднимаем для того же потока
   * новую активную попытку со сбросом на baseStake (ровно как при
   * проигрыше) — бот продолжает торговать потоком без ручного рестарта.
   *
   * Если на потоке в этот момент есть открытое (positioned/pending) окно —
   * его резолвер довьёт обычным порядком (см. resolvePendingMarkets); он
   * уже привязан к СТАРОМУ attemptId по снимку в MarketState (см. п.7 сессии
   * 6), так что не "утечёт" в новую попытку и не исказит её прогрессию.
   */
  async closeAttemptEarly(attemptId: string): Promise<Attempt> {
    const attempt = await this.attemptRepo.findOne({ where: { id: attemptId } });
    if (!attempt) {
      throw new NotFoundException(`Попытка ${attemptId} не найдена.`);
    }
    if (attempt.status !== 'active') {
      throw new BadRequestException(`Попытка ${attemptId} уже не активна (status=${attempt.status}) — закрывать нечего.`);
    }

    attempt.status = 'closed_early';
    attempt.finishedAt = new Date();
    await this.attemptRepo.save(attempt);

    const stream = this.streamByKey.get(attempt.streamKey);
    const baseStake = stream?.baseStake ?? attempt.baseStake;
    const next = this.attemptRepo.create({
      attemptNumber: attempt.attemptNumber + 1,
      streamKey: attempt.streamKey,
      currentStep: 0,
      targetSteps: attempt.targetSteps,
      targetProfitUsd: attempt.targetProfitUsd,
      realizedProfit: 0,
      baseStake,
      currentStake: baseStake,
      status: 'active',
      isSmoke: attempt.isSmoke,
      finishedAt: null,
    });
    const saved = await this.attemptRepo.save(next);
    this.currentAttempts.set(attempt.streamKey, saved);

    this.logger.log(
      `[${attempt.streamKey}] Попытка #${attempt.attemptNumber} закрыта досрочно вручную на шаге ${attempt.currentStep} ` +
        `(стейк $${attempt.currentStake.toFixed(2)}). Открыта новая попытка #${saved.attemptNumber} со стейком $${baseStake.toFixed(2)}.`,
    );

    return saved;
  }

  // ---------------------------------------------------------------------
  // Обнаружение маркетов — независимо по каждому настроенному потоку.
  // ---------------------------------------------------------------------
  private async startDiscoveryLoop() {
    while (!this.stopped) {
      try {
        await this.discoveryTick();
      } catch (err) {
        this.logger.error(`Сбой в цикле обнаружения маркета: ${this.errMsg(err)}`);
      }
      await this.sleep(this.discoveryPollMs);
    }
  }

  /** Троттлинг лога блокировки входа ATR-гейтом (Сессия 9) — см. MarketState.lastEntryGateLogAt. */
  private logEntryGateBlockThrottled(marketState: MarketState, message: string): void {
    const now = Date.now();
    if (now - marketState.lastEntryGateLogAt < TradingService.ENTRY_GATE_LOG_THROTTLE_MS) return;
    marketState.lastEntryGateLogAt = now;
    this.logger.log(message);
  }

  /** Троттлинг спама из discoveryTick (Сессия 8, баг №2) — см. lastGateSkipLogAt. */
  private logGateSkipThrottled(streamKey: string, message: string): void {
    const key = `${streamKey}`;
    const last = this.lastGateSkipLogAt.get(key) ?? 0;
    const now = Date.now();
    if (now - last < TradingService.GATE_SKIP_LOG_THROTTLE_MS) return;
    this.lastGateSkipLogAt.set(key, now);
    this.logger.debug(message);
  }

  private async discoveryTick(): Promise<void> {
    for (const stream of this.streams) {
      if (this.enabledByStream.get(stream.streamKey) === false) {
        // На паузе (см. setEnabled/PATCH /trading/settings) — просто не
        // открываем НОВОЕ окно. Уже открытая позиция (если есть) продолжает
        // жить своей жизнью через остальные тики/резолвер/клейм — этот гейт
        // трогает только точку входа в новое окно, ничего не обрывает.
        this.logGateSkipThrottled(
          `${stream.streamKey}:paused`,
          `[${stream.streamKey}] поток на паузе (enabled=false) — новое окно не открываем.`,
        );
        continue;
      }

      const startTs = this.gamma.currentIntervalStartTimestampSec(stream.intervalSec);
      const closeTs = this.gamma.currentIntervalCloseTimestampSec(stream.intervalSec);
      const slug = this.gamma.buildSlugForStart(stream, startTs);

      if (this.activeMarkets.get(stream.streamKey)?.slug === slug) continue; // уже отслеживаем

      let forcedBetAmount: number | null = null;
      let predictedFromLogId: string | null = null;

      const pending = this.pendingByStream.get(stream.streamKey);
      if (pending && pending.size > 0) {
        if (this.pendingGateMode === 'block') {
          // Не долбим лог на каждый тик обнаружения (1.5с) — это ожидаемое,
          // а не аварийное состояние (Gamma просто ещё не ответила closed:true).
          // Троттлинг (Сессия 8, баг №2): максимум раз в GATE_SKIP_LOG_THROTTLE_MS.
          this.logGateSkipThrottled(
            stream.streamKey,
            `[${stream.streamKey}] пропуск нового окна: ${pending.size} шаг(ов) ещё не зарезолвлены ` +
              `(PENDING_GATE_MODE=block) — ждём резолва, прежде чем снимать новый снимок стейка.`,
          );
          continue;
        }
        if (this.pendingGateMode === 'pre_resolve') {
          const prediction = await this.tryPreResolve(stream.streamKey);
          if (!prediction) {
            // Предсказание недоступно/неуверенное — на ЭТОМ тике ведём себя
            // как 'block', не открываем окно вслепую (см. tryPreResolve).
            this.logGateSkipThrottled(
              stream.streamKey,
              `[${stream.streamKey}] pre_resolve: предсказание недоступно/неуверенное — пропуск нового окна на этот тик.`,
            );
            continue;
          }
          forcedBetAmount = prediction.betAmount;
          predictedFromLogId = prediction.logId;
        }
        // 'open' — намеренно легаси-поведение без гейта, ничего не делаем.
      }

      const market = await this.gamma.fetchMarketBySlug(slug, closeTs);
      if (!market) continue; // ещё не создан на Gamma — попробуем на следующем тике

      await this.openMarket(
        stream,
        market.slug,
        market.closesAt,
        market.yesTokenId,
        market.noTokenId,
        market.negRisk,
        market.conditionId,
        forcedBetAmount,
        predictedFromLogId,
        startTs,
      );
    }
  }

  /**
   * Пре-резолв (Сессия 7, см. CONTEXT.md) — предсказывает исход ПОСЛЕДНЕГО
   * ещё не зарезолвленного официально шага потока по живой цене Chainlink,
   * чтобы можно было корректно (не вслепую и не "как было раньше") снять
   * сумму стейка для СЛЕДУЮЩЕГО окна ДО того, как Gamma подтвердит closed:true.
   *
   * Идея (предложена пользователем): открытие следующего окна фиксирует цену,
   * которая по факту и есть цена ЗАКРЫТИЯ предыдущего окна (это одна и та же
   * непрерывная лента Chainlink) — то есть если сравнить эту цену со
   * страйком (referencePrice) предыдущего шага, можно почти достоверно
   * узнать его исход ДО официального резолва Gamma, который использует
   * именно Chainlink как источник истины.
   *
   * Возвращает null (== "не уверены, лучше подождать") если:
   *  - самого pending-лога нет, либо в нём нет chosenOutcome/referencePrice
   *    (например фид был недоступен на момент ЕГО открытия);
   *  - живой фид сейчас недоступен;
   *  - дистанция от страйка меньше preResolveMinAtrRatio * ATR — слишком
   *    близко к границе, чтобы доверять предсказанию (могло дёрнуться к
   *    моменту официального резолва);
   *  - уже preResolveMaxChain окон подряд открыты через предсказание без
   *    хотя бы одного официального подтверждения между ними — не даём
   *    риску накапливаться бесконтрольно, если Gamma зависла надолго.
   */
  private async tryPreResolve(streamKey: string): Promise<{ betAmount: number; logId: string } | null> {
    const chain = this.provisionalChainByStream.get(streamKey) ?? 0;
    if (chain >= this.preResolveMaxChain) {
      this.logger.warn(
        `[${streamKey}] pre_resolve: достигнут потолок ${this.preResolveMaxChain} окон подряд без официального ` +
          `подтверждения — временно откатываемся к ожиданию резолва (защита от накопления риска).`,
      );
      return null;
    }

    const lastPending = await this.marketLogRepo.findOne({
      where: { assetPrefix: streamKey, status: 'pending_resolve' },
      order: { createdAt: 'DESC' },
    });
    if (!lastPending || !lastPending.chosenOutcome || lastPending.referencePrice == null) return null;

    const snap = this.priceFeed.getSnapshot(streamKey);
    if (snap.price == null) return null;

    const dist = Math.abs(snap.price - lastPending.referencePrice);
    const ratio = snap.atr && snap.atr > 0 ? dist / snap.atr : null;
    if (ratio == null || ratio < this.preResolveMinAtrRatio) return null;

    const predictedSide: Outcome = snap.price >= lastPending.referencePrice ? 'YES' : 'NO';
    const predictedWin = predictedSide === lastPending.chosenOutcome;

    const stream = this.streamByKey.get(streamKey);
    const baseStake = stream?.baseStake ?? lastPending.betAmount;
    const spentUsd = lastPending.filledAmount ?? lastPending.betAmount;
    const entryPrice = lastPending.entryPrice ?? this.maxMarketPrice;
    // Тот же фикс, что и в resolvePendingMarkets (Сессия 8, баг №1) — не
    // теряем неисполненный остаток заявки при частичном филле предыдущего шага.
    const unfilledUsd = Math.max(0, lastPending.betAmount - spentUsd);
    const betAmount = predictedWin ? spentUsd / entryPrice + unfilledUsd : baseStake;

    this.logger.log(
      `[${streamKey}] pre_resolve: предсказан ${predictedWin ? 'ВЫИГРЫШ' : 'ПРОИГРЫШ'} шага ${lastPending.slug} ` +
        `(дистанция ${ratio.toFixed(2)}x ATR от страйка ¢${(lastPending.referencePrice * 100).toFixed(2)}, ` +
        `текущая цена ${snap.price}) — открываю следующее окно со стейком $${betAmount.toFixed(2)} ДО официального ` +
        `резолва Gamma. Официальный резолв всё равно наступит и остаётся источником истины для профита/прогрессии.`,
    );

    this.provisionalChainByStream.set(streamKey, chain + 1);
    return { betAmount, logId: lastPending.id };
  }

  // Сессия 15: пишем/чистим указатель активного окна для параллельного
  // tick-recorder процесса (см. active-window.entity.ts). Намеренно
  // fire-and-forget с try/catch — сбой здесь (БД временно недоступна,
  // репозиторий не передан в тестах) НИКОГДА не должен ронять основной
  // торговый флоу, это чисто вспомогательная инфраструктура для бэктеста.
  private recordActiveWindow(marketState: MarketState): void {
    if (!this.activeWindowRepo) return;
    this.activeWindowRepo
      .save({
        streamKey: marketState.assetPrefix,
        slug: marketState.slug,
        yesTokenId: marketState.yesTokenId,
        noTokenId: marketState.noTokenId,
        tickSize: marketState.books.YES.tickSize,
        windowStartMs: marketState.windowStartMs,
        closesAtMs: marketState.closesAt.getTime(),
      })
      .catch((err) => this.logger.warn(`[${marketState.assetPrefix}] Не удалось записать ActiveWindow-указатель для tick-recorder: ${this.errMsg(err)}`));
  }

  private clearActiveWindow(streamKey: string, slug: string): void {
    if (!this.activeWindowRepo) return;
    // Удаляем ТОЛЬКО если там всё ещё наша слаг (защита от гонки: если
    // discoveryTick для следующего окна уже успел перезаписать указатель
    // раньше, чем закрылось это окно — не затираем более свежую запись).
    this.activeWindowRepo
      .delete({ streamKey, slug } as any)
      .catch((err) => this.logger.warn(`[${streamKey}] Не удалось очистить ActiveWindow-указатель: ${this.errMsg(err)}`));
  }

  private async openMarket(
    stream: StreamDefinition,
    slug: string,
    closesAt: Date,
    yesTokenId: string,
    noTokenId: string,
    negRisk: boolean,
    conditionId: string | null,
    forcedBetAmount: number | null = null,
    predictedFromLogId: string | null = null,
    windowStartTs: number | null = null,
  ): Promise<void> {
    const streamKey = stream.streamKey;

    // Разовый REST-бутстрап: тик-сайз + официальный минимальный размер ордера биржи.
    const [yesBoot, noBoot] = await Promise.all([
      this.clobPublic.getBestQuote(yesTokenId),
      this.clobPublic.getBestQuote(noTokenId),
    ]);
    const initialTickSize = yesBoot?.tickSize ?? noBoot?.tickSize ?? '0.01';
    const minOrderSize = yesBoot?.minOrderSize ?? noBoot?.minOrderSize ?? 5;

    // Снимок стейка реинвест-прогрессии ЭТОГО потока на момент открытия окна
    // (п.1 бэклога) — фиксируем один раз здесь, дальше в течение всего окна
    // используем именно это число, а не текущее значение Attempt. Если гейт
    // (см. discoveryTick/tryPreResolve) уже посчитал предсказанную сумму —
    // используем её вместо Attempt.currentStake (который на данный момент
    // ещё НЕ обновлён официальным резолвером и был бы попросту устаревшим).
    const attempt = this.currentAttempts.get(streamKey);
    if (!attempt) {
      this.logger.error(`[${streamKey}] ${slug}: нет активного Attempt для потока — пропуск окна (не должно происходить).`);
      return;
    }
    const betAmount = forcedBetAmount ?? attempt.currentStake;
    const attemptId = attempt.id;
    const attemptStepNumber = attempt.currentStep + 1;

    // Гейт по РЕАЛЬНОМУ балансу CLOB (см. BalanceService и обсуждение
    // "клейм не мгновенный") — только в лайве, в смоуке баланс не тратится
    // по-настоящему и всегда достаточен. Явно пропускаем окно, а не пытаемся
    // исполнить ордер урезанным размером молча — недостача видна в логе и в
    // marketLogRepo.status='skipped', а не как загадочный низкий fillRatio.
    if (!this.isSmoke) {
      const availableUsd = await this.balanceService.getUsdBalance();
      const required = betAmount + this.minBalanceBufferUsd;
      if (availableUsd == null) {
        this.logger.warn(
          `[${streamKey}] ${slug}: не удалось получить баланс CLOB — открываю окно без гейта по балансу ` +
            '(лучше urgently проверить вручную, что баланс реально достаточен).',
        );
      } else if (availableUsd < required) {
        this.logger.warn(
          `[${streamKey}] ${slug}: пропуск окна — недостаточно свободного USDC на CLOB-балансе ` +
            `($${availableUsd.toFixed(2)} доступно, нужно $${required.toFixed(2)} = стейк $${betAmount.toFixed(2)} ` +
            `+ буфер $${this.minBalanceBufferUsd.toFixed(2)}). Вероятная причина — выигрыши ещё не заклеймлены ` +
            '(см. RedeemService) либо баланс аккаунта реально исчерпан.',
        );
        await this.marketLogRepo.save(
          this.marketLogRepo.create({
            attemptId,
            stepNumber: attemptStepNumber,
            assetPrefix: streamKey,
            slug,
            closesAt,
            betAmount,
            isSmoke: this.isSmoke,
            executed: false,
            status: 'skipped',
            skipReason: `insufficient_balance: available=$${availableUsd.toFixed(2)} required=$${required.toFixed(2)}`,
            conditionId,
            negRisk,
          }),
        );
        return;
      }
    }

    // Фиксируем ориентир по внешнему фиду В МОМЕНТ ОФИЦИАЛЬНОГО СТАРТА ОКНА
    // (windowStartTs), а НЕ "текущую" цену на момент, когда мы вообще успели
    // сюда дойти (Сессия 11, см. CONTEXT.md — реальный найденный пользователем
    // баг: между обнаружением нового окна в discoveryTick и этой строкой уже
    // произошло 1-3 сетевых round-trip'а — fetchMarketBySlug + getBestQuote x2
    // выше в этой же функции — и наш референс систематически отставал по
    // времени от официального страйка Polymarket на эти самые секунды).
    // getPriceAt ищет цену ИЗ БУФЕРА СЫРЫХ ТИКОВ ровно на нужный момент
    // времени, а не "текущий" снимок — см. PriceFeedService.getPriceAt.
    let referenceSnapshot: { price: number | null; source?: string | null };
    let referenceLagMs: number | null = null;
    if (windowStartTs != null) {
      const atStart = this.priceFeed.getPriceAt(streamKey, windowStartTs * 1000);
      referenceSnapshot = { price: atStart.price };
      referenceLagMs = atStart.lagMs;
      if (atStart.price != null && referenceLagMs != null && referenceLagMs > TradingService.REFERENCE_LAG_WARN_MS) {
        // Тик нашёлся, но подозрительно далеко от истинной границы окна —
        // похоже на дыру в потоке трейдов ровно рядом со стартом (просадка
        // ликвидности/провайдера), а не на нормальную работу буфера.
        // Не блокируем (у нас и так нет ничего точнее) — просто фиксируем
        // в логе, чтобы было видно при разборе, если что-то пойдёт не так.
        this.logger.warn(
          `[${streamKey}] ${slug}: ближайший тик к истинному старту окна найден с лагом ${referenceLagMs}мс ` +
            `(> ${TradingService.REFERENCE_LAG_WARN_MS}мс) — похоже на дыру в потоке трейдов рядом с границей окна, ` +
            `референс всё равно используется (это лучшее, что есть), но точность может быть снижена.`,
        );
      }
      if (atStart.price == null) {
        // Буфер тиков не достаёт так далеко назад (например, фид только что
        // переподключился, либо ещё вообще не прислал ни одного тика) —
        // единственный доступный фолбэк это "текущая" цена, но это ХУЖЕ, чем
        // честно считать референс недоступным (именно приближение и было
        // исходным багом) — поэтому НЕ подставляем getSnapshot() сюда молча.
        this.logger.warn(
          `[${streamKey}] ${slug}: не нашли в буфере тиков цену на точный момент старта окна ` +
            `(буфер не достаёт так далеко назад — фид либо только что переподключился, либо ещё ` +
            `не прислал ни одного тика) — referencePrice для этого окна будет недоступен, ` +
            `а не приближён текущей ценой. Диагностика/ATR-гейт для этого окна будут недоступны.`,
        );
      }
    } else {
      // Вызвано не из штатного discoveryTick (напр. тесты) — фолбэк на старое поведение.
      referenceSnapshot = this.priceFeed.getSnapshot(streamKey);
      if (referenceSnapshot.price == null) {
        this.logger.warn(
          `[${streamKey}] ${slug}: внешний ценовой фид ещё не отдал ни одного тика — ` +
            `диагностика/ATR-гейт для этого окна будут недоступны.`,
        );
      }
    }

    const marketState: MarketState = {
      assetPrefix: streamKey,
      slug,
      closesAt,
      yesTokenId,
      noTokenId,
      negRisk,
      conditionId,
      minOrderSize,
      stream: null as any,
      books: {
        YES: EMPTY_BOOK('YES', initialTickSize),
        NO: EMPTY_BOOK('NO', initialTickSize),
      },
      positioned: false,
      finalized: false,
      logWritten: false,
      restingOrder: null,
      skippedLimitTier: null,
      lastMarketAttemptAt: 0,
      lastLimitAttemptAt: 0,
      limitInFlight: false,
      closeTimer: null as any,
      referencePrice: referenceSnapshot.price,
      marketLogId: null,
      betAmount,
      attemptId,
      attemptStepNumber,
      stakePredicted: forcedBetAmount != null,
      predictedFromLogId,
      lastEntryGateLogAt: 0,
      lastMinuteAnnounced: false,
      intervalSec: stream.intervalSec,
      windowStartMs: windowStartTs != null ? windowStartTs * 1000 : null,
      lastEntryWindowSec: stream.lastEntryWindowSec ?? this.lastEntryWindowSec,
      tier2Seconds: stream.tier2Seconds ?? this.tier2Seconds,
      tier3Seconds: stream.tier3Seconds ?? this.tier3Seconds,
    };

    const wsStream = new MarketWsStream(
      yesTokenId,
      noTokenId,
      initialTickSize,
      (outcome, book) => this.onBookUpdate(marketState, outcome, book),
    );
    marketState.stream = wsStream;

    const msUntilClose = Math.max(0, closesAt.getTime() - Date.now());
    marketState.closeTimer = setTimeout(() => this.finalizeMarket(marketState), msUntilClose);

    this.activeMarkets.set(streamKey, marketState);
    this.recordActiveWindow(marketState);
    wsStream.connect();

    this.logger.log(
      `[${streamKey}] ${slug}: открыт WS-поток (закрытие через ${(msUntilClose / 1000).toFixed(0)}с, стейк шага $${betAmount.toFixed(2)} ` +
        `[попытка #${attempt.attemptNumber}, шаг ${attempt.currentStep + 1}/${attempt.targetSteps}], min_order_size=${minOrderSize}, tick=${initialTickSize}, ` +
        `referencePrice=${referenceSnapshot.price ?? 'н/д'}` +
        // Лаг найденного тика от истинной границы окна (Сессия 12, см. CONTEXT.md) —
        // видимость точности референса прямо в логе открытия, как просил пользователь.
        (referenceLagMs != null ? `, лаг референса от истинного старта окна ${referenceLagMs}мс` : '') +
        ')',
    );
  }

  // ---------------------------------------------------------------------
  // Реакция на каждое обновление стакана (book / price_change / tick_size_change)
  // ---------------------------------------------------------------------
  private onBookUpdate(marketState: MarketState, outcome: Outcome, book: LiveBook): void {
    marketState.books[outcome] = book;
    if (marketState.positioned || marketState.finalized) return;

    const timeLeftSec = (marketState.closesAt.getTime() - Date.now()) / 1000;
    if (timeLeftSec <= 0) return; // finalizeMarket сам разберётся по таймеру

    // 0) Если по этому исходу уже стоит наша (смоук-)лимитка — проверяем, накопилось
    //    ли ДОСТАТОЧНО объёма продавцов по нашей цене или ниже (а не просто "касание").
    //
    //    ФИКС (Сессия 10, микрокейс №1): раньше ATR-гейт перепроверялся ТОЛЬКО в
    //    момент ВЫСТАВЛЕНИЯ резюм-лимитки (placeOrReplaceLimit), а не в момент
    //    её ФАКТИЧЕСКОГО исполнения — который может наступить намного позже
    //    (лимитка просто ждёт, пока стакан накопит нужный объём продавцов).
    //    Диагностика для лога бралась ЗАНОВО в момент исполнения
    //    (captureDiagnostics), но сам факт исполнения гейтом уже не проверялся —
    //    из-за этого капитал реально рисковался в момент, когда цена успела
    //    "отскочить" почти обратно к референсу (см. прод-кейс: гейт пропустил
    //    вход при ATR-рацио ~1.7x на выставлении, а к моменту исполнения
    //    рацио упало до 0.44x — сделка всё равно прошла, хотя в момент
    //    РЕАЛЬНОГО риска условие входа уже не выполнялось). Теперь гейт
    //    перепроверяется заново прямо здесь: если к моменту накопления объёма
    //    условие входа больше не выполняется — лимитка отменяется, а не
    //    исполняется вслепую на устаревшем разрешении.
    if (this.isSmoke && marketState.restingOrder?.outcome === outcome) {
      const resting = marketState.restingOrder;
      const targetUsd = this.limitOrderTargetUsd(marketState, resting.price);
      const availableUsd = cumulativeUsdAtOrBelow(book.asks, resting.price);
      if (availableUsd >= targetUsd) {
        const gate = this.evaluateEntryGate(marketState, outcome);
        if (!gate.allow) {
          marketState.restingOrder = null;
          marketState.skippedLimitTier = resting.tier;
          this.logger.log(
            `[${marketState.assetPrefix}][SMOKE][Limit] ${marketState.slug}: тир ${resting.tier} — объём продавцов накопился, ` +
              `НО гейт при повторной проверке НА МОМЕНТ ИСПОЛНЕНИЯ уже не пропускает вход (цена откатилась к референсу ` +
              `с момента выставления лимитки) — отменяем, не рискуем капиталом на устаревшем разрешении. ${gate.reason}`,
          );
          return;
        }
        marketState.positioned = true;
        marketState.restingOrder = null;
        const filledShares = targetUsd / resting.price;
        void this.writeLog(marketState, {
          chosenOutcome: outcome,
          chosenTokenId: outcome === 'YES' ? marketState.yesTokenId : marketState.noTokenId,
          entryPrice: resting.price,
          filledAmount: targetUsd,
          fillRatio: targetUsd / marketState.betAmount,
          executed: true,
          orderType: 'SIMULATED_LIMIT',
          limitTier: resting.tier,
          status: 'pending_resolve',
          logMessage: `SMOKE: лимитка не отправлялась на биржу — эмуляция; накопленный объём продавцов по ¢${(resting.price * 100).toFixed(2)} и ниже составил $${availableUsd.toFixed(2)}, взяли ${filledShares.toFixed(2)} шт.`,
          orderSentAt: resting.placedAt,
          orderFilledAt: new Date(),
          ...gate.diagnostics,
        });
        this.logger.log(
          `[${marketState.assetPrefix}][SMOKE][Limit fill] ${marketState.slug}: ${outcome} по ¢${(resting.price * 100).toFixed(2)} (тир ${resting.tier})`,
        );
        return;
      }
    }

    // 1) Ждём последней минуты (LAST_ENTRY_WINDOW_SEC, per-stream — Сессия 14)
    //    перед тем, как вообще пытаться войти — ни маркетом, ни лимиткой.
    //    Чем раньше вход, тем менее рынок ещё "определился": именно так
    //    случились оба недавних слива (ask по ¢99 появлялся за 2-3 минуты до
    //    закрытия, а потом цена успевала развернуться). Лучше пропустить шаг
    //    целиком, чем рисковать капиталом на неопределившемся рынке.
    const effLastEntryWindowSec = marketState.lastEntryWindowSec ?? this.lastEntryWindowSec;
    if (timeLeftSec > effLastEntryWindowSec) return;
    if (!marketState.lastMinuteAnnounced) {
      marketState.lastMinuteAnnounced = true;
      this.logger.log(
        `[${marketState.assetPrefix}] ${marketState.slug}: вошли в окно входа (последние ${effLastEntryWindowSec}с) — начинаем искать вход.`,
      );
    }

    // 2) Правило A — агрессивный маркет-тейк: реально проходим по уровням стакана
    //    (не делаем вид, что весь объём взяли по единственной лучшей цене).
    for (const oc of ['YES', 'NO'] as const) {
      const b = marketState.books[oc];
      if (b.bestAsk == null || b.bestAsk < this.minMarketPrice || b.bestAsk > this.maxMarketPrice) continue;

      const now = Date.now();
      if (now - marketState.lastMarketAttemptAt < 800) continue; // не долбим биржу на каждом тике подряд
      marketState.lastMarketAttemptAt = now;
      void this.tryMarketBuy(marketState, oc, b);
      return;
    }

    // 3) Правило B — лимитка-фолбэк, если у фаворита реально нет предложений на продажу.
    const favorite = this.pickFavorite(marketState.books);
    if (!favorite) return;
    const fb = marketState.books[favorite];
    if (fb.bestBid == null || fb.bestBid < this.favoriteBidThreshold) return;
    if (fb.bestAsk != null && fb.bestAsk <= this.maxMarketPrice) return; // предложение есть — им займётся Правило A

    const tier = this.computeTier(timeLeftSec, marketState);
    if (marketState.skippedLimitTier === tier) return; // уже проверяли этот тир — бюджета/минимума не хватает, ждём смены тира
    const desiredPrice = this.roundToTick(this.tierPrices[tier], fb.tickSize);
    const existing = marketState.restingOrder;
    if (existing && existing.tier === tier && existing.outcome === favorite) return; // уже стоит нужный уровень

    void this.placeOrReplaceLimit(marketState, favorite, tier, desiredPrice, fb.tickSize);
  }

  // Сессия 16: pickFavorite/computeTier/roundToTick/limitOrderTargetUsd не
  // зависят ни от Nest DI, ни от MarketState как такового — чистые функции,
  // вынесены в market-decision.util.ts и переиспользуются 1-в-1 модулем
  // бэктеста (см. BacktestRunnerService). Обёртки-методы оставлены с той же
  // сигнатурой, чтобы не трогать остальной код TradingService/тесты.
  private pickFavorite(books: Record<Outcome, LiveBook>): Outcome | null {
    return pickFavoritePure({ YES: { bestBid: books.YES.bestBid }, NO: { bestBid: books.NO.bestBid } });
  }

  private computeTier(timeLeftSec: number, marketState: MarketState): LimitTier {
    const t2 = marketState.tier2Seconds ?? this.tier2Seconds;
    const t3 = marketState.tier3Seconds ?? this.tier3Seconds;
    return computeTierPure(timeLeftSec, t2, t3);
  }

  private roundToTick(price: number, tickSizeStr: string): number {
    return roundToTickPure(price, tickSizeStr);
  }

  /** Сколько $ нужно набрать нашей резюм-лимиткой, чтобы удовлетворить минимум биржи
   *  (не капая эту сумму обратно до стейка шага — иначе проверка допустимого перерасхода
   *  в вызывающем коде никогда не сработает). */
  private limitOrderTargetUsd(marketState: MarketState, price: number): number {
    return limitOrderTargetUsdPure(marketState.betAmount, marketState.minOrderSize, price);
  }

  // ---------------------------------------------------------------------
  // ATR-гейт: считает диагностику по внешнему фиду (по умолчанию Chainlink —
  // тот же фид, которым Polymarket резолвит крипто-маркеты, см. README) и
  // (если включено через .env) решает, достаточно ли убедительно цена
  // отошла от точки старта окна относительно недавней волатильности этого
  // же таймфрейма (ATR теперь считается в масштабе окна потока, не в
  // фиксированных 20 секундах — см. PriceFeedService).
  // ---------------------------------------------------------------------
  /**
   * Сессия 16: тонкая обёртка над EntryGateEngine.captureDiagnostics —
   * собирает GateContext из MarketState и делегирует всю фактическую логику
   * (ровно ту же, что раньше жила прямо здесь) в переиспользуемый движок,
   * см. entry-gate.engine.ts. Оставлена как метод именно с этой сигнатурой,
   * чтобы не трогать вызывающий код и существующие тесты.
   */
  private captureDiagnostics(marketState: MarketState, outcome: Outcome, timeLeftSec: number, checkPrice: number | null = null): EntryDiagnostics {
    return this.gateEngine.captureDiagnostics(this.toGateContext(marketState, outcome, timeLeftSec, checkPrice));
  }

  private toGateContext(marketState: MarketState, outcome: Outcome, timeLeftSec: number, checkPrice: number | null = null): GateContext {
    return {
      streamKey: marketState.assetPrefix,
      outcome,
      referencePrice: marketState.referencePrice,
      intervalSec: marketState.intervalSec,
      windowStartMs: marketState.windowStartMs,
      timeLeftSec,
      // Сессия 18: если конкретная цена сделки не передана — берём best ask
      // выбранной стороны как текущую implied-вероятность рынка.
      checkPrice: checkPrice ?? marketState.books[outcome]?.bestAsk ?? null,
    };
  }

  /**
   * Сессия 16: тонкая обёртка над EntryGateEngine.evaluateEntryGate — см.
   * комментарий у captureDiagnostics выше. Сигнатура и поведение идентичны
   * тому, что было здесь раньше (перенесена только реализация).
   */
  private evaluateEntryGate(marketState: MarketState, outcome: Outcome): { allow: boolean; diagnostics: EntryDiagnostics; reason: string | null } {
    const timeLeftSec = Math.max(0, (marketState.closesAt.getTime() - Date.now()) / 1000);
    return this.gateEngine.evaluateEntryGate(this.toGateContext(marketState, outcome, timeLeftSec));
  }

  // ---------------------------------------------------------------------
  // Правило A: маркет-тейк — честно проходим по уровням стакана (VWAP),
  // не выше maxMarketPrice, и не принимаем сделку, если реальной глубины
  // хватает меньше чем на minFillRatio от заявленной ставки (иначе легко
  // словить дребезг тонкой лимитки, а не настоящее направление рынка).
  // ---------------------------------------------------------------------
  private async tryMarketBuy(marketState: MarketState, outcome: Outcome, book: LiveBook): Promise<void> {
    const tokenId = outcome === 'YES' ? marketState.yesTokenId : marketState.noTokenId;
    const fill = walkAsksForFill(book.asks, marketState.betAmount, this.maxMarketPrice);

    if (fill.filledShares <= 0) {
      this.logger.debug(`[${marketState.assetPrefix}][Market] ${marketState.slug}: нет реальной ликвидности по ${outcome} в диапазоне — пропуск`);
      return;
    }
    if (fill.filledRatio < this.minFillRatio) {
      this.logger.debug(
        `[${marketState.assetPrefix}][Market] ${marketState.slug}: ${outcome} — глубины стакана хватает только на ${(fill.filledRatio * 100).toFixed(0)}% ставки ` +
          `(нужно минимум ${(this.minFillRatio * 100).toFixed(0)}%), похоже на дребезг тонкой заявки — пропуск.`,
      );
      return;
    }
    if (fill.filledShares < marketState.minOrderSize) {
      this.logger.debug(
        `[${marketState.assetPrefix}][Market] ${marketState.slug}: ${outcome} — реально исполнимо только ${fill.filledShares.toFixed(2)} шт, ` +
          `меньше минимума биржи (${marketState.minOrderSize}) — пропуск.`,
      );
      return;
    }

    const gate = this.evaluateEntryGate(marketState, outcome);
    if (!gate.allow) {
      this.logEntryGateBlockThrottled(
        marketState,
        `[${marketState.assetPrefix}][Market] ${marketState.slug}: ${outcome} — вход заблокирован. ${gate.reason}`,
      );
      return;
    }

    try {
      if (this.isSmoke) {
        const orderSentAt = new Date();
        marketState.positioned = true;
        await this.cancelRestingIfAny(marketState);
        const savedId = await this.writeLog(marketState, {
          chosenOutcome: outcome,
          chosenTokenId: tokenId,
          entryPrice: fill.vwapPrice,
          filledAmount: fill.filledUsd,
          fillRatio: fill.filledRatio,
          executed: true,
          orderType: 'SIMULATED_MARKET',
          status: 'pending_resolve',
          logMessage:
            fill.filledRatio < 0.999
              ? `SMOKE: частичное исполнение — забрали $${fill.filledUsd.toFixed(2)} из $${marketState.betAmount.toFixed(2)} (${(fill.filledRatio * 100).toFixed(0)}%) по VWAP ¢${(fill.vwapPrice! * 100).toFixed(2)}.`
              : `SMOKE: маркет-ордер не отправлялся, только эмуляция прохода по стакану (VWAP ¢${(fill.vwapPrice! * 100).toFixed(2)}).`,
          orderSentAt,
          orderFilledAt: new Date(),
          ...gate.diagnostics,
        });
        marketState.marketLogId = savedId;
        this.logger.log(
          `[${marketState.assetPrefix}][SMOKE][Market] ${marketState.slug}: ${outcome} по VWAP ¢${(fill.vwapPrice! * 100).toFixed(2)} ` +
            `($${fill.filledUsd.toFixed(2)}${fill.filledRatio < 0.999 ? `, ${(fill.filledRatio * 100).toFixed(0)}% от заявки` : ''})`,
        );
        return;
      }

      const orderSentAt = new Date();
      const result = await this.trader.placeMarketBuy({
        tokenId,
        amountUsd: marketState.betAmount,
        worstPrice: this.maxMarketPrice,
      });
      const orderFilledAt = new Date();

      this.logger.log(
        `[${marketState.assetPrefix}][LIVE] Результат ордера: ${JSON.stringify(result)}`
      )

      if (!result.success) {
        this.logger.warn(`[${marketState.assetPrefix}][LIVE][Market] ${marketState.slug}: ордер не исполнился (success=false), пробуем дальше`);
        return;
      }

      marketState.positioned = true;
      await this.cancelRestingIfAny(marketState);
      // Реальный размер исполнения биржа возвращает в takingAmount/makingAmount —
      // если поле есть, используем его; если нет, используем нашу локальную оценку
      // по стакану как честное приближение (и явно это помечаем в логе).
      const raw: any = result.raw;
      const actualUsd = this.parseFloatSafe(raw?.makingAmount) ?? fill.filledUsd;
      const savedId = await this.writeLog(marketState, {
        chosenOutcome: outcome,
        chosenTokenId: tokenId,
        entryPrice: fill.vwapPrice,
        filledAmount: actualUsd,
        fillRatio: actualUsd / marketState.betAmount,
        executed: true,
        orderType: 'FAK',
        orderId: result.orderId,
        status: 'pending_resolve',
        orderSentAt,
        orderFilledAt,
        ...gate.diagnostics,
      });
      marketState.marketLogId = savedId;
      this.logger.log(
        `[${marketState.assetPrefix}][LIVE][Market] ${marketState.slug}: ${outcome} ордер отправлен (потолок ¢${(this.maxMarketPrice * 100).toFixed(1)}), orderId=${result.orderId}`,
      );
    } catch (err) {
      this.logger.error(`[${marketState.assetPrefix}][Error][Market] ${marketState.slug}: ${this.errMsg(err)}`);
    }
  }

  // ---------------------------------------------------------------------
  // Правило B: лимитка-фолбэк, переставляется по тирам T1(¢99)/T2(¢99.5)/T3(¢99.9)
  // ---------------------------------------------------------------------
  private async placeOrReplaceLimit(
    marketState: MarketState,
    outcome: Outcome,
    tier: LimitTier,
    price: number,
    tickSize: string,
  ): Promise<void> {
    // Анти-флуд: не даём наложиться параллельным постановкам (метод вызывается
    // void'ом на каждый тик стакана) и не долбим биржу чаще раза в секунду —
    // иначе любая ошибка постановки превращается в шторм 429 на /order.
    if (marketState.limitInFlight) return;
    if (Date.now() - marketState.lastLimitAttemptAt < 1000) return;
    marketState.lastLimitAttemptAt = Date.now();
    marketState.limitInFlight = true;
    try {
      const tokenId = outcome === 'YES' ? marketState.yesTokenId : marketState.noTokenId;
      const targetUsd = this.limitOrderTargetUsd(marketState, price);

      if (targetUsd > marketState.betAmount * this.maxOverspendMultiplier) {
        marketState.skippedLimitTier = tier;
        this.logger.debug(
          `[${marketState.assetPrefix}][Limit] ${marketState.slug}: пропуск тира ${tier} — нужно ~$${targetUsd.toFixed(2)} для минимума биржи, больше допустимого.`,
        );
        return;
      }

      const gate = this.evaluateEntryGate(marketState, outcome);
      if (!gate.allow) {
        marketState.skippedLimitTier = tier;
        this.logEntryGateBlockThrottled(
          marketState,
          `[${marketState.assetPrefix}][Limit] ${marketState.slug}: тир ${tier} — выставление заблокировано. ${gate.reason}`,
        );
        return;
      }

      const size = Number((targetUsd / price).toFixed(2));

      if (this.isSmoke) {
        marketState.restingOrder = { tier, outcome, price, orderId: null, placedAt: new Date() };
        this.logger.log(
          `[${marketState.assetPrefix}][SMOKE][Limit] ${marketState.slug}: тир ${tier} — ${outcome} по ¢${(price * 100).toFixed(2)} (эмуляция, ждём накопления объёма продавцов $${targetUsd.toFixed(2)})`,
        );
        return;
      }

      await this.cancelRestingIfAny(marketState);
      const result = await this.trader.placeLimitBuy({
        tokenId,
        price,
        size,
        expirationUnixSec: Math.floor(marketState.closesAt.getTime() / 1000),
      });

      if (!result.success || !result.orderId) {
        this.logger.warn(`[${marketState.assetPrefix}][LIVE][Limit] ${marketState.slug}: не удалось выставить тир ${tier}`);
        return;
      }

      marketState.restingOrder = { tier, outcome, price, orderId: result.orderId, placedAt: new Date() };
      this.logger.log(
        `[${marketState.assetPrefix}][LIVE][Limit] ${marketState.slug}: тир ${tier} — ${outcome} по ¢${(price * 100).toFixed(2)} выставлен, orderId=${result.orderId}`,
      );
    } catch (err) {
      this.logger.error(`[${marketState.assetPrefix}][Error][Limit] ${marketState.slug}: ${this.errMsg(err)}`);
    } finally {
      marketState.limitInFlight = false;
    }
  }

  private async cancelRestingIfAny(marketState: MarketState): Promise<void> {
    if (marketState.restingOrder?.orderId) {
      await this.trader.cancelOrder(marketState.restingOrder.orderId);
    }
    marketState.restingOrder = null;
    marketState.skippedLimitTier = null;
  }

  // ---------------------------------------------------------------------
  // Закрытие окна: ровно один терминальный MarketLog на маркет.
  // ---------------------------------------------------------------------
  private async finalizeMarket(marketState: MarketState): Promise<void> {
    if (marketState.finalized) return;
    marketState.finalized = true;
    marketState.stream.close();
    this.activeMarkets.delete(marketState.assetPrefix);
    this.clearActiveWindow(marketState.assetPrefix, marketState.slug);

    try {
      if (marketState.positioned) {
        await this.cancelRestingIfAny(marketState);
        // Лог уже записан в момент входа — дописываем только "фото" цены/ATR
        // на момент закрытия окна, чтобы потом было видно, что произошло с
        // ценой между входом и резолвом (это и есть материал для разбора сливов).
        if (marketState.marketLogId) {
          // Сессия 12 (симметрично фиксу референса открытия в Сессии 11):
          // раньше здесь брали getSnapshot() — "текущую" цену на момент,
          // когда фактически выполнился этот код, а не цену РОВНО на
          // marketState.closesAt. Между вызовом finalizeMarket по таймеру
          // и этой строкой уже прошёл await cancelRestingIfAny (для LIVE —
          // реальный сетевой запрос отмены ордера) — та же категория лага,
          // что была у референса открытия. Теперь ищем цену в буфере тиков
          // точно на официальный момент закрытия, а не "как получится".
          const closeSnap = this.priceFeed.getPriceAt(marketState.assetPrefix, marketState.closesAt.getTime());
          await this.marketLogRepo.update(marketState.marketLogId, {
            priceAtClose: closeSnap.price,
            atrAtClose: this.priceFeed.getSnapshot(marketState.assetPrefix).atr,
          });
        }
        return;
      }

      if (marketState.restingOrder) {
        const resting = marketState.restingOrder;
        const tokenId = resting.outcome === 'YES' ? marketState.yesTokenId : marketState.noTokenId;

        if (this.isSmoke) {
          await this.writeLog(marketState, {
            chosenOutcome: resting.outcome,
            chosenTokenId: tokenId,
            entryPrice: null,
            executed: false,
            orderType: 'SIMULATED_LIMIT',
            limitTier: resting.tier,
            status: 'unfilled',
            skipReason: 'Симулированная лимитка не была перекрыта достаточным объёмом продавцов до конца окна.',
            orderSentAt: resting.placedAt,
          });
          return;
        }

        const status = resting.orderId ? await this.trader.getOrderStatus(resting.orderId) : null;
        if (status && status.sizeMatched > 0) {
          const filledAmount = status.sizeMatched * resting.price;
          const savedId = await this.writeLog(marketState, {
            chosenOutcome: resting.outcome,
            chosenTokenId: tokenId,
            entryPrice: resting.price,
            filledAmount,
            fillRatio: filledAmount / marketState.betAmount,
            executed: true,
            orderType: 'GTD',
            limitTier: resting.tier,
            orderId: resting.orderId,
            status: 'pending_resolve',
            logMessage: `Исполнено ${status.sizeMatched}/${status.originalSize} шт.`,
            orderSentAt: resting.placedAt,
            // Момент фактического исполнения GTD-лимитки биржа не отдаёт отдельным
            // полем в этом ответе — используем момент проверки статуса как приближение
            // (честно позже реального момента матча, но точнее, чем ничего).
            orderFilledAt: new Date(),
          });
          if (savedId) {
            // Сессия 12 — тот же фикс, что и выше: точка на момент истинного
            // закрытия окна, а не "текущая" цена на момент, когда мы сюда дошли.
            const closeSnap = this.priceFeed.getPriceAt(marketState.assetPrefix, marketState.closesAt.getTime());
            await this.marketLogRepo.update(savedId, {
              priceAtClose: closeSnap.price,
              atrAtClose: this.priceFeed.getSnapshot(marketState.assetPrefix).atr,
            });
          }
        } else {
          await this.writeLog(marketState, {
            chosenOutcome: resting.outcome,
            chosenTokenId: tokenId,
            entryPrice: resting.price,
            executed: false,
            orderType: 'GTD',
            limitTier: resting.tier,
            orderId: resting.orderId,
            status: 'unfilled',
            skipReason: 'Лимитка не исполнилась до истечения (GTD).',
            orderSentAt: resting.placedAt,
          });
        }
        await this.cancelRestingIfAny(marketState);
        return;
      }

      await this.writeLog(marketState, {
        chosenOutcome: null,
        executed: false,
        status: 'skipped',
        skipReason: 'Ни один исход не вошёл в диапазон маркет-тейка, фаворит не определился.',
      });
    } catch (err) {
      this.logger.error(`Ошибка финализации ${marketState.slug}: ${this.errMsg(err)}`);
    }
  }

  private async writeLog(
    marketState: MarketState,
    fields: {
      chosenOutcome: ChosenOutcome;
      chosenTokenId?: string | null;
      entryPrice?: number | null;
      filledAmount?: number | null;
      fillRatio?: number | null;
      executed: boolean;
      orderType?: OrderKind | null;
      limitTier?: LimitTier | null;
      orderId?: string | null;
      status: MarketLogStatus;
      skipReason?: string | null;
      logMessage?: string | null;
      referencePrice?: number | null;
      priceAtEntry?: number | null;
      atrAtEntry?: number | null;
      atrRatioAtEntry?: number | null;
      priceSource?: string | null;
      orderSentAt?: Date | null;
      orderFilledAt?: Date | null;
      // Сессия 13 — доп. диагностика фильтров входа (см. EntryDiagnostics).
      blackoutHourAtEntry?: number | null;
      requiredDeltaAtEntry?: number | null;
      driftRateAtEntry?: number | null;
      zoneRatioAtEntry?: number | null;
    },
  ): Promise<string | null> {
    if (marketState.logWritten) return null;
    marketState.logWritten = true;

    // ВАЖНО: используем снимок attemptId/attemptStepNumber, сделанный в
    // openMarket в момент открытия ЭТОГО окна, а не currentAttempts.get(...)
    // "сейчас" — см. комментарий у полей MarketState.attemptId (п.7 сессии 6).
    const saved = await this.marketLogRepo.save(
      this.marketLogRepo.create({
        attemptId: marketState.attemptId,
        stepNumber: marketState.attemptStepNumber,
        assetPrefix: marketState.assetPrefix,
        slug: marketState.slug,
        closesAt: marketState.closesAt,
        betAmount: marketState.betAmount,
        stakePredicted: marketState.stakePredicted,
        predictedFromLogId: marketState.predictedFromLogId,
        isSmoke: this.isSmoke,
        conditionId: marketState.conditionId,
        negRisk: marketState.negRisk,
        // На запись лог ещё не знает исхода (status='pending_resolve' на
        // executed=true шагах) — redeemStatus проставляется резолвером
        // (resolvePendingMarkets) в момент, когда исход становится известен.
        redeemStatus: 'not_applicable',
        chosenOutcome: fields.chosenOutcome,
        chosenTokenId: fields.chosenTokenId ?? null,
        entryPrice: fields.entryPrice ?? null,
        filledAmount: fields.filledAmount ?? null,
        fillRatio: fields.fillRatio ?? null,
        executed: fields.executed,
        orderType: fields.orderType ?? null,
        limitTier: fields.limitTier ?? null,
        orderId: fields.orderId ?? null,
        status: fields.status,
        skipReason: fields.skipReason ?? null,
        logMessage: fields.logMessage ?? null,
        referencePrice: fields.referencePrice ?? null,
        priceAtEntry: fields.priceAtEntry ?? null,
        atrAtEntry: fields.atrAtEntry ?? null,
        atrRatioAtEntry: fields.atrRatioAtEntry ?? null,
        priceSource: fields.priceSource ?? null,
        orderSentAt: fields.orderSentAt ?? null,
        orderFilledAt: fields.orderFilledAt ?? null,
        blackoutHourAtEntry: fields.blackoutHourAtEntry ?? null,
        requiredDeltaAtEntry: fields.requiredDeltaAtEntry ?? null,
        driftRateAtEntry: fields.driftRateAtEntry ?? null,
        zoneRatioAtEntry: fields.zoneRatioAtEntry ?? null,
      }),
    );

    if (fields.status === 'pending_resolve') {
      const set = this.pendingByStream.get(marketState.assetPrefix) ?? new Set<string>();
      set.add(saved.id);
      this.pendingByStream.set(marketState.assetPrefix, set);
    }

    return saved.id;
  }

  // ---------------------------------------------------------------------
  // Резолвер: REST-опрос Gamma API по закрытым маркетам, продвигает шаги
  // и считает профит по факту исхода. Общий для всех потоков (сами логи уже
  // несут attemptId/assetPrefix=streamKey, поэтому один цикл резолва работает
  // одинаково независимо от того, сколько потоков сконфигурировано).
  // ---------------------------------------------------------------------
  private async startResolverLoop() {
    while (!this.stopped) {
      try {
        await this.resolvePendingMarkets();
        await this.warnStaleUnresolved();
      } catch (err) {
        this.logger.error(`Сбой резолвера: ${this.errMsg(err)}`);
      }
      await this.sleep(this.resolvePollMs);
    }
  }

  /**
   * Отдельная, независимая от "горячего пути" входа проверка: не завис ли
   * какой-то маркет в pending_resolve дольше разумного. НЕ блокирует открытие
   * новых окон (см. discoveryTick) — только предупреждает в логах и попадает
   * в /analytics/summary, чтобы это было видно на фронте, а не только "по ощущениям".
   */
  private async warnStaleUnresolved(): Promise<void> {
    const staleBefore = new Date(Date.now() - this.staleResolveWarnMs);
    const stale = await this.marketLogRepo.find({
      where: { status: 'pending_resolve', createdAt: LessThan(staleBefore) },
      order: { createdAt: 'ASC' },
      take: 20,
    });
    if (stale.length === 0) return;

    for (const log of stale) {
      // Троттлинг (Сессия 8, баг №2): без него это тоже спамило WARN каждые
      // resolvePollMs (10с) на весь срок зависания шага. Раз в staleResolveWarnMs
      // достаточно, чтобы держать в курсе, что проблема ещё не решена.
      const key = `stale:${log.id}`;
      const last = this.lastGateSkipLogAt.get(key) ?? 0;
      const now = Date.now();
      if (now - last < this.staleResolveWarnMs) continue;
      this.lastGateSkipLogAt.set(key, now);

      const ageSec = Math.round((now - log.createdAt.getTime()) / 1000);
      this.logger.warn(
        `[STALE] ${log.assetPrefix} ${log.slug}: висит в pending_resolve уже ${ageSec}с — ` +
          `резолв Gamma задерживается сильнее обычного (текущее окно закрытия ~30с). Проверь вручную.`,
      );
    }
  }

  private async resolvePendingMarkets(): Promise<void> {
    const pending = await this.marketLogRepo.find({
      where: { status: 'pending_resolve' },
      order: { createdAt: 'ASC' },
      take: 20,
    });

    for (const log of pending) {
      const outcome = await this.gamma.fetchOutcome(log.slug);
      if (!outcome || !outcome.closed) continue;

      const won =
        (log.chosenOutcome === 'YES' && outcome.yesWon === true) ||
        (log.chosenOutcome === 'NO' && outcome.noWon === true);

      const spentUsd = log.filledAmount ?? log.betAmount;
      const entryPrice = log.entryPrice ?? this.maxMarketPrice;
      log.status = won ? 'win' : 'loss';
      log.resolvedAt = new Date();
      log.profit = won ? (spentUsd / entryPrice) * (1 - entryPrice) : -spentUsd;
      if (!won) {
        log.failReason = this.buildFailReason(log);
      }
      // Клейм актуален только для реальных (не смоук) выигрышей — реальные
      // деньги на бирже физически заперты в conditional-токене, пока
      // RedeemService не проведёт redeemPositions через relayer (см. CONTEXT.md,
      // раздел "Клейм резолва"). Без conditionId клеймить нечем — это может
      // случиться, если Gamma не отдала его на момент открытия окна; тогда
      // редим этого шага придётся делать вручную (RedeemService залогирует).
      if (won && !log.isSmoke) {
        log.redeemStatus = log.conditionId ? 'pending' : 'failed';
        if (!log.conditionId) {
          log.redeemError = 'conditionId отсутствовал на момент открытия окна — авторедим невозможен, клеймить вручную через UI.';
        }
      }
      await this.marketLogRepo.save(log);

      // Снимаем шаг с "занято" СРАЗУ после того, как узнали исход — именно
      // это разблокирует discoveryTick на открытие следующего окна потока с
      // ПРАВИЛЬНЫМ (уже обновлённым) стейком (см. п.7 сессии 6). Делаем это
      // до проверки attempt.status ниже, чтобы досрочно закрытая попытка
      // (closeAttemptEarly) тоже корректно снимала блокировку по потоку.
      this.pendingByStream.get(log.assetPrefix)?.delete(log.id);
      // Сбрасываем троттлинг лога пропуска (Сессия 8, баг №2) — если поток
      // снова застрянет в pending, следующий пропуск должен залогироваться
      // сразу, а не молчать оставшиеся секунды от предыдущего эпизода.
      this.lastGateSkipLogAt.delete(log.assetPrefix);
      // Официальное подтверждение от Gamma пришло — цепочка непроверенных
      // pre_resolve-окон обнуляется (см. preResolveMaxChain/tryPreResolve).
      this.provisionalChainByStream.set(log.assetPrefix, 0);

      const attempt = await this.attemptRepo.findOneOrFail({ where: { id: log.attemptId } });
      if (attempt.status !== 'active') continue; // попытка уже закрыта ранее (в т.ч. закрыта досрочно)

      const streamKey = log.assetPrefix;
      const stream = this.streamByKey.get(streamKey);
      const baseStake = stream?.baseStake ?? attempt.baseStake;

      if (won) {
        attempt.currentStep += 1;
        // Реинвест-прогрессия (п.1 бэклога): следующий стейк = реально
        // полученные деньги за этот шаг = spentUsd/entryPrice (то, что даёт
        // выплата $1/акцию победителя) — считаем по ФАКТИЧЕСКОЙ цене
        // исполнения (VWAP шага), а не по константе ¢99, потому что VWAP
        // гуляет по тирам лимитки/маркет-тейка.
        //
        // ФИКС (Сессия 8, баг №1): при ЧАСТИЧНОМ филле (spentUsd < betAmount —
        // например тир маркет-тейка исполнился лишь на 78% от заявки из-за
        // нехватки глубины стакана) неисполненный остаток (betAmount-spentUsd)
        // никуда не делся — эти деньги просто не были поставлены и остались
        // "в кармане". Раньше он терялся из формулы: currentStake считался
        // ТОЛЬКО от spentUsd, из-за чего прогрессия почти обнулялась до
        // базового стейка при каждом частичном филле, даже подряд идущих
        // выигрышах (наблюдалось в проде: заявка $6.36, филл $4.95 -> новый
        // стейк $5.00 вместо ожидаемого роста). Теперь неисполненный остаток
        // прибавляется обратно к следующему стейку — деньги "возвращаются
        // в оборот" вместо того, чтобы молча выпадать из прогрессии.
        const unfilledUsd = Math.max(0, log.betAmount - spentUsd);
        attempt.currentStake = spentUsd / entryPrice + unfilledUsd;
        if (unfilledUsd > 0.01) {
          this.logger.log(
            `[${streamKey}] Шаг ${log.slug} исполнен частично ($${spentUsd.toFixed(2)} из $${log.betAmount.toFixed(2)}) — ` +
              `неисполненный остаток $${unfilledUsd.toFixed(2)} добавлен к следующему стейку ($${attempt.currentStake.toFixed(2)}), ` +
              `чтобы частичный филл не "съедал" прогрессию.`,
          );
        }
        // Накапливаем прибыль ПОПЫТКИ (не банкролла) — считается независимо
        // от того, какой режим сейчас активен, чтобы переключение
        // steps<->profit на полпути попытки ничего не теряло (Сессия 18).
        attempt.realizedProfit = (attempt.realizedProfit ?? 0) + log.profit;

        const closeMode = await this.getCloseMode(streamKey);
        const reachedByMode =
          closeMode === 'profit' ? attempt.realizedProfit >= attempt.targetProfitUsd : attempt.currentStep >= attempt.targetSteps;

        if (reachedByMode) {
          attempt.status = 'completed_target';
          attempt.finishedAt = new Date();
          this.logger.log(
            closeMode === 'profit'
              ? `[GOAL] [${streamKey}] Попытка #${attempt.attemptNumber} (${attempt.isSmoke ? 'smoke' : 'live'}) достигла цели по прибыли $${attempt.targetProfitUsd.toFixed(2)} (факт $${attempt.realizedProfit.toFixed(2)})!`
              : `[GOAL] [${streamKey}] Попытка #${attempt.attemptNumber} (${attempt.isSmoke ? 'smoke' : 'live'}) дошла до ${attempt.targetSteps} шага!`,
          );
        }
        await this.attemptRepo.save(attempt);
        this.currentAttempts.set(streamKey, attempt);
      } else {
        attempt.status = 'failed';
        attempt.finishedAt = new Date();
        await this.attemptRepo.save(attempt);
        this.logger.warn(
          `[LOSS] [${streamKey}] Попытка #${attempt.attemptNumber} (${attempt.isSmoke ? 'smoke' : 'live'}) слита на шаге ${attempt.currentStep} ` +
            `(профит шага $${log.profit.toFixed(2)}, ${log.resolvedAt.toISOString()}). ${log.failReason ?? ''} Открываю новую попытку (стейк сброшен на базовый $${baseStake.toFixed(2)}).`,
        );

        const next = this.attemptRepo.create({
          attemptNumber: attempt.attemptNumber + 1,
          streamKey,
          currentStep: 0,
          targetSteps: this.targetSteps,
          targetProfitUsd: this.targetProfitUsd,
          realizedProfit: 0,
          baseStake,
          currentStake: baseStake, // сброс прогрессии на базовый стейк потока
          status: 'active',
          isSmoke: attempt.isSmoke,
          finishedAt: null,
        });
        const saved = await this.attemptRepo.save(next);
        this.currentAttempts.set(streamKey, saved);
      }
    }
  }

  /**
   * Собирает человекочитаемое объяснение слива из уже накопленной по фиду
   * диагностики (референс/цена на входе/ATR/цена на закрытии). Если фида на
   * момент входа или закрытия не было — честно об этом пишет, а не гадает.
   */
  private buildFailReason(log: MarketLog): string {
    const {
      referencePrice,
      priceAtEntry,
      atrAtEntry,
      atrRatioAtEntry,
      priceAtClose,
      atrAtClose,
      priceSource,
      blackoutHourAtEntry,
      requiredDeltaAtEntry,
      driftRateAtEntry,
      zoneRatioAtEntry,
      pModelAtEntry,
      impliedProbAtEntry,
      edgeWouldEnter,
    } = log;

    if (referencePrice == null || priceAtEntry == null) {
      return 'Слив без диагностики фида (referencePrice/priceAtEntry недоступны на момент входа — ' +
        'см. логи PriceFeedService, вероятно ни один из настроенных провайдеров (FEED_PROVIDERS) не был доступен в этот момент).';
    }

    const sourceNote = priceSource
      ? priceSource === 'chainlink'
        ? '(источник: chainlink — тот же фид, которым резолвится сам маркет)'
        : `(источник: ${priceSource} — приближение, не тот фид, которым резолвится маркет)`
      : '(источник неизвестен)';

    const deltaAtEntry = priceAtEntry - referencePrice;
    const entrySide = deltaAtEntry >= 0 ? 'YES (цена была выше референса)' : 'NO (цена была ниже референса)';
    const chosenMatchesEntrySide =
      (log.chosenOutcome === 'YES' && deltaAtEntry >= 0) || (log.chosenOutcome === 'NO' && deltaAtEntry < 0);

    const parts: string[] = [
      `На входе: цена ${priceAtEntry}, референс окна ${referencePrice} (дельта ${deltaAtEntry.toFixed(2)}, сторона ${entrySide}) ${sourceNote}` +
        (atrRatioAtEntry != null ? `, ATR-рацио ${atrRatioAtEntry.toFixed(2)}x` : ', ATR недоступен'),
    ];

    if (!chosenMatchesEntrySide) {
      parts.push('ВНИМАНИЕ: выбранный исход не совпадает со стороной фида на входе — проверить рассинхрон фида/страйка вручную.');
    }

    // Сессия 13: диагностика доп. фильтров — пишется всегда (SHADOW), даже
    // если сами фильтры выключены, именно для разбора подобных сливов.
    const extras: string[] = [];
    if (zoneRatioAtEntry != null) extras.push(`Time-in-Zone(выбранная сторона)=${(zoneRatioAtEntry * 100).toFixed(1)}%`);
    if (requiredDeltaAtEntry != null) extras.push(`Expected-move требуемая дельта=${requiredDeltaAtEntry.toFixed(4)}`);
    if (driftRateAtEntry != null) extras.push(`Directional-drift=${driftRateAtEntry.toFixed(4)}/${this.driftLookbackSec}с`);
    if (blackoutHourAtEntry != null) extras.push(`час UTC=${blackoutHourAtEntry}`);
    if (extras.length > 0) {
      parts.push(`Доп. диагностика (Сессия 13): ${extras.join(', ')}.`);
    }

    // Сессия 18: edge-модель — пишем ВСЕГДА, когда посчиталась, даже если
    // EDGE_GATE_ENABLED=false, чтобы по логам сливов было видно, отсеяла бы
    // их модель или нет (именно этот ретро-анализ и нужен для калибровки).
    if (pModelAtEntry != null) {
      parts.push(
        `Edge-модель (Сессия 18): p_model=${pModelAtEntry.toFixed(3)}` +
          (impliedProbAtEntry != null ? `, implied(цена)=${impliedProbAtEntry.toFixed(3)}` : ', implied недоступна') +
          (edgeWouldEnter != null ? `, вход по модели: ${edgeWouldEnter ? 'ДА' : 'НЕТ (модель отсеяла бы)'}` : '') +
          '.',
      );
    }

    if (priceAtClose != null) {
      const deltaAtClose = priceAtClose - referencePrice;
      const closeSide = deltaAtClose >= 0 ? 'YES' : 'NO';
      const flipped = (deltaAtEntry >= 0 && deltaAtClose < 0) || (deltaAtEntry < 0 && deltaAtClose >= 0);
      const atrRatioAtClose = atrAtClose && atrAtClose > 0 ? Math.abs(deltaAtClose) / atrAtClose : null;
      parts.push(
        `На закрытии: цена ${priceAtClose} (дельта ${deltaAtClose.toFixed(2)}, сторона ${closeSide}` +
          (atrRatioAtClose != null ? `, ATR-рацио ${atrRatioAtClose.toFixed(2)}x` : '') +
          `). ${flipped ? 'Цена РАЗВЕРНУЛАСЬ относительно момента входа — классический поздний разворот.' : 'Разворота по нашему фиду не зафиксировано (расхождение с резолвом Polymarket — вероятно микро-разница момента фиксации/источника).'}`,
      );
    } else {
      parts.push('Цена на закрытии по фиду недоступна (WS отвалился ближе к концу окна).');
    }

    return parts.join(' ');
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private parseFloatSafe(v: unknown): number | null {
    const n = parseFloat(String(v));
    return Number.isFinite(n) && n > 0 ? n : null;
  }

  private errMsg(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
