import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  ManyToOne,
  Index,
  JoinColumn,
} from 'typeorm';
import { Attempt } from './attempt.entity';

export type MarketLogStatus =
  | 'pending_resolve'
  | 'win'
  | 'loss'
  | 'skipped'
  | 'unfilled'
  | 'error';

export type ChosenOutcome = 'YES' | 'NO' | null;

export type OrderKind =
  | 'FAK' // реальный маркет-тейк (Правило A)
  | 'GTD' // реальная лимитка (Правило B)
  | 'SIMULATED_MARKET' // смоук-эмуляция Правила A
  | 'SIMULATED_LIMIT'; // смоук-эмуляция Правила B

@Entity('market_logs')
export class MarketLog {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @ManyToOne(() => Attempt, (attempt) => attempt.logs, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'attemptId' })
  attempt: Attempt;

  @Column({ type: 'uuid' })
  attemptId: string;

  // Шаг попытки, на который претендует этот маркет (полезно при разборе логов утром).
  @Column({ type: 'int' })
  stepNumber: number;

  // Префикс актива (btc-updown-5m, eth-updown-5m, ...) — для мультиассетного режима.
  @Index()
  @Column({ type: 'varchar', length: 64, default: 'btc-updown-5m' })
  assetPrefix: string;

  @Index()
  @Column({ type: 'varchar', length: 128 })
  slug: string;

  @Column({ type: 'timestamptz' })
  closesAt: Date;

  @Column({ type: 'varchar', length: 8, nullable: true })
  chosenOutcome: ChosenOutcome;

  @Column({ type: 'varchar', length: 128, nullable: true })
  chosenTokenId: string | null;

  @Column({ type: 'double precision', nullable: true })
  entryPrice: number | null;

  // Целевой стейк этого шага — снимок Attempt.currentStake потока на момент
  // открытия окна (реинвест-прогрессия, см. README/BACKLOG п.1), не константа.
  @Column({ type: 'double precision', default: 1 })
  betAmount: number;

  // Сколько реально удалось "потратить" при проходе по стакану (может быть
  // меньше betAmount, если глубины не хватило) — это то, что реально стоит
  // на кону для расчёта профита/лосса, а не заявленная цель.
  @Column({ type: 'double precision', nullable: true })
  filledAmount: number | null;

  // Доля betAmount, которую реально удалось исполнить (0..1). Для честности
  // смоука: 1.0 если стакана хватило целиком, меньше — если пришлось резать заявку.
  @Column({ type: 'double precision', nullable: true })
  fillRatio: number | null;

  // Профит в долларах, посчитанный резолвером после исхода: на выигрыше —
  // filledAmount/entryPrice*(1-entryPrice), на проигрыше — минус filledAmount.
  @Column({ type: 'double precision', nullable: true })
  profit: number | null;

  // true = ордер (реальный или симулированный) реально исполнился (хотя бы частично)
  @Column({ type: 'boolean', default: false })
  executed: boolean;

  @Column({ type: 'boolean' })
  isSmoke: boolean;

  @Column({ type: 'varchar', length: 24, nullable: true })
  orderType: OrderKind | null;

  // Для лимиток (Правило B): какой ценовой уровень был выставлен последним (T1/T2/T3).
  @Column({ type: 'varchar', length: 8, nullable: true })
  limitTier: 'T1' | 'T2' | 'T3' | null;

  @Column({ type: 'varchar', length: 128, nullable: true })
  orderId: string | null;

  @Index()
  @Column({ type: 'varchar', length: 32, default: 'pending_resolve' })
  status: MarketLogStatus;

  @Column({ type: 'text', nullable: true })
  skipReason: string | null;

  @Column({ type: 'text', nullable: true })
  errorMessage: string | null;

  @Column({ type: 'text', nullable: true })
  logMessage: string | null;

  // --- Диагностика по внешнему ценовому фиду. По умолчанию источник —
  // Chainlink (см. PriceFeedService/README): это ТОТ ЖЕ фид, которым
  // Polymarket резолвит крипто-маркеты (data.chain.link), получен через
  // официальный публичный Polymarket RTDS (wss://ws-live-data.polymarket.com,
  // топик crypto_prices_chainlink) — без API-ключа. Раньше здесь стоял
  // Binance/Bybit (приближение, не то же самое, что видел резолвер) — это
  // и было причиной расхождений между нашей диагностикой и фактическим
  // исходом маркета. Binance/Bybit остаются как фолбэк-провайдеры на случай
  // недоступности RTDS (см. priceSource ниже — какой источник реально дал
  // это конкретное значение). ---

  // Цена базового актива по фиду в момент открытия 5-минутного окна (наш локальный
  // ориентир "тригера" UP/DOWN — сам Polymarket точную секунду фиксации не отдаёт).
  @Column({ type: 'double precision', nullable: true })
  referencePrice: number | null;

  // Цена по фиду в момент входа в позицию.
  @Column({ type: 'double precision', nullable: true })
  priceAtEntry: number | null;

  // ATR (среднее high-low за последние FEED_ATR_CANDLES свечей фида) на момент входа.
  @Column({ type: 'double precision', nullable: true })
  atrAtEntry: number | null;

  // |priceAtEntry - referencePrice| / atrAtEntry — насколько "убедительно" далеко
  // была цена от точки старта окна относительно недавней волатильности.
  @Column({ type: 'double precision', nullable: true })
  atrRatioAtEntry: number | null;

  // То же самое, но зафиксированное в момент закрытия окна (для сравнения "было/стало").
  @Column({ type: 'double precision', nullable: true })
  priceAtClose: number | null;

  @Column({ type: 'double precision', nullable: true })
  atrAtClose: number | null;

  // Человекочитаемая причина, заполняется резолвером при status='loss'.
  @Column({ type: 'text', nullable: true })
  failReason: string | null;

  // Момент, когда бот принял решение и отправил заявку (маркет-ордер — сразу
  // перед FAK/симуляцией; лимитка — момент выставления резюм-ордера). Нужно
  // для разбора инцидентов вида "цена успела уйти между решением и фактом" —
  // раньше в логах была только цена, но не точный момент отправки/факта.
  @Column({ type: 'timestamptz', nullable: true })
  orderSentAt: Date | null;

  // Момент фактического исполнения (когда получили реальный/симулированный факт
  // сделки — для маркета почти сразу же после orderSentAt, для лимитки может
  // быть намного позже, вплоть до истечения окна).
  @Column({ type: 'timestamptz', nullable: true })
  orderFilledAt: Date | null;

  // Источник цены, из которого взяты referencePrice/priceAtEntry/ATR
  // ('chainlink' | 'binance' | 'bybit' | null). Chainlink — это буквально то,
  // чем Polymarket резолвит крипто-маркеты (см. README) — диагностика на этом
  // источнике надёжна; diagnостика на binance/bybit — лишь приближение и может
  // расходиться с фактическим резолвом.
  @Column({ type: 'varchar', length: 16, nullable: true })
  priceSource: string | null;

  @CreateDateColumn()
  createdAt: Date;

  @Column({ type: 'timestamptz', nullable: true })
  resolvedAt: Date | null;

  // --- Пре-резолв (Сессия 7, см. CONTEXT.md) ---
  // true, если betAmount этого шага был снят НЕ с подтверждённого
  // Attempt.currentStake, а с ПРЕДСКАЗАНИЯ исхода предыдущего (ещё не
  // зарезолвленного официально Gamma) шага по живой цене Chainlink —
  // см. TradingService.tryPreResolve. Профит/статус САМОГО этого шага
  // всегда считается официальным резолвером Gamma как обычно — этот флаг
  // только про то, откуда взялась СУММА ставки на входе.
  @Column({ type: 'boolean', default: false })
  stakePredicted: boolean;

  // id лога ПРЕДЫДУЩЕГО (на момент открытия этого окна ещё не
  // зарезолвленного) шага, на предсказании исхода которого была основана
  // сумма стейка — для ручного аудита, если предсказание разойдётся с
  // официальным резолвом Gamma.
  @Column({ type: 'uuid', nullable: true })
  predictedFromLogId: string | null;

  // --- Сессия 13: 4 доп. фильтра входа (обсуждение с Gemini по реальным
  // сливам, см. CONTEXT.md) — каждый включается своим ENV независимо от
  // остальных. Поля ниже пишутся ВСЕГДА (SHADOW-диагностика), даже когда
  // соответствующий фильтр выключен — материал для калибровки порогов до
  // включения блокировки, как и с исходным ATR-гейтом на старте проекта. ---

  // Час UTC на момент входа (см. BLACKOUT_HOURS_UTC).
  @Column({ type: 'int', nullable: true })
  blackoutHourAtEntry: number | null;

  // Требуемый запас цены от референса (Expected Move): atrAtEntry *
  // sqrt(t_rem/intervalSec) * SAFETY_K_FACTOR — см. EXPECTED_MOVE_FILTER_ENABLED.
  @Column({ type: 'double precision', nullable: true })
  requiredDeltaAtEntry: number | null;

  // Скорость изменения сигнатурной дельты цена-референс за DRIFT_LOOKBACK_SEC
  // секунд — положительно = дельта растёт к YES, отрицательно = к NO.
  @Column({ type: 'double precision', nullable: true })
  driftRateAtEntry: number | null;

  // Доля прошедшего времени текущего окна, когда цена была на стороне
  // ВЫБРАННОГО исхода относительно референса (Time-in-Zone, см. MIN_ZONE_RATIO).
  @Column({ type: 'double precision', nullable: true })
  zoneRatioAtEntry: number | null;

  // --- Сессия 18: edge-модель (см. edge-score.util.ts). Заполняются ВСЕГДА
  // (shadow), независимо от EDGE_GATE_ENABLED — чтобы по истории сделок
  // можно было ретроспективно оценить, что модель отсеяла бы, и на этом же
  // наборе потом калибровать веса. ---
  @Column({ type: 'double precision', nullable: true })
  atrRobustAtEntry: number | null;

  @Column({ type: 'double precision', nullable: true })
  smoothnessAtEntry: number | null;

  @Column({ type: 'double precision', nullable: true })
  pModelAtEntry: number | null;

  @Column({ type: 'double precision', nullable: true })
  impliedProbAtEntry: number | null;

  @Column({ type: 'boolean', nullable: true })
  edgeWouldEnter: boolean | null;
}
