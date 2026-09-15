export interface BacktestRunRequest {
  streamKey: string;
  /** ISO-строка или unix ms — начало диапазона. */
  from: string | number;
  /** ISO-строка или unix ms — конец диапазона. */
  to: string | number;
  /**
   * Любые ENV-переменные гейта/фильтров/окон входа, которые нужно
   * переопределить именно для этого прогона (напр. "поиграться с
   * настройками", как и попросил пользователь) — те же ключи, что и в
   * .env.example: ENTRY_FILTER_ENABLED, MIN_DISTANCE_ATR_RATIO,
   * BLACKOUT_HOURS_FILTER_ENABLED, BLACKOUT_HOURS_UTC,
   * EXPECTED_MOVE_FILTER_ENABLED, SAFETY_K_FACTOR,
   * DIRECTIONAL_DRIFT_FILTER_ENABLED, DRIFT_LOOKBACK_SEC,
   * TIME_IN_ZONE_FILTER_ENABLED, MIN_ZONE_RATIO, LAST_ENTRY_WINDOW_SEC,
   * LIMIT_TIER2_SECONDS, LIMIT_TIER3_SECONDS, LIMIT_TIER1/2/3_PRICE,
   * MIN_MARKET_PRICE, MAX_MARKET_PRICE, FAVORITE_BID_THRESHOLD,
   * MIN_FILL_RATIO, MAX_OVERSPEND_MULTIPLIER, FEED_ATR_CANDLES,
   * FEED_STALE_MS, FEED_RECENT_TICKS_RETENTION_MS. Не заданные ключи берут
   * значение из текущего `.env` процесса (те же дефолты, что и у боевого
   * TradingService), совпадая по умолчанию с тем, как реально торговал бот.
   */
  envOverrides?: Record<string, string>;
  /** Порядок приоритета источников тиков (см. ReplayPriceSource). По
   *  умолчанию chainlink -> binance -> bybit (как FEED_PROVIDERS). */
  providerPriority?: string[];
  /** Предполагаемый минимальный размер ордера биржи (в реале приходит от
   *  CLOB API, в истории не сохранён) — см. ограничения в README ответа. */
  assumedMinOrderSize?: number;
}

export interface BacktestTradeResult {
  slug: string; // синтетический id окна (closesAt в ISO) — в бэктесте не запрашивается Gamma
  windowStartMs: number;
  closesAtMs: number;
  betAmount: number;
  chosenOutcome: 'YES' | 'NO' | null;
  entryPrice: number | null;
  filledAmount: number | null;
  fillRatio: number | null;
  orderType: 'SIMULATED_MARKET' | 'SIMULATED_LIMIT' | null;
  limitTier: 'T1' | 'T2' | 'T3' | null;
  status: 'win' | 'loss' | 'skipped' | 'unfilled';
  referencePrice: number | null;
  priceAtEntry: number | null;
  atrRatioAtEntry: number | null;
  priceAtClose: number | null;
  impliedWinnerSide: 'YES' | 'NO' | null;
  profit: number | null;
  skipReason: string | null;
}

export interface BacktestSummary {
  streamKey: string;
  fromMs: number;
  toMs: number;
  windowsTotal: number;
  wins: number;
  losses: number;
  skipped: number;
  unfilled: number;
  totalProfit: number;
  avgEntryPrice: number | null;
  usedPriceSource: string | null;
  dataQuality: {
    priceTicksLoaded: number;
    polymarketTicksLoaded: number;
    windowsWithoutPmTicks: number;
    windowsWithoutReferencePrice: number;
  };
  limitations: string[];
  trades: BacktestTradeResult[];
}
