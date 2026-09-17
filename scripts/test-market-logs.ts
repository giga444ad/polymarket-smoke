import "reflect-metadata";
import { DataSource, DataSourceOptions } from "typeorm";
import * as dotenv from "dotenv";

dotenv.config();

// ============================================================================
// НАСТРОЙКА И СПИСОК ВЕРСИЙ
// Добавляйте сюда нужные теги: ["V3", "V4", "V5", "V6"]
// ============================================================================
const CONFIG_TAGS = ["V3", "V4"] as const;

const FIXED_STAKE = 5;
const SLOTS_PER_DAY = 288;

type ConfigName = string;

/**
 * Унифицированное получение конфигурации Postgres по тегу.
 * Формирует имена переменных окружения по шаблону: ${TAG}_POSTGRES_...
 */
function getPGConfig(tag: string): DataSourceOptions {
  const host = process.env[`${tag}_POSTGRES_HOST`];
  const port = Number(process.env[`${tag}_POSTGRES_PORT`] || 5432);
  const username = process.env[`${tag}_POSTGRES_USER`];
  const password = process.env[`${tag}_POSTGRES_PASSWORD`];
  const database = process.env[`${tag}_POSTGRES_DB`];

  if (!host || !username || !database) {
    console.warn(`[WARNING] Missing Postgres credentials for tag: ${tag}`);
  }

  return {
    type: "postgres",
    host,
    port,
    username,
    password,
    database,
  };
}

interface Trade {
  id: number;
  slug: string;
  status: string;
  executed: boolean;
  chosenOutcome: string | null;

  entryPrice: number | null;
  betAmount: number | null;
  filledAmount: number | null;
  fillRatio: number | null;
  profit: number | null;

  orderType: string | null;
  limitTier: string | null;

  orderSentAt: string | null;
  orderFilledAt: string | null;
  resolvedAt: string | null;

  atrRatioAtEntry: number | null;
  driftRateAtEntry: number | null;
  zoneRatioAtEntry: number | null;
}

async function getTrades(
  name: ConfigName,
  from?: string,
  to?: string
): Promise<Trade[]> {
  const pgConfig = getPGConfig(name);
  const ds = new DataSource(pgConfig);

  await ds.initialize();

  let sql = `
    SELECT
      id,
      slug,
      status,
      executed,
      "chosenOutcome",
      "entryPrice",
      "betAmount",
      "filledAmount",
      "fillRatio",
      profit,
      "orderType",
      "limitTier",
      "orderSentAt",
      "orderFilledAt",
      "resolvedAt",
      "atrRatioAtEntry",
      "driftRateAtEntry",
      "zoneRatioAtEntry"
    FROM market_logs
    WHERE "isSmoke" = true
      AND "assetPrefix" = 'btc-updown-5m'
  `;

  const params: any[] = [];

  if (from) {
    params.push(from);
    sql += ` AND "orderSentAt" >= $${params.length}`;
  }

  if (to) {
    params.push(to);
    sql += ` AND "orderSentAt" <= $${params.length}`;
  }

  sql += ` ORDER BY "orderSentAt" ASC`;

  const rows = await ds.query(sql, params);

  await ds.destroy();

  return rows;
}

function num(x: any): number {
  if (x === null || x === undefined || x === "") return 0;
  return Number(x);
}

function pct(x: number): string {
  return `${(x * 100).toFixed(2)}%`;
}

function money(x: number): string {
  return `$${x.toFixed(4)}`;
}

function avg(values: number[]): number {
  if (!values.length) return 0;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

function median(values: number[]): number {
  if (!values.length) return 0;

  const a = [...values].sort((x, y) => x - y);
  const mid = Math.floor(a.length / 2);

  return a.length % 2
    ? a[mid]
    : (a[mid - 1] + a[mid]) / 2;
}

function percentile(values: number[], p: number): number {
  if (!values.length) return 0;

  const a = [...values].sort((x, y) => x - y);
  const index = (a.length - 1) * p;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);

  if (lower === upper) return a[lower];

  return a[lower] + (a[upper] - a[lower]) * (index - lower);
}

function normalizedProfit(
  profit: number,
  betAmount: number
): number {
  if (!betAmount || betAmount <= 0) return 0;

  return (profit / betAmount) * FIXED_STAKE;
}

function printPriceBuckets(trades: Trade[]) {
  const buckets = [
    { name: "0.90-0.92", min: 0.90, max: 0.92 },
    { name: "0.92-0.94", min: 0.92, max: 0.94 },
    { name: "0.94-0.96", min: 0.94, max: 0.96 },
    { name: "0.96-0.98", min: 0.96, max: 0.98 },
    { name: "0.98-0.99", min: 0.98, max: 0.991 },
  ];

  console.log("\nEntry price distribution:");

  for (const b of buckets) {
    const rows = trades.filter(t => {
      const p = num(t.entryPrice);
      return p >= b.min && p < b.max;
    });

    const profits = rows.map(t =>
      normalizedProfit(
        num(t.profit),
        num(t.betAmount)
      )
    );

    console.log(
      `${b.name.padEnd(12)} ` +
      `n=${String(rows.length).padStart(3)} ` +
      `normProfit=${money(profits.reduce((a, b) => a + b, 0))}`
    );
  }
}

function analyze(name: ConfigName, trades: Trade[]) {
  const validDates = trades
    .map(t => (t.orderSentAt ? new Date(t.orderSentAt).getTime() : 0))
    .filter(d => d > 0)
    .sort((a, b) => a - b);

  const startTime = validDates.length > 0 ? new Date(validDates[0]) : null;
  const endTime = validDates.length > 0 ? new Date(validDates[validDates.length - 1]) : null;

  const durationMs = startTime && endTime ? endTime.getTime() - startTime.getTime() : 0;
  const totalDays = Math.max(durationMs / (1000 * 60 * 60 * 24), 1 / 288);

  const scannedMarkets = new Set(
    trades.map(t => t.slug).filter(Boolean)
  ).size;

  const executed = trades.filter(t => t.executed);

  const resolved = executed.filter(t =>
    t.status === "win" || t.status === "loss"
  );

  const wins = resolved.filter(t => t.status === "win");
  const losses = resolved.filter(t => t.status === "loss");

  const filled = executed.filter(t =>
    num(t.filledAmount) > 0
  );

  const totalBetAmount = filled.reduce(
    (sum, t) => sum + num(t.betAmount),
    0
  );

  const totalFilled = filled.reduce(
    (sum, t) => sum + num(t.filledAmount),
    0
  );

  const actualProfit = resolved.reduce(
    (sum, t) => sum + num(t.profit),
    0
  );

  const normalizedProfits = resolved.map(t =>
    normalizedProfit(
      num(t.profit),
      num(t.betAmount)
    )
  );

  const normalizedTotalProfit =
    normalizedProfits.reduce((a, b) => a + b, 0);

  const normalizedROI =
    totalBetAmount > 0
      ? normalizedTotalProfit /
        (filled.length * FIXED_STAKE)
      : 0;

  const actualROI =
    totalFilled > 0
      ? actualProfit / totalFilled
      : 0;

  const entries = filled
    .map(t => num(t.entryPrice))
    .filter(x => x > 0);

  const fillRatios = filled
    .map(t => num(t.fillRatio))
    .filter(x => x > 0);

  const progressionStakes = filled
    .map(t => num(t.betAmount))
    .filter(x => x > 0);

  const marketOrders = filled.filter(
    t => t.orderType === "FAK" ||
         t.orderType === "SIMULATED_MARKET"
  ).length;

  const limitOrders = filled.filter(
    t => t.orderType === "GTD" ||
         t.orderType === "SIMULATED_LIMIT"
  ).length;

  const tiers = {
    T1: filled.filter(t => t.limitTier === "T1").length,
    T2: filled.filter(t => t.limitTier === "T2").length,
    T3: filled.filter(t => t.limitTier === "T3").length,
  };

  const maxStake = progressionStakes.length
    ? Math.max(...progressionStakes)
    : 0;

  const avgStake = avg(progressionStakes);

  const maxWinStreak = (() => {
    let current = 0;
    let max = 0;

    for (const t of resolved) {
      if (t.status === "win") {
        current++;
        max = Math.max(max, current);
      } else {
        current = 0;
      }
    }

    return max;
  })();

  const scannedPerDay = scannedMarkets / totalDays;
  const skippedPerDay = Math.max(0, SLOTS_PER_DAY - scannedPerDay);
  const executedPerDay = executed.length / totalDays;
  const winsPerDay = wins.length / totalDays;
  const lossesPerDay = losses.length / totalDays;
  const actualProfitPerDay = actualProfit / totalDays;
  const normalizedProfitPerDay = normalizedTotalProfit / totalDays;

  return {
    name,

    startTime,
    endTime,
    totalDays,

    scannedMarkets,
    executed: executed.length,
    resolved: resolved.length,
    wins: wins.length,
    losses: losses.length,

    scannedPerDay,
    skippedPerDay,
    executedPerDay,
    winsPerDay,
    lossesPerDay,
    actualProfitPerDay,
    normalizedProfitPerDay,

    actualProfit,
    actualROI,

    normalizedTotalProfit,
    normalizedROI,

    totalBetAmount,
    totalFilled,

    avgEntry: avg(entries),
    medianEntry: median(entries),
    p25Entry: percentile(entries, 0.25),
    p75Entry: percentile(entries, 0.75),

    avgStake,
    maxStake,

    avgFillRatio: avg(fillRatios),

    profitPerMarket:
      scannedMarkets > 0
        ? actualProfit / scannedMarkets
        : 0,

    normalizedProfitPerMarket:
      scannedMarkets > 0
        ? normalizedTotalProfit / scannedMarkets
        : 0,

    marketOrders,
    limitOrders,

    T1: tiers.T1,
    T2: tiers.T2,
    T3: tiers.T3,

    maxWinStreak,

    trades: resolved,
  };
}

function printAnalysis(a: ReturnType<typeof analyze>) {
  const startStr = a.startTime ? a.startTime.toISOString().replace("T", " ").slice(0, 19) : "N/A";
  const endStr = a.endTime ? a.endTime.toISOString().replace("T", " ").slice(0, 19) : "N/A";

  console.log(`
${a.name}
----------------------------------------
TIME RANGE
Start time            : ${startStr}
End time              : ${endStr}
Duration              : ${a.totalDays.toFixed(2)} days

TOTAL METRICS
Markets scanned       : ${a.scannedMarkets}
Executed              : ${a.executed}
Resolved              : ${a.resolved}
Wins / Losses         : ${a.wins} / ${a.losses}

DAILY METRICS (1 day = 288 slots)
Scanned per day       : ${a.scannedPerDay.toFixed(1)} / 288 (${pct(a.scannedPerDay / SLOTS_PER_DAY)})
Skipped per day       : ${a.skippedPerDay.toFixed(1)} / 288 (${pct(a.skippedPerDay / SLOTS_PER_DAY)})
Executions per day    : ${a.executedPerDay.toFixed(1)}
Wins / Losses per day : ${a.winsPerDay.toFixed(1)} / ${a.lossesPerDay.toFixed(1)}
Actual profit / day   : ${money(a.actualProfitPerDay)}
Norm profit / day     : ${money(a.normalizedProfitPerDay)}

ACTUAL PROGRESSION
Total bet amount      : ${money(a.totalBetAmount)}
Total filled          : ${money(a.totalFilled)}
Actual profit         : ${money(a.actualProfit)}
Actual ROI            : ${pct(a.actualROI)}

NORMALIZED ($${FIXED_STAKE})
Normalized profit     : ${money(a.normalizedTotalProfit)}
Normalized ROI        : ${pct(a.normalizedROI)}
Norm profit / market  : ${money(a.normalizedProfitPerMarket)}

ENTRY
Average entry         : ${a.avgEntry.toFixed(4)}
Median entry          : ${a.medianEntry.toFixed(4)}
P25 / P75             : ${a.p25Entry.toFixed(4)} / ${a.p75Entry.toFixed(4)}

PROGRESSION
Average stake         : ${money(a.avgStake)}
Max stake             : ${money(a.maxStake)}
Max win streak        : ${a.maxWinStreak}

EXECUTION
Average fill ratio    : ${pct(a.avgFillRatio)}
Market orders         : ${a.marketOrders}
Limit orders          : ${a.limitOrders}
T1 / T2 / T3          : ${a.T1} / ${a.T2} / ${a.T3}
`);

  printPriceBuckets(a.trades);
}

async function main() {
  const from = process.env.ANALYSIS_FROM;
  const to = process.env.ANALYSIS_TO;

  if (from || to) {
    console.log("\nAnalysis period filter:");
    console.log(`FROM: ${from || "beginning"}`);
    console.log(`TO  : ${to || "end"}`);
  }

  const results: Record<string, ReturnType<typeof analyze>> = {};

  // Итерируемся по массиву конфигурационных тегов
  for (const name of CONFIG_TAGS) {
    console.log(`\nAnalyzing ${name}...`);

    const trades = await getTrades(name, from, to);

    results[name] = analyze(name, trades);
  }

  console.log("\n========================================");
  console.log("5m PROGRESSION-ADJUSTED ECONOMICS");
  console.log("========================================");

  for (const name of CONFIG_TAGS) {
    printAnalysis(results[name]);
  }

  console.log("\n========================================");
  console.log("FINAL COMPARISON");
  console.log("========================================");

  console.log(
    "\nConfig | Days  | ActProfit/Day | NormProfit/Day | Wins/Day | Loss/Day | Skip/Day | ActualROI | NormROI"
  );

  for (const name of CONFIG_TAGS) {
    const a = results[name];

    console.log(
      `${name.padEnd(6)} | ` +
      `${a.totalDays.toFixed(1).padStart(5)} | ` +
      `${money(a.actualProfitPerDay).padStart(13)} | ` +
      `${money(a.normalizedProfitPerDay).padStart(14)} | ` +
      `${a.winsPerDay.toFixed(1).padStart(8)} | ` +
      `${a.lossesPerDay.toFixed(1).padStart(8)} | ` +
      `${a.skippedPerDay.toFixed(1).padStart(8)} | ` +
      `${pct(a.actualROI).padStart(9)} | ` +
      `${pct(a.normalizedROI).padStart(7)}`
    );
  }

  console.log("\n========================================");
  console.log("INTERPRETATION");
  console.log("========================================");

  const ranked = [...CONFIG_TAGS].sort(
    (a, b) =>
      results[b].normalizedProfitPerMarket -
      results[a].normalizedProfitPerMarket
  );

  console.log("\nNormalized profit/market order:");

  ranked.forEach((name, i) => {
    console.log(
      `${i + 1}. ${name}: ` +
      money(results[name].normalizedProfitPerMarket)
    );
  });
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});