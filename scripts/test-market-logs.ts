import "reflect-metadata";
import { DataSource } from "typeorm";
import * as dotenv from "dotenv";

dotenv.config();

const configs = {
  V2: {
    host: process.env.V2_POSTGRES_HOST,
    port: Number(process.env.V2_POSTGRES_PORT || 5432),
    username: process.env.V2_POSTGRES_USER,
    password: process.env.V2_POSTGRES_PASSWORD,
    database: process.env.V2_POSTGRES_DB,
  },
  V3: {
    host: process.env.V3_POSTGRES_HOST,
    port: Number(process.env.V3_POSTGRES_PORT || 5432),
    username: process.env.V3_POSTGRES_USER,
    password: process.env.V3_POSTGRES_PASSWORD,
    database: process.env.V3_POSTGRES_DB,
  },
  V4: {
    host: process.env.V4_POSTGRES_HOST,
    port: Number(process.env.V4_POSTGRES_PORT || 5432),
    username: process.env.V4_POSTGRES_USER,
    password: process.env.V4_POSTGRES_PASSWORD,
    database: process.env.V4_POSTGRES_DB,
  },
};

const FIXED_STAKE = 5;

type ConfigName = keyof typeof configs;

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
  const cfg = configs[name];

  const ds = new DataSource({
    type: "postgres",
    host: cfg.host,
    port: cfg.port,
    username: cfg.username,
    password: cfg.password,
    database: cfg.database,
  });

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

/**
 * Нормализация progression.
 *
 * Если реальная ставка была $X и реальная прибыль была $P,
 * то прибыль при условной ставке $5:
 *
 * normalizedProfit = P / X * 5
 *
 * Таким образом размер progression НЕ влияет на результат.
 */
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

  return {
    name,

    scannedMarkets,
    executed: executed.length,
    resolved: resolved.length,
    wins: wins.length,
    losses: losses.length,

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
  console.log(`
${a.name}
----------------------------------------
Markets scanned       : ${a.scannedMarkets}
Executed              : ${a.executed}
Resolved              : ${a.resolved}
Wins / Losses         : ${a.wins} / ${a.losses}

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
    console.log("\nAnalysis period:");
    console.log(`FROM: ${from || "beginning"}`);
    console.log(`TO  : ${to || "end"}`);
  }

  const results: Record<string, ReturnType<typeof analyze>> = {};

  for (const name of ["V2", "V3", "V4"] as ConfigName[]) {
    console.log(`\nAnalyzing ${name}...`);

    const trades = await getTrades(name, from, to);

    results[name] = analyze(name, trades);
  }

  console.log("\n========================================");
  console.log("5m PROGRESSION-ADJUSTED ECONOMICS");
  console.log("========================================");

  for (const name of ["V2", "V3", "V4"] as ConfigName[]) {
    printAnalysis(results[name]);
  }

  console.log("\n========================================");
  console.log("FINAL COMPARISON");
  console.log("========================================");

  console.log(
    "\nConfig | ActualProfit | ActualROI | NormProfit | NormROI | NormProfit/Market | AvgEntry | MaxStake"
  );

  for (const name of ["V2", "V3", "V4"] as ConfigName[]) {
    const a = results[name];

    console.log(
      `${name.padEnd(6)} | ` +
      `${money(a.actualProfit).padStart(12)} | ` +
      `${pct(a.actualROI).padStart(9)} | ` +
      `${money(a.normalizedTotalProfit).padStart(10)} | ` +
      `${pct(a.normalizedROI).padStart(7)} | ` +
      `${money(a.normalizedProfitPerMarket).padStart(17)} | ` +
      `${a.avgEntry.toFixed(4).padStart(8)} | ` +
      `${money(a.maxStake).padStart(8)}`
    );
  }

  console.log("\n========================================");
  console.log("INTERPRETATION");
  console.log("========================================");

  const ranked = (["V2", "V3", "V4"] as ConfigName[])
    .sort(
      (a, b) =>
        results[b].normalizedProfitPerMarket -
        results[a].normalizedProfitPerMarket
    );

  console.log(
    "\nNormalized profit/market order:"
  );

  ranked.forEach((name, i) => {
    console.log(
      `${i + 1}. ${name}: ` +
      money(results[name].normalizedProfitPerMarket)
    );
  });

  console.log(`
IMPORTANT:
Normalized profit removes the effect of progression stake size.
Actual profit preserves the real progression economics.

For choosing the strategy itself, pay particular attention to:
1. Normalized profit / market
2. Normalized ROI
3. Number of executions
4. Fill rate
5. Entry price distribution

Actual profit is still relevant for the real bankroll,
but it is NOT sufficient for comparing the underlying configs
when progression sizes differ.
`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});