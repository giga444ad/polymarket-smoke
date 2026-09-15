/* Ad-hoc verification script — not part of the app. Аналог
 * verify-trading-logic.ts, но для модуля бэктеста: фейковые TypeORM-репозитории
 * (in-memory), без реальной БД — проверяем, что BacktestRunnerService не падает
 * и даёт разумный результат на синтетическом сценарии "уверенный вход маркет-тейком".
 */
import 'reflect-metadata';
import { BacktestRunnerService } from '../src/backtest/backtest-runner.service';

let failed = 0;
function check(label: string, cond: boolean) {
  console.log(`[${label}] ${cond}`);
  if (!cond) failed++;
}

const fakeConfig = {
  overrides: {
    STREAMS_CONFIG: JSON.stringify([
      { streamKey: 'btc-updown-5m', kind: 'interval', intervalSec: 300, slugPrefix: 'btc-updown-5m', baseStake: 5 },
    ]),
  } as Record<string, string>,
  get(key: string, def?: string) {
    return this.overrides[key] ?? def;
  },
};

function fakeRepo(rows: any[]) {
  const qb: any = {
    where: () => qb,
    andWhere: () => qb,
    orderBy: () => qb,
    getMany: async () => rows,
  };
  return { createQueryBuilder: () => qb } as any;
}

// Окно закрывается в T0. Референс (T0 - 300_000) должен быть далеко от цены
// на входе, чтобы ATR-гейт (по умолчанию включён, 20 закрытых свечей) точно
// пропустил вход — поэтому прогреваем ATR историей нескольких ПРОШЛЫХ окон
// с небольшим и стабильным размахом, а затем даём цене уверенно уйти вверх
// прямо перед закрытием этого окна.
const T0 = 1_800_000_000_000; // произвольная опорная точка, мс
const intervalMs = 300_000;
const atrCandles = 20;

const priceTicks: any[] = [];
// Прогрев: 25 прошлых окон с колебанием +-0.05 вокруг 100 (маленький ATR).
for (let i = 25; i >= 1; i--) {
  const start = T0 - i * intervalMs;
  priceTicks.push({ ts: start + 1000, price: 100, source: 'chainlink' });
  priceTicks.push({ ts: start + intervalMs / 2, price: 100.05, source: 'chainlink' });
  priceTicks.push({ ts: start + intervalMs - 1000, price: 99.97, source: 'chainlink' });
}
// Референс текущего окна — ровно на границе (T0 - intervalMs).
priceTicks.push({ ts: T0 - intervalMs, price: 100, source: 'chainlink' });
// Уверенный уход цены вверх на входе (намного больше ATR ~0.08).
priceTicks.push({ ts: T0 - 5000, price: 100.5, source: 'chainlink' });

const pmTicks: any[] = [
  { ts: T0 - 4000, slug: 'btc-updown-5m-fake', yesBestBid: 0.97, yesBestAsk: 0.985, noBestBid: null, noBestAsk: null },
  { ts: T0 - 2000, slug: 'btc-updown-5m-fake', yesBestBid: 0.98, yesBestAsk: 0.991, noBestBid: null, noBestAsk: null },
  { ts: T0, slug: 'btc-updown-5m-fake', yesBestBid: 0.985, yesBestAsk: 0.992, noBestBid: null, noBestAsk: null },
];

async function main() {
  const svc = new BacktestRunnerService(fakeConfig as any, fakeRepo(priceTicks), fakeRepo(pmTicks));

  const result = await svc.run({
    streamKey: 'btc-updown-5m',
    from: T0 - intervalMs,
    to: T0,
  });

  check('Один найденный оконный слот (по PM-тикам)', result.windowsTotal === 1);
  check('usedPriceSource = chainlink (единственный источник в фейковых данных)', result.usedPriceSource === 'chainlink');
  check('Есть ровно одна сделка в результате', result.trades.length === 1);
  const trade = result.trades[0];
  check('Вход маркет-тейком (SIMULATED_MARKET)', trade.orderType === 'SIMULATED_MARKET');
  check('Выбран YES (цена ушла выше референса)', trade.chosenOutcome === 'YES');
  check('entryPrice взят из лучшего ask (~0.991 или 0.992)', trade.entryPrice != null && trade.entryPrice >= 0.99);
  check('referencePrice = 100 (граница окна)', trade.referencePrice === 100);
  check('atrRatioAtEntry посчитан (ATR прогрет 25 прошлыми окнами >= 20)', trade.atrRatioAtEntry != null);
  check('Статус — win или loss (исход определён, не unfilled/skipped)', trade.status === 'win' || trade.status === 'loss');
  check('limitations непустой (ограничения бэктеста задокументированы в ответе)', result.limitations.length > 0);

  console.log(JSON.stringify(result, null, 2));

  if (failed > 0) {
    console.error(`\n${failed} ПРОВЕРОК ПРОВАЛЕНО.`);
    process.exit(1);
  }
  console.log('\nВСЕ ПРОВЕРКИ ВЫПОЛНЕНЫ.');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
