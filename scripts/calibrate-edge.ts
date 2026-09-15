/**
 * Сессия 19 — калибровка edge-модели (см. edge-score.util.ts) на данных
 * EdgeSamplerService (таблица edge_score_samples).
 *
 * ВАЖНО про природу данных: сэмплер пишет по строке КАЖДУЮ секунду на
 * каждое активное окно, поэтому "количество строк" НЕ равно "количеству
 * независимых наблюдений" — соседние секунды одного окна почти идентичны
 * и по фичам, и по итоговому исходу (сильная автокорреляция). Реальная
 * статистическая мощность зависит от числа ОКОН (уникальных slug), а не
 * строк — этот скрипт явно печатает оба числа, чтобы не создавать иллюзию
 * большой выборки там, где её нет.
 *
 * Что делает:
 * 1) Тянет все строки edge_score_samples.
 * 2) Для каждого (assetPrefix, slug) берёт ПОСЛЕДНИЙ по времени сэмпл
 *    (минимальный timeLeftSec) как прокси финального исхода — это самый
 *    свежий снимок цены относительно референса перед закрытием окна,
 *    достаточно точный прокси для "какая сторона в итоге победила"
 *    (тот же принцип, что и priceAtClose в market-log.entity.ts).
 * 3) Каждую БОЛЕЕ РАННЮЮ строку того же окна размечает как
 *    win=1/0 — совпал ли impliedSide той строки с финальной стороной.
 * 4) Даёт ДВЕ выборки на выбор: "все строки" (много, но автокоррелированные)
 *    и "одна строка на окно, ближайшая к TARGET_TIME_LEFT_SEC" (мало, но
 *    независимые) — обучает логрегрессию на обеих и печатает обе, чтобы
 *    сравнить, насколько выводы устойчивы.
 * 5) Обучает простую логистическую регрессию (батч-градиентный спуск,
 *    без внешних ML-зависимостей) на train/test СПЛИТЕ ПО ВРЕМЕНИ (не
 *    перемешивая случайно — иначе соседние секунды одного окна попадут и
 *    в train, и в test, и AUC будет обманчиво хорошим).
 *
 * Запуск (нужен доступ к той же Postgres, что и бот):
 *   npx ts-node scripts/calibrate-edge.ts
 * Читает переменные подключения из тех же POSTGRES_* ENV, что и сам бот.
 */
import 'dotenv/config';
import { Client } from 'pg';
import { computeEdgeFeatures, EdgeFeatures } from '../src/trading/edge-score.util';

interface Row {
  assetPrefix: string;
  slug: string;
  sampledAtMs: number;
  timeLeftSec: number;
  delta: number | null;
  atrRobust: number | null;
  driftRate: number | null;
  zoneRatio: number | null;
  smoothness: number | null;
  impliedSide: 'YES' | 'NO' | null;
}

// Насколько близко к закрытию окна брать "тестовую" точку для выборки
// "одна строка на окно" (п.4 выше) — по умолчанию 20с, ориентировочно
// соответствует зоне тир-2/тир-3 текущих лимитников.
const TARGET_TIME_LEFT_SEC = parseFloat(process.env.CALIBRATE_TARGET_TIME_LEFT_SEC ?? '20');
// Доля данных (по ВРЕМЕНИ, не случайно) на train — остальное на test.
const TRAIN_RATIO = parseFloat(process.env.CALIBRATE_TRAIN_RATIO ?? '0.7');

async function main() {
  if (!process.env.POSTGRES_HOST) {
    console.error(
      'POSTGRES_HOST не задан — .env не найден или запущен не из корня проекта. ' +
        'Проверь, что файл .env лежит рядом с package.json и содержит POSTGRES_HOST/POSTGRES_DB/POSTGRES_USER/POSTGRES_PASSWORD.',
    );
    process.exit(1);
  }
  const client = new Client({
    host: process.env.POSTGRES_HOST,
    port: parseInt(process.env.POSTGRES_PORT ?? '5432', 10),
    database: process.env.POSTGRES_DB,
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();

  const { rows: raw } = await client.query<Row>(`
    SELECT "assetPrefix", slug, "sampledAtMs", "timeLeftSec", delta,
           "atrRobust", "driftRate", "zoneRatio", smoothness, "impliedSide"
    FROM edge_score_samples
    ORDER BY "sampledAtMs" ASC
  `);
  await client.end();

  console.log(`Строк всего: ${raw.length}`);

  // --- Группировка по окну ---
  const byWindow = new Map<string, Row[]>();
  for (const r of raw) {
    const key = `${r.assetPrefix}::${r.slug}`;
    if (!byWindow.has(key)) byWindow.set(key, []);
    byWindow.get(key)!.push(r);
  }
  console.log(`Уникальных окон (реальная статистическая мощность): ${byWindow.size}`);
  if (byWindow.size < 100) {
    console.log(
      `\n⚠️  Меньше 100 независимых окон — коэффициентам ниже НЕ стоит доверять как финальным.\n` +
        `   Это ориентир направления (знаки, порядок величины), не готовые к EDGE_GATE_ENABLED=true веса.\n`,
    );
  }

  type Labeled = { features: EdgeFeatures; win: boolean; sampledAtMs: number; slug: string };
  const allLabeled: Labeled[] = [];
  const oneRowPerWindow: Labeled[] = [];

  for (const [, windowRows] of byWindow) {
    windowRows.sort((a, b) => a.timeLeftSec - b.timeLeftSec); // по возрастанию timeLeftSec = от конца к началу
    const finalRow = windowRows[0]; // минимальный timeLeftSec = ближе всего к закрытию
    if (finalRow.delta == null) continue; // не знаем, куда в итоге пришла цена — окно без исхода, пропуск
    const finalSide: 'YES' | 'NO' = finalRow.delta >= 0 ? 'YES' : 'NO';

    let closestToTarget: Labeled | null = null;
    let closestDiff = Infinity;

    for (const r of windowRows) {
      if (r.impliedSide == null) continue;
      const win = r.impliedSide === finalSide;
      const features = computeEdgeFeatures({
        delta: r.delta,
        atrRobust: r.atrRobust,
        driftRate: r.driftRate,
        zoneRatio: r.zoneRatio,
        smoothness: r.smoothness,
        outcome: r.impliedSide,
      });
      const labeled: Labeled = { features, win, sampledAtMs: r.sampledAtMs, slug: r.slug };
      allLabeled.push(labeled);

      const diff = Math.abs(r.timeLeftSec - TARGET_TIME_LEFT_SEC);
      if (diff < closestDiff) {
        closestDiff = diff;
        closestToTarget = labeled;
      }
    }
    if (closestToTarget) oneRowPerWindow.push(closestToTarget);
  }

  console.log(`\n=== Выборка A: ВСЕ строки (автокоррелированные, n=${allLabeled.length}) ===`);
  runCalibration(allLabeled);

  console.log(
    `\n=== Выборка B: ОДНА строка на окно, ближайшая к ${TARGET_TIME_LEFT_SEC}с до закрытия (независимые, n=${oneRowPerWindow.length}) ===`,
  );
  runCalibration(oneRowPerWindow);

  console.log(
    `\nЕсли знаки коэффициентов и общая картина (AUC) в выборках A и B заметно РАСХОДЯТСЯ — верь выборке B, ` +
      `а не A: A просто мощнее статистически ВЫГЛЯДИТ из-за автокорреляции, но это иллюзия.`,
  );
}

function runCalibration(data: { features: EdgeFeatures; win: boolean; sampledAtMs: number }[]) {
  const complete = data.filter(
    (d) => d.features.z != null && d.features.driftAligned != null && d.features.zoneRatio != null && d.features.smoothSigned != null,
  );
  if (complete.length < 10) {
    console.log(`Недостаточно строк с полным набором фич (${complete.length}) — пропуск.`);
    return;
  }
  console.log(`Строк с полным набором фич (z/drift/zone/smooth все не null): ${complete.length} из ${data.length}`);

  complete.sort((a, b) => a.sampledAtMs - b.sampledAtMs);
  const splitIdx = Math.floor(complete.length * TRAIN_RATIO);
  const train = complete.slice(0, splitIdx);
  const test = complete.slice(splitIdx);

  const X = train.map((d) => [1, d.features.z!, d.features.driftAligned!, d.features.zoneRatio!, d.features.smoothSigned!]);
  const y = train.map((d) => (d.win ? 1 : 0));

  const weights = trainLogisticRegression(X, y);
  console.log(
    `Обученные веса (train n=${train.length}): bias=${weights[0].toFixed(3)}, z=${weights[1].toFixed(3)}, ` +
      `drift=${weights[2].toFixed(3)}, zone=${weights[3].toFixed(3)}, smooth=${weights[4].toFixed(3)}`,
  );

  if (test.length >= 5) {
    const auc = computeAuc(test, weights);
    console.log(`AUC на отложенной выборке (test n=${test.length}, идёт ПОСЛЕ train по времени): ${auc.toFixed(3)} (0.5 = случайность, 1.0 = идеал)`);
  } else {
    console.log('Тестовой выборки почти нет — AUC не считаю, накопи больше данных.');
  }
}

/** Батч-градиентный спуск, без внешних ML-зависимостей — фич всего 5, сходится быстро. */
function trainLogisticRegression(X: number[][], y: number[], lr = 0.1, iters = 3000): number[] {
  const n = X.length;
  const d = X[0].length;
  let w = new Array(d).fill(0);
  for (let it = 0; it < iters; it++) {
    const grad = new Array(d).fill(0);
    for (let i = 0; i < n; i++) {
      const z = dot(w, X[i]);
      const p = 1 / (1 + Math.exp(-z));
      const err = p - y[i];
      for (let j = 0; j < d; j++) grad[j] += err * X[i][j];
    }
    for (let j = 0; j < d; j++) w[j] -= (lr * grad[j]) / n;
  }
  return w;
}

function dot(a: number[], b: number[]): number {
  let s = 0;
  for (let i = 0; i < a.length; i++) s += a[i] * b[i];
  return s;
}

function computeAuc(test: { features: EdgeFeatures; win: boolean }[], weights: number[]): number {
  const scored = test.map((d) => ({
    score: dot(weights, [1, d.features.z!, d.features.driftAligned!, d.features.zoneRatio!, d.features.smoothSigned!]),
    win: d.win,
  }));
  const positives = scored.filter((s) => s.win);
  const negatives = scored.filter((s) => !s.win);
  if (positives.length === 0 || negatives.length === 0) return NaN;
  let concordant = 0;
  for (const p of positives) {
    for (const n of negatives) {
      if (p.score > n.score) concordant++;
      else if (p.score === n.score) concordant += 0.5;
    }
  }
  return concordant / (positives.length * negatives.length);
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
