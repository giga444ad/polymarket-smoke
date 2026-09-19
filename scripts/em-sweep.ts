/**
 * EM (Expected-Move) свип по market_logs — оценка ЦЕНЫ и ПОЛЬЗЫ фильтра
 * Expected-Move при разных значениях коэффициента k (SAFETY_K_FACTOR).
 *
 * Зачем: EM блокирует вход, если |цена - референс| < ATR×√(t_rem/interval)×k.
 * Он режет часть сделок. Этот скрипт по фактической истории считает, сколько
 * ВЫИГРЫШЕЙ и сколько ЛУЗОВ каждый k заблокировал бы — то есть цену (потерянные
 * W) против пользы (пойманные L). Решение по EM принимаем по этой таблице, а не
 * по одной сделке.
 *
 * Методологические оговорки (ВАЖНО, читать перед выводами):
 *  - Диагностика (requiredDeltaAtEntry/priceAtEntry/referencePrice) пишется
 *    только на ИСПОЛНЕННЫЕ входы. Если в этом прогоне EM был ВКЛЮЧЁН, выборка
 *    "цензурирована": все исполненные и так прошли EM, поэтому wins_blocked=0 —
 *    такой датасет для оценки EM бесполезен. Гони по периоду, где EM был OFF.
 *  - requiredDeltaAtEntry сохранён с тем k, что стоял в рантайме на момент
 *    записи. Скрипт делит его на BASE_K (по умолчанию 1.25), приводя к "юниту"
 *    ATR×√(t_rem/interval), и умножает на тестируемый k. Если исторический k
 *    отличался от BASE_K — задай его через EM_SWEEP_BASE_K.
 *  - Нужны ЛУЗЫ. Без них колонка loss_blocked = 0 и польза EM неизмерима —
 *    только цена. Копи реальные лузы на EM-off периоде, потом возвращайся сюда.
 *
 * Подключение — как у остальных скриптов: POSTGRES_* из .env (dotenv/config).
 * Чтобы прогнать по другой БД (смоук vs лайв) — подставь нужный env-файл, напр.:
 *   POSTGRES_HOST=... POSTGRES_USER=... POSTGRES_PASSWORD=... npm run em:sweep
 * либо `set -a; . env_list/env.btc-final; set +a; npm run em:sweep`.
 *
 * Настройки через env:
 *   EM_SWEEP_KS       — список k через запятую (по умолчанию 0.3,0.4,0.5,0.6,0.7,0.8,1.0,1.25)
 *   EM_SWEEP_BASE_K   — k, с которым записан requiredDeltaAtEntry (по умолчанию 1.25)
 */
import 'dotenv/config';
import { Client } from 'pg';

async function main(): Promise<void> {
  if (!process.env.POSTGRES_HOST) {
    console.error('POSTGRES_HOST не задан — нужен .env с POSTGRES_* (как у остальных скриптов).');
    process.exit(1);
  }

  const ks = (process.env.EM_SWEEP_KS ?? '0.3,0.4,0.5,0.6,0.7,0.8,1.0,1.25')
    .split(',')
    .map((s) => parseFloat(s.trim()))
    .filter((n) => Number.isFinite(n) && n > 0);
  const baseK = parseFloat(process.env.EM_SWEEP_BASE_K ?? '1.25');

  const client = new Client({
    host: process.env.POSTGRES_HOST,
    port: parseInt(process.env.POSTGRES_PORT ?? '5432', 10),
    database: process.env.POSTGRES_DB,
    user: process.env.POSTGRES_USER,
    password: process.env.POSTGRES_PASSWORD,
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();

  try {
    const comp = await client.query(`
      SELECT
        count(*)                                         AS total,
        count(*) FILTER (WHERE executed)                 AS executed,
        count(*) FILTER (WHERE status='win')             AS wins,
        count(*) FILTER (WHERE status='loss')            AS losses,
        count(*) FILTER (WHERE status IN ('win','loss')
          AND "requiredDeltaAtEntry" IS NOT NULL
          AND "priceAtEntry" IS NOT NULL
          AND "referencePrice" IS NOT NULL)              AS wl_with_diag,
        min("createdAt")                                 AS oldest,
        max("createdAt")                                 AS newest
      FROM market_logs
    `);
    console.log(`=== market_logs состав (БД: ${process.env.POSTGRES_HOST}) ===`);
    console.table(comp.rows);

    const sweep = await client.query(
      `
      WITH e AS (
        SELECT status,
               abs("priceAtEntry" - "referencePrice") AS d,
               "requiredDeltaAtEntry" / $1::numeric   AS unit
        FROM market_logs
        WHERE executed AND status IN ('win','loss')
          AND "requiredDeltaAtEntry" IS NOT NULL
          AND "priceAtEntry" IS NOT NULL
          AND "referencePrice" IS NOT NULL
      )
      SELECT k::text AS k,
        count(*) FILTER (WHERE status='win'  AND d < unit*k) AS wins_blocked,
        count(*) FILTER (WHERE status='win')                 AS wins_total,
        round(100.0 * count(*) FILTER (WHERE status='win' AND d < unit*k)
              / nullif(count(*) FILTER (WHERE status='win'), 0), 1) AS win_block_pct,
        count(*) FILTER (WHERE status='loss' AND d < unit*k) AS loss_blocked,
        count(*) FILTER (WHERE status='loss')                AS loss_total
      FROM e, unnest($2::numeric[]) AS k
      GROUP BY k ORDER BY k
      `,
      [baseK, ks],
    );
    console.log(`\n=== EM свип k (base_k=${baseK}): сколько W/L заблокировал бы каждый k ===`);
    console.table(sweep.rows);

    const losses = Number(comp.rows[0]?.losses ?? 0);
    if (losses === 0) {
      console.log(
        '\n⚠️  В выборке 0 лузов — измерена только ЦЕНА EM (потерянные W), польза неизмерима. ' +
          'Нужны реальные лузы (EM-off период), чтобы loss_blocked стал > 0.',
      );
    }
  } finally {
    await client.end();
  }
}

main().catch((err) => {
  console.error('ERR', err instanceof Error ? err.message : err);
  process.exit(1);
});
