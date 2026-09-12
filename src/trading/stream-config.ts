export type StreamKind = 'interval' | 'hourly-et';

/**
 * Описание одного независимого потока = актив × таймфрейм. Каждый поток
 * ведёт свой собственный Attempt (свой streamKey, свой прогресс, свой
 * currentStake) — см. п.3 бэклога. Формат слага у разных таймфреймов Gamma
 * разный (проверено вручную на живых данных, см. README):
 *
 *  - 5m / 15m ("interval"):  `${slugPrefix}-${unix_start}`
 *      напр. btc-updown-5m-1788998100, btc-updown-15m-1788997500
 *  - 1h ("hourly-et"):       `${etSlugBase}-${month}-${day}-${year}-${hour}${am|pm}-et`
 *      напр. bitcoin-up-or-down-september-9-2026-7pm-et
 *    Часовые границы совпадают что в UTC, что в ET (оффсет ET — целое число
 *    часов), поэтому битовая арифметика "начало/конец интервала" общая для
 *    обоих видов слага — различается только СТРОКА слага.
 */
export interface StreamDefinition {
  streamKey: string;
  kind: StreamKind;
  intervalSec: number;
  // interval
  slugPrefix?: string;
  // hourly-et
  etSlugBase?: string;
  // Базовый стейк этого потока (см. Attempt.baseStake) — специально
  // конфигурируется на поток, а не глобально: на пике прогрессии каждый
  // поток запрашивает у рынка меньше глубины, чем один суперпоток с одним
  // большим общим стейком (см. п.7 бэклога — политика капитала по потокам).
  baseStake: number;
  // Сколько ПРОШЛЫХ ЗАВЕРШЁННЫХ ОКОН этого же таймфрейма усредняем в ATR
  // (см. PriceFeedService) — переопределяет глобальный FEED_ATR_CANDLES для
  // конкретно этого потока. Одна свеча ATR = ровно одно окно потока
  // (intervalSec), поэтому прогрев = atrCandles*intervalSec — для часового
  // потока 20 окон это 20 ЧАСОВ прогрева, что может быть избыточно строго;
  // здесь можно задать меньше, не трогая 5m/15m.
  atrCandles?: number;
}

// 4ч/1д сознательно не включены сейчас (см. BACKLOG п.5) — только 5m/15m/1h.
// atrCandles у часового потока сознательно уменьшен с глобального дефолта
// (20 -> 8): при candleMs=intervalSec (см. PriceFeedService) 20 окон для 1h
// это 20 ЧАСОВ прогрева ATR-гейта — избыточно долго для старта; 8 часов —
// разумный компромисс между статистической устойчивостью и временем прогрева.
const DEFAULT_STREAMS_CONFIG = JSON.stringify([
  { streamKey: 'btc-updown-5m', kind: 'interval', intervalSec: 300, slugPrefix: 'btc-updown-5m', baseStake: 5 },
  { streamKey: 'btc-updown-15m', kind: 'interval', intervalSec: 900, slugPrefix: 'btc-updown-15m', baseStake: 5 },
  { streamKey: 'bitcoin-up-or-down', kind: 'hourly-et', intervalSec: 3600, etSlugBase: 'bitcoin-up-or-down', baseStake: 5, atrCandles: 8 },
]);

export function parseStreamsConfig(raw: string | undefined): StreamDefinition[] {
  const json = raw && raw.trim().length > 0 ? raw : DEFAULT_STREAMS_CONFIG;

  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch (err) {
    throw new Error(
      `STREAMS_CONFIG: не удалось распарсить JSON (${err instanceof Error ? err.message : err}). Значение: ${json}`,
    );
  }
  if (!Array.isArray(parsed) || parsed.length === 0) {
    throw new Error('STREAMS_CONFIG: ожидался непустой JSON-массив описаний потоков.');
  }

  const seen = new Set<string>();
  return parsed.map((entry: any, idx: number) => {
    const streamKey = String(entry?.streamKey ?? '').trim();
    if (!streamKey) throw new Error(`STREAMS_CONFIG[${idx}]: отсутствует streamKey.`);
    if (seen.has(streamKey)) throw new Error(`STREAMS_CONFIG: дублирующийся streamKey "${streamKey}".`);
    seen.add(streamKey);

    const kind: StreamKind = entry?.kind === 'hourly-et' ? 'hourly-et' : 'interval';

    const intervalSec = parseInt(entry?.intervalSec, 10);
    if (!Number.isFinite(intervalSec) || intervalSec <= 0) {
      throw new Error(`STREAMS_CONFIG[${streamKey}]: intervalSec обязателен и должен быть > 0.`);
    }

    const baseStake = parseFloat(entry?.baseStake);
    if (!Number.isFinite(baseStake) || baseStake <= 0) {
      throw new Error(`STREAMS_CONFIG[${streamKey}]: baseStake обязателен и должен быть > 0.`);
    }

    let atrCandles: number | undefined;
    if (entry?.atrCandles != null) {
      atrCandles = parseInt(entry.atrCandles, 10);
      if (!Number.isFinite(atrCandles) || atrCandles <= 0) {
        throw new Error(`STREAMS_CONFIG[${streamKey}]: atrCandles, если задан, должен быть > 0.`);
      }
    }

    if (kind === 'hourly-et') {
      const etSlugBase = String(entry?.etSlugBase ?? streamKey).trim();
      return { streamKey, kind, intervalSec, etSlugBase, baseStake, atrCandles };
    }

    const slugPrefix = String(entry?.slugPrefix ?? streamKey).trim();
    return { streamKey, kind, intervalSec, slugPrefix, baseStake, atrCandles };
  });
}
