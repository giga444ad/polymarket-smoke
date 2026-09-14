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
  // Сессия 14 (разбор реальной статистики: UNFILLED на 15m — 61.5% против
  // 45% на 5m) — переопределяют глобальные LAST_ENTRY_WINDOW_SEC/
  // LIMIT_TIER2_SECONDS/LIMIT_TIER3_SECONDS конкретно для этого потока.
  // Гипотеза: одинаковое ОКНО В СЕКУНДАХ на конце разных по длине рынков —
  // это разная ДОЛЯ рынка (60с из 300с = 20%, но 60с из 900с = 6.7%), а
  // значит и разная динамика конвергенции книги/глубины к моменту входа.
  // Если не заданы — используется глобальный ENV-дефолт (обратная
  // совместимость, поведение не меняется, пока явно не переопределат).
  lastEntryWindowSec?: number;
  tier2Seconds?: number;
  tier3Seconds?: number;
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

    // Сессия 14: опциональные per-stream переопределения окна входа/тиров
    // (см. комментарий у полей в StreamDefinition выше).
    let lastEntryWindowSec: number | undefined;
    if (entry?.lastEntryWindowSec != null) {
      lastEntryWindowSec = parseInt(entry.lastEntryWindowSec, 10);
      if (!Number.isFinite(lastEntryWindowSec) || lastEntryWindowSec <= 0) {
        throw new Error(`STREAMS_CONFIG[${streamKey}]: lastEntryWindowSec, если задан, должен быть > 0.`);
      }
      if (lastEntryWindowSec >= intervalSec) {
        throw new Error(
          `STREAMS_CONFIG[${streamKey}]: lastEntryWindowSec (${lastEntryWindowSec}с) должен быть меньше intervalSec (${intervalSec}с) — иначе окно входа охватывает весь рынок или больше.`,
        );
      }
    }
    let tier2Seconds: number | undefined;
    if (entry?.tier2Seconds != null) {
      tier2Seconds = parseInt(entry.tier2Seconds, 10);
      if (!Number.isFinite(tier2Seconds) || tier2Seconds <= 0) {
        throw new Error(`STREAMS_CONFIG[${streamKey}]: tier2Seconds, если задан, должен быть > 0.`);
      }
    }
    let tier3Seconds: number | undefined;
    if (entry?.tier3Seconds != null) {
      tier3Seconds = parseInt(entry.tier3Seconds, 10);
      if (!Number.isFinite(tier3Seconds) || tier3Seconds <= 0) {
        throw new Error(`STREAMS_CONFIG[${streamKey}]: tier3Seconds, если задан, должен быть > 0.`);
      }
    }
    // Инвариант T3 < T2 <= lastEntryWindowSec проверяем, только если ОБЕ
    // стороны сравнения заданы явно на уровне потока — частичное
    // переопределение (например только lastEntryWindowSec) разрешено,
    // смешанная валидация со глобальными ENV-дефолтами делается отдельно в
    // TradingService при старте (там доступны оба источника разом).
    if (tier2Seconds != null && tier3Seconds != null && tier2Seconds <= tier3Seconds) {
      throw new Error(`STREAMS_CONFIG[${streamKey}]: tier2Seconds (${tier2Seconds}) должен быть больше tier3Seconds (${tier3Seconds}).`);
    }
    if (lastEntryWindowSec != null && tier2Seconds != null && tier2Seconds > lastEntryWindowSec) {
      throw new Error(
        `STREAMS_CONFIG[${streamKey}]: tier2Seconds (${tier2Seconds}) не должен превышать lastEntryWindowSec (${lastEntryWindowSec}) — иначе T2 недостижим.`,
      );
    }

    if (kind === 'hourly-et') {
      const etSlugBase = String(entry?.etSlugBase ?? streamKey).trim();
      return { streamKey, kind, intervalSec, etSlugBase, baseStake, atrCandles, lastEntryWindowSec, tier2Seconds, tier3Seconds };
    }

    const slugPrefix = String(entry?.slugPrefix ?? streamKey).trim();
    return { streamKey, kind, intervalSec, slugPrefix, baseStake, atrCandles, lastEntryWindowSec, tier2Seconds, tier3Seconds };
  });
}
