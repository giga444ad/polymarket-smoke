import { Outcome } from '../polymarket/market-ws-stream';
import { FeedSnapshot, PriceAtResult } from '../polymarket/price-feed.service';
import { EdgeWeights, computeEdgeFeatures, computeEdgeProb } from './edge-score.util';

/**
 * Сессия 16 (модуль бэктеста, см. BACKTEST-PLAN.md, раздел 2.1/2.2).
 *
 * Раньше вся эта логика (captureDiagnostics/evaluateEntryGate) жила прямо
 * внутри TradingService и была наглухо завязана на `this.priceFeed`
 * (реальный WS-фид) и `Date.now()`. Пользователь явно потребовал, чтобы
 * бэктест был "честным повтором" существующей логики, а не отдельной
 * моделью, реализующей ту же идею заново — иначе бэктест и live легко
 * разойдутся по причинам, не имеющим отношения к самим фильтрам входа.
 *
 * Решение: вынести саму логику гейта в этот класс, параметризованный двумя
 * маленькими интерфейсами:
 *  - IPriceSource — "откуда брать цену/ATR/зону" (live: PriceFeedService как
 *    есть, без единой правки — он уже структурно соответствует этому
 *    интерфейсу; бэктест: ReplayPriceSource поверх сохранённых price_ticks).
 *  - IClock — "что считать текущим временем" (live: системные часы; бэктест:
 *    виртуальные часы, идущие по историческим таймстемпам).
 *
 * TradingService и BacktestRunnerService используют РОВНО ОДИН И ТОТ ЖЕ
 * класс — при следующей правке фильтра входа его достаточно поменять
 * здесь один раз, и она автоматически применится и к live, и к бэктесту.
 */

export interface IClock {
  now(): number;
}

export const SYSTEM_CLOCK: IClock = { now: () => Date.now() };

/**
 * Структурно совпадает с публичным API PriceFeedService (getSnapshot/
 * getPriceAt/getTimeInZoneRatio) — PriceFeedService можно передавать сюда
 * как есть, без единой правки в нём самом (TypeScript duck typing).
 */
export interface IPriceSource {
  getSnapshot(streamKey: string): FeedSnapshot;
  getPriceAt(streamKey: string, targetMs: number): PriceAtResult;
  getTimeInZoneRatio(
    streamKey: string,
    windowStartMs: number,
    nowMs: number,
    referencePrice: number,
    side: Outcome,
  ): number | null;
  // Сессия 18 — см. edge-score.util.ts. Оба метода реализованы 1-в-1 и в
  // PriceFeedService (live), и в ReplayPriceSource (бэктест) — без этого
  // edge-гейт нельзя было бы честно бэктестить, только гонять вслепую в live.
  getAtrRobust(streamKey: string): number | null;
  getSmoothnessRatio(streamKey: string, sinceMs: number, nowMs: number): number | null;
}

export interface EntryDiagnostics {
  referencePrice: number | null;
  priceAtEntry: number | null;
  atrAtEntry: number | null;
  atrRatioAtEntry: number | null;
  // Источник цены ('chainlink' | 'binance' | 'bybit' | null, либо метка
  // реплей-источника в бэктесте, см. ReplayPriceSource) — Chainlink это
  // буквально то, чем Polymarket резолвит крипто-маркеты; binance/bybit —
  // лишь приближение (см. PriceFeedService и README).
  priceSource: string | null;

  // --- Сессия 13: доп. фильтры входа (обсуждение с Gemini, см. CONTEXT.md).
  // Все четыре поля считаются ВСЕГДА (независимо от того, включён ли
  // соответствующий *_FILTER_ENABLED) — та же SHADOW-логика, что и у
  // исходного ATR-гейта на старте проекта: сначала копим статистику по
  // market_logs, потом калибруем пороги и включаем блокировку осознанно. ---

  // Текущий час UTC на момент входа — для BLACKOUT_HOURS_UTC.
  blackoutHourAtEntry: number | null;
  // Требуемый запас цены от референса: atrAtEntry * sqrt(t_rem/intervalSec) * SAFETY_K_FACTOR
  // (Expected Move, см. EXPECTED_MOVE_FILTER_ENABLED) — масштабирование в
  // ТЕХ ЖЕ единицах времени, что и сам ATR (целое окно потока), а не
  // смешение с фиксированной "1-минутной" волатильностью.
  requiredDeltaAtEntry: number | null;
  // Скорость изменения СИГНАТУРНОЙ (не абсолютной) дельты цена-референс за
  // последние DRIFT_LOOKBACK_SEC секунд: (priceNow-ref) - (pricePast-ref).
  // Положительно = дельта растёт в сторону YES, отрицательно = в сторону NO.
  driftRateAtEntry: number | null;
  // Доля прошедшего времени текущего окна, когда цена была на стороне
  // ВЫБРАННОГО исхода относительно референса (см. MIN_ZONE_RATIO).
  zoneRatioAtEntry: number | null;

  // --- Сессия 18: edge-модель (см. edge-score.util.ts) — считается ВСЕГДА
  // (та же shadow-философия, что у полей Сессии 13 выше), независимо от
  // EDGE_GATE_ENABLED. ---
  atrRobustAtEntry: number | null;
  smoothnessAtEntry: number | null;
  pModelAtEntry: number | null;
  impliedProbAtEntry: number | null;
  edgeWouldEnter: boolean | null;
}

/** Всё, что гейту нужно знать про конкретный момент принятия решения — не
 *  привязано к MarketState (NestJS-специфичный тип), чтобы бэктест мог
 *  собрать этот же контекст из своих реплей-структур. */
export interface GateContext {
  streamKey: string;
  outcome: Outcome;
  referencePrice: number | null;
  intervalSec: number;
  windowStartMs: number | null;
  timeLeftSec: number;
  // Сессия 18: цена, которую мы реально готовы заплатить прямо сейчас (best
  // ask при маркет-тейке, желаемая цена лимитки при тир-фолбэке) — это и
  // есть implied-вероятность рынка, с которой сравнивается p_model. null,
  // если гейт вызван вне контекста конкретной попытки входа (сейчас так не
  // бывает, но интерфейс явно допускает "не знаем цену" как честный случай).
  checkPrice: number | null;
}

export interface EntryGateConfig {
  entryFilterEnabled: boolean;
  minDistanceAtrRatio: number;
  blackoutHoursFilterEnabled: boolean;
  blackoutHoursUtc: Set<number>;
  expectedMoveFilterEnabled: boolean;
  safetyKFactor: number;
  directionalDriftFilterEnabled: boolean;
  driftLookbackSec: number;
  timeInZoneFilterEnabled: boolean;
  minZoneRatio: number;
  // Сессия 18 (см. edge-score.util.ts).
  edgeGateEnabled: boolean;
  edgeWeights: EdgeWeights;
  edgeMargin: number;
  edgeSmoothnessLookbackSec: number;
  // Сессия 20: режим edge-гейта.
  //  - edgeMinPModelActive=false (дефолт) — VALUE-гейт: пускаем, если
  //    p_model > implied_price + edgeMargin (перевес над рынком). На ¢90-99
  //    структурно режет почти всё (p_model не может побить ¢99) — см. разбор.
  //  - edgeMinPModelActive=true — CONFIDENCE FLOOR: пускаем, если
  //    p_model > edgeMinPModel (абсолютный порог уверенности), без сравнения
  //    с ценой. Отсекает только реально неуверенные входы, ¢99 при высоком
  //    p_model пропускает. Подходит для скальпинга почти-верных исходов.
  edgeMinPModelActive: boolean;
  edgeMinPModel: number;
}

export interface GateResult {
  allow: boolean;
  diagnostics: EntryDiagnostics;
  reason: string | null;
}

export class EntryGateEngine {
  constructor(
    private readonly cfg: EntryGateConfig,
    private readonly priceSource: IPriceSource,
    private readonly clock: IClock = SYSTEM_CLOCK,
  ) {}

  captureDiagnostics(ctx: GateContext): EntryDiagnostics {
    const snap = this.priceSource.getSnapshot(ctx.streamKey);
    const referencePrice = ctx.referencePrice;
    const priceAtEntry = snap.price;
    const atrAtEntry = snap.atr;
    let atrRatioAtEntry: number | null = null;
    if (referencePrice != null && priceAtEntry != null && atrAtEntry != null && atrAtEntry > 0) {
      atrRatioAtEntry = Math.abs(priceAtEntry - referencePrice) / atrAtEntry;
    }

    const blackoutHourAtEntry = new Date(this.clock.now()).getUTCHours();

    let requiredDeltaAtEntry: number | null = null;
    if (atrAtEntry != null && atrAtEntry > 0 && ctx.intervalSec > 0) {
      requiredDeltaAtEntry =
        atrAtEntry * Math.sqrt(Math.max(0, ctx.timeLeftSec) / ctx.intervalSec) * this.cfg.safetyKFactor;
    }

    let driftRateAtEntry: number | null = null;
    if (referencePrice != null && priceAtEntry != null) {
      const past = this.priceSource.getPriceAt(ctx.streamKey, this.clock.now() - this.cfg.driftLookbackSec * 1000);
      if (past.price != null) {
        driftRateAtEntry = (priceAtEntry - referencePrice) - (past.price - referencePrice);
      }
    }

    let zoneRatioAtEntry: number | null = null;
    if (referencePrice != null && ctx.windowStartMs != null) {
      zoneRatioAtEntry = this.priceSource.getTimeInZoneRatio(
        ctx.streamKey,
        ctx.windowStartMs,
        this.clock.now(),
        referencePrice,
        ctx.outcome,
      );
    }

    // --- Сессия 18: edge-модель, всегда считается (shadow), см. class-comment EntryDiagnostics. ---
    const atrRobustAtEntry = this.priceSource.getAtrRobust(ctx.streamKey);
    const smoothnessAtEntry = this.priceSource.getSmoothnessRatio(
      ctx.streamKey,
      this.clock.now() - this.cfg.edgeSmoothnessLookbackSec * 1000,
      this.clock.now(),
    );
    const features = computeEdgeFeatures({
      delta: referencePrice != null && priceAtEntry != null ? priceAtEntry - referencePrice : null,
      atrRobust: atrRobustAtEntry,
      driftRate: driftRateAtEntry,
      zoneRatio: zoneRatioAtEntry,
      smoothness: smoothnessAtEntry,
      outcome: ctx.outcome,
    });
    const pModelAtEntry = computeEdgeProb(features, this.cfg.edgeWeights);
    const impliedProbAtEntry = ctx.checkPrice;
    const edgeWouldEnter =
      pModelAtEntry != null && impliedProbAtEntry != null ? pModelAtEntry > impliedProbAtEntry + this.cfg.edgeMargin : null;

    return {
      referencePrice,
      priceAtEntry,
      atrAtEntry,
      atrRatioAtEntry,
      priceSource: snap.source,
      blackoutHourAtEntry,
      requiredDeltaAtEntry,
      driftRateAtEntry,
      zoneRatioAtEntry,
      atrRobustAtEntry,
      smoothnessAtEntry,
      pModelAtEntry,
      impliedProbAtEntry,
      edgeWouldEnter,
    };
  }

  evaluateEntryGate(ctx: GateContext): GateResult {
    const diagnostics = this.captureDiagnostics(ctx);
    const outcome = ctx.outcome;

    if (this.cfg.entryFilterEnabled) {
      if (diagnostics.atrRatioAtEntry == null) {
        return {
          allow: false,
          diagnostics,
          reason:
            'ATR-гейт включён, но диагностика недоступна (нет цены/ATR по внешнему фиду на момент входа) — ' +
            'пропускаем шаг: упустить сделку лучше, чем рисковать капиталом вслепую.',
        };
      }
      if (diagnostics.atrRatioAtEntry < this.cfg.minDistanceAtrRatio) {
        return {
          allow: false,
          diagnostics,
          reason:
            `ATR-гейт: дистанция до референса ${diagnostics.atrRatioAtEntry.toFixed(2)}x ATR ` +
            `меньше требуемых ${this.cfg.minDistanceAtrRatio}x — похоже на болтанку у границы, а не уверенное движение.`,
        };
      }
    }

    if (
      this.cfg.blackoutHoursFilterEnabled &&
      diagnostics.blackoutHourAtEntry != null &&
      this.cfg.blackoutHoursUtc.has(diagnostics.blackoutHourAtEntry)
    ) {
      return {
        allow: false,
        diagnostics,
        reason:
          `Blackout-фильтр: текущий час ${diagnostics.blackoutHourAtEntry} UTC входит в BLACKOUT_HOURS_UTC ` +
          `(историческая повышенная доля поздних разворотов в этот час) — пропуск шага.`,
      };
    }

    if (this.cfg.expectedMoveFilterEnabled) {
      if (diagnostics.requiredDeltaAtEntry == null || diagnostics.priceAtEntry == null || diagnostics.referencePrice == null) {
        return {
          allow: false,
          diagnostics,
          reason: 'Expected-move фильтр включён, но диагностика недоступна (нет ATR/цены по фиду) — пропуск шага.',
        };
      }
      const delta = Math.abs(diagnostics.priceAtEntry - diagnostics.referencePrice);
      if (delta < diagnostics.requiredDeltaAtEntry) {
        return {
          allow: false,
          diagnostics,
          reason:
            `Expected-move фильтр: дельта ${delta.toFixed(4)} меньше требуемого запаса ${diagnostics.requiredDeltaAtEntry.toFixed(4)} ` +
            `(ATR×√(t_rem/intervalSec)×${this.cfg.safetyKFactor}) — похоже на шум, а не на уверенное движение.`,
        };
      }
    }

    if (this.cfg.directionalDriftFilterEnabled) {
      if (diagnostics.driftRateAtEntry == null) {
        return {
          allow: false,
          diagnostics,
          reason: `Directional-drift фильтр включён, но история дельты недоступна (буфер не достаёт на ${this.cfg.driftLookbackSec}с назад) — пропуск шага.`,
        };
      }
      const adverse = outcome === 'YES' ? diagnostics.driftRateAtEntry < 0 : diagnostics.driftRateAtEntry > 0;
      if (adverse) {
        return {
          allow: false,
          diagnostics,
          reason:
            `Directional-drift фильтр: дельта схлопывается к референсу против стороны ${outcome} ` +
            `(скорость ${diagnostics.driftRateAtEntry.toFixed(4)} за ${this.cfg.driftLookbackSec}с) — похоже на встречный импульс, вход отменён.`,
        };
      }
    }

    if (this.cfg.timeInZoneFilterEnabled) {
      if (diagnostics.zoneRatioAtEntry == null) {
        return {
          allow: false,
          diagnostics,
          reason: 'Time-in-Zone фильтр включён, но буфер тиков не достаёт до начала окна — пропуск шага.',
        };
      }
      if (diagnostics.zoneRatioAtEntry < this.cfg.minZoneRatio) {
        return {
          allow: false,
          diagnostics,
          reason:
            `Time-in-Zone фильтр: сторона ${outcome} держалась только ${(diagnostics.zoneRatioAtEntry * 100).toFixed(1)}% ` +
            `прошедшего времени окна (нужно ${(this.cfg.minZoneRatio * 100).toFixed(0)}%) — похоже на прострел против доминирующей стороны.`,
        };
      }
    }

    // Сессия 18: edge-гейт — ПОСЛЕДНИМ шагом, поверх уже прошедших фильтров.
    // ЧЕСТНОЕ предупреждение (см. class-comment edge-score.util.ts): веса
    // ниже НЕ откалиброваны на истории — включение EDGE_GATE_ENABLED=true
    // означает торговлю на текущей рабочей гипотезе о весах, а не на
    // проверенной модели. diagnostics.pModelAtEntry/edgeWouldEnter уже
    // посчитаны выше ВСЕГДА (shadow) — включение этого блока лишь решает,
    // используются ли они для реальной блокировки входа.
    if (this.cfg.edgeGateEnabled) {
      if (diagnostics.pModelAtEntry == null) {
        return {
          allow: false,
          diagnostics,
          reason:
            'Edge-гейт включён, но p_model недоступен (не хватает фич — ATR-робаст/дрейф/zone/smoothness) — пропуск шага.',
        };
      }
      if (this.cfg.edgeMinPModelActive) {
        // CONFIDENCE FLOOR: p_model выше абсолютного порога уверенности, без сравнения с ценой.
        if (diagnostics.pModelAtEntry <= this.cfg.edgeMinPModel) {
          return {
            allow: false,
            diagnostics,
            reason:
              `Edge-floor: p_model ${diagnostics.pModelAtEntry.toFixed(3)} ниже порога уверенности ` +
              `${this.cfg.edgeMinPModel} — модель недостаточно уверена в исходе, пропуск шага.`,
          };
        }
      } else {
        // VALUE-гейт (как было): p_model должен побить цену + маржу.
        if (diagnostics.impliedProbAtEntry == null) {
          return {
            allow: false,
            diagnostics,
            reason: 'Edge-гейт (value-режим) включён, но цена сделки недоступна — пропуск шага.',
          };
        }
        if (!diagnostics.edgeWouldEnter) {
          return {
            allow: false,
            diagnostics,
            reason:
              `Edge-гейт: p_model ${diagnostics.pModelAtEntry.toFixed(3)} не превышает implied-цену ` +
              `${diagnostics.impliedProbAtEntry.toFixed(3)} + маржу ${this.cfg.edgeMargin} — модель не видит достаточного преимущества над рынком.`,
          };
        }
      }
    }

    return { allow: true, diagnostics, reason: null };
  }
}
