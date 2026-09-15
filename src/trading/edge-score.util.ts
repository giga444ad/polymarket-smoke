/**
 * Сессия 18 (см. CONTEXT.md — "давай сделаем edge полноценным, с весами
 * через ENV, и начнём применять").
 *
 * Чистые функции, без DI и без времени "снаружи" — ПО ТОЙ ЖЕ причине, что и
 * market-decision.util.ts (см. class-comment там): чтобы EntryGateEngine
 * (реальный гейт, живые деньги) и EdgeSamplerService (чистый shadow-лог)
 * считали p_model буквально одной и той же математикой, а не "по мотивам"
 * друг друга — иначе shadow-логи и реальные решения будут расходиться по
 * причинам, не имеющим отношения к самой модели.
 *
 * ВАЖНО, честно: веса ниже НЕ откалиброваны на исторических данных — это
 * ручные дефолты (по знаку — то, что мы обсуждали: неблагоприятный
 * дрейф/несглаженное движение снижают p_model, время в зоне и попутный
 * дрейф повышают), не более того. Пока EDGE_GATE_ENABLED=false — это НЕ
 * бага, у гейта нет доступа к реальным деньгам. Как только выставишь
 * EDGE_GATE_ENABLED=true — эти веса начинают решать, входить или нет,
 * ровно как и любой другой ручной порог (ATR-рацио 1.5x тоже когда-то был
 * ручной цифрой) — то есть режим "настраиваем на живых логах" ты и получаешь,
 * просто явно предупреждён, что это не откалиброванная модель, а текущая
 * рабочая гипотеза.
 */

export interface EdgeWeights {
  bias: number;
  /** Вес дистанции до референса, нормированной на робастный ATR (z). */
  z: number;
  /** Вес дрейфа, СОГЛАСОВАННОГО со стороной входа (положительный = дрейф в нашу пользу). */
  drift: number;
  /** Вес доли времени окна, проведённой на нашей стороне (0..1). */
  zone: number;
  /** Вес гладкости движения, СО ЗНАКОМ направления (см. computeEdgeFeatures). */
  smoothSigned: number;
}

export const DEFAULT_EDGE_WEIGHTS: EdgeWeights = {
  bias: 0,
  z: 1,
  drift: 1,
  zone: 1,
  smoothSigned: 1,
};

export interface EdgeFeatureInputs {
  /** price - referencePrice, знаковая (не модуль). */
  delta: number | null;
  /** Робастный (медианный) ATR — см. PriceFeedService.getAtrRobust. Предпочтителен обычному ATR для этой модели именно потому, что обычный ATR временно раздувается после спайка (см. разбор прод-кейса, CONTEXT.md Сессия 17). */
  atrRobust: number | null;
  /** Скорость сигнатурной дельты за driftLookbackSec, см. EntryGateEngine.captureDiagnostics. */
  driftRate: number | null;
  /** Time-in-Zone для ВЫБРАННОЙ стороны, 0..1. */
  zoneRatio: number | null;
  /** |netDisplacement|/Σ|тик-дельт| за smoothness-lookback, 0..1, БЕЗ знака направления. */
  smoothness: number | null;
  /** Сторона, для которой считаем score — нужна, чтобы согласовать знак drift/delta с "нашей" стороной. */
  outcome: 'YES' | 'NO';
}

export interface EdgeFeatures {
  z: number | null;
  driftAligned: number | null;
  zoneRatio: number | null;
  smoothSigned: number | null;
}

/**
 * Приводит сырую диагностику к фичам МОДЕЛИ, все — "положительное = в пользу
 * входа по стороне outcome", чтобы веса были знаково интерпретируемы
 * одинаково для YES и NO (иначе пришлось бы держать разные веса на сторону,
 * что бессмысленно — рынок не должен "предпочитать" YES или NO сам по себе).
 */
export function computeEdgeFeatures(inp: EdgeFeatureInputs): EdgeFeatures {
  const sideSign = inp.outcome === 'YES' ? 1 : -1;

  let z: number | null = null;
  if (inp.delta != null && inp.atrRobust != null && inp.atrRobust > 0) {
    // Дистанция в СВОЮ сторону — если цена ушла в сторону, противоположную
    // нашему outcome, z будет отрицательным (это не должно случаться при
    // корректном выборе фаворита, но модель должна честно это отразить, а
    // не считать модуль вслепую).
    z = (inp.delta * sideSign) / inp.atrRobust;
  }

  let driftAligned: number | null = null;
  if (inp.driftRate != null && inp.atrRobust != null && inp.atrRobust > 0) {
    driftAligned = (inp.driftRate * sideSign) / inp.atrRobust;
  }

  let smoothSigned: number | null = null;
  if (inp.smoothness != null && inp.driftRate != null) {
    const driftSign = inp.driftRate === 0 ? 0 : Math.sign(inp.driftRate * sideSign);
    smoothSigned = inp.smoothness * driftSign;
  }

  return { z, driftAligned, zoneRatio: inp.zoneRatio, smoothSigned };
}

function sigmoid(x: number): number {
  return 1 / (1 + Math.exp(-x));
}

/** null, если хотя бы одна фича недоступна — fail-closed философия проекта: лучше честно "не считаем", чем считать по частичным данным. */
export function computeEdgeProb(features: EdgeFeatures, weights: EdgeWeights): number | null {
  const { z, driftAligned, zoneRatio, smoothSigned } = features;
  if (z == null || driftAligned == null || zoneRatio == null || smoothSigned == null) return null;
  const x = weights.bias + weights.z * z + weights.drift * driftAligned + weights.zone * zoneRatio + weights.smoothSigned * smoothSigned;
  return sigmoid(x);
}
