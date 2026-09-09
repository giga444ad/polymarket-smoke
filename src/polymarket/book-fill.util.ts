import { BookLevel } from './market-ws-stream';

export interface FillResult {
  /** Сколько денег реально удалось бы потратить (<= запрошенного, если не хватило глубины/упёрлись в потолок цены). */
  filledUsd: number;
  /** Сколько токенов реально куплено. */
  filledShares: number;
  /** Средневзвешенная цена исполнения (null, если не купили вообще ничего). */
  vwapPrice: number | null;
  /** Доля исходно запрошенной суммы, которую удалось реально исполнить (0..1). */
  filledRatio: number;
  /** Упёрлись ли в потолок цены (maxPrice), не пройдя всю глубину подряд. */
  cappedByMaxPrice: boolean;
}

/**
 * Эмулирует "съедание" реального стакана маркет-ордером на requestedUsd
 * долларов, идя по уровням asks от самой дешёвой цены вверх и не заходя
 * выше maxPrice — ровно так, как исполнился бы настоящий FAK-ордер с
 * ценовым потолком. Например: asks = [{0.99, 40}, {0.995, 2}, {0.998, 15}],
 * requestedUsd = 42 → берём все 40 по 0.99 (=$39.6), остаток $2.4 добираем
 * по 0.995 (~2.41 шт) — средняя цена окажется чуть выше 0.99, а не ровно 0.99.
 */
export function walkAsksForFill(
  asks: BookLevel[],
  requestedUsd: number,
  maxPrice: number,
): FillResult {
  let remainingUsd = requestedUsd;
  let filledShares = 0;
  let filledUsd = 0;
  let cappedByMaxPrice = false;

  for (const level of asks) {
    if (remainingUsd <= 0) break;
    if (level.price > maxPrice) {
      cappedByMaxPrice = true;
      break;
    }
    const usdAvailableAtLevel = level.price * level.size;
    const usdToTake = Math.min(remainingUsd, usdAvailableAtLevel);
    const sharesToTake = usdToTake / level.price;

    filledShares += sharesToTake;
    filledUsd += usdToTake;
    remainingUsd -= usdToTake;
  }

  if (remainingUsd > 0.000001 && !cappedByMaxPrice) {
    // Стакан кончился раньше потолка цены — просто не хватило глубины.
    cappedByMaxPrice = false;
  }

  return {
    filledUsd,
    filledShares,
    vwapPrice: filledShares > 0 ? filledUsd / filledShares : null,
    filledRatio: requestedUsd > 0 ? filledUsd / requestedUsd : 0,
    cappedByMaxPrice,
  };
}

/**
 * Сколько денег (в сумме, по цене каждого уровня) лежит в asks по цене
 * <= maxPrice — используется, чтобы понять, действительно ли по нашей
 * резюм-лимитке накопилось достаточно продавцов, а не просто "касание".
 */
export function cumulativeUsdAtOrBelow(asks: BookLevel[], maxPrice: number): number {
  let sum = 0;
  for (const level of asks) {
    if (level.price > maxPrice) break;
    sum += level.price * level.size;
  }
  return sum;
}
