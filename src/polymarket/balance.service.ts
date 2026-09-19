import { Injectable, Logger } from '@nestjs/common';
import { AssetType } from '@polymarket/bindings/clob';
import { fetchBalanceAllowance } from '@polymarket/client/actions';
import { PolymarketSecureClientService } from './secure-client.service';

/**
 * Реальный баланс свободного USDC на аккаунте (не расчётный бэнкролл из
 * analytics.controller.ts). Используется как гейт перед открытием нового
 * окна в лайве. Через @polymarket/client (см. secure-client.service.ts).
 *
 * BalanceAllowanceResponse.balance — строка в базовых единицах USDC.e
 * (6 знаков после запятой).
 */
@Injectable()
export class BalanceService {
  private readonly logger = new Logger(BalanceService.name);
  private cachedUsd: number | null = null;
  private cachedAt = 0;
  private cachedEquityUsd: number | null = null;
  private cachedEquityAt = 0;
  private readonly ttlMs = 20_000;

  constructor(private readonly secureClientService: PolymarketSecureClientService) {}

  /**
   * Истинный bankroll в лайве = свободный USDC (cash) + стоимость открытых
   * позиций по рынку. getUsdBalance() выше отдаёт только cash и ЗАНИЖАЕТ
   * капитал, пока деньги заперты в позициях (в т.ч. в ещё не заклеймленных
   * выигрышах — запрос open возвращает и redeemable-строки, их currentValue
   * ≈ размер×$1, так что незаклеймленные победы тоже честно учитываются).
   *
   * Считаем cash + Σ Position.currentValue (mark-to-market USD через
   * client.listPositions), а НЕ client.fetchPortfolioValue() — последний по
   * факту вернул value=0 при непустом cash, т.е. это не «кэш+позиции».
   *
   * Кэш 20с, чтобы не долбить API на каждый poll фронта. Только лайв (в
   * смоуке secure-клиент не поднимется — вернёт последнее известное/null).
   */
  async getPortfolioValueUsd(forceRefresh = false): Promise<number | null> {
    const now = Date.now();
    if (!forceRefresh && this.cachedEquityUsd != null && now - this.cachedEquityAt < this.ttlMs) {
      return this.cachedEquityUsd;
    }
    try {
      const cash = await this.getUsdBalance(forceRefresh);
      if (cash == null) return this.cachedEquityUsd; // без cash equity не посчитать честно
      const client = await this.secureClientService.getClient();
      let positionsValue = 0;
      let pages = 0;
      for await (const page of client.listPositions()) {
        for (const pos of (page.items ?? []) as Array<{ currentValue?: string | number }>) {
          const v = parseFloat(String(pos.currentValue ?? '0'));
          if (Number.isFinite(v)) positionsValue += v;
        }
        if (++pages >= 20) break; // защита от неожиданной длинной пагинации
      }
      const equity = cash + positionsValue;
      this.cachedEquityUsd = equity;
      this.cachedEquityAt = now;
      return equity;
    } catch (err) {
      this.logger.warn(
        `Не удалось получить equity портфеля: ${err instanceof Error ? err.message : String(err)} ` +
          '— используем последнее известное значение (если есть).',
      );
      return this.cachedEquityUsd;
    }
  }

  async getUsdBalance(forceRefresh = false): Promise<number | null> {
    const now = Date.now();
    if (!forceRefresh && this.cachedUsd != null && now - this.cachedAt < this.ttlMs) {
      return this.cachedUsd;
    }
    try {
      const client = await this.secureClientService.getClient();
      const resp = await fetchBalanceAllowance(client, { assetType: AssetType.COLLATERAL });
      const usd = parseFloat(resp.balance) / 1e6;
      this.cachedUsd = usd;
      this.cachedAt = now;
      return usd;
    } catch (err) {
      this.logger.warn(
        `Не удалось получить баланс: ${err instanceof Error ? err.message : String(err)} ` +
          '— используем последнее известное значение (если есть).',
      );
      return this.cachedUsd;
    }
  }
}
