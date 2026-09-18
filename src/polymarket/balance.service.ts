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
  private readonly ttlMs = 20_000;

  constructor(private readonly secureClientService: PolymarketSecureClientService) {}

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
