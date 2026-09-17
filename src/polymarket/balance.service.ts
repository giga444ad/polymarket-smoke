import { Injectable, Logger } from '@nestjs/common';
import { AssetType } from '@polymarket/clob-client';
import { PolymarketTraderService } from './polymarket-trader.service';

/**
 * Реальный баланс свободного USDC на CLOB-аккаунте (не расчётный бэнкролл
 * из analytics.controller.ts — см. обсуждение "баланс считается, не
 * считывается"). Используется как гейт перед открытием нового окна в
 * лайве: если свободных денег не хватает на стейк следующего шага —
 * пропускаем окно явно, а не пытаемся исполнить урезанный ордер молча.
 *
 * getBalanceAllowance возвращает баланс в базовых единицах USDC.e (6
 * знаков), строкой — см. types.d.ts пакета @polymarket/clob-client. Кэш
 * короткоживущий (по умолчанию 20с), чтобы не дёргать биржу на каждый тик
 * discoveryTick, но при этом не отставать намного от реальности.
 */
@Injectable()
export class BalanceService {
  private readonly logger = new Logger(BalanceService.name);
  private cachedUsd: number | null = null;
  private cachedAt = 0;
  private readonly ttlMs: number;

  constructor(private readonly trader: PolymarketTraderService) {
    this.ttlMs = 20_000;
  }

  /** Реальный доступный баланс USDC на CLOB-аккаунте, либо null, если ещё не удалось получить. */
  async getUsdBalance(forceRefresh = false): Promise<number | null> {
    const now = Date.now();
    if (!forceRefresh && this.cachedUsd != null && now - this.cachedAt < this.ttlMs) {
      return this.cachedUsd;
    }
    try {
      const client = await this.trader.ensureClient();
      const resp = await client.getBalanceAllowance({ asset_type: AssetType.COLLATERAL });
      const usd = parseFloat(resp.balance) / 1e6;
      this.cachedUsd = usd;
      this.cachedAt = now;
      return usd;
    } catch (err) {
      this.logger.warn(
        `Не удалось получить баланс CLOB: ${err instanceof Error ? err.message : String(err)} ` +
          '— используем последнее известное значение (если есть).',
      );
      // Отдаём последний известный баланс, даже если он "протух" по TTL —
      // лучше немного устаревшая цифра, чем полное отсутствие гейта.
      return this.cachedUsd;
    }
  }
}
