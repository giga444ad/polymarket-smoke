import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { MarketLog } from '../entities/market-log.entity';
import { PolymarketSecureClientService } from './secure-client.service';

/**
 * Раз в REDEEM_POLL_MS проверяет все реальные (не смоук) выигрышные шаги,
 * ещё не заклейменные, и вызывает client.redeemPositions({ conditionId })
 * для каждого — это высокоуровневый вызов @polymarket/client, который сам
 * разбирается, что редимить (обычный CTF или neg-risk путь), в отличие от
 * более раннего варианта этого сервиса, где ABI кодировался вручную. Это
 * заодно СНИМАЕТ прежнее ограничение "neg-risk не поддержан" — SDK сам
 * знает нужный контракт.
 *
 * См. secure-client.service.ts — там разбор, почему используется именно
 * @polymarket/client (canary) с простым Relayer API Key, а не
 * @polymarket/builder-relayer-client с HMAC Builder-креды.
 */
@Injectable()
export class RedeemService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RedeemService.name);
  private stopped = false;
  private timer: NodeJS.Timeout | null = null;
  private readonly enabled: boolean;
  private readonly pollMs: number;

  constructor(
    private readonly config: ConfigService,
    private readonly secureClientService: PolymarketSecureClientService,
    @InjectRepository(MarketLog) private readonly marketLogRepo: Repository<MarketLog>,
  ) {
    this.enabled = this.config.get<string>('SMOKE_START', 'true') !== 'true';
    this.pollMs = parseInt(this.config.get<string>('REDEEM_POLL_MS', '120000'), 10);
  }

  onModuleInit() {
    if (!this.enabled) {
      this.logger.log('RedeemService выключен (SMOKE_START=true) — клеймить в смоуке нечего.');
      return;
    }
    void this.loop();
  }

  onModuleDestroy() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
  }

  private async loop() {
    while (!this.stopped) {
      try {
        await this.redeemPendingBatch();
      } catch (err) {
        this.logger.error(`Сбой цикла клейма: ${this.errMsg(err)}`);
      }
      await new Promise<void>((resolve) => {
        this.timer = setTimeout(resolve, this.pollMs);
      });
    }
  }

  private async redeemPendingBatch(): Promise<void> {
    const pending = await this.marketLogRepo.find({
      where: { redeemStatus: 'pending', isSmoke: false },
      order: { resolvedAt: 'ASC' },
      take: 50,
    });
    if (pending.length === 0) return;

    const withCondition = pending.filter((l) => !!l.conditionId);
    const withoutCondition = pending.filter((l) => !l.conditionId);
    for (const log of withoutCondition) {
      log.redeemStatus = 'failed';
      log.redeemError = 'conditionId отсутствует — авторедим невозможен, клеймите вручную через UI Polymarket.';
      this.logger.warn(`[REDEEM] ${log.slug}: ${log.redeemError}`);
    }
    if (withoutCondition.length > 0) {
      await this.marketLogRepo.save(withoutCondition);
    }
    if (withCondition.length === 0) return;

    let client;
    try {
      client = await this.secureClientService.getClient();
    } catch (err) {
      this.logger.error(`Не удалось инициализировать secure client: ${this.errMsg(err)}`);
      return;
    }

    // Дедуплицируем по conditionId — редим одного и того же условия дважды
    // безвреден, но не нужно дважды дёргать relayer.
    const seen = new Set<string>();
    for (const log of withCondition) {
      const conditionId = log.conditionId as string;
      if (seen.has(conditionId)) continue;
      seen.add(conditionId);

      const sameCondition = withCondition.filter((l) => l.conditionId === conditionId);
      try {
        this.logger.log(`[REDEEM] Клеймлю conditionId=${conditionId} (${log.slug})...`);
        const handle = await client.redeemPositions({ conditionId });
        const outcome = await handle.wait();
        for (const l of sameCondition) {
          l.redeemStatus = 'redeemed';
          l.redeemedAt = new Date();
          l.redeemTransactionId = outcome.transactionId ?? outcome.transactionHash;
        }
        await this.marketLogRepo.save(sameCondition);
        this.logger.log(`[REDEEM] Успешно: conditionId=${conditionId}, tx=${outcome.transactionHash}`);
      } catch (err) {
        const message = this.errMsg(err);
        for (const l of sameCondition) {
          l.redeemStatus = 'failed';
          l.redeemError = message;
        }
        await this.marketLogRepo.save(sameCondition);
        this.logger.error(`[REDEEM] Ошибка редима conditionId=${conditionId}: ${message}`);
      }
    }
  }

  private errMsg(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
