import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Wallet, utils as ethersUtils } from 'ethers';
import { RelayClient, RelayerTxType, Transaction as RelayTransaction } from '@polymarket/builder-relayer-client';
import { BuilderConfig } from '@polymarket/builder-signing-sdk';
import { MarketLog } from '../entities/market-log.entity';

/**
 * ВАЖНО (прочитать перед боевым запуском):
 *
 * Клеймит выигрышные позиции ЧЕРЕЗ РЕЛЕЙЕР Polymarket (gasless) — POL/газ
 * не нужен, платит Polymarket. Требует Relayer API Key, который делается на
 * https://polymarket.com в Settings → API Keys (см. RELAYER_API_KEY/
 * RELAYER_API_SECRET/RELAYER_API_PASSPHRASE в .env). Проверено по
 * официальной документации docs.polymarket.com/trading/gasless на момент
 * написания (сентябрь 2026) — конструктор RelayClient взят из фактически
 * установленной версии пакета @polymarket/builder-relayer-client (0.0.10):
 * `new RelayClient(relayerUrl, chainId, signer, builderConfig, relayTxType)`.
 * В части онлайн-документации Polymarket на тот же момент фигурировал ДРУГОЙ,
 * более новый синтаксис конструктора (объектом, с relayerApiKey/
 * relayerApiKeyAddress без HMAC) — похоже на ещё не выпущенную в npm версию
 * клиента. Перед боевым запуском стоит проверить `npm view
 * @polymarket/builder-relayer-client version` и при необходимости обновить
 * этот файл под актуальный конструктор.
 *
 * RELAYER_TX_TYPE=SAFE подходит для большинства обычных интеграций (кошелёк
 * задеплоен как Safe) — см. доку "Wallet Types". Если ваш FUNDER_ADDRESS —
 * не Safe, а Proxy-кошелёк (например, вход через Magic/почту), поставьте
 * RELAYER_TX_TYPE=PROXY.
 *
 * NegRisk-маркеты (redeem через другой контракт, NegRiskAdapter, с ДРУГОЙ
 * сигнатурой redeemPositions(conditionId, amounts) — не indexSets) НЕ
 * реализованы: семантика amounts там нетривиальна (нужен точный ончейн-баланс
 * позиции в wei), а крипто Up/Down 5-минутки почти наверняка не neg-risk.
 * Если попадётся log с negRisk=true — сервис его пропускает и явно логирует,
 * а не пытается угадать редим.
 */

// CTF (Conditional Tokens) на Polygon — см. README/CONTEXT.md, проверено по
// нескольким независимым источникам (Cobo Agentic Wallet recipes) сентябрь 2026.
const CTF_ADDRESS = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045';
const USDC_E_ADDRESS = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174';
const ZERO_BYTES32 = '0x0000000000000000000000000000000000000000000000000000000000000000'.slice(0, 66);

const CTF_REDEEM_ABI = [
  'function redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets) external',
];

@Injectable()
export class RedeemService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RedeemService.name);
  private relayClient: RelayClient | null = null;
  private stopped = false;
  private timer: NodeJS.Timeout | null = null;
  private readonly enabled: boolean;
  private readonly pollMs: number;

  constructor(
    private readonly config: ConfigService,
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

  private ensureClient(): RelayClient {
    if (this.relayClient) return this.relayClient;

    const privateKeyRaw = this.config.get<string>('PRIVATE_KEY');
    if (!privateKeyRaw) {
      throw new Error('PRIVATE_KEY не задан — клейм невозможен.');
    }
    const privateKey = privateKeyRaw.startsWith('0x') ? privateKeyRaw : `0x${privateKeyRaw}`;
    const signer = new Wallet(privateKey);

    const relayerUrl = this.config.get<string>('RELAYER_URL', 'https://relayer-v2.polymarket.com/');
    const chainId = 137;

    const key = this.config.get<string>('RELAYER_API_KEY', '');
    const secret = this.config.get<string>('RELAYER_API_SECRET', '');
    const passphrase = this.config.get<string>('RELAYER_API_PASSPHRASE', '');
    if (!key || !secret || !passphrase) {
      throw new Error(
        'RELAYER_API_KEY/RELAYER_API_SECRET/RELAYER_API_PASSPHRASE не заданы — ' +
          'создайте Relayer API Key в Settings → API Keys на polymarket.com.',
      );
    }
    const builderConfig = new BuilderConfig({ localBuilderCreds: { key, secret, passphrase } });

    const relayTxTypeRaw = this.config.get<string>('RELAYER_TX_TYPE', 'SAFE').toUpperCase();
    const relayTxType = relayTxTypeRaw === 'PROXY' ? RelayerTxType.PROXY : RelayerTxType.SAFE;

    this.relayClient = new RelayClient(relayerUrl, chainId, signer, builderConfig, relayTxType);
    this.logger.warn(
      `RelayClient инициализирован (relayTxType=${relayTxType}) — клейм резолвов будет отправляться через relayer.`,
    );
    return this.relayClient;
  }

  /**
   * Берёт все не заклеймленные реальные выигрыши, группирует по conditionId
   * (если по одному и тому же маркету случайно оказалось несколько логов —
   * не должно происходить в норме, но защищаемся) и отправляет ОДНОЙ пачкой
   * через relayer — экономия на количестве relayer-транзакций.
   */
  private async redeemPendingBatch(): Promise<void> {
    const pending = await this.marketLogRepo.find({
      where: { redeemStatus: 'pending', isSmoke: false },
      order: { resolvedAt: 'ASC' },
      take: 50,
    });
    if (pending.length === 0) return;

    // negRisk сейчас не поддержан (см. комментарий в шапке файла) — не трогаем.
    const negRiskLogs = pending.filter((l) => l.negRisk === true);
    const redeemable = pending.filter((l) => l.conditionId && !l.negRisk);

    if (negRiskLogs.length > 0) {
      for (const log of negRiskLogs) {
        log.redeemStatus = 'failed';
        log.redeemError = 'negRisk-маркет — авторедим не реализован, клеймите вручную через UI Polymarket.';
        this.logger.warn(`[REDEEM] ${log.slug}: ${log.redeemError}`);
      }
      await this.marketLogRepo.save(negRiskLogs);
    }

    if (redeemable.length === 0) return;

    // Один conditionId — один redeemPositions-вызов; если несколько логов
    // ссылаются на один и тот же conditionId, дедуплицируем (redeem одного
    // и того же условия дважды безвреден для контракта, но лишняя трата
    // relayer-транзакции).
    const uniqueConditionIds = Array.from(new Set(redeemable.map((l) => l.conditionId as string)));

    const iface = new ethersUtils.Interface(CTF_REDEEM_ABI);
    const txns: RelayTransaction[] = uniqueConditionIds.map((conditionId) => ({
      to: CTF_ADDRESS,
      data: iface.encodeFunctionData('redeemPositions', [
        USDC_E_ADDRESS,
        ZERO_BYTES32,
        conditionId,
        [1, 2], // партиция бинарного маркета (YES=1, NO=2)
      ]),
      value: '0',
    }));

    let client: RelayClient;
    try {
      client = this.ensureClient();
    } catch (err) {
      this.logger.error(`Не удалось инициализировать RelayClient: ${this.errMsg(err)}`);
      return;
    }

    this.logger.log(`[REDEEM] Отправляю пачку из ${txns.length} redeemPositions через relayer...`);
    try {
      const response = await client.execute(txns, 'polymarket-smoke-auto-redeem');
      const final = await response.wait();

      const success = final?.state === 'STATE_MINED' || final?.state === 'STATE_CONFIRMED';
      for (const log of redeemable) {
        log.redeemTransactionId = response.transactionID;
        if (success) {
          log.redeemStatus = 'redeemed';
          log.redeemedAt = new Date();
        } else {
          log.redeemStatus = 'failed';
          log.redeemError = `relayer state=${final?.state ?? 'unknown (timeout ожидания)'}`;
        }
      }
      await this.marketLogRepo.save(redeemable);

      if (success) {
        this.logger.log(`[REDEEM] Успешно: ${uniqueConditionIds.length} conditionId, tx=${response.transactionID}`);
      } else {
        this.logger.error(
          `[REDEEM] Relayer вернул состояние ${final?.state ?? 'unknown'} для tx=${response.transactionID} — требует ручного разбора.`,
        );
      }
    } catch (err) {
      const message = this.errMsg(err);
      this.logger.error(`[REDEEM] Ошибка отправки через relayer: ${message}`);
      for (const log of redeemable) {
        log.redeemStatus = 'failed';
        log.redeemError = message;
      }
      await this.marketLogRepo.save(redeemable);
    }
  }

  private errMsg(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
