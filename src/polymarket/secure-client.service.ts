import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Wallet } from 'ethers';
import { createSecureClient, relayerApiKey, SecureClient } from '@polymarket/client';
import { signerFrom } from '@polymarket/client/ethers-v5';

/**
 * ВАЖНО (прочитать перед боевым запуском):
 *
 * Использует @polymarket/client — НОВЫЙ, более высокоуровневый SDK
 * Polymarket, который умеет авторизовываться ПРОСТЫМ Relayer API Key
 * (только RELAYER_API_KEY + RELAYER_API_KEY_ADDRESS, БЕЗ secret/passphrase)
 * — именно то, что реально выдаёт страница Settings -> API Keys на
 * polymarket.com. Более старый пакет @polymarket/builder-relayer-client
 * (которым изначально был написан этот сервис) требует HMAC-креды
 * (key+secret+passphrase, т.н. Builder API), которых страница API Keys
 * тебе не давала — отсюда и была нестыковка.
 *
 * ПРОВЕРЕНО ПО ФАКТИЧЕСКИ УСТАНОВЛЕННОМУ ПАКЕТУ (сентябрь 2026):
 * - На npm тег `latest` (0.10.0) поле `apiKey`/`relayerApiKey` в
 *   createSecureClient ЕЩЁ НЕ содержит — это есть только в canary-версии
 *   (0.0.0-canary-20260914153900, дата сборки — буквально дни назад).
 *   Именно поэтому package.json сейчас пинит ТОЧНУЮ canary-версию, а не
 *   диапазон (^/~) — canary не следует семверу и может исчезнуть/измениться
 *   без предупреждения. ЭТО РИСК для продакшена сам по себе.
 * - Периодически проверяй `npm view @polymarket/client dist-tags` — как
 *   только `apiKey: relayerApiKey(...)` появится в стабильном `latest`,
 *   стоит перейти на него и убрать пин на конкретный canary.
 * - Перед боевым запуском обязательно прогони РУЧНОЙ тестовый вызов
 *   `client.redeemPositions({ conditionId })` на одной уже резолвнутой
 *   позиции (можно смоук-выигрыш вручную довести до реального маленького
 *   входа) — эта ветка SDK ещё нигде в проекте не проверялась вживую.
 *
 * SIGNATURE_TYPE у тебя в .env — это тип подписи для CLOB (торговля).
 * Здесь это НЕ используется: у @polymarket/client свой параметр `wallet`
 * (адрес фондирующего кошелька — тот же FUNDER_ADDRESS) и он сам
 * определяет тип кошелька (EOA/Deposit Wallet/Safe/Proxy) по цепочке.
 */
@Injectable()
export class PolymarketSecureClientService {
  private readonly logger = new Logger(PolymarketSecureClientService.name);
  private clientPromise: Promise<SecureClient> | null = null;

  constructor(private readonly config: ConfigService) {}

  async getClient(): Promise<SecureClient> {
    if (!this.clientPromise) {
      this.clientPromise = this.buildClient().catch((err) => {
        // Не кэшируем неудачную попытку — следующий вызов попробует снова
        // (например, если ключи ещё не были заданы на момент первого старта).
        this.clientPromise = null;
        throw err;
      });
    }
    return this.clientPromise;
  }

  private async buildClient(): Promise<SecureClient> {
    const privateKeyRaw = this.config.get<string>('PRIVATE_KEY');
    if (!privateKeyRaw) {
      throw new Error('PRIVATE_KEY не задан — нужен для @polymarket/client (подписант).');
    }
    const privateKey = privateKeyRaw.startsWith('0x') ? privateKeyRaw : `0x${privateKeyRaw}`;

    const relayerApiKeyValue = this.config.get<string>('RELAYER_API_KEY', '');
    const relayerApiKeyAddress = this.config.get<string>('RELAYER_API_KEY_ADDRESS', '');
    if (!relayerApiKeyValue || !relayerApiKeyAddress) {
      throw new Error(
        'RELAYER_API_KEY/RELAYER_API_KEY_ADDRESS не заданы — возьми их на polymarket.com ' +
          'в Settings -> API Keys (там же, где ты уже нашёл ключ).',
      );
    }

    const funderAddress = this.config.get<string>('FUNDER_ADDRESS', '');
    if (!funderAddress) {
      throw new Error('FUNDER_ADDRESS не задан — нужен как wallet для @polymarket/client.');
    }

    const signer = signerFrom(new Wallet(privateKey));

    const client = await createSecureClient({
      wallet: funderAddress,
      signer,
      apiKey: relayerApiKey({ key: relayerApiKeyValue, address: relayerApiKeyAddress }),
    });

    this.logger.warn(
      `PolymarketSecureClientService инициализирован (wallet=${funderAddress}) — ` +
        'клейм и проверка баланса будут идти через @polymarket/client (canary).',
    );
    return client;
  }
}
