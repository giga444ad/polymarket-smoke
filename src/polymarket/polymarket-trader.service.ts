import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ClobClient, OrderType, Side } from '@polymarket/clob-client';
import { Wallet } from 'ethers';

export interface PlaceOrderParams {
  tokenId: string;
  price: number;
  size: number;
  tickSize: string;
  negRisk: boolean;
}

export interface PlaceOrderResult {
  orderId: string | null;
  raw: unknown;
}

/**
 * ВАЖНО (прочитать перед боевым запуском, т.е. SMOKE_START=false):
 *
 * На конец сентября 2026 GitHub-репозиторий Polymarket/clob-client помечен
 * как archived и содержит предупреждение "The client is no longer functional
 * and should not be used for new or existing integrations" — Polymarket
 * рекомендует переходить на новый унифицированный SDK @polymarket/client,
 * который на этот же момент имеет статус beta и API, который ещё меняется.
 * При этом страница docs.polymarket.com/trading/quickstart на тот же момент
 * всё ещё описывала установку именно @polymarket/clob-client — то есть сами
 * официальные источники противоречат друг другу.
 *
 * Поэтому этот сервис нарочно изолирован в один файл и используется ТОЛЬКО
 * когда SMOKE_START=false. Смоук-тест (чтение стакана, вся логика БД/шагов)
 * от этого файла не зависит вообще и будет работать даже если пакет
 * действительно не функционирует.
 *
 * Перед реальным запуском: проверьте актуальное состояние на
 * https://docs.polymarket.com/trading/quickstart и https://github.com/Polymarket/ts-sdk,
 * и обязательно проведите один ручной раунд (create → post → проверить исполнение
 * в личном кабинете) до того, как оставлять бота работать без присмотра ночью.
 */
@Injectable()
export class PolymarketTraderService {
  private readonly logger = new Logger(PolymarketTraderService.name);
  private client: ClobClient | null = null;
  private initPromise: Promise<ClobClient> | null = null;

  constructor(private readonly config: ConfigService) {}

  /** Лениво инициализирует боевой клиент. Бросает исключение, если не получилось —
   *  вызывающий код (TradingService) обязан на этой ошибке принудительно уйти в смоук. */
  async ensureClient(): Promise<ClobClient> {
    if (this.client) return this.client;
    if (!this.initPromise) {
      this.initPromise = this.initClient();
    }
    this.client = await this.initPromise;
    return this.client;
  }

  private async initClient(): Promise<ClobClient> {
    const privateKeyRaw = this.config.get<string>('PRIVATE_KEY');
    if (!privateKeyRaw) {
      throw new Error('PRIVATE_KEY не задан в .env — боевая торговля невозможна');
    }
    const privateKey = privateKeyRaw.startsWith('0x')
      ? privateKeyRaw
      : `0x${privateKeyRaw}`;

    const host = this.config.get<string>(
      'CLOB_HOST',
      'https://clob.polymarket.com',
    );
    const chainId = 137;
    const signer = new Wallet(privateKey);

    const signatureType = parseInt(
      this.config.get<string>('SIGNATURE_TYPE', '0'),
      10,
    );
    const funder =
      this.config.get<string>('FUNDER_ADDRESS', '') || signer.address;

    const apiKey = this.config.get<string>('POLY_API_KEY', '');
    const apiSecret = this.config.get<string>('POLY_SECRET', '');
    const apiPassphrase = this.config.get<string>('POLY_PASSPHRASE', '');

    let creds;
    if (apiKey && apiSecret && apiPassphrase) {
      creds = { key: apiKey, secret: apiSecret, passphrase: apiPassphrase };
      this.logger.log('Использую L2 API ключи из .env');
    } else {
      this.logger.log(
        'L2 API ключи не заданы в .env — пробую derive/createOrDerive через приватный ключ...',
      );
      const bootstrapClient = new ClobClient(host, chainId, signer);
      creds = await bootstrapClient.createOrDeriveApiKey();
    }

    const client = new ClobClient(
      host,
      chainId,
      signer,
      creds,
      signatureType,
      funder,
    );

    this.logger.warn(
      `Боевой CLOB-клиент инициализирован (funder=${funder}, signatureType=${signatureType}). ` +
        'Реальные ордера будут отправляться на биржу.',
    );

    return client;
  }

  /**
   * FOK-ордер на покупку по конкретной цене (снятие ликвидности из последнего
   * известного стакана). Если не исполнится целиком мгновенно — биржа отменит его сама.
   */
  async placeFokBuy(params: PlaceOrderParams): Promise<PlaceOrderResult> {
    const client = await this.ensureClient();

    const order = await client.createOrder(
      {
        tokenID: params.tokenId,
        price: params.price,
        size: params.size,
        side: Side.BUY,
      },
      { tickSize: params.tickSize as any, negRisk: params.negRisk },
    );

    const resp: any = await client.postOrder(order, OrderType.FOK);

    return {
      orderId: resp?.orderID ?? resp?.orderId ?? null,
      raw: resp,
    };
  }
}
