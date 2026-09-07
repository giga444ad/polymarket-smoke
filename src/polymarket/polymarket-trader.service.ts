import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ClobClient, OrderType, Side } from '@polymarket/clob-client';
import { Wallet } from 'ethers';

export interface PlaceMarketOrderParams {
  tokenId: string;
  /** $$$-сумма к покупке (не количество токенов) — см. UserMarketOrder.amount в SDK. */
  amountUsd: number;
  /** Худшая цена, дальше которой не идём (у нас — верхняя граница входа, напр. 0.999). */
  worstPrice: number;
  tickSize: string;
  negRisk: boolean;
}

export interface PlaceLimitOrderParams {
  tokenId: string;
  price: number;
  size: number;
  tickSize: string;
  negRisk: boolean;
  /** unix-секунды, после которых биржа сама снимет ордер (обычно = закрытие маркета). */
  expirationUnixSec: number;
}

export interface PlaceOrderResult {
  orderId: string | null;
  success: boolean;
  takingAmount: string | null;
  makingAmount: string | null;
  raw: unknown;
}

export interface OrderStatus {
  id: string;
  status: string;
  originalSize: number;
  sizeMatched: number;
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
   * Правило A (агрессивный вход): "рыночный" ордер с потолком цены.
   * У Polymarket нет чистого market-ордера без ценового потолка — ближайший
   * аналог это FAK (fill-and-kill = IOC): берёт всё, что есть в стакане по
   * цене <= worstPrice, остаток снимает сам. amount передаём в $$$, а не в
   * штуках токена — так работает UserMarketOrder в SDK.
   */
  async placeMarketBuy(params: PlaceMarketOrderParams): Promise<PlaceOrderResult> {
    const client = await this.ensureClient();

    const resp: any = await client.createAndPostMarketOrder(
      {
        tokenID: params.tokenId,
        price: params.worstPrice,
        amount: params.amountUsd,
        side: Side.BUY,
        orderType: OrderType.FAK,
      },
      { tickSize: params.tickSize as any, negRisk: params.negRisk },
      OrderType.FAK,
    );

    return this.toResult(resp);
  }

  /**
   * Правило B (лимитка на случай отсутствия предложений): GTD-ордер
   * (good-till-date) с истечением ровно на закрытии маркета — если не
   * успели сами отменить/переставить, биржа снимет его сама и деньги не
   * повиснут в воздухе после резолва маркета.
   */
  async placeLimitBuy(params: PlaceLimitOrderParams): Promise<PlaceOrderResult> {
    const client = await this.ensureClient();

    const resp: any = await client.createAndPostOrder(
      {
        tokenID: params.tokenId,
        price: params.price,
        size: params.size,
        side: Side.BUY,
        expiration: params.expirationUnixSec,
      },
      { tickSize: params.tickSize as any, negRisk: params.negRisk },
      OrderType.GTD,
    );

    return this.toResult(resp);
  }

  async cancelOrder(orderId: string): Promise<void> {
    const client = await this.ensureClient();
    try {
      await client.cancelOrder({ orderID: orderId });
    } catch (err) {
      // Ордер мог уже исполниться/истечь сам — это не критично, просто логируем.
      this.logger.warn(`Не удалось отменить ордер ${orderId}: ${this.errMsg(err)}`);
    }
  }

  async getOrderStatus(orderId: string): Promise<OrderStatus | null> {
    const client = await this.ensureClient();
    try {
      const order = await client.getOrder(orderId);
      return {
        id: order.id,
        status: order.status,
        originalSize: parseFloat(order.original_size),
        sizeMatched: parseFloat(order.size_matched),
      };
    } catch (err) {
      this.logger.warn(`Не удалось получить статус ордера ${orderId}: ${this.errMsg(err)}`);
      return null;
    }
  }

  private toResult(resp: any): PlaceOrderResult {
    return {
      orderId: resp?.orderID ?? resp?.orderId ?? null,
      success: resp?.success !== false,
      takingAmount: resp?.takingAmount ?? null,
      makingAmount: resp?.makingAmount ?? null,
      raw: resp,
    };
  }

  private errMsg(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
