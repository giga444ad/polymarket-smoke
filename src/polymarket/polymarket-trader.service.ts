import { Injectable, Logger } from '@nestjs/common';
import { OrderSide, OrderType } from '@polymarket/client';
import { PolymarketSecureClientService } from './secure-client.service';

export interface PlaceMarketOrderParams {
  tokenId: string;
  /** $$$-сумма к покупке (не количество токенов) — amount у placeMarketOrder в SDK. */
  amountUsd: number;
  /** Худшая цена, дальше которой не идём (у нас — верхняя граница входа, напр. 0.999). */
  worstPrice: number;
}

export interface PlaceLimitOrderParams {
  tokenId: string;
  price: number;
  size: number;
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
 * Боевая отправка ордеров через НОВЫЙ унифицированный SDK @polymarket/client
 * (см. secure-client.service.ts — там же разбор авторизации по Relayer API Key).
 *
 * ПОЧЕМУ НЕ @polymarket/clob-client:
 * На конец сентября 2026 архивный @polymarket/clob-client перестал приниматься
 * биржей — createAndPostMarketOrder/createAndPostOrder возвращают
 * {"error":"invalid order version, please use the latest clob-client"}.
 * Официальный quickstart и остальной боевой код проекта (клейм/баланс) уже
 * работают через @polymarket/client, поэтому и создание ордеров переведено на
 * тот же клиент (SecureClient из PolymarketSecureClientService).
 *
 * ВАЖНО (прочитать перед боевым запуском, т.е. SMOKE_START=false):
 * - tickSize/negRisk новому SDK передавать НЕ нужно — он резолвит их сам.
 * - GTD-лимитка требует expiration ≥ 3 минут в будущем; в 5-минутном маркете
 *   это часто нарушается, поэтому при малом остатке времени ставим GTC без
 *   expiration (бот всё равно снимает резервный ордер сам, см.
 *   cancelRestingIfAny/finalizeMarket в trading.service.ts).
 * - SIGNATURE_TYPE=2 (прокси-кошелёк): подпись ордера этим путём вживую ещё
 *   не гонялась — обязателен один ручной боевой раунд (create → проверить
 *   исполнение в личном кабинете) до автозапуска без присмотра.
 */
@Injectable()
export class PolymarketTraderService {
  private readonly logger = new Logger(PolymarketTraderService.name);

  /** Минимальный запас времени (сек) до истечения, при котором ещё ставим GTD.
   *  Ниже порога SDK отвергнет GTD (нужно ≥3 мин) — ставим GTC вместо этого. */
  private static readonly GTD_MIN_LEAD_SEC = 210;

  constructor(private readonly secureClientService: PolymarketSecureClientService) {}

  /** Прогрев боевого клиента на старте: если авторизация/ключи невалидны — бросит,
   *  и вызывающий (TradingService.onModuleInit) принудительно уйдёт в SMOKE. */
  async ensureClient(): Promise<void> {
    await this.secureClientService.getClient();
  }

  /**
   * Правило A (агрессивный вход): "рыночный" ордер с потолком цены.
   * У Polymarket нет чистого market-ордера без ценового потолка — ближайший
   * аналог это FAK (fill-and-kill = IOC): берёт всё, что есть в стакане по
   * цене <= maxPrice, остаток снимает сам. amount передаём в $$$, а не в
   * штуках токена.
   */
  async placeMarketBuy(params: PlaceMarketOrderParams): Promise<PlaceOrderResult> {
    const client = await this.secureClientService.getClient();

    const resp = await client.placeMarketOrder({
      assetId: params.tokenId,
      side: OrderSide.BUY,
      amount: params.amountUsd,
      maxPrice: params.worstPrice,
      orderType: OrderType.FAK,
    });

    return this.toResult(resp);
  }

  /**
   * Правило B (лимитка на случай отсутствия предложений): GTD-ордер
   * (good-till-date) с истечением ровно на закрытии маркета. Если до закрытия
   * осталось меньше GTD_MIN_LEAD_SEC — SDK отверг бы GTD, поэтому ставим GTC
   * без expiration; висящий ордер всё равно снимается ботом при
   * пересборке/финализации окна.
   */
  async placeLimitBuy(params: PlaceLimitOrderParams): Promise<PlaceOrderResult> {
    const client = await this.secureClientService.getClient();

    const nowSec = Math.floor(Date.now() / 1000);
    const useGtd =
      params.expirationUnixSec - nowSec >= PolymarketTraderService.GTD_MIN_LEAD_SEC;

    const resp = await client.placeLimitOrder({
      assetId: params.tokenId,
      price: params.price,
      size: params.size,
      side: OrderSide.BUY,
      ...(useGtd ? { expiration: params.expirationUnixSec } : {}),
    });

    return this.toResult(resp);
  }

  async cancelOrder(orderId: string): Promise<void> {
    try {
      const client = await this.secureClientService.getClient();
      await client.cancelOrder({ orderId });
    } catch (err) {
      // Ордер мог уже исполниться/истечь сам — это не критично, просто логируем.
      this.logger.warn(`Не удалось отменить ордер ${orderId}: ${this.errMsg(err)}`);
    }
  }

  async getOrderStatus(orderId: string): Promise<OrderStatus | null> {
    try {
      const client = await this.secureClientService.getClient();
      const order = await client.fetchOrder({ orderId });
      return {
        id: order.id,
        status: order.status,
        originalSize: parseFloat(order.originalSize),
        sizeMatched: parseFloat(order.sizeMatched),
      };
    } catch (err) {
      this.logger.warn(`Не удалось получить статус ордера ${orderId}: ${this.errMsg(err)}`);
      return null;
    }
  }

  private toResult(resp: any): PlaceOrderResult {
    if (resp?.ok === false) {
      this.logger.warn(
        `Ордер отвергнут биржей: code=${resp?.code ?? '?'} message=${resp?.message ?? '?'}`,
      );
      return {
        orderId: null,
        success: false,
        takingAmount: null,
        makingAmount: null,
        raw: resp,
      };
    }
    return {
      orderId: resp?.orderId ?? null,
      success: true,
      takingAmount: resp?.takingAmount ?? null,
      makingAmount: resp?.makingAmount ?? null,
      raw: resp,
    };
  }

  private errMsg(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}
