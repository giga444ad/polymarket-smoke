import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { EdgeScoreSample } from '../entities/edge-score-sample.entity';
import { PriceFeedService } from '../polymarket/price-feed.service';
import { TradingService } from './trading.service';

/**
 * Сессия 17 (см. CONTEXT.md — "непрерывный поиск входа" / edge-модель).
 *
 * СОЗНАТЕЛЬНО отдельный сервис, а не правка внутри onBookUpdate:
 * - НЕ трогает ни одной строчки live-логики входа (EntryGateEngine,
 *   TradingService.onBookUpdate) — ноль риска сломать то, что уже стабильно
 *   торгует реальным/смоук капиталом.
 * - Работает по СВОЕМУ таймеру (EDGE_SAMPLE_INTERVAL_MS), а не по частоте
 *   тиков стакана — иначе на активных потоках это будет писать в БД
 *   десятки раз в секунду без всякой пользы для последующей калибровки.
 * - Включается/выключается ОДНИМ флагом (EDGE_SAMPLER_ENABLED) — можно
 *   держать выключенным на проде без единой правки в остальном коде и
 *   включить в любой момент, когда решишь начать сбор.
 *
 * Пишет по одному ряду EdgeScoreSample на активное окно на каждый тик
 * таймера, начиная со второй половины окна (EDGE_SAMPLER_START_RATIO) — по
 * ВСЕМ активным окнам, не только тем, где бот реально пытается войти (см.
 * class-comment EdgeScoreSample про survivorship bias).
 */
@Injectable()
export class EdgeSamplerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(EdgeSamplerService.name);
  private timer: NodeJS.Timeout | null = null;

  private readonly enabled: boolean;
  private readonly intervalMs: number;
  private readonly startRatio: number;
  private readonly smoothnessLookbackSec: number;

  constructor(
    private readonly config: ConfigService,
    private readonly priceFeed: PriceFeedService,
    private readonly trading: TradingService,
    @InjectRepository(EdgeScoreSample) private readonly repo: Repository<EdgeScoreSample>,
  ) {
    this.enabled = this.config.get<string>('EDGE_SAMPLER_ENABLED', 'false') === 'true';
    this.intervalMs = parseInt(this.config.get<string>('EDGE_SAMPLE_INTERVAL_MS', '1000'), 10);
    this.startRatio = parseFloat(this.config.get<string>('EDGE_SAMPLER_START_RATIO', '0.5'));
    this.smoothnessLookbackSec = parseInt(this.config.get<string>('EDGE_SMOOTHNESS_LOOKBACK_SEC', '30'), 10);
  }

  onModuleInit(): void {
    if (!this.enabled) {
      this.logger.log('EdgeSamplerService: выключен (EDGE_SAMPLER_ENABLED=false) — не пишет диагностику.');
      return;
    }
    this.logger.log(
      `EdgeSamplerService: включён, интервал ${this.intervalMs}мс, старт с ${(this.startRatio * 100).toFixed(0)}% прошедшего времени окна, ` +
        `smoothness-lookback ${this.smoothnessLookbackSec}с. Чисто диагностика — реальные ордера не трогает.`,
    );
    this.timer = setInterval(() => void this.tick(), this.intervalMs);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
  }

  private async tick(): Promise<void> {
    const nowMs = Date.now();
    const markets = this.trading.getActiveMarketsForSampling();

    for (const m of markets) {
      const timeLeftSec = (m.closesAt.getTime() - nowMs) / 1000;
      if (timeLeftSec <= 0 || m.windowStartMs == null || m.intervalSec <= 0) continue;

      const elapsedRatio = 1 - timeLeftSec / m.intervalSec;
      if (elapsedRatio < this.startRatio) continue; // ещё первая половина окна — рано

      try {
        await this.sampleOne(m, nowMs, timeLeftSec);
      } catch (err) {
        this.logger.warn(`EdgeSamplerService: сэмпл ${m.slug} не записан: ${(err as Error)?.message ?? err}`);
      }
    }
  }

  private async sampleOne(
    m: ReturnType<TradingService['getActiveMarketsForSampling']>[number],
    nowMs: number,
    timeLeftSec: number,
  ): Promise<void> {
    const snap = this.priceFeed.getSnapshot(m.assetPrefix);
    const referencePrice = m.referencePrice;
    const price = snap.price;

    let delta: number | null = null;
    let atrRatio: number | null = null;
    let atrRobustRatio: number | null = null;
    let impliedSide: 'YES' | 'NO' | null = null;

    if (referencePrice != null && price != null) {
      delta = price - referencePrice;
      impliedSide = delta >= 0 ? 'YES' : 'NO';
      if (snap.atr != null && snap.atr > 0) atrRatio = Math.abs(delta) / snap.atr;
    }

    const atrRobust = this.priceFeed.getAtrRobust(m.assetPrefix);
    if (delta != null && atrRobust != null && atrRobust > 0) {
      atrRobustRatio = Math.abs(delta) / atrRobust;
    }

    const driftPast = this.priceFeed.getPriceAt(m.assetPrefix, nowMs - this.smoothnessLookbackSec * 1000);
    let driftRate: number | null = null;
    if (referencePrice != null && price != null && driftPast.price != null) {
      driftRate = (price - referencePrice) - (driftPast.price - referencePrice);
    }

    const zoneRatio =
      referencePrice != null && impliedSide != null
        ? this.priceFeed.getTimeInZoneRatio(m.assetPrefix, m.windowStartMs!, nowMs, referencePrice, impliedSide)
        : null;

    const smoothness = this.priceFeed.getSmoothnessRatio(m.assetPrefix, nowMs - this.smoothnessLookbackSec * 1000, nowMs);

    const row = this.repo.create({
      assetPrefix: m.assetPrefix,
      slug: m.slug,
      sampledAtMs: nowMs,
      timeLeftSec,
      referencePrice,
      price,
      delta,
      atr: snap.atr,
      atrRatio,
      atrRobust,
      atrRobustRatio,
      driftRate,
      zoneRatio,
      smoothness,
      impliedSide,
      favoriteBestAsk: m.favoriteBestAsk,
      favoriteOutcome: m.favoriteOutcome,
    });
    await this.repo.save(row);
  }
}
