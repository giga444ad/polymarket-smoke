# Сессия 15 — параллельное хранение тиков (Часть 1 плана бэктеста)

Модуль бэктеста как таковой отложен — план зафиксирован в `BACKTEST-PLAN.md`. Здесь только хранение сырых данных, чтобы оно уже копилось параллельно с вашим экспериментом по фильтрам/окнам.

## Архитектура

Отдельный процесс (`npm run ticks:record` / `npx tsx scripts/tick-recorder.ts`) — своё подключение к БД (не через Nest), не трогает основной торговый воркер вообще. Связь только через БД, в одну сторону:

- Основной воркер **пишет** указатель "какое окно сейчас открыто" (`active_windows`) при открытии/закрытии окна — 2 точечные записи на окно (раз в 5/15/60 минут), не постоянное чтение.
- Recorder **читает** этот указатель раз в `TICK_RECORDER_WINDOW_POLL_MS` (по умолчанию 1.5с) и сам открывает read-only `MarketWsStream` на нужные токены — без дублирования дискавери по Gamma API (не удваиваем внешний трафик, один источник правды о том, что сейчас открыто).

## Новые таблицы

- `price_ticks` — сырые тики chainlink/binance/bybit (каждый пришедший тик, все три источника независимо, не только "активный с фолбэком", как в live-боте — для бэктеста выгоднее иметь все три).
- `polymarket_price_ticks` — снимки bid/ask по обеим сторонам (YES/NO) раз в `TICK_RECORDER_PM_SNAPSHOT_MS` (по умолчанию 500мс) для каждого сейчас открытого окна.
- `active_windows` — служебный указатель (см. выше), не для анализа.

## Новые ENV (все опциональны, есть дефолты)

| ENV | По умолчанию | Что делает |
|---|---|---|
| `TICK_RECORDER_PROVIDERS` | `chainlink,binance,bybit` | Какие источники цены слушать (все сразу, не один активный) |
| `TICK_RECORDER_PM_SNAPSHOT_MS` | `500` | Частота снимка книги Polymarket |
| `TICK_RECORDER_WINDOW_POLL_MS` | `1500` | Частота опроса `active_windows` |
| `TICK_RECORDER_FLUSH_MS` | `1000` | Частота сброса буфера тиков в БД (пачкой, не по одной строке) |

## Что физически изменено

- Новые сущности: `ActiveWindow`, `PriceTick`, `PolymarketPriceTick` — зарегистрированы в `app.module.ts`/`trading.module.ts`.
- `trading.service.ts` — `recordActiveWindow`/`clearActiveWindow`, вызываются из `openMarket`/`finalizeMarket`. Fire-and-forget, обёрнуто в try/catch — сбой здесь никогда не роняет торговлю. Репозиторий передаётся как **опциональный** параметр конструктора — существующие тесты не потребовали правок.
- `price-feed.service.ts` — экспортированы ранее приватные адаптеры источников (`PROVIDERS`, `CHAINLINK_RTDS_ADAPTER`, `BINANCE_ADAPTER`, `BYBIT_ADAPTER`) для переиспользования один-в-один в recorder'е, без дублирования протокольных деталей.
- `scripts/tick-recorder.ts` — сам процесс.
- `scripts/verify-tick-recorder.ts` — 11 тестов на чистую логику (`diffActiveWindows`, `BufferedWriter`), без реальных БД/WS.
- `package.json` — добавлен `dotenv` в зависимости, npm-скрипт `ticks:record`.

## Как запустить

```bash
npm run ticks:record
```

Отдельный терминал/процесс (или systemd unit / pm2), параллельно с основным `npm run start:prod`. Использует те же переменные окружения из `.env` (POSTGRES_*, STREAMS_CONFIG, FEED_SYMBOL_OVERRIDES, FEED_PROXY_URL).

## Проверено

- `npx tsc --noEmit` — чисто.
- Все три тестовых скрипта — 0 `false` (включая 20 тестов `verify-trading-logic.ts`, тесты `verify-price-feed.ts`, и 11 новых в `verify-tick-recorder.ts`).

## Дальше

Дайте этому процессу поработать параллельно с вашим экспериментом по окнам/фильтрам — за несколько дней накопится история, достаточная для старта Части 2 (`BACKTEST-PLAN.md`).
