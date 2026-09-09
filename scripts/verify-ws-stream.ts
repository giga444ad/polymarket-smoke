/* Ad-hoc verification script — not part of the app, run once with ts-node/tsx and deleted. */
import { WebSocketServer } from 'ws';
import { MarketWsStream } from '../src/polymarket/market-ws-stream';

const YES = 'yes-token-123';
const NO = 'no-token-456';

async function main() {
  const wss = new WebSocketServer({ port: 8765 });
  let client: any = null;

  wss.on('connection', (ws) => {
    client = ws;
    ws.on('message', (raw) => {
      const text = raw.toString();
      if (text === 'PING') {
        ws.send('PONG');
        return;
      }
      const msg = JSON.parse(text);
      if (msg.type === 'market') {
        console.log('[server] got subscribe for', msg.assets_ids);
        // Отправляем начальный book-снапшот сразу после подписки, как реальный сервер.
        ws.send(
          JSON.stringify({
            event_type: 'book',
            asset_id: YES,
            market: '0xabc',
            bids: [{ price: '0.45', size: '100' }],
            asks: [{ price: '0.5', size: '80' }],
            tick_size: '0.01',
            timestamp: `${Date.now()}`,
          }),
        );
        ws.send(
          JSON.stringify({
            event_type: 'book',
            asset_id: NO,
            market: '0xabc',
            bids: [{ price: '0.48', size: '90' }],
            asks: [{ price: '0.55', size: '70' }],
            tick_size: '0.01',
            timestamp: `${Date.now()}`,
          }),
        );
      }
    });
  });

  const stream = new MarketWsStream(
    YES,
    NO,
    '0.01',
    (outcome, quote) => {
      console.log(`[update] ${outcome} bid=${quote.bestBid} ask=${quote.bestAsk} tick=${quote.tickSize}`);
    },
    'ws://localhost:8765',
  );
  stream.connect();

  await sleep(500);

  // Симулируем ценовое движение к порогу через price_change: сначала снимаем
  // старый уровень 0.5 (size=0 => уровень убирается), затем добавляем новый по 0.99.
  client.send(
    JSON.stringify({
      event_type: 'price_change',
      market: '0xabc',
      price_changes: [
        { asset_id: YES, price: '0.5', size: '0', side: 'SELL' },
        { asset_id: YES, price: '0.99', size: '20', side: 'SELL' },
      ],
      timestamp: `${Date.now()}`,
    }),
  );

  await sleep(300);

  // Симулируем tick_size_change (цена ушла выше 0.96).
  client.send(
    JSON.stringify({
      event_type: 'tick_size_change',
      asset_id: YES,
      market: '0xabc',
      old_tick_size: '0.01',
      new_tick_size: '0.001',
      timestamp: `${Date.now()}`,
    }),
  );

  await sleep(300);

  client.send(
    JSON.stringify({
      event_type: 'best_bid_ask',
      market: '0xabc',
      asset_id: YES,
      best_bid: '0.998',
      best_ask: '0.999',
      spread: '0.001',
      timestamp: `${Date.now()}`,
    }),
  );

  await sleep(12_000); // достаточно, чтобы поймать хотя бы один PING/PONG цикл (раз в 10с)

  stream.close();
  wss.close();
  process.exit(0);
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

main();
