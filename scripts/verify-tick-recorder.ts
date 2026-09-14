/**
 * Тесты чистой логики scripts/tick-recorder.ts — БЕЗ реальной БД/WS
 * (см. комментарий require.main-guard в самом файле, почему его вообще
 * можно безопасно импортировать из теста).
 *
 * Запуск: npx tsx scripts/verify-tick-recorder.ts
 */
import { diffActiveWindows, BufferedWriter } from './tick-recorder';
import { ActiveWindow } from '../src/entities/active-window.entity';

function row(over: Partial<ActiveWindow> = {}): ActiveWindow {
  return {
    streamKey: 'btc-updown-5m',
    slug: 'btc-updown-5m-1000',
    yesTokenId: 'yes-1',
    noTokenId: 'no-1',
    tickSize: '0.01',
    windowStartMs: 1000_000,
    closesAtMs: 1_300_000,
    updatedAt: new Date(),
    ...over,
  };
}

async function main() {
  // --- diffActiveWindows ---

  // Тест A: ничего не отслеживалось, пришла одна свежая строка -> toStart=[X], toStop=[].
  {
    const { toStop, toStart } = diffActiveWindows(new Map(), [row()], 500_000);
    console.log('[Тест A] Новое окно без предыдущего трекинга -> toStart содержит его, toStop пуст:', toStart.length === 1 && toStop.length === 0);
  }

  // Тест B: отслеживали X, строка X всё ещё та же (тот же slug, не закрылась) -> ничего не меняется.
  {
    const tracked = new Map([['btc-updown-5m', 'btc-updown-5m-1000']]);
    const { toStop, toStart } = diffActiveWindows(tracked, [row()], 500_000);
    console.log('[Тест B] Тот же slug, окно ещё не закрылось -> toStop и toStart оба пустые:', toStop.length === 0 && toStart.length === 0);
  }

  // Тест C: отслеживали X, но указателя для X больше нет в таблице (окно закрылось и вычищено) -> toStop=[X].
  {
    const tracked = new Map([['btc-updown-5m', 'btc-updown-5m-1000']]);
    const { toStop, toStart } = diffActiveWindows(tracked, [], 500_000);
    console.log('[Тест C] Указатель исчез (окно закрыто и вычищено) -> toStop содержит поток, toStart пуст:', toStop.length === 1 && toStart.length === 0);
  }

  // Тест D: отслеживали X со старым slug, в таблице уже НОВЫЙ slug того же потока
  //         (следующее окно успело открыться между опросами) -> toStop=[X] И toStart=[новая строка].
  {
    const tracked = new Map([['btc-updown-5m', 'btc-updown-5m-1000']]);
    const newRow = row({ slug: 'btc-updown-5m-2000' });
    const { toStop, toStart } = diffActiveWindows(tracked, [newRow], 500_000);
    console.log(
      '[Тест D] Слаг сменился (новое окно) -> старое остановлено И новое запущено:',
      toStop[0] === 'btc-updown-5m' && toStart[0]?.slug === 'btc-updown-5m-2000',
    );
  }

  // Тест E: строка есть, но closesAtMs уже в прошлом (страховка на случай не
  //         дошедшего clearActiveWindow) -> не трекаем вообще (ни toStop, т.к.
  //         не отслеживалось, ни toStart).
  {
    const staleRow = row({ closesAtMs: 100_000 });
    const { toStop, toStart } = diffActiveWindows(new Map(), [staleRow], 500_000);
    console.log('[Тест E] closesAtMs уже в прошлом -> НЕ начинаем слушать (toStart пуст):', toStart.length === 0 && toStop.length === 0);
  }

  // Тест F: отслеживали X, closesAtMs строки X теперь в прошлом (не успели вычистить на основном воркере) -> toStop=[X].
  {
    const tracked = new Map([['btc-updown-5m', 'btc-updown-5m-1000']]);
    const staleRow = row({ closesAtMs: 100_000 });
    const { toStop, toStart } = diffActiveWindows(tracked, [staleRow], 500_000);
    console.log('[Тест F] closesAtMs строки в прошлом при активном трекинге -> перестаём слушать:', toStop.length === 1 && toStart.length === 0);
  }

  // Тест G: два независимых потока — один продолжает, другой закрылся, третий новый появился.
  {
    const tracked = new Map([
      ['btc-updown-5m', 'slug-5m-old'],
      ['btc-updown-15m', 'slug-15m-old'],
    ]);
    const rows = [
      row({ streamKey: 'btc-updown-5m', slug: 'slug-5m-old', closesAtMs: 900_000 }), // продолжает
      row({ streamKey: 'bitcoin-up-or-down', slug: 'slug-1h-new', closesAtMs: 900_000 }), // новый
      // btc-updown-15m отсутствует -> закрылся
    ];
    const { toStop, toStart } = diffActiveWindows(tracked, rows, 500_000);
    console.log(
      '[Тест G] Смешанный случай (продолжает/закрылся/новый) -> корректный diff:',
      toStop.length === 1 &&
        toStop[0] === 'btc-updown-15m' &&
        toStart.length === 1 &&
        toStart[0].streamKey === 'bitcoin-up-or-down',
    );
  }

  // --- BufferedWriter ---

  // Тест H: push несколько строк, flush -> repo.insert вызван ОДИН раз со всем батчем, буфер очищен.
  {
    const inserted: any[][] = [];
    const fakeRepo: any = { insert: async (rows: any[]) => inserted.push(rows) };
    const writer = new BufferedWriter(fakeRepo, 'test');
    writer.push({ a: 1 } as any);
    writer.push({ a: 2 } as any);
    await writer.flush();
    console.log('[Тест H] flush пишет один батч из обеих строк:', inserted.length === 1 && inserted[0].length === 2);
    await writer.flush(); // пустой буфер — insert больше не должен вызываться
    console.log('[Тест H] Повторный flush на пустом буфере — insert не вызывается снова:', inserted.length === 1);
  }

  // Тест I: insert упал -> строки возвращаются в буфер (не теряются), следующий flush пробует снова.
  {
    let shouldFail = true;
    const inserted: any[][] = [];
    const fakeRepo: any = {
      insert: async (rows: any[]) => {
        if (shouldFail) throw new Error('БД временно недоступна');
        inserted.push(rows);
      },
    };
    const writer = new BufferedWriter(fakeRepo, 'test');
    writer.push({ a: 1 } as any);
    await writer.flush(); // упадёт
    console.log('[Тест I] Первый flush упал, ничего не записано:', inserted.length === 0);
    shouldFail = false;
    writer.push({ a: 2 } as any); // добавляем ещё одну строку поверх вернувшейся в буфер
    await writer.flush(); // теперь пройдёт
    console.log('[Тест I] Второй flush прошёл и содержит ОБЕ строки (старая не потерялась):', inserted.length === 1 && inserted[0].length === 2);
  }

  console.log('\nВСЕ ПРОВЕРКИ ВЫПОЛНЕНЫ.');
  process.exit(0);
}

main();
