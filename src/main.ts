import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Logger, ValidationPipe } from '@nestjs/common';
import { NestExpressApplication } from '@nestjs/platform-express';
import helmet from 'helmet';
import { join } from 'path';
import * as fs from 'fs';
import { AppModule } from './app.module';

function parseCorsOrigins(): string[] | boolean {
  const raw = process.env.CORS_ORIGIN;
  // Без явного CORS_ORIGIN в проде лучше упасть в "ничего не разрешено", чем
  // молча открыться всем (см. полный аудит безопасности — раньше здесь было
  // origin: '*' + credentials: true).
  if (!raw) return false;
  if (raw === '*') return true;
  return raw.split(',').map((s) => s.trim()).filter(Boolean);
}

async function bootstrap() {
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    // В проде не палим стек-трейсы и внутренние детали в теле ответа.
    logger: ['error', 'warn', 'log'],
  });

  app.set('trust proxy', 1);

  // Базовый набор security-заголовков (CSP отключаем на API — это JSON-бэкенд
  // без HTML-рендера, а не сайт; отдельная защита нужна фронтенду на Vercel).
  app.use(
    helmet({
      contentSecurityPolicy: false,
      crossOriginResourcePolicy: { policy: 'cross-origin' },
    }),
  );

  app.enableCors({
    origin: parseCorsOrigins(),
    methods: 'GET,HEAD,POST',
    credentials: false,
    maxAge: 600,
  });

  // Все ручки — под /api (см. задачу: единая точка входа api.axiomis.ru/api).
  app.setGlobalPrefix('api');

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  // Легаси-режим раздачи index.html из этого же процесса — оставлен только
  // для обратной совместимости/локальной отладки. Боевой фронтенд теперь
  // отдельный React-проект на Vercel (см. /frontend в корне репозитория).
  if (process.env.IS_STATIC === 'true') {
    const rootPublic = join(process.cwd(), 'public');
    const distPublic = join(__dirname, '..', 'public');
    const staticPath = fs.existsSync(rootPublic) ? rootPublic : distPublic;
    app.useStaticAssets(staticPath);
    Logger.log(`Раздача статики включена из: ${staticPath}`, 'Bootstrap');
  }

  const port = process.env.PORT || 3000;
  await app.listen(port);

  const isMain = process.env.IS_MAIN === 'true';
  Logger.log(`Polymarket bot запущен на порту ${port} (IS_MAIN=${isMain})`, 'Bootstrap');
  Logger.log(`Health: http://localhost:${port}/api/health`, 'Bootstrap');
}

bootstrap().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Не удалось запустить приложение:', err);
  process.exit(1);
});
