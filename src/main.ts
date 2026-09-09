import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // Включаем CORS для всех источников (чтобы локальный HTML файл мог делать запросы)
  app.enableCors({
    origin: '*',
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
    credentials: true,
  });

  const port = process.env.PORT || 3000;
  await app.listen(port);
  
  Logger.log(`Polymarket 5m Bot запущен на порту ${port}`, 'Bootstrap');
  Logger.log(
    `Аналитика: http://localhost:${port}/analytics/summary`,
    'Bootstrap',
  );
}

bootstrap().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Не удалось запустить приложение:', err);
  process.exit(1);
});