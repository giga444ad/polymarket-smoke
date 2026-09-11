import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { NestExpressApplication } from '@nestjs/platform-express';
import { join } from 'path';
import * as fs from 'fs';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestExpressApplication>(AppModule);

  app.enableCors({
    origin: '*',
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
    credentials: true,
  });

  if (process.env.IS_STATIC === 'true') {
    const rootPublic = join(process.cwd(), 'public');
    const distPublic = join(__dirname, '..', 'public');
    
    // Безопасное определение пути к публичной папке
    const staticPath = fs.existsSync(rootPublic) ? rootPublic : distPublic;
    
    app.useStaticAssets(staticPath);
    Logger.log(`Раздача статики включена из: ${staticPath}`, 'Bootstrap');
  }

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