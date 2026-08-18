// ============================================================
// main.ts  (User Service)
//
// WHY IT EXISTS:
//   NestJS application entry point.
//   Configures global pipes (validation) and starts the HTTP
//   server on the port defined in configuration.ts.
//
// ValidationPipe:
//   Automatically validates every incoming request body against
//   its DTO class using class-validator decorators.
//   whitelist: true   → strips unknown properties (security)
//   forbidNonWhitelisted → returns 400 for unknown properties
//   transform: true   → casts plain objects to DTO class instances
// ============================================================

import { ValidationPipe } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  const configService = app.get(ConfigService);
  const port = configService.get<number>('port');

  await app.listen(port);
  console.log(`\n🚀 User Service running on http://localhost:${port}`);
  console.log(`   POST http://localhost:${port}/users`);
  console.log(`   GET  http://localhost:${port}/users\n`);
}

bootstrap();
