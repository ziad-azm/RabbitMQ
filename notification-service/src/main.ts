// ============================================================
// main.ts  (Notification Service)
//
// WHY IT EXISTS:
//   Entry point.  Starts the NestJS application.
//   The HTTP server is kept (port 3002) only for a health-check
//   endpoint – the real work happens in UserCreatedConsumer.
// ============================================================

import { ConfigService } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  const configService = app.get(ConfigService);
  const port = configService.get<number>('port');

  await app.listen(port);
  console.log(`\n🔔 Notification Service running on http://localhost:${port}`);
  console.log(`   Listening on RabbitMQ queue: notification.queue\n`);
}

bootstrap();
