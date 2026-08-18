// ============================================================
// rabbitmq.module.ts  (User Service)
//
// WHY IT EXISTS:
//   Encapsulates RabbitMQService so it can be injected into
//   any other module with a single import.
//   Exports RabbitMQService so UsersModule can use it.
// ============================================================

import { Module } from '@nestjs/common';
import { RabbitMQService } from './rabbitmq.service';

@Module({
  providers: [RabbitMQService],
  exports: [RabbitMQService],
})
export class RabbitMQModule {}
