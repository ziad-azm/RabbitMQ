// ============================================================
// users.module.ts
//
// WHY IT EXISTS:
//   Groups everything related to the Users domain:
//   controller, service, and the RabbitMQ module it depends on.
// ============================================================

import { Module } from '@nestjs/common';
import { RabbitMQModule } from '../rabbitmq/rabbitmq.module';
import { UsersController } from './users.controller';
import { UsersService } from './users.service';

@Module({
  imports: [RabbitMQModule],
  controllers: [UsersController],
  providers: [UsersService],
})
export class UsersModule {}
