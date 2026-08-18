// ============================================================
// notifications.module.ts
//
// WHY IT EXISTS:
//   Groups the NotificationsService and UserCreatedConsumer.
//   The consumer starts (onModuleInit) as soon as this module
//   is loaded, which happens at application startup.
// ============================================================

import { Module } from '@nestjs/common';
import { UserCreatedConsumer } from './consumers/user-created.consumer';
import { NotificationsService } from './notifications.service';

@Module({
  providers: [NotificationsService, UserCreatedConsumer],
})
export class NotificationsModule {}
