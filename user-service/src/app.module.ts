// ============================================================
// app.module.ts  (User Service)
//
// WHY IT EXISTS:
//   The root NestJS module.  Imports ConfigModule (env vars),
//   and UsersModule (which transitively imports RabbitMQModule).
//
// ConfigModule.forRoot():
//   - isGlobal: true  → ConfigService is available everywhere
//   - load: [configuration]  → maps env vars to typed config
// ============================================================

import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import configuration from './config/configuration';
import { UsersModule } from './users/users.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      load: [configuration],
      envFilePath: '.env',
    }),
    UsersModule,
  ],
})
export class AppModule {}
