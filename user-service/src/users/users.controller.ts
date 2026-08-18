// ============================================================
// users.controller.ts
//
// WHY IT EXISTS:
//   HTTP entry point.  Handles request validation (via the
//   global ValidationPipe) and delegates to UsersService.
//
//   The controller's only job is to translate HTTP → service
//   call and service result → HTTP response.
//
// ENDPOINTS:
//   POST /users  – create a user and emit a RabbitMQ event
//   GET  /users  – list all users (for demo verification)
// ============================================================

import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Post,
} from '@nestjs/common';
import { CreateUserDto } from './dto/create-user.dto';
import { UsersService } from './users.service';

@Controller('users')
export class UsersController {
  constructor(private readonly usersService: UsersService) {}

  /**
   * POST /users
   *
   * Request body:
   *   { "name": "Ziad", "email": "ziad@example.com" }
   *
   * What happens after this returns 201:
   *   1. User is saved in memory
   *   2. UserCreatedEvent published to RabbitMQ exchange
   *   3. Notification Service receives and processes the event
   *      (asynchronously – the HTTP response does not wait for it)
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  async create(@Body() dto: CreateUserDto) {
    const user = await this.usersService.create(dto);
    return {
      message: 'User created. Notification event published to RabbitMQ.',
      user,
    };
  }

  /**
   * GET /users
   *
   * Returns all users stored in memory.
   * Useful to verify that POST /users actually saved the user.
   */
  @Get()
  findAll() {
    return this.usersService.findAll();
  }
}
