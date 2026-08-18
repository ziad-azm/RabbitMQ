// ============================================================
// users.service.ts
//
// WHY IT EXISTS:
//   Contains all business logic for the User domain:
//     1. "Save" the user (in-memory array for simplicity).
//     2. Build and publish a UserCreatedEvent to RabbitMQ.
//
// RABBITMQ CONCEPT – Producer:
//   A producer is any application component that publishes
//   messages to an exchange.  UsersService IS the producer.
//
// FLOW when createUser() is called:
//   ┌──────────────────────────────────────────────────────┐
//   │  HTTP POST /users  ──►  UsersController              │
//   │                             │                         │
//   │                             ▼                         │
//   │                        UsersService                   │
//   │                         │       │                     │
//   │                    save user  publish event           │
//   │                         │       │                     │
//   │                    in-memory  RabbitMQService         │
//   │                         │       │                     │
//   │                         │    channel.publish()        │
//   │                         │       │                     │
//   │                         │    RabbitMQ broker          │
//   │                         ▼       ▼                     │
//   │                       HTTP 201 response               │
//   └──────────────────────────────────────────────────────┘
//
// NOTE: The HTTP response is returned BEFORE RabbitMQ processes
// the message.  This is by design – the caller does not wait for
// the notification to be sent.  This is the power of async
// messaging: it decouples the User Service from the
// Notification Service.
// ============================================================

import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { RabbitMQService } from '../rabbitmq/rabbitmq.service';
import { CreateUserDto } from './dto/create-user.dto';
import { UserCreatedEvent } from './events/user-created.event';
import {
  RABBITMQ_EXCHANGES,
  RABBITMQ_ROUTING_KEYS,
} from '../common/constants/rabbitmq.constants';

// Simple in-memory store – replace with a real DB in production
export interface User {
  id: string;
  name: string;
  email: string;
  createdAt: string;
}

@Injectable()
export class UsersService {
  private readonly logger = new Logger(UsersService.name);
  private readonly users: User[] = [];

  constructor(private readonly rabbitMQService: RabbitMQService) {}

  // ── Step 1: Create and "save" the user ────────────────────
  async create(dto: CreateUserDto): Promise<User> {
    const user: User = {
      id: randomUUID(),
      name: dto.name,
      email: dto.email,
      createdAt: new Date().toISOString(),
    };

    // Simulate DB save
    this.users.push(user);
    this.logger.log(`User saved: ${user.id} (${user.email})`);

    // ── Step 2: Build the event ────────────────────────────
    //
    //   RABBITMQ CONCEPT – Event vs Command:
    //     An "event" announces something that already happened.
    //     The publisher does NOT care who listens.
    //     Any number of consumers can subscribe to the same event.
    //
    const event = new UserCreatedEvent({
      userId: user.id,
      name: user.name,
      email: user.email,
    });

    // ── Step 3: Publish to RabbitMQ ────────────────────────
    //
    //   RABBITMQ CONCEPT – Exchange + Routing Key:
    //     We publish to the EXCHANGE (not directly to a queue).
    //     The exchange routes the message to all queues that have
    //     a binding matching the routing key "user.created".
    //
    //   This means:
    //     - Adding a second consumer requires ONLY a new queue
    //       binding – no code change in User Service.
    //     - Removing a consumer has zero effect on User Service.
    //
    this.rabbitMQService.publish(
      RABBITMQ_EXCHANGES.USER_EVENTS,
      RABBITMQ_ROUTING_KEYS.USER_CREATED,
      event,
    );

    this.logger.log(
      `Event "${RABBITMQ_ROUTING_KEYS.USER_CREATED}" published for user ${user.id}`,
    );

    return user;
  }

  findAll(): User[] {
    return this.users;
  }
}
