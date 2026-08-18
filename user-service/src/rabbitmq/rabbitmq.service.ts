// ============================================================
// rabbitmq.service.ts  (User Service)
//
// WHY IT EXISTS:
//   Wraps amqplib into a NestJS-injectable service.
//   Manages the connection lifecycle and exposes a clean
//   publish() method for the rest of the application.
//
// RABBITMQ CONCEPTS COVERED:
//   Producer    – this service IS the producer; it publishes
//                 messages to an exchange.
//   Exchange    – declared here (user.events, topic type).
//   Persistent  – messages survive a RabbitMQ broker restart
//                 when { persistent: true } is set.
//   Connection  – a TCP connection to the broker (expensive,
//                 keep alive for the app lifetime).
//   Channel     – a lightweight virtual connection inside a
//                 real TCP connection (cheap, one per task).
// ============================================================

import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as amqp from 'amqplib';
import {
  RABBITMQ_EXCHANGES,
} from '../common/constants/rabbitmq.constants';

@Injectable()
export class RabbitMQService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RabbitMQService.name);

  // One long-lived TCP connection shared by the whole service.
  private connection: amqp.ChannelModel;

  // One channel for publishing.  Channels are cheap; you could
  // create one per message, but a shared channel is more efficient.
  private channel: amqp.Channel;

  constructor(private readonly configService: ConfigService) {}

  // ─── Lifecycle ──────────────────────────────────────────────

  async onModuleInit(): Promise<void> {
    await this.connect();
  }

  async onModuleDestroy(): Promise<void> {
    await this.disconnect();
  }

  // ─── Connection ─────────────────────────────────────────────

  private async connect(): Promise<void> {
    const url = this.configService.get<string>('rabbitmq.url');

    this.logger.log(`Connecting to RabbitMQ at ${url}…`);
    this.connection = await amqp.connect(url);
    this.channel = await this.connection.createChannel();

    // ── Declare the exchange ──────────────────────────────────
    //
    //   assertExchange() creates the exchange if it does not exist,
    //   or verifies that an existing one has the same settings.
    //
    //   Exchange type "topic":
    //     Routes messages to queues whose binding key matches the
    //     routing key pattern.
    //     '*' matches one word.  '#' matches zero or more words.
    //     e.g. binding "user.*" receives "user.created", "user.deleted"
    //
    //   { durable: true }:
    //     The exchange survives a broker restart.
    //     Without this, the exchange (and all its bindings) would
    //     disappear when RabbitMQ restarts.
    //
    await this.channel.assertExchange(
      RABBITMQ_EXCHANGES.USER_EVENTS,
      'topic',
      { durable: true },
    );

    this.logger.log(
      `Exchange "${RABBITMQ_EXCHANGES.USER_EVENTS}" asserted. Ready to publish.`,
    );

    // Reconnect on unexpected connection loss.
    this.connection.on('close', () => {
      this.logger.warn('RabbitMQ connection closed. Reconnecting in 5 s…');
      setTimeout(() => this.connect(), 5000);
    });
  }

  private async disconnect(): Promise<void> {
    try {
      await this.channel?.close();
      await this.connection?.close();
      this.logger.log('RabbitMQ connection closed gracefully.');
    } catch {
      // Ignore errors during shutdown
    }
  }

  // ─── Publishing ─────────────────────────────────────────────

  /**
   * Publishes a message to the given exchange with a routing key.
   *
   * FLOW:
   *   1. Serialise the payload to JSON.
   *   2. Call channel.publish() – non-blocking, fire-and-forget.
   *   3. RabbitMQ receives the message and routes it to queues
   *      whose binding key matches `routingKey`.
   *
   * OPTIONS EXPLAINED:
   *   persistent: true  → stored to disk; survives broker restart
   *   contentType       → metadata hint for consumers
   *   timestamp         → Unix epoch; useful for message age checks
   *   messageId         → correlate logs / detect duplicates
   */
  publish(exchange: string, routingKey: string, payload: unknown): boolean {
    const content = Buffer.from(JSON.stringify(payload));

    const result = this.channel.publish(exchange, routingKey, content, {
      persistent: true,
      contentType: 'application/json',
      timestamp: Math.floor(Date.now() / 1000),
      messageId: `${routingKey}-${Date.now()}`,
    });

    if (result) {
      this.logger.debug(
        `Published to [${exchange}] with key "${routingKey}": ${JSON.stringify(payload)}`,
      );
    } else {
      // channel.publish() returns false when the write buffer is full
      // (back-pressure).  In production you should pause and wait for
      // the 'drain' event before publishing again.
      this.logger.warn('Channel write buffer full – message may be dropped!');
    }

    return result;
  }
}
