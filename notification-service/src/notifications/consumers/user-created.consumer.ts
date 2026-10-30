// ============================================================
// user-created.consumer.ts
//
// WHY IT EXISTS:
//   The RabbitMQ consumer for UserCreatedEvents.
//   Sets up the FULL message topology (exchanges + queues +
//   bindings) on startup, then listens for messages.
//
// RABBITMQ CONCEPTS COVERED HERE:
//
//  ┌──────────────────────────────────────────────────────────┐
//  │                   TOPOLOGY DIAGRAM                        │
//  │                                                           │
//  │  Producer                                                 │
//  │  (User Service)                                          │
//  │       │ publish(user.events, user.created, msg)          │
//  │       ▼                                                   │
//  │  ┌─────────────────────┐                                 │
//  │  │  user.events        │  Exchange (topic, durable)      │
//  │  │  (topic exchange)   │                                 │
//  │  └────────┬────────────┘                                 │
//  │           │ binding: user.created                        │
//  │           ▼                                               │
//  │  ┌─────────────────────────────────────────────────┐    │
//  │  │  notification.queue                             │    │
//  │  │  (durable)                                      │    │
//  │  │  x-dead-letter-exchange  → user.dlx             │    │
//  │  │  x-dead-letter-rk        → notification.retry   │    │
//  │  └────────────────────┬────────────────────────────┘    │
//  │                        │ Consumer reads message          │
//  │              ┌─────────┴───────────┐                    │
//  │              │                     │                     │
//  │          [SUCCESS]              [FAILURE]               │
//  │              │                     │                     │
//  │           channel.ack()      channel.nack(false, false) │
//  │              │                     │                     │
//  │         (removed from        (routed via DLX)           │
//  │            queue)                  │                     │
//  │                                    ▼                     │
//  │                        ┌───────────────────────┐        │
//  │                        │    user.dlx            │        │
//  │                        │  (direct exchange)     │        │
//  │                        └──────────┬────────────┘        │
//  │                                   │ binding: notification.retry │
//  │                                   ▼                     │
//  │                        ┌───────────────────────────────┐│
//  │                        │  notification.retry.queue     ││
//  │                        │  x-message-ttl: 10 000 ms     ││
//  │                        │  x-dead-letter-exchange →     ││
//  │                        │      user.events              ││
//  │                        │  x-dead-letter-rk →           ││
//  │                        │      user.created             ││
//  │                        └───────────────┬───────────────┘│
//  │                                        │ After TTL       │
//  │                                        │ re-published to │
//  │                                        │ user.events     │
//  │                                        ▼                 │
//  │                              (back to notification.queue)│
//  │                                                           │
//  │  After MAX_RETRY_ATTEMPTS:                                │
//  │       channel.publish('', notification.dead.queue, msg)  │
//  │       channel.ack(msg)   ← removes from retry loop       │
//  │            │                                              │
//  │            ▼                                              │
//  │  ┌──────────────────────────┐                            │
//  │  │  notification.dead.queue │  Dead-Letter Queue (DLQ)  │
//  │  │  (permanent storage)     │  Inspect in Management UI │
//  │  └──────────────────────────┘                            │
//  └──────────────────────────────────────────────────────────┘
//
// ACK  (Acknowledgement):
//   Tells RabbitMQ "I processed this message successfully."
//   RabbitMQ removes it from the queue.
//
// NACK (Negative Acknowledgement):
//   Tells RabbitMQ "I failed to process this."
//   With requeue=false + DLX configured → message is dead-lettered.
//
// Dead-Letter Exchange (DLX):
//   A normal exchange that receives messages when they:
//   - Are NACKed with requeue=false
//   - Expire (x-message-ttl)
//   - Are rejected because the queue is full
//
// Durable Queues:
//   Survive broker restarts.  Without durable=true, a restarted
//   broker loses all queues and all pending messages.
//
// Prefetch Count:
//   channel.prefetch(1) means: send me ONE unacknowledged message
//   at a time.  Without this, RabbitMQ would flood the consumer
//   with all queued messages at once.
//   In production, tune this based on your processing time and
//   number of consumer instances.
//
// x-death header:
//   RabbitMQ automatically adds this header when a message is
//   dead-lettered.  It accumulates an entry each time the message
//   goes through a DLX.  The `count` field tracks how many times
//   the message died from the same original queue.
//   We use this to implement MAX_RETRY_ATTEMPTS.
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
  RABBITMQ_QUEUES,
  RABBITMQ_ROUTING_KEYS,
} from '../../common/constants/rabbitmq.constants';
import {
  NotificationsService,
  UserCreatedPayload,
} from '../notifications.service';

@Injectable()
export class UserCreatedConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(UserCreatedConsumer.name);

  private connection: amqp.ChannelModel;
  private channel: amqp.Channel;

  constructor(
    private readonly configService: ConfigService,
    private readonly notificationsService: NotificationsService,
  ) {}

  // ─── Lifecycle ──────────────────────────────────────────────

  async onModuleInit(): Promise<void> {
    await this.connect();
  }

  async onModuleDestroy(): Promise<void> {
    await this.disconnect();
  }

  // ─── Connection + Topology Setup ────────────────────────────

  private async connect(): Promise<void> {
    const url = this.configService.get<string>('rabbitmq.url');
    const maxRetries = this.configService.get<number>('rabbitmq.maxRetryAttempts');
    const retryDelay = this.configService.get<number>('rabbitmq.retryDelayMs');

    this.logger.log(`Connecting to RabbitMQ at ${url}…`);

    this.connection = await amqp.connect(url);
    this.channel = await this.connection.createChannel();

    // ── 1. Assert Exchanges ──────────────────────────────────

    // user.events – Topic exchange (same as User Service asserts).
    // assertExchange is idempotent: safe to call on both sides.
    await this.channel.assertExchange(
      RABBITMQ_EXCHANGES.USER_EVENTS,
      'topic',
      { durable: true },
    );

    // user.dlx – Dead-Letter Exchange (direct).
    // Receives NACKed messages from notification.queue.
    await this.channel.assertExchange(
      RABBITMQ_EXCHANGES.USER_DLX,
      'direct',
      { durable: true },
    );

    // ── 2. Assert Queues ─────────────────────────────────────

    // Main processing queue.
    //
    // Arguments explained:
    //   x-dead-letter-exchange:  When a message is NACKed or
    //     expired, forward it to user.dlx instead of dropping it.
    //   x-dead-letter-routing-key: Override the routing key so
    //     user.dlx routes it to notification.retry.queue.
    //
    await this.channel.assertQueue(RABBITMQ_QUEUES.NOTIFICATION, {
      durable: true,  // survives broker restart
      arguments: {
        'x-dead-letter-exchange': RABBITMQ_EXCHANGES.USER_DLX,
        'x-dead-letter-routing-key': RABBITMQ_ROUTING_KEYS.NOTIFICATION_RETRY,
      },
    });

    // Retry queue – holds failed messages for `retryDelay` ms,
    // then re-publishes them back to the main queue.
    //
    //   x-message-ttl:  Every message expires after `retryDelay` ms.
    //     On expiry the message is dead-lettered (forwarded to the
    //     configured DLX).
    //   x-dead-letter-exchange: user.events (the main exchange).
    //   x-dead-letter-routing-key: user.created
    //     → message lands back in notification.queue.
    //
    await this.channel.assertQueue(RABBITMQ_QUEUES.NOTIFICATION_RETRY, {
      durable: true,
      arguments: {
        'x-message-ttl': retryDelay,
        'x-dead-letter-exchange': RABBITMQ_EXCHANGES.USER_EVENTS,
        'x-dead-letter-routing-key': RABBITMQ_ROUTING_KEYS.USER_CREATED,
      },
    });

    // Dead-letter queue – permanent graveyard for exhausted messages.
    // No TTL, no DLX.  Inspect in Management UI for debugging.
    await this.channel.assertQueue(RABBITMQ_QUEUES.NOTIFICATION_DEAD, {
      durable: true,
    });

    // ── 3. Bind Queues to Exchanges ──────────────────────────

    // notification.queue ← user.events (routing key: user.created)
    await this.channel.bindQueue(
      RABBITMQ_QUEUES.NOTIFICATION,
      RABBITMQ_EXCHANGES.USER_EVENTS,
      RABBITMQ_ROUTING_KEYS.USER_CREATED,
    );

    // notification.retry.queue ← user.dlx (routing key: notification.retry)
    await this.channel.bindQueue(
      RABBITMQ_QUEUES.NOTIFICATION_RETRY,
      RABBITMQ_EXCHANGES.USER_DLX,
      RABBITMQ_ROUTING_KEYS.NOTIFICATION_RETRY,
    );

    // notification.dead.queue – we publish directly to this queue
    // by name (default exchange, routing key = queue name).
    // No binding needed for the default exchange.

    // ── 4. Set Prefetch Count ─────────────────────────────────
    //
    //   RABBITMQ CONCEPT – Prefetch Count:
    //   Tell RabbitMQ: "send me at most 1 unacknowledged message."
    //   Without this, RabbitMQ round-robins ALL pending messages
    //   to this consumer at once, potentially overloading it.
    //   With prefetch(1): fair dispatch – a busy consumer gets
    //   no new messages until it ACKs the current one.
    //
    await this.channel.prefetch(1);

    // ── 5. Start Consuming ───────────────────────────────────
    await this.channel.consume(
      RABBITMQ_QUEUES.NOTIFICATION,
      (msg) => this.handleMessage(msg, maxRetries),
      { noAck: false },  // manual ACK mode
    );

    this.logger.log(
      `Consumer started on "${RABBITMQ_QUEUES.NOTIFICATION}" ` +
      `(prefetch=1, maxRetries=${maxRetries}, retryDelay=${retryDelay}ms)`,
    );
  }

  private async disconnect(): Promise<void> {
    try {
      await this.channel?.close();
      await this.connection?.close();
    } catch {
      // Ignore shutdown errors
    }
  }

  // ─── Message Handler ─────────────────────────────────────────

  private async handleMessage(
    msg: amqp.ConsumeMessage | null,
    maxRetries: number,
  ): Promise<void> {
    if (!msg) return; // Consumer cancelled by broker

    const retryCount = this.getRetryCount(msg);
    let payload: UserCreatedPayload;

    try {
      payload = JSON.parse(msg.content.toString()) as UserCreatedPayload;
    } catch {
      // Malformed JSON – send directly to DLQ, never retry
      this.logger.error('Malformed JSON payload – routing to DLQ');
      this.sendToDeadQueue(msg);
      this.channel.ack(msg);
      return;
    }

    this.logger.log(
      `\n${'─'.repeat(60)}\n` +
      `📩 Received UserCreatedEvent (retry attempt: ${retryCount}/${maxRetries})\n` +
      `   userId: ${payload.userId}\n` +
      `   email:  ${payload.email}\n` +
      `${'─'.repeat(60)}`,
    );

    try {
      // ── Step 6: Process the message ──────────────────────────
      await this.notificationsService.sendWelcomeEmail(payload);
      await this.notificationsService.sendWelcomeSms(payload);

      // ── Step 7: ACK the message ───────────────────────────────
      //
      //   RABBITMQ CONCEPT – Acknowledgement (ACK):
      //   After successful processing, we MUST call channel.ack().
      //   This signals RabbitMQ: "message processed, safe to delete."
      //
      //   What happens without ACK:
      //   - If noAck:false (our setting), the message stays "unacked"
      //   - If the consumer crashes, RabbitMQ re-queues it (good!)
      //   - If the consumer stays alive but never ACKs, eventually
      //     the channel's prefetch buffer fills up and no new messages
      //     are delivered (bad – looks like a stalled consumer).
      //
      this.channel.ack(msg);

      // ── Step 8: Message removed from queue ─────────────────
      this.logger.log(
        `✅ Message ACKed and removed from "${RABBITMQ_QUEUES.NOTIFICATION}"`,
      );

    } catch (error) {
      this.logger.error(
        `❌ Processing failed for ${payload.email}: ${error.message}`,
      );

      if (retryCount >= maxRetries) {
        // ── Exhausted all retries → Dead-Letter Queue ─────────
        //
        //   RABBITMQ CONCEPT – Dead-Letter Queue (DLQ):
        //   A message that cannot be processed after N retries is
        //   considered "poisoned".  We move it to the DLQ so it
        //   can be inspected, replayed, or alerted on — without
        //   blocking the main queue.
        //
        //   We manually publish to the DLQ (via the default exchange)
        //   and ACK the original message to break the retry loop.
        //
        this.logger.warn(
          `☠️  Max retries (${maxRetries}) exceeded. Moving to DLQ: ` +
          `"${RABBITMQ_QUEUES.NOTIFICATION_DEAD}"`,
        );

        this.sendToDeadQueue(msg, error.message);
        this.channel.ack(msg);

      } else {
        // ── Retry: NACK without requeue → triggers DLX chain ──
        //
        //   RABBITMQ CONCEPT – NACK + Dead-Letter Exchange:
        //   channel.nack(msg, false, false):
        //     arg1: msg   – the message to nack
        //     arg2: false – don't nack multiple messages at once
        //     arg3: false – DON'T requeue (triggers DLX instead)
        //
        //   Because notification.queue has x-dead-letter-exchange
        //   set to user.dlx, RabbitMQ forwards the message to
        //   user.dlx with routing key "notification.retry".
        //   user.dlx routes it to notification.retry.queue.
        //   After x-message-ttl ms, the retry queue re-publishes
        //   the message to user.events → notification.queue.
        //   The x-death header is incremented by RabbitMQ.
        //
        this.logger.warn(
          `🔁 NACKing message (retry ${retryCount + 1}/${maxRetries} will follow in ` +
          `${this.configService.get<number>('rabbitmq.retryDelayMs')}ms)`,
        );

        this.channel.nack(msg, false, false);
      }
    }
  }

  // ─── Helpers ─────────────────────────────────────────────────

  /**
   * Reads the x-death header added by RabbitMQ to determine how
   * many times this message has been dead-lettered from
   * notification.queue.
   *
   * x-death is an array of death records.  Each record has:
   *   queue       – the queue the message was dead-lettered from
   *   reason      – "rejected" | "expired" | "maxlen"
   *   count       – how many times it died from that queue/reason
   *   time        – when it first died
   *   exchange    – exchange it was published to
   *   routing-keys – routing keys used
   */
  private getRetryCount(msg: amqp.ConsumeMessage): number {
    const xDeath = msg.properties.headers?.['x-death'];
    if (!xDeath || !Array.isArray(xDeath) || xDeath.length === 0) {
      return 0;
    }

    // Find the entry for our main notification queue
    const entry = xDeath.find(
      (d: any) => d.queue === RABBITMQ_QUEUES.NOTIFICATION,
    );

    return entry ? Number(entry.count) : 0;
  }

  /**
   * Publishes the message to the Dead-Letter Queue via the
   * default exchange (routing key = queue name).
   */
  private sendToDeadQueue(
    msg: amqp.ConsumeMessage,
    reason?: string,
  ): void {
    const deadHeaders = {
      ...msg.properties.headers,
      'x-dead-reason': reason || 'unknown',
      'x-dead-timestamp': new Date().toISOString(),
    };

    this.channel.publish(
      '',                             // default exchange
      RABBITMQ_QUEUES.NOTIFICATION_DEAD,  // routing key = queue name
      msg.content,
      {
        persistent: true,
        headers: deadHeaders,
        contentType: msg.properties.contentType,
      },
    );

    this.logger.log(
      `📬 Message published to DLQ "${RABBITMQ_QUEUES.NOTIFICATION_DEAD}"`,
    );
  }
}
