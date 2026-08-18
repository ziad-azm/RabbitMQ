// ============================================================
// rabbitmq.constants.ts  (User Service)
//
// WHY IT EXISTS:
//   Centralises all RabbitMQ topology names so they never drift
//   between producer and consumer.  A typo in a routing key
//   means messages are silently lost — constants prevent that.
//
// RABBITMQ CONCEPTS:
//   Exchange  – the routing hub; producers publish TO exchanges.
//   Routing Key – the label attached to each message; exchanges
//                 use it to decide which queue(s) receive the msg.
//   Queue     – the buffer that holds messages until a consumer
//                reads them.
// ============================================================

/** Exchanges ─────────────────────────────────────────────── */
export const RABBITMQ_EXCHANGES = {
  /**
   * Topic exchange for all user-related events.
   * Topic exchanges route by pattern matching on the routing key.
   * e.g. "user.*"  matches  "user.created", "user.deleted"
   */
  USER_EVENTS: 'user.events',

  /**
   * Dead-Letter Exchange.
   * When a message is rejected (NACK, TTL expired, queue overflow)
   * RabbitMQ re-publishes it here instead of dropping it.
   */
  USER_DLX: 'user.dlx',
} as const;

/** Queues ─────────────────────────────────────────────────── */
export const RABBITMQ_QUEUES = {
  /** Main processing queue in Notification Service */
  NOTIFICATION: 'notification.queue',

  /**
   * Retry queue – holds failed messages for RETRY_DELAY_MS
   * before sending them back to the main queue.
   * Uses x-message-ttl + its own DLX to create the delay.
   */
  NOTIFICATION_RETRY: 'notification.retry.queue',

  /**
   * Dead-Letter Queue – permanent resting place for messages
   * that exhausted all retry attempts.
   * Inspect this queue in the Management UI to debug failures.
   */
  NOTIFICATION_DEAD: 'notification.dead.queue',
} as const;

/** Routing Keys ──────────────────────────────────────────── */
export const RABBITMQ_ROUTING_KEYS = {
  /** Published by User Service when a new user is created */
  USER_CREATED: 'user.created',

  /** Used internally by the DLX to route to the retry queue */
  NOTIFICATION_RETRY: 'notification.retry',
} as const;
