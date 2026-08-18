// ============================================================
// rabbitmq.constants.ts  (Notification Service)
//
// Identical topology names as the User Service.
// Both services MUST agree on these strings – any mismatch
// means messages are silently lost or routed to the wrong place.
// ============================================================

export const RABBITMQ_EXCHANGES = {
  USER_EVENTS: 'user.events',
  USER_DLX: 'user.dlx',
} as const;

export const RABBITMQ_QUEUES = {
  NOTIFICATION: 'notification.queue',
  NOTIFICATION_RETRY: 'notification.retry.queue',
  NOTIFICATION_DEAD: 'notification.dead.queue',
} as const;

export const RABBITMQ_ROUTING_KEYS = {
  USER_CREATED: 'user.created',
  NOTIFICATION_RETRY: 'notification.retry',
} as const;
