// ============================================================
// user-created.event.ts
//
// WHY IT EXISTS:
//   Defines the shape of the message that travels through
//   RabbitMQ when a user is created.
//
//   Think of it as a contract between the producer (User Service)
//   and the consumer (Notification Service).
//   Both sides must agree on this structure.
//
// RABBITMQ CONCEPT – Message Payload:
//   RabbitMQ treats message bodies as opaque byte arrays.
//   We serialise this class to JSON before publishing and
//   deserialise back to this shape in the consumer.
//
// BEST PRACTICE:
//   Always include an `eventType` discriminator so consumers
//   can handle multiple event shapes from the same queue,
//   and a `timestamp` for observability / ordering checks.
// ============================================================

export class UserCreatedEvent {
  /** Unique identifier of the newly created user */
  userId: string;

  /** User's full name */
  name: string;

  /** User's email – used by Notification Service to send email */
  email: string;

  /** ISO-8601 timestamp of when the event was emitted */
  timestamp: string;

  /** Discriminator – lets consumers route to the right handler */
  eventType: 'USER_CREATED';

  constructor(partial: Partial<UserCreatedEvent>) {
    Object.assign(this, partial);
    this.eventType = 'USER_CREATED';
    this.timestamp = new Date().toISOString();
  }
}
