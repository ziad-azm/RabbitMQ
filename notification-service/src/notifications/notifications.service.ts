// ============================================================
// notifications.service.ts
//
// WHY IT EXISTS:
//   Contains the actual business logic that runs when a
//   UserCreatedEvent is received.  Simulates sending an email
//   and an SMS notification.
//
//   Keeping this separate from the consumer (UserCreatedConsumer)
//   means the retry / DLQ logic lives in the consumer, and the
//   actual "do work" logic lives here.  Easier to test each part.
//
// FAILURE SIMULATION:
//   The SIMULATE_FAILURE env var (or a flag on the event itself)
//   makes this service throw on the first N calls.  This lets you
//   watch the retry + DLQ flow in the Management UI without
//   writing a real broken email server.
// ============================================================

import { Injectable, Logger } from '@nestjs/common';

export interface UserCreatedPayload {
  userId: string;
  name: string;
  email: string;
  timestamp: string;
  eventType: string;
}

@Injectable()
export class NotificationsService {
  private readonly logger = new Logger(NotificationsService.name);

  // Tracks how many times each userId has been attempted.
  // In production this would be a Redis counter.
  private readonly attemptCounts = new Map<string, number>();

  async sendWelcomeEmail(payload: UserCreatedPayload): Promise<void> {
    this.logger.log(`📧 Sending welcome email to ${payload.email}…`);

    // ── Failure simulation ─────────────────────────────────
    // Toggle SIMULATE_EMAIL_FAILURE=true in .env to force failures.
    // This lets you watch the retry/DLQ flow without a real SMTP.
    if (process.env.SIMULATE_EMAIL_FAILURE === 'true') {
      const count = (this.attemptCounts.get(payload.userId) || 0) + 1;
      this.attemptCounts.set(payload.userId, count);

      if (count <= 3) {
        this.logger.warn(
          `💥 Simulated email failure for ${payload.email} (attempt ${count}/3)`,
        );
        throw new Error(
          `Simulated SMTP failure for ${payload.email} on attempt ${count}`,
        );
      }
    }
    // ── End simulation ─────────────────────────────────────

    // Simulate network latency
    await this.delay(200);

    this.logger.log(`✅ Welcome email sent to ${payload.email}`);
    this.logger.log(
      `   Subject: "Welcome to our platform, ${payload.name}!"`,
    );
  }

  async sendWelcomeSms(payload: UserCreatedPayload): Promise<void> {
    this.logger.log(`📱 Sending welcome SMS to user ${payload.userId}…`);
    await this.delay(100);
    this.logger.log(`✅ Welcome SMS sent to user ${payload.name}`);
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
