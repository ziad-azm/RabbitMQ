// ============================================================
// configuration.ts  (Notification Service)
// ============================================================

export default () => ({
  port: parseInt(process.env.PORT, 10) || 3002,
  rabbitmq: {
    url: process.env.RABBITMQ_URL || 'amqp://admin:admin123@localhost:5672',
    maxRetryAttempts: parseInt(process.env.MAX_RETRY_ATTEMPTS, 10) || 3,
    retryDelayMs: parseInt(process.env.RETRY_DELAY_MS, 10) || 10000,
  },
});
