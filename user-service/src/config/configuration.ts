// ============================================================
// configuration.ts  (User Service)
//
// WHY IT EXISTS:
//   NestJS ConfigModule reads this factory at startup, validates
//   environment variables, and makes them available via
//   ConfigService throughout the app.
//
//   Centralising config here prevents scattered process.env calls
//   and gives you a single place to add validation later.
// ============================================================

export default () => ({
  port: parseInt(process.env.PORT, 10) || 3001,
  rabbitmq: {
    url: process.env.RABBITMQ_URL || 'amqp://admin:admin123@localhost:5672',
  },
});
