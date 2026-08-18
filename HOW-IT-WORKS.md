# How This Project Works

A quick operational guide to the NestJS + RabbitMQ demo: how to run it, what happens on each request, and where every concept lives in the code.

For the full tutorial with deep explanations, see [README.md](README.md).

---

## 1. Running it

The `.env` files are git-ignored, so create them from the committed templates first:

```powershell
cd e:\Work\MyProjects\Learn\RabbitMQ
copy .env.example .env
copy user-service\.env.example user-service\.env
copy notification-service\.env.example notification-service\.env
```

Then:

```powershell
docker-compose up --build
```

That starts three containers (see [docker-compose.yml](docker-compose.yml)):

| Service | Port | Role |
|---|---|---|
| `rabbitmq` | 5672 (AMQP), 15672 (UI) | The broker |
| `user-service` | 3001 | REST API + **producer** |
| `notification-service` | 3002 | **consumer** |

Management UI: <http://localhost:15672> — login `admin` / `admin123` (from [.env](.env)).

### Docker vs. local

The `.env` file inside each service points at `amqp://...@localhost:5672`. Those values are for running a service directly on your machine:

```powershell
cd user-service
npm install
npm run start:dev
```

Under Docker Compose, `RABBITMQ_URL` is overridden to use the `rabbitmq` container hostname, so both modes work without editing anything.

---

## 2. The happy path

```powershell
curl.exe -X POST http://localhost:3001/users `
  -H "Content-Type: application/json" `
  -d '{\"name\":\"Ziad\",\"email\":\"ziad@example.com\"}'

curl.exe http://localhost:3001/users
```

What happens, in order:

1. **HTTP arrives** — [users.controller.ts](user-service/src/users/users.controller.ts). The body is validated by `class-validator` through [create-user.dto.ts](user-service/src/users/dto/create-user.dto.ts); a bad email is rejected with 400 before RabbitMQ is ever touched.
2. **User saved** — in-memory array in [users.service.ts:73](user-service/src/users/users.service.ts#L73). (A real DB would go here.)
3. **Event published** — [users.service.ts:101](user-service/src/users/users.service.ts#L101) calls
   `publish('user.events', 'user.created', event)`.
   The producer never names a queue. It publishes to the **exchange** and lets the broker route.
4. **Broker routes it** — `user.events` is a *topic* exchange; `notification.queue` is bound with the key `user.created`, so the message lands there.
5. **HTTP 201 returns immediately** — the caller does not wait for the notification to be sent. That is the whole point of async messaging: the User Service does not know or care that a Notification Service exists.
6. **Consumer picks it up** — [user-created.consumer.ts:249](notification-service/src/notifications/consumers/user-created.consumer.ts#L249), running with `prefetch(1)` and `noAck: false` (manual acknowledgement).
7. **ACK** — after `sendWelcomeEmail` + `sendWelcomeSms` both succeed, [`channel.ack(msg)`](notification-service/src/notifications/consumers/user-created.consumer.ts#L317) tells RabbitMQ the message is done.
8. **Message removed from the queue.**

Watch it live:

```powershell
docker logs -f notification-service
```

### Sequence

```
Client        user-service        RabbitMQ              notification-service
  │                │                  │                          │
  ├─ POST /users ─►│                  │                          │
  │                ├─ save user       │                          │
  │                ├─ publish ───────►│                          │
  │                │            (user.events, user.created)      │
  │◄── 201 ────────┤                  ├─ route to queue          │
  │                │                  ├─ deliver ───────────────►│
  │                │                  │                    send email
  │                │                  │                    send SMS
  │                │                  │◄──────── ack ────────────┤
  │                │                  ├─ delete message          │
```

---

## 3. The retry / DLQ path

The consumer declares the entire topology on startup — exchanges, queues, bindings ([user-created.consumer.ts:156-231](notification-service/src/notifications/consumers/user-created.consumer.ts#L156-L231)):

```
user.events (topic, durable)
      │  binding: user.created
      ▼
notification.queue (durable)
  x-dead-letter-exchange: user.dlx
  x-dead-letter-routing-key: notification.retry
      │
      │  nack(msg, false, false)   ← failure, requeue = false
      ▼
user.dlx (direct, durable)
      │  binding: notification.retry
      ▼
notification.retry.queue
  x-message-ttl: 10000
  x-dead-letter-exchange: user.events
  x-dead-letter-routing-key: user.created
      │
      │  after 10s the message expires and is dead-lettered again…
      └──────────► back into user.events ──► notification.queue

after MAX_RETRY_ATTEMPTS rounds:
      channel.publish('', 'notification.dead.queue', msg)  +  ack
      ▼
notification.dead.queue   ← the graveyard; no TTL, no DLX
```

Two things worth internalising:

- **RabbitMQ has no native "retry in N seconds."** The delay is produced by a queue with a TTL whose dead-letter target is the original exchange. That is the standard delayed-retry pattern.
- **The attempt counter is not ours.** RabbitMQ stamps an `x-death` header on every dead-lettering, incrementing `count`. [`getRetryCount()`](notification-service/src/notifications/consumers/user-created.consumer.ts#L391) reads it. No external state needed.

### Triggering a failure

`SIMULATE_EMAIL_FAILURE` is read at [notifications.service.ts:44](notification-service/src/notifications/notifications.service.ts#L44). It ships as `false` in `notification-service/.env.example`.

Flipping it to `true` in the `.env` file works only for **local** runs — the Dockerfile never copies `.env` into the image, so a containerised service sees only what Compose passes it. For the Docker flow, add it to the `notification-service` environment block in [docker-compose.yml](docker-compose.yml#L69):

```yaml
    environment:
      - NODE_ENV=production
      - PORT=3002
      - RABBITMQ_URL=amqp://admin:admin123@rabbitmq:5672
      - SIMULATE_EMAIL_FAILURE=true
```

Then rebuild just that service and post a user:

```powershell
docker-compose up -d --build notification-service
curl.exe -X POST http://localhost:3001/users `
  -H "Content-Type: application/json" `
  -d '{\"name\":\"Ziad\",\"email\":\"ziad@example.com\"}'
```

Expected timeline in the logs:

| Time | Event |
|---|---|
| 0s | Received, `retry attempt: 0/3` → simulated SMTP failure → NACK |
| 0–10s | Message sits in `notification.retry.queue` |
| 10s | TTL expires → back to `notification.queue`, `retry attempt: 1/3` → fails |
| 20s | `retry attempt: 2/3` → fails |
| 30s | `retry attempt: 3/3` → max reached → published to `notification.dead.queue`, then ACKed |

Open the Management UI → **Queues** while this runs and watch the message counts move between the three queues in real time.

A useful contrast: the simulation only throws on the first 3 attempts per `userId`. Raise `MAX_RETRY_ATTEMPTS` to `5` and the 4th attempt *succeeds* — the message recovers instead of dying. That is the difference between a transient failure and a poison message.

### Inspecting the DLQ

Management UI → Queues → `notification.dead.queue` → **Get messages**. Each dead message carries the original payload plus the `x-dead-reason` and `x-dead-timestamp` headers added in [`sendToDeadQueue()`](notification-service/src/notifications/consumers/user-created.consumer.ts#L409).

---

## 4. Where each concept lives

| Concept | File |
|---|---|
| Producer, exchange declaration, persistent messages, connection vs. channel | [rabbitmq.service.ts](user-service/src/rabbitmq/rabbitmq.service.ts) |
| Event shape (`UserCreatedEvent`) | [user-created.event.ts](user-service/src/users/events/user-created.event.ts) |
| Publishing on business action | [users.service.ts](user-service/src/users/users.service.ts) |
| Consumer, topology, ACK / NACK, prefetch, DLX, TTL, `x-death` | [user-created.consumer.ts](notification-service/src/notifications/consumers/user-created.consumer.ts) |
| Business logic, deliberately kept out of the consumer | [notifications.service.ts](notification-service/src/notifications/notifications.service.ts) |
| Shared topology names | [user-service constants](user-service/src/common/constants/rabbitmq.constants.ts) · [notification-service constants](notification-service/src/common/constants/rabbitmq.constants.ts) |
| Env config | `configuration.ts` in each service, loaded via `ConfigModule` |

**The two constants files must stay identical.** A typo in a queue or routing-key name does not throw — the message is simply routed nowhere and silently disappears. This is the single most common RabbitMQ bug in practice.

---

## 5. Design decisions worth noticing

- **The consumer owns the topology, not the producer.** The producer only asserts the exchange it publishes to. Adding a second consumer (say, an analytics service) means declaring a new queue bound to `user.events` — zero changes to `user-service`.
- **`prefetch(1)`** means RabbitMQ sends one unacknowledged message at a time. Without it, the broker floods the consumer with every pending message and fair dispatch across multiple consumer instances is lost. Tune upward once processing is fast and idempotent.
- **`durable: true` everywhere + `persistent: true` on publish.** Both are required for a message to survive a broker restart: durability protects the queue, persistence protects the message inside it.
- **Manual ACK (`noAck: false`).** If the consumer crashes mid-processing, the unacked message is redelivered rather than lost. With `noAck: true` the broker deletes on delivery and a crash loses the work.
- **The DLQ is written to manually, then the original is ACKed.** ACKing is what breaks the retry loop — without it the message would cycle forever.

---

## 6. Troubleshooting

| Symptom | Cause |
|---|---|
| Services exit on startup with `ECONNREFUSED` | RabbitMQ was not ready. Compose has a healthcheck + `depends_on: service_healthy`; if it still happens, `docker-compose restart user-service notification-service`. |
| Message published but consumer never receives it | Routing-key or queue-name mismatch between the two constants files. Check the exchange's bindings in the Management UI. |
| `PRECONDITION_FAILED - inequivalent arg` | A queue already exists with different arguments (e.g. you changed the TTL). Delete the queue in the Management UI, or `docker-compose down -v` to wipe the volume. |
| Messages pile up as "Unacked" | The consumer received them but never ACKed — usually an exception thrown outside the `try` block. |
| Retry never fires | `notification.retry.queue` is missing its `x-dead-letter-exchange`, or `x-message-ttl` is unset. |

Full reset:

```powershell
docker-compose down -v
docker-compose up --build
```

The `-v` drops the `rabbitmq_data` volume, so all exchanges, queues, and stored messages are recreated from scratch.
