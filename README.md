# RabbitMQ with NestJS — Beginner to Advanced Tutorial

> **Audience:** Backend developers who know NestJS but have never used RabbitMQ.
> Every file in this project includes inline comments that explain the *why*, not just the *what*.

---

## Table of Contents

1. [What is RabbitMQ and Why Use It?](#1-what-is-rabbitmq-and-why-use-it)
2. [Core Concepts Glossary](#2-core-concepts-glossary)
3. [Project Architecture](#3-project-architecture)
4. [Folder Structure](#4-folder-structure)
5. [Quick Start](#5-quick-start)
6. [Step-by-Step: What Happens on POST /users](#6-step-by-step-what-happens-on-post-users)
7. [Advanced: Retry + Dead-Letter Queue](#7-advanced-retry--dead-letter-queue)
8. [RabbitMQ Management UI Guide](#8-rabbitmq-management-ui-guide)
9. [Testing Instructions](#9-testing-instructions)
10. [Common Pitfalls](#10-common-pitfalls)

---

## 1. What is RabbitMQ and Why Use It?

Imagine two services that need to communicate:

**Without a message broker (tight coupling):**
```
User Service  ──HTTP──►  Notification Service
```
Problems:
- If Notification Service is down, user creation fails.
- User Service must know Notification Service's URL.
- Scaling Notification Service requires User Service config changes.
- Slow notification sending blocks the HTTP response.

**With RabbitMQ (loose coupling):**
```
User Service  ──publish──►  [RabbitMQ]  ──consume──►  Notification Service
```
Benefits:
- Notification Service can be down; messages queue up and are processed when it recovers.
- User Service has zero knowledge of Notification Service.
- Add any number of consumers without touching User Service.
- HTTP response returns immediately; notifications happen asynchronously.

---

## 2. Core Concepts Glossary

| Concept | Plain English | This Project |
|---------|--------------|-------------|
| **Producer** | The app that sends messages | `UsersService.create()` |
| **Consumer** | The app that receives messages | `UserCreatedConsumer` |
| **Message** | A payload sent through the system | `UserCreatedEvent` JSON |
| **Exchange** | A routing hub; receives from producers, routes to queues | `user.events` (topic) |
| **Queue** | A buffer that stores messages until consumed | `notification.queue` |
| **Routing Key** | A label on each message used by the exchange to route | `user.created` |
| **Binding** | A rule connecting an exchange to a queue via a routing key pattern | `user.events` → `notification.queue` via `user.created` |
| **ACK** | "I processed this successfully — delete it from the queue" | `channel.ack(msg)` |
| **NACK** | "I failed — do something with this message" | `channel.nack(msg, false, false)` |
| **DLX** | Dead-Letter Exchange — receives rejected/expired messages | `user.dlx` |
| **DLQ** | Dead-Letter Queue — final resting place for poisoned messages | `notification.dead.queue` |
| **Durable** | Survives broker restart | All queues and exchanges use `durable: true` |
| **Persistent** | Message survives broker restart | All messages use `persistent: true` |
| **Prefetch** | Max unacknowledged messages sent to a consumer at once | `channel.prefetch(1)` |

---

## 3. Project Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Docker Network                           │
│                                                                   │
│  ┌──────────────┐    publish     ┌─────────────────────────────┐ │
│  │  User Service│ ─────────────► │         RabbitMQ            │ │
│  │  (port 3001) │                │                             │ │
│  │              │                │  Exchanges:                 │ │
│  │  POST /users │                │    user.events (topic)      │ │
│  │  GET  /users │                │    user.dlx    (direct)     │ │
│  └──────────────┘                │                             │ │
│                                  │  Queues:                    │ │
│                                  │    notification.queue       │ │
│                                  │    notification.retry.queue │ │
│                                  │    notification.dead.queue  │ │
│                                  │                             │ │
│                                  │  Management UI: :15672      │ │
│                                  └──────────────┬──────────────┘ │
│                                                 │ consume         │
│                                                 ▼                 │
│                                  ┌──────────────────────────────┐│
│                                  │   Notification Service       ││
│                                  │   (port 3002)                ││
│                                  │                              ││
│                                  │   Sends: Email + SMS         ││
│                                  │   Retries: 3× with delay     ││
│                                  │   DLQ: after 3 failures      ││
│                                  └──────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### Exchange Types Explained

```
TOPIC EXCHANGE (user.events)
─────────────────────────────
Routing Key: "user.created"
             "user.updated"
             "user.deleted"

Binding patterns:
  "user.*"  → matches "user.created", "user.updated" (one word after dot)
  "user.#"  → matches "user.created.v2", "user.a.b.c"  (zero or more words)
  "user.created" → exact match only

DIRECT EXCHANGE (user.dlx)
───────────────────────────
Routes by exact routing key match.
Used for DLX routing — we control the exact key.
```

---

## 4. Folder Structure

```
rabbitmq-nestjs-tutorial/
│
├── docker-compose.yml          ← Starts RabbitMQ + both services
├── .env                        ← RabbitMQ credentials for Compose
│
├── user-service/               ← REST API + Event Producer
│   ├── src/
│   │   ├── main.ts             ← App bootstrap, global validation pipe
│   │   ├── app.module.ts       ← Root module
│   │   ├── config/
│   │   │   └── configuration.ts ← Env var mapping
│   │   ├── common/constants/
│   │   │   └── rabbitmq.constants.ts ← Exchange/Queue/RoutingKey names
│   │   ├── rabbitmq/
│   │   │   ├── rabbitmq.module.ts
│   │   │   └── rabbitmq.service.ts   ← publish() wrapper around amqplib
│   │   └── users/
│   │       ├── dto/create-user.dto.ts       ← Input validation
│   │       ├── events/user-created.event.ts ← Message payload contract
│   │       ├── users.controller.ts          ← HTTP endpoints
│   │       ├── users.service.ts             ← Business logic + publish
│   │       └── users.module.ts
│   ├── Dockerfile
│   └── .env
│
└── notification-service/       ← RabbitMQ Consumer
    ├── src/
    │   ├── main.ts
    │   ├── app.module.ts
    │   ├── config/
    │   │   └── configuration.ts
    │   ├── common/constants/
    │   │   └── rabbitmq.constants.ts  ← Must match user-service constants!
    │   └── notifications/
    │       ├── notifications.service.ts      ← Email/SMS simulation
    │       ├── notifications.module.ts
    │       └── consumers/
    │           └── user-created.consumer.ts  ← Full topology + DLQ logic
    ├── Dockerfile
    └── .env
```

---

## 5. Quick Start

### Prerequisites
- Docker Desktop
- Node.js 20+ (for local development)

### Option A: Full Docker (Recommended)

```bash
# Start everything
docker-compose up --build

# Wait for services to be ready (~30 seconds on first run)
# Watch logs in real-time
docker-compose logs -f
```

Services:
| Service | URL |
|---------|-----|
| User Service API | http://localhost:3001 |
| Notification Service | http://localhost:3002 |
| RabbitMQ Management UI | http://localhost:15672 |

RabbitMQ credentials: `admin` / `admin123`

### Option B: Local Development

```bash
# Terminal 1 – Start RabbitMQ only
docker-compose up rabbitmq

# Terminal 2 – User Service
cd user-service
npm install
npm run start:dev

# Terminal 3 – Notification Service
cd notification-service
npm install
npm run start:dev
```

---

## 6. Step-by-Step: What Happens on POST /users

### Request
```bash
curl -X POST http://localhost:3001/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Ziad", "email": "ziad@example.com"}'
```

### Sequence Diagram

```
HTTP Client        User Service      RabbitMQ          Notification Service
     │                  │               │                       │
     │  POST /users      │               │                       │
     │─────────────────►│               │                       │
     │                  │               │                       │
     │            [1] Validate DTO      │                       │
     │            [2] Save user         │                       │
     │            [3] Build event       │                       │
     │                  │               │                       │
     │                  │ publish(      │                       │
     │                  │   user.events,│                       │
     │                  │   user.created│                       │
     │                  │   event{...}) │                       │
     │                  │──────────────►│                       │
     │                  │               │                       │
     │                  │           [4] Message                 │
     │                  │           arrives at                  │
     │                  │           user.events                 │
     │                  │               │                       │
     │◄─────────────────│           [5] Exchange routes        │
     │  HTTP 201        │           to notification.queue      │
     │  { user: {...} } │               │                       │
     │                  │           [6] Queue stores            │
     │                  │           message                     │
     │                  │               │                       │
     │                  │               │  channel.consume()    │
     │                  │               │──────────────────────►│
     │                  │               │                       │
     │                  │               │             [7] Process message
     │                  │               │             Send email + SMS
     │                  │               │                       │
     │                  │               │             [8] channel.ack(msg)
     │                  │               │◄──────────────────────│
     │                  │               │                       │
     │                  │           Message removed             │
     │                  │           from queue                  │
```

### What You'll See in the Logs

**User Service:**
```
[UsersService] User saved: abc-123 (ziad@example.com)
[RabbitMQService] Published to [user.events] with key "user.created": {...}
[UsersService] Event "user.created" published for user abc-123
```

**Notification Service:**
```
[UserCreatedConsumer] ────────────────────────────────────────────
[UserCreatedConsumer] 📩 Received UserCreatedEvent (retry attempt: 0/3)
[UserCreatedConsumer]    userId: abc-123
[UserCreatedConsumer]    email:  ziad@example.com
[UserCreatedConsumer] ────────────────────────────────────────────
[NotificationsService] 📧 Sending welcome email to ziad@example.com…
[NotificationsService] ✅ Welcome email sent to ziad@example.com
[NotificationsService] 📱 Sending welcome SMS to user abc-123…
[NotificationsService] ✅ Welcome SMS sent to user Ziad
[UserCreatedConsumer] ✅ Message ACKed and removed from "notification.queue"
```

---

## 7. Advanced: Retry + Dead-Letter Queue

### Enabling Failure Simulation

Add to `notification-service/.env`:
```
SIMULATE_EMAIL_FAILURE=true
```

Then restart the service and call `POST /users` again.

### What Happens (Retry Flow)

```
notification.queue
     │
     │  Consumer receives message (retry count: 0)
     │  sendWelcomeEmail() throws "Simulated SMTP failure"
     │
     ▼  channel.nack(msg, false, false)
     │
     │  RabbitMQ dead-letters to user.dlx
     │  with routing key "notification.retry"
     │
     ▼
notification.retry.queue  ← message sits here for 10 seconds
     │
     │  x-message-ttl expires
     │  RabbitMQ re-publishes to user.events with key "user.created"
     │
     ▼
notification.queue
     │
     │  Consumer receives message (retry count: 1)
     │  x-death header: [{queue: "notification.queue", count: 1, ...}]
     │  Still failing → NACK again
     │
     ▼  (same flow... wait 10s...)
     │
notification.queue
     │
     │  Consumer receives message (retry count: 2)
     │  Still failing → NACK again
     │
     ▼  (same flow... wait 10s...)
     │
notification.queue
     │
     │  Consumer receives message (retry count: 3)
     │  retryCount (3) >= maxRetries (3)
     │
     ▼  channel.publish('', 'notification.dead.queue', msg)
        channel.ack(msg)  ← breaks the loop
     │
notification.dead.queue  ← message stays here forever
```

### Log Output (Failure Simulation)

```
📩 Received UserCreatedEvent (retry attempt: 0/3)
💥 Simulated email failure for ziad@example.com (attempt 1/3)
❌ Processing failed: Simulated SMTP failure...
🔁 NACKing message (retry 1/3 will follow in 10000ms)

... 10 seconds later ...

📩 Received UserCreatedEvent (retry attempt: 1/3)
💥 Simulated email failure for ziad@example.com (attempt 2/3)
🔁 NACKing message (retry 2/3 will follow in 10000ms)

... 10 seconds later ...

📩 Received UserCreatedEvent (retry attempt: 2/3)
💥 Simulated email failure for ziad@example.com (attempt 3/3)
🔁 NACKing message (retry 3/3 will follow in 10000ms)

... 10 seconds later ...

📩 Received UserCreatedEvent (retry attempt: 3/3)
☠️  Max retries (3) exceeded. Moving to DLQ: "notification.dead.queue"
📬 Message published to DLQ "notification.dead.queue"
```

### The x-death Header Explained

RabbitMQ automatically adds this header every time a message is dead-lettered:

```json
{
  "x-death": [
    {
      "queue": "notification.queue",
      "reason": "rejected",
      "count": 3,
      "time": "2024-01-15T10:30:00Z",
      "exchange": "user.events",
      "routing-keys": ["user.created"]
    }
  ]
}
```

`count` increments each time the message dies from the same queue with the same reason.
We read `count` to know how many retries have occurred.

---

## 8. RabbitMQ Management UI Guide

Open http://localhost:15672 and log in with `admin` / `admin123`.

### Exchanges Tab
After both services start, you should see:
| Exchange | Type | Features |
|----------|------|----------|
| `user.events` | topic | D (Durable) |
| `user.dlx` | direct | D (Durable) |

### Queues Tab
| Queue | Features | Arguments |
|-------|----------|-----------|
| `notification.queue` | D | DLX: user.dlx, DLK: notification.retry |
| `notification.retry.queue` | D | TTL: 10000ms, DLX: user.events |
| `notification.dead.queue` | D | — |

### What to Look For
1. **Ready** column: messages waiting to be consumed
2. **Unacked** column: messages being processed (non-zero means slow consumer or crash)
3. **Total** column: total messages that have passed through

### Simulating Scenarios in the UI
- **Publish a message manually:** Go to the exchange → "Publish message" → set routing key and body
- **Inspect a queued message:** Click the queue → "Get messages" → peek without ACKing
- **Watch message lifecycle:** Enable "Auto-refresh" and watch counters change in real-time

---

## 9. Testing Instructions

### Test 1: Happy Path
```bash
# Create a user
curl -X POST http://localhost:3001/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Ziad", "email": "ziad@example.com"}'

# Expected: HTTP 201, User Service logs publish, Notification Service logs email+SMS sent

# Verify user was saved
curl http://localhost:3001/users
```

### Test 2: Input Validation
```bash
# Missing required field
curl -X POST http://localhost:3001/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Ziad"}'
# Expected: HTTP 400 "email must be an email"

# Invalid email
curl -X POST http://localhost:3001/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Ziad", "email": "not-an-email"}'
# Expected: HTTP 400

# Unknown field (whitelist: true)
curl -X POST http://localhost:3001/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Ziad", "email": "ziad@example.com", "role": "admin"}'
# Expected: HTTP 400 "property role should not exist"
```

### Test 3: Retry + DLQ Flow
```bash
# 1. Add SIMULATE_EMAIL_FAILURE=true to notification-service/.env
# 2. Restart notification-service
# 3. Create a user
curl -X POST http://localhost:3001/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Test", "email": "fail@example.com"}'

# 4. Watch notification-service logs for retry attempts (10s apart)
# 5. After ~40 seconds (3 retries + final attempt), check the DLQ:
#    Open Management UI → Queues → notification.dead.queue
#    Click "Get messages" to inspect the dead letter
```

### Test 4: Consumer Resilience
```bash
# 1. Stop notification-service
docker-compose stop notification-service

# 2. Create several users (messages queue up in RabbitMQ)
curl -X POST http://localhost:3001/users -H "Content-Type: application/json" \
  -d '{"name": "Queued 1", "email": "q1@example.com"}'
curl -X POST http://localhost:3001/users -H "Content-Type: application/json" \
  -d '{"name": "Queued 2", "email": "q2@example.com"}'

# 3. Check Management UI – notification.queue should show 2 ready messages

# 4. Restart notification-service
docker-compose start notification-service

# 5. Watch it drain the queue and process all pending messages
```

---

## 10. Common Pitfalls

### 1. Queue/Exchange Mismatch
**Symptom:** Messages are published but never consumed.
**Cause:** Producer and consumer use different exchange or queue names.
**Fix:** Keep all topology constants in one shared file. Both services in this project share identical `rabbitmq.constants.ts`.

### 2. Forgetting `durable: true`
**Symptom:** All queued messages disappear when RabbitMQ restarts.
**Cause:** Queues and exchanges default to non-durable.
**Fix:** Always use `{ durable: true }` for production queues/exchanges, and `{ persistent: true }` on messages.

### 3. No ACK in Consumer
**Symptom:** Consumer processes messages but `Unacked` count in Management UI keeps growing.
**Cause:** Missing `channel.ack(msg)` after processing.
**Fix:** Every code path in the consumer must call either `ack`, `nack`, or `reject`.

### 4. Infinite Retry Loop
**Symptom:** Same message bounces between queue and retry queue forever.
**Cause:** Consumer NACKs without checking retry count, so it retries indefinitely.
**Fix:** Check `x-death` count (as shown in `UserCreatedConsumer.getRetryCount()`) and route to DLQ when exhausted.

### 5. Exchange Type Mismatch on Assert
**Symptom:** Channel closes with error: "PRECONDITION_FAILED – inequivalent arg 'type'"
**Cause:** One service asserts the exchange as "topic", another as "direct".
**Fix:** Both services must assert the same exchange type. Or only one service asserts (the consumer in this project asserts everything; the producer only re-asserts the same exchange type).

### 6. Connection vs Channel Confusion
**Production Rule:**
- One **connection** per service (shared TCP connection, expensive to create)
- One **channel** per concurrent task (lightweight, create/destroy freely)
- For publishers: one long-lived channel
- For consumers: one channel per consumer (channels are not thread-safe)
