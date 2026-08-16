# Kafka Order Pipeline — Learning Project

A hands-on project to learn **Kotlin + Spring Boot + Apache Kafka**, built incrementally with
production-oriented practices (layered architecture, DTOs, idempotent producers, manual ack,
error handling) — while intentionally staying small enough to build in ~3 hours.

---

## 1. Tech Stack

| Component | Choice | Why |
|---|---|---|
| Language | Kotlin | Modern JVM language, null-safety, less boilerplate than Java |
| Framework | Spring Boot 4.1 | REST, DI, JPA, Kafka integration |
| Build | Gradle (Kotlin DSL) | `build.gradle.kts` |
| Messaging | Apache Kafka (KRaft mode) | No Zookeeper — modern Kafka setup |
| DB | PostgreSQL 16 | Order persistence |
| Visualization | Kafka UI | Inspect topics, messages, consumer groups, lag |
| JSON | Jackson 3 (`tools.jackson`) | Ships with Spring Boot 4 by default |

**Note on versions:** this project uses genuinely current, bleeding-edge versions
(Spring Boot 4.1, Spring Kafka 4.1, Jackson 3). Several of the errors we hit and fixed were
*because* of this — e.g. Jackson 2→3 serializer renames. If you're following along later with
older Spring Boot 3.x + Jackson 2, some class names below (`JacksonJsonSerializer` etc.)
won't apply — use the classic `JsonSerializer`/`JsonDeserializer` instead.

---

## 2. Project Idea: Order Processing Pipeline

A single Spring Boot app simulating a small event-driven system:

```
 [Client]
    │  POST /api/v1/orders
    ▼
[OrderController] → [OrderService] ──ONE transaction──► [Postgres: orders table]      (status=PENDING)
                                                     └──► [Postgres: outbox_events]    (status=PENDING)
                                                                    │
                                                        [OutboxPublisher] (@Scheduled, every 2s)
                                                                    │  reads PENDING rows, publishes,
                                                                    │  marks PUBLISHED only after Kafka acks
                                                                    ▼
                                                        topic: order-created
                                                                    │
                                                                    ▼
                                                        [NotificationListener]
                                                        (simulates sending confirmation)
                                                                    │
                                                                    └──publish──► topic: order-confirmed
                                                                                        │
                                                                                        ▼
                                                                            [OrderConfirmationListener]
                                                                            updates DB status → CONFIRMED
```

**Why the outbox instead of publishing directly from `OrderService`:** the DB write and the Kafka
publish used to be two separate, un-linked operations — if the process crashed between them, the
order stayed `PENDING` forever with no event ever published, silently. Now the order row and the
outbox row are written in the **same DB transaction**, so they always succeed or fail together;
a background poller handles the actual Kafka publish and only marks the outbox row `PUBLISHED`
once Kafka confirms. Verified live by killing the Kafka container mid-flow — the order still
persisted, the outbox row sat `PENDING` and retried, and self-healed to `PUBLISHED` the moment
Kafka came back, with zero manual replay.

Two Kafka consumer groups (`notification-service-group`, `order-status-update-group`)
simulate what would be two separate microservices in a real system, kept in one module
for learning speed.

---

## 3. Repo Layout

```
kafka-order-pipeline/          <- repo root
├── docker-compose.yml         <- Kafka (KRaft), Kafka UI, Postgres, topic-init job
└── order-service/             <- Spring Boot Kotlin app (from start.spring.io)
    ├── build.gradle.kts
    └── src/main/kotlin/com/learning/order_service/     <- note: UNDERSCORE (Initializr auto-converts hyphens)
        ├── OrderServiceApplication.kt
        ├── domain/
        │   ├── Order.kt              (JPA entity — plain class, not data class)
        │   ├── OrderStatus.kt        (enum: PENDING, CONFIRMED, FAILED)
        │   ├── OutboxEvent.kt        (transactional outbox row)
        │   └── OutboxStatus.kt       (enum: PENDING, PUBLISHED, FAILED)
        ├── repository/
        │   ├── OrderRepository.kt    (findByOrderReference, updateStatus)
        │   └── OutboxEventRepository.kt
        ├── dto/
        │   ├── CreateOrderRequest.kt (validated API input)
        │   └── OrderResponse.kt      (API output, never expose entity directly)
        ├── event/
        │   ├── OrderCreatedEvent.kt  (Kafka payload — separate from entity/DTO)
        │   └── OrderConfirmedEvent.kt
        ├── service/
        │   └── OrderService.kt       (business logic; writes order + outbox row in ONE transaction)
        ├── controller/
        │   ├── OrderController.kt
        │   └── GlobalExceptionHandler.kt (@RestControllerAdvice, clean error JSON)
        └── kafka/
            ├── config/
            │   └── KafkaErrorHandlingConfig.kt  (DefaultErrorHandler + DeadLetterPublishingRecoverer)
            ├── outbox/
            │   └── OutboxPublisher.kt    (@Scheduled poller: outbox row → Kafka, marks PUBLISHED)
            ├── producer/
            │   └── OrderEventProducer.kt   (publish() async + publishSync() for the outbox poller)
            └── consumer/
                ├── NotificationListener.kt
                └── OrderConfirmationListener.kt
```

---

## 4. How to Run

```bash
# 1. Start infra
cd kafka-order-pipeline
docker compose up -d
docker compose ps          # confirm kafka, postgres healthy; kafka-init-topics exited(0)

# 2. Run the app
cd order-service
./gradlew bootRun

# 3. Create an order
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d "{\"customerId\": \"cust-101\", \"productName\": \"Mechanical Keyboard\", \"quantity\": 1, \"amount\": 4999.00}"
```

**Check the result:**
- Console logs: `Order persisted` → `Published event topic=order-created` →
  `Simulating notification send` → `Published event topic=order-confirmed` →
  `Order status updated to CONFIRMED`
- Postgres: `orders.status` should read `CONFIRMED`
- Kafka UI (`http://localhost:8090`): both topics show 1 message each; both consumer
  groups show 0 lag

---

## 5. Key Kotlin Concepts Covered

- **`val` vs `var`** — immutability by default, mutability as a deliberate signal
- **Nullable types (`Long?`)** — compiler-enforced null handling (`Order?` return types force
  callers to handle "not found")
- **`data class`** — free `equals`/`hashCode`/`toString`/`copy()`, used for DTOs and Kafka
  events, deliberately **not** used for JPA entities (proxy/lazy-loading concerns)
- **Constructor injection** — no `@Autowired` needed, single constructor auto-detected
- **`@field:` use-site targets** — required for Bean Validation annotations on constructor
  params (`@field:NotBlank`), otherwise validation silently no-ops
- **Companion objects** — Kotlin's static-factory-method equivalent (`OrderResponse.fromEntity()`)

---

## 6. Key Kafka / Production Concepts Covered

- **KRaft mode** — Kafka without Zookeeper
- **Explicit topic creation** — `KAFKA_AUTO_CREATE_TOPICS_ENABLE: false`, topics created
  deliberately via a one-shot `kafka-init-topics` job, not implicitly
- **Idempotent producer** (`acks=all` + `enable.idempotence=true`) — avoids duplicate
  messages on retry
- **Keyed messages** — publishing with `orderReference` as the key guarantees per-order
  ordering (same partition)
- **Manual acknowledgment** (`ack-mode: manual`) — offset only commits after business logic
  actually succeeds, not on a timer
- **`ErrorHandlingDeserializer` wrapping the real deserializer** — bad messages don't crash
  the consumer thread; flows into Spring's retry/error-handling path instead
- **`spring.json.trusted.packages`** — real security control against deserialization
  gadget-chain attacks, not just config noise
- **Consumer groups** — two separate group IDs simulate two independent services each
  getting their own copy of relevant messages (pub/sub fan-out)
- **Dead Letter Topics (provisioned, not yet wired)** — `order-created-dlt` /
  `order-confirmed-dlt` exist on the broker; Phase 4 will actually route permanently-failed
  messages there instead of losing them silently

---

## 7. Known Gaps / Deliberate Simplifications (flagged along the way)

- **`ddl-auto: update`**: convenient for this learning project, but doesn't rewrite existing
  constraints (this bit us — see debugging log). Real production uses `validate` +
  Flyway/Liquibase migrations.
- **Outbox is polling-based, not CDC-based**: `OutboxPublisher` polls every 2s. This adds a
  latency floor and idle DB load. The more advanced production version uses Change Data
  Capture (e.g., Debezium reading Postgres's WAL) to publish the instant a row commits, with
  zero polling. Worth knowing the name; not built here.
- **`order-confirmed` (from `NotificationListener`) isn't outboxed** — that publish doesn't
  pair with a DB write the same way `order-created` does, so the dual-write risk doesn't
  apply there in the same shape.
- **Idempotent consumers**: `OrderConfirmationListener` is naturally idempotent (safe to
  re-run). `NotificationListener` is not — a real notification provider call would double-send
  on redelivery. Real fix: a `processed_events` table with a unique constraint, checked in the
  same transaction as the side effect. Not implemented (time-boxed).
- **No tests yet** — Testcontainers-based integration tests are the natural next step.

---

## 8. Debugging Log (real issues hit + root causes)

Kept here because *this* is the actual valuable part of a learning project — real errors,
not a sanitized happy path.

| Symptom | Root Cause | Fix |
|---|---|---|
| `FATAL: password authentication failed for user "orders_user"` | Native Windows Postgres service also listening on port 5432, silently intercepting the connection instead of the Docker container | Remapped Docker Postgres to host port `5433` |
| `ClassNotFoundException: com.fasterxml.jackson.databind.JavaType` | Used deprecated `JsonSerializer` (Jackson 2) on a Spring Boot 4 / Jackson 3 stack | Switched to `JacksonJsonSerializer` |
| `UNKNOWN_TOPIC_OR_PARTITION` (`order_created` vs `order-created`) | Underscore/hyphen mismatch between code constant and actual topic name | Matched the constant exactly to the compose-defined topic name |
| `IllegalArgumentException: class ... not in trusted packages` | `application.yml` had `com.learning.orderservice.event` but Initializr generated `com.learning.**order_service**.event` (hyphen→underscore conversion) | Corrected `spring.json.trusted.packages` to match the real package |
| `violates check constraint "orders_status_check"` | Table was created earlier (via `ddl-auto: update`) with a stale constraint (`COMPLETED` instead of current `CONFIRMED` enum value); `update` mode never rewrites constraints | Manual `ALTER TABLE` to fix constraint (or `docker compose down -v` to recreate schema fresh) |

---

## 9. Phase 4 — Production Hardening (done)

Added `kafka/config/KafkaErrorHandlingConfig.kt`:
- `DeadLetterPublishingRecoverer` — routes permanently-failed messages to `<topic>-dlt`,
  using partition `-1` (let Kafka assign) since DLT topics have fewer partitions (1) than
  source topics (3) — publishing to a fixed partition index would fail on mismatch
- `DefaultErrorHandler` with `FixedBackOff(1000L, 3L)` — 3 retries, 1s apart, then recover
  to DLT. Wired automatically into every `@KafkaListener` by Spring Boot autoconfiguration
  (detects the single `DefaultErrorHandler` bean, no per-listener config needed)
- `DeserializationException` marked non-retryable — malformed bytes fail identically every
  time, so skip straight to DLT instead of wasting 3 retries

**Verified live**: forced `OrderConfirmationListener` to throw, watched 3 retries in logs,
confirmed the message landed in `order-confirmed-dlt` via Kafka UI with diagnostic headers
(`kafka_dlt-exception-message`, `kafka_dlt-original-topic`, etc.), confirmed consumer group
lag returned to 0 despite the failure (offset commits after recovery, not just success).
Reverted the forced failure and reconfirmed the happy path still works end-to-end.

**Idempotency** — noted but not fully implemented (time-boxed):
- `OrderConfirmationListener` is naturally idempotent (`updateStatus` is safe to re-run)
- `NotificationListener` is not (a real notification provider call would double-send on
  redelivery) — real fix is a `processed_events` table with a unique constraint, checked in
  the same transaction as the side effect

**Structured logging** — `MDC.put("orderReference", ...)` in both listeners, wrapped in
`try/finally { MDC.clear() }` (mandatory — thread pools reuse threads across messages, so a
missing `clear()` leaks one order's ID into unrelated log lines). Logback pattern updated to
include `%X{orderReference}`.

---

## 10. Phase 5 — Wrap-up / What's Genuinely Production-Grade vs. Deliberate Shortcuts

| Area | This project | Real production would add |
|---|---|---|
| DB writes | JPA, transactional service layer | Same — already production-shaped |
| Event publishing | **Outbox pattern** — DB write + outbox row in one transaction, poller publishes and confirms | Same shape; would likely swap polling for CDC (Debezium) to remove the latency floor |
| Event payloads | Hand-written `data class` + JSON | **Schema registry** (Avro/Protobuf) — enforces producer/consumer contract |
| Consumer resilience | Manual ack, retry + backoff, DLT — verified live | Same — solid as-is |
| Schema management | `ddl-auto: update` | **Flyway/Liquibase** — versioned migrations (this bit us directly, see debugging log) |
| Testing | None yet | **Testcontainers** — real Kafka + Postgres in tests, not mocks |
| Service boundaries | One module, two listener "personas" | Separate deployables with independent repos/pipelines |
| Observability | MDC correlation IDs in app logs | Distributed tracing (OpenTelemetry) across service boundaries |

---

## 11. Outbox Pattern — Implementation Notes

**Files added:**
- `domain/OutboxEvent.kt`, `domain/OutboxStatus.kt` — generic event-agnostic table:
  `payload` stored as a JSON string, `eventType` as a string tag, so new event types don't
  require schema changes
- `repository/OutboxEventRepository.kt` — `findByStatusOrderByCreatedAtAsc` with `Pageable`
  to cap batch size per poll cycle
- `kafka/outbox/OutboxPublisher.kt` — `@Scheduled(fixedDelay = 2000)` poller; publishes
  **synchronously** (`publishSync`, blocks on `.get()`) so it only marks a row `PUBLISHED`
  after Kafka actually confirms, not optimistically
- `OrderService.kt` — no longer touches Kafka at all; writes the `Order` row and the
  `OutboxEvent` row in the same `@Transactional` method
- `OrderServiceApplication.kt` — added `@EnableScheduling` (required for `@Scheduled` to run
  at all — silently no-ops without it, no error)

**Verified live (crash test):**
```bash
docker stop kafka
curl -X POST http://localhost:8080/api/v1/orders -d "..."   # order saves fine
# outbox_events row: status=PENDING, attempt_count climbing
docker start kafka
# within ~2-10s: status flips to PUBLISHED automatically, no manual replay
```
This is the actual proof the dual-write gap is closed — not just claimed.

---

*Final status: full pipeline (REST → transactional outbox → Kafka → consumer → DB update)
works end-to-end, survives a real Kafka outage without data loss (outbox self-heals), and has
retry + DLT recovery verified live on a forced consumer failure, with correlation-ID logging
throughout. Natural next steps: Testcontainers integration test, or split into a second
microservice module for real service boundaries.*