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
[OrderController] → [OrderService] ──save──► [Postgres: orders table] (status=PENDING)
                            │
                            └──publish──► topic: order-created
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
        │   └── OrderStatus.kt        (enum: PENDING, CONFIRMED, FAILED)
        ├── repository/
        │   └── OrderRepository.kt    (findByOrderReference, updateStatus)
        ├── dto/
        │   ├── CreateOrderRequest.kt (validated API input)
        │   └── OrderResponse.kt      (API output, never expose entity directly)
        ├── event/
        │   ├── OrderCreatedEvent.kt  (Kafka payload — separate from entity/DTO)
        │   └── OrderConfirmedEvent.kt
        ├── service/
        │   └── OrderService.kt       (business logic, ties DB + Kafka together)
        ├── controller/
        │   ├── OrderController.kt
        │   └── GlobalExceptionHandler.kt (@RestControllerAdvice, clean error JSON)
        └── kafka/
            ├── producer/
            │   └── OrderEventProducer.kt   (generic publish(topic, key, event))
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

- **Dual-write problem**: DB save and Kafka publish aren't atomic. If the publish fails after
  the DB commit succeeds, the order stays `PENDING` forever with no retry — a real production
  system would use the **Outbox Pattern** to fix this. Out of scope for this 3-hour project,
  but worth knowing exists.
- **`ddl-auto: update`**: convenient for this learning project, but doesn't rewrite existing
  constraints (this bit us — see debugging log). Real production uses `validate` +
  Flyway/Liquibase migrations.
- **No tests yet** — Testcontainers-based integration tests are a good next step beyond
  Phase 4.

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

## 9. Roadmap (remaining phases)

- **Phase 4 — Production hardening**
  - Wire `DeadLetterPublishingRecoverer` so permanently-failed consumer messages actually
    land in `order-created-dlt` / `order-confirmed-dlt` instead of vanishing after retries
    exhaust
  - Retry policy tuning (backoff, max attempts)
  - Idempotent consumer handling (avoid double-processing on redelivery)
  - Structured logging / correlation IDs
- **Phase 5 — Wrap-up**
  - What we'd add next: Outbox pattern, schema registry, Testcontainers-based integration
    tests, splitting into separate microservices/modules, distributed tracing

---

*Status as of last session: happy path (REST → DB → Kafka → consumer → DB update) works
end-to-end after fixing the port conflict, Jackson version mismatch, topic name mismatch,
trusted-packages mismatch, and stale check constraint. Ready to resume at Phase 4.*