# kafka-payments

A runnable slice of the system design in [`docs/Kafka_Payments_System_Design.docx`](docs/Kafka_Payments_System_Design.docx) (diagrams: [architecture](docs/kafka_payments_architecture.png), [sequence](docs/kafka_payments_sequence.png)). It covers real-time fraud scoring and an idempotent double-entry ledger on Apache Kafka, built with Java 21, Spring Boot 3, Kafka Streams, PostgreSQL and Redis.

```
POST /payments ──► payment-api ──(1 DB txn)──► payments + outbox
                                                   │ OutboxRelay (Debezium in prod)
                                                   ▼
                       payments.initiated ──► fraud-scoring (Kafka Streams, exactly_once_v2)
   account.profiles (compacted KTable) ──┘        │ join · velocity state (RocksDB) · score · decide
                                                   ▼
                                           payments.decided
                        ┌──────────────────────────┼───────────────────────────┐
                        ▼                          ▼                           ▼
              ledger-service              notification-service           payment-api
     (processed_payments + 2 legs     (Redis SETNX dedup, SMS stub)    (status: PENDING → X)
      + outbox in ONE txn; DLQ)
                        │ outbox
                        ▼
                  ledger.posted
```

## Modules

| Module | What it shows |
|---|---|
| `common-events` | Event records (amounts in paise, event-time millis), topic names, Jackson `Serde` |
| `outbox-support` | `OutboxWriter` (must run inside the caller's transaction) and `OutboxRelay` (polling publisher; at-least-once) |
| `payment-api` | REST API with `Idempotency-Key`; payment row + outbox row in one transaction; consumer that updates status |
| `fraud-scoring` | Kafka Streams topology: KTable left join → `processValues` with a RocksDB store for 1m/1h/24h velocity → scorer → `payments.scored` → authorizer → `payments.decided` |
| `ledger-service` | At-least-once consumer that has an exactly-once effect through the `processed_payments` primary key. Exponential backoff, then `payments.decided.dlq` |
| `notification-service` | At-least-once consumer that dedups with Redis `SET NX EX` |

## Run everything in Docker (only Docker Desktop needed)

```bash
# 1. Compile + run unit tests inside a Maven container (Linux/macOS/Git Bash; PowerShell: use ${PWD})
docker run --rm -v "$PWD":/src -v kafka-payments-m2:/root/.m2 -w /src maven:3.9-eclipse-temurin-21 mvn -B clean verify

# 2. Start infra + all four services (builds the images)
cd infra && docker compose --profile apps up -d --build && cd ..

# 3. Seed profiles, run the demo, watch logs
./scripts/seed-profiles.sh
./scripts/demo.sh
docker compose -f infra/docker-compose.yml logs -f fraud-scoring ledger-service

# 4. Tear down (deletes data)
cd infra && docker compose --profile apps down -v
```

## Run it locally (services on the host)

Prerequisites: JDK 21, Maven 3.9+, Docker.

```bash
# 1. Infrastructure: Kafka (KRaft), PostgreSQL, Redis, and the topics
cd infra && docker compose up -d && cd ..
docker compose -f infra/docker-compose.yml logs kafka-init   # topic list should print

# 2. Build and run the unit tests (TopologyTestDriver, no broker needed)
mvn clean verify

# 3. Seed account profiles into the compacted topic
./scripts/seed-profiles.sh

# 4. Start the four services, one terminal each
mvn -pl payment-api spring-boot:run
mvn -pl fraud-scoring spring-boot:run
mvn -pl ledger-service spring-boot:run
mvn -pl notification-service spring-boot:run

# 5. Drive the demo
./scripts/demo.sh
```

Expected behaviour:
- The first payment from `acc-100` becomes **APPROVED**, and two ledger legs are written.
- Retrying with the same `Idempotency-Key` returns the same `paymentId` and emits no new event.
- The large payment from `acc-200` is **DECLINED**. The reasons are `VELOCITY_1M`, `AMOUNT_24H`, `NEW_PAYEE_HIGH_AMOUNT`, `LOW_KYC` and `NEW_ACCOUNT`, and nothing is posted to the ledger.

Useful checks:

```bash
docker exec -it kp-postgres psql -U payments -d ledger -c "SELECT * FROM ledger_imbalances;"   # must be empty
docker exec -it kp-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
docker exec -it kp-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic payments.decided --from-beginning --property print.key=true --isolation-level read_committed
```

## Failure experiments (good interview talking points)

1. **Redelivery to the ledger.** Stop `ledger-service`, reset its group to the earliest offset (`kafka-consumer-groups.sh --reset-offsets --to-earliest --group ledger-service --topic payments.decided --execute`), then restart it. Every record is logged as `DUPLICATE` and no rows are added.
2. **Poison message.** Publish `not-json` to `payments.decided`. The ledger sends it to `payments.decided.dlq` and keeps consuming.
3. **Crash mid-transaction in Streams.** Kill `fraud-scoring` while the demo runs, then restart it. `read_committed` consumers never see duplicate decisions.

## Production differences (see design doc)

| Here | Production |
|---|---|
| `OutboxRelay` polling | Debezium + Outbox Event Router (`infra/debezium-outbox-connector.json`), with `outbox.relay.enabled=false` |
| JSON events | Avro or Protobuf with Schema Registry (BACKWARD compatibility) |
| 1 broker, RF=1, 6 partitions | 3+ brokers across AZs, RF=3, `min.insync.replicas=2`, 48 partitions |
| Rule-based `FraudScorer` | Rules + ML model; model version stamped on every score |
| Authorization inside the Streams app | Its own deployable |
| Non-partitioned `ledger_entries` | Monthly range partitions; `processed_payments` keeps dedup global |
| No auth | mTLS/SASL, topic ACLs, field-level encryption of PII |

## Verify before relying on it

- **Versions:** the parent POM targets Spring Boot `3.5.0`, which manages spring-kafka, kafka-clients, kafka-streams and Flyway, and Docker uses `apache/kafka:3.9.0`. Check that these tags still exist and match your environment. Bump them if needed.
- **Debezium config keys:** check `infra/debezium-outbox-connector.json` against the documentation for your Debezium version.
