# customer-service

Customer domain microservice, written in Go. It owns customer accounts, publishes
its state changes as Avro events, and reacts to the order domain's events.

It is designed to run inside the
[`eda_microservices`](https://github.com/MagicRodri/eda_microservices) monorepo,
which supplies Kafka, Schema Registry, Kafka Connect and the connector
configuration. Nothing here talks to Kafka to *produce*: publication is the
outbox connector's job.

## The two event streams

This service is on both sides of both streams, and they are not
interchangeable.

**Technical events** are raw change-data-capture rows for the `customers`
table, published by a Debezium Postgres connector to
`tech.customer.public.customers`. Their shape is the physical table: a column
rename changes the event. They are consumed here only to fill
`technical_audit_log`. No business decision may depend on them.

**Business events** are explicit, versioned facts — `CustomerCreated`,
`CustomerBlocked`, `CustomerUnblocked`, `CustomerTierChanged` — whose contracts
live in [`schemas/`](schemas). They are written to the `outbox` table and routed
by the Debezium outbox event router to `business.customer.events`. This is the
only surface other services are allowed to couple to.

## Why the outbox

Writing to Postgres and publishing to Kafka are two systems, so doing both
directly means one can succeed while the other fails, and the event stream
starts lying about the database. The outbox removes the second system from the
write path: the state change and the event describing it are inserted in the
same transaction, so they commit or roll back together.

```go
err := a.store.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
    if err := tx.InsertCustomer(ctx, customer); err != nil {
        return err
    }
    return tx.AppendOutbox(ctx, store.OutboxRecord{ /* CustomerCreated */ })
})
```

Publication then happens out-of-band: the connector tails the WAL, so an event
that committed will eventually reach Kafka even if this process dies the
instant after `COMMIT`.

That gives at-least-once delivery, not exactly-once. Consumers deduplicate: each
payload carries an `event_id`, and handlers insert it into `processed_events`
inside the very transaction that applies the effect, so a redelivery is a no-op.

## The event loop with order-service

```
POST /customers ──► customers + outbox (one tx)
                          │
                          ▼  Debezium outbox router
                    business.customer.events
                          │
                          ▼
                    order-service updates its local customer view
                    (blocked customers rejected, tier drives discount)
                          │
                     POST /orders
                          ▼
                    business.order.events   ── OrderCreated / OrderCancelled
                          │
                          ▼
   this service: lifetime_spend_cents ± total_cents
                 crossing a threshold ⇒ CustomerTierChanged (same tx)
                          │
                          └──► back to business.customer.events
```

`OrderCancelled` reverses the spend the order once contributed, which can demote
a customer back down a tier. Lifetime spend is clamped at zero.

Tier thresholds are in `internal/domain/customer.go`: GOLD at 50 000 cents,
PLATINUM at 200 000, granting 5% and 10% discounts respectively.

## API

| Method | Path                        | Effect                                   |
| ------ | --------------------------- | ---------------------------------------- |
| POST   | `/customers`                | Create; emits `CustomerCreated`          |
| GET    | `/customers`                | List (`?limit=`, default 50, max 200)    |
| GET    | `/customers/{id}`           | Fetch one                                |
| POST   | `/customers/{id}/block`     | Emits `CustomerBlocked` (`{"reason":""}`)|
| POST   | `/customers/{id}/unblock`   | Emits `CustomerUnblocked`                |
| GET    | `/healthz`                  | Liveness                                 |

```bash
curl -X POST localhost:8091/customers \
  -H 'content-type: application/json' \
  -d '{"email":"ada@example.com","name":"Ada Lovelace"}'
```

## Configuration

| Variable              | Default                         |
| --------------------- | ------------------------------- |
| `HTTP_ADDR`           | `:8080`                         |
| `DATABASE_URL`        | *(required)*                    |
| `KAFKA_BROKERS`       | `localhost:9092`                |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081`         |
| `CONSUMER_GROUP`      | `customer-service`              |
| `BUSINESS_TOPIC`      | `business.order.events`         |
| `TECHNICAL_TOPIC`     | `tech.customer.public.customers`|
| `LOG_LEVEL`           | `info` (`debug` for verbose)    |

Migrations in [`migrations/`](migrations) are embedded in the binary and applied
at startup.

## Consuming Avro

Messages are decoded with the **writer's** schema, fetched from the registry by
the ID in the Confluent wire header (`internal/eventing/decoder.go`). Decoding
with the writer's schema rather than a compiled-in one is what lets a producer
add a field without breaking this consumer: the unknown field lands in the map
and is ignored.

## Development

```bash
make test    # unit tests, including a parse check of every published .avsc
make vet
make build
```

Requires Go 1.26+. Running the full pipeline requires the monorepo's
`docker compose up`.
