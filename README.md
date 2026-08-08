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

**Technical events** are raw change-data-capture rows, one topic per table,
named `tech.customer.<schema>.<table>` — derived by the connector rather than
listed, so a new table needs no configuration change. Their shape is the
physical table: a column rename changes the event. They are consumed here only
to fill `technical_audit_log`. No business decision may depend on them.

**Business events** are explicit, versioned facts whose contracts live in
[`schemas/`](schemas). They are written to the `outbox` table and routed by the
Debezium outbox event router onto a topic chosen per event type. This is the
only surface other services are allowed to couple to.

| Event                 | Channel              | Topic                                |
| --------------------- | -------------------- | ------------------------------------ |
| `CustomerCreated`     | `customer.lifecycle` | `business.customer.lifecycle.events` |
| `CustomerBlocked`     | `customer.lifecycle` | `business.customer.lifecycle.events` |
| `CustomerUnblocked`   | `customer.lifecycle` | `business.customer.lifecycle.events` |
| `CustomerTierChanged` | `customer.loyalty`   | `business.customer.loyalty.events`   |

## Splitting events across topics

Each outbox row carries a `channel`, and the connector rewrites the topic to
`business.<channel>.events`. It routes on that column alone and never learns an
event type, so the split lives here, in `internal/app/events.go`:

```go
func channelFor(eventType string) string {
    switch eventType {
    case "CustomerCreated", "CustomerBlocked", "CustomerUnblocked":
        return channelCustomerLifecycle
    case "CustomerTierChanged":
        return channelCustomerLoyalty
    default:
        return aggregateTypeCustomer   // never empty
    }
}
```

Giving an event its own topic is a change to that function plus a redeploy — no
connector edit. `AppendOutbox` falls back to the aggregate type when `Channel`
is empty, so the simple case stays one topic per domain.

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
| `BUSINESS_TOPIC_PATTERN`  | `^business\.order\..*`     |
| `BUSINESS_TOPICS`     | *(unset — overrides the pattern)* |
| `TECHNICAL_TOPIC_PATTERN` | `^tech\.customer\..*`      |
| `TECHNICAL_TOPICS`    | *(unset — overrides the pattern)* |
| `LOG_LEVEL`           | `info` (`debug` for verbose)    |

### Subscribing by pattern

Topic names are derived — one per captured table, one per outbox channel — so
this service subscribes to a *family* rather than to names. The client
re-resolves the pattern on every metadata refresh, so a topic created later is
picked up without a restart.

Set `BUSINESS_TOPICS` or `TECHNICAL_TOPICS` to a comma-separated list to pin an
exact set instead; an explicit list always wins over the pattern.

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
