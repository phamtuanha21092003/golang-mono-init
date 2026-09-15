# Outbox relay

Polls `outbox_events` in PostgreSQL and publishes each row to **RabbitMQ** or **Redis Streams**. Publish attempts are logged to **PostgreSQL** (`outbox_logs`) and, when configured, **MongoDB** (`outbox_logs`).

Producers never talk to the broker in the request path. They insert an outbox row in the same SQL transaction as the business write. This process is the relay.

```
service TX: business row + outbox_events (pending)
                 |
                 v
            cmd/out-box
                 |
      +----------+-----------+
      |                      |
  RabbitMQ              Redis stream
      |
 outbox_logs (Postgres + Mongo)
```

## Run

Apply migrations first (`000001`, `000002`).

| Variable | Required | Meaning |
|---|---|---|
| `SQL_URI` | yes | Postgres (source of truth for events + SQL logs) |
| `RABBITMQ_URI` | one of these | Publish `broker=rabbitmq` rows |
| `REDIS_URI` | one of these | Publish `broker=redis` rows |
| `MONGO_URI` | no | Also write logs to Mongo |
| `APP_NAME` | recommended | RabbitMQ connection name |
| `OUTBOX_POLL` | no | Tick interval (default `1s`) |
| `OUTBOX_BATCH` | no | Claim size (default `50`) |
| `OUTBOX_MAX_ATTEMPTS` | no | Then mark `failed` (default `5`) |

```bash
export SQL_URI='postgres://user:pass@localhost:5432/db?sslmode=disable'
export RABBITMQ_URI='amqp://guest:guest@localhost:5672/'
export REDIS_URI='redis://localhost:6379'
export MONGO_URI='mongodb://localhost:27017/app'

go run ./cmd/out-box
```

`.env` at the repo root or `environment/.env.out-box` is loaded if present.

## Enqueue from a service

```go
store := outbox.NewStore(db)
logs := outbox.NewLogSinks(db, mongoDatabase) // mongoDatabase may be nil
writer := outbox.NewWriter(store, logs)

_, err := tm.AtomicTx(ctx, func(tx *gorm.DB) (any, error) {
    if err := orderRepo.WithDB(tx).Create(ctx, order); err != nil {
        return nil, err
    }
    ev, err := writer.Enqueue(ctx, tx, outbox.Message{
        EventType:  "order.created",
        Payload:    dto.Envelope{Kind: "order.created", Payload: body},
        Broker:     outbox.BrokerRabbitMQ, // or outbox.BrokerRedis
        Exchange:   "common.exchange",
        RoutingKey: "common.routing-key", // Redis: stream name
    })
    return ev, err
})
```

After commit, `writer.LogEnqueue(ctx, ev)` writes an enqueue log to Postgres and Mongo.

- `BrokerRabbitMQ`: `Publish(exchange, routingKey, payload)` with confirm.
- `BrokerRedis`: `XADD` to stream `routingKey`.

If the consumer has never declared topology, RabbitMQ messages to a missing queue are dropped unless the publisher uses mandatory returns. Start the consumer first, or call `SetupTopology`.

## Status

| Status | Meaning |
|---|---|
| `pending` | Waiting for the relay |
| `publishing` | Claimed this tick (or stale claim after 2 minutes is retried) |
| `published` | Broker accepted the message |
| `failed` | `OUTBOX_MAX_ATTEMPTS` exhausted |

A publish error increments `attempts` and returns the row to `pending` until the cap.

## Logs

Each publish or fail writes `outbox_logs`:

- Postgres table `outbox_logs`
- Mongo collection `outbox_logs` (if `MONGO_URI` is set)

One sink failing does not skip the other.

## Tests

```bash
go test ./internal/domain/outbox/
```
