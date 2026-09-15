# Internal consumer

RabbitMQ worker (no HTTP). The consumer **declares** the exchange, binding, queue, retry queue, and DLX/DLQ. Producers (API or other jobs) **only publish**.

```
Producer  --publish-->  exchange + routing key
                              |
                              v
                         queue  -->  handler  -->  service
                              |
                    fail --> queue.retry (TTL) --> back to queue
                    max retries --> queue.dlq
```

## Run

Environment variables (a `.env` file at the repo root is loaded by `godotenv`):

| Variable | Required | Meaning |
|---|---|---|
| `APP_NAME` | recommended | Prefix for the consumer tag (so replicas do not share a tag) |
| `APP_DEBUG` | no | `true` enables debug logs |
| `SQL_URI` | yes | Postgres; boot fails if missing |
| `RABBITMQ_URI` | yes | e.g. `amqp://guest:guest@localhost:5672/` |

Queue config: [`setting/internal-consumer.toml`](../../setting/internal-consumer.toml). In a container, also: `/app/settings/internal-consumer.toml`.

```bash
export APP_NAME=internal-consumer
export SQL_URI='postgres://user:pass@localhost:5432/db?sslmode=disable'
export RABBITMQ_URI='amqp://guest:guest@localhost:5672/'

go run ./cmd/internal-consumer
```

Stop with `Ctrl+C` / SIGTERM. The process cancels consume, waits up to 15s for in-flight work, then closes the connection.

After startup, the broker has (for queue `common.queue`):

- `common.exchange` (direct) bound with `common.routing-key` → `common.queue`
- `common.queue` (durable; `x-max-priority` if config > 0)
- `common.queue.retry` (per-message TTL → dead-letters back to `common.queue`)
- `common.queue.dlx` + `common.queue.dlq`

## Queue config

```toml
[[queues]]
name = "common.queue"
exchange = "common.exchange"
binding_key = "common.routing-key"
exchange_kind = "direct"   # default: direct
handler = "common"         # registry key, required
max_priority = 10          # 0 = do not set x-max-priority
```

- `name` and `handler` are required.
- Empty `binding_key` defaults to `name`.
- Empty `exchange` skips exchange declare; publish via the default exchange using the queue name.
- Do not set the AMQP consumer tag in TOML. The adapter assigns a unique tag (`APP_NAME-queue-id`) so replicas can scale.

To add a queue: add a `[[queues]]` block and register a handler (below). An unknown handler **fails at boot**.

## Code flow

| Layer | Path | Role |
|---|---|---|
| Boot | `cmd/internal-consumer/main.go` | env, logger, SQL, RabbitMQ, `StartConsume`, shutdown |
| Config | `internal/config` | TOML `[[queues]]`, `SQL_URI`, `RABBITMQ_URI` |
| Registry | `internal/consumer/registry.go` | TOML `handler` → factory |
| Handler | `internal/consumer/*.go` | Decode envelope, log, call service. `nil` = ack, `error` = retry/DLQ |
| Service | `internal/service` | Business logic (currently a stub) |
| Adapter | `internal/transport/rabbitmq` | Topology, consume, confirms, TTL retry |

Keep handlers **thin**. Put logic in service/repo; do not put SQL in `common.go`.

## Message envelope

One queue can carry multiple work types:

```json
{
  "kind": "order.created",
  "payload": { "id": 1 }
}
```

Struct: `internal/dto.Envelope`. If JSON does not parse, `kind` is empty and the raw body is still passed to the service.

The `"common"` handler currently logs, then calls `CommonService.Handle` (no-op, always acks).

## Retry / DLQ

The adapter (not the handler) decides:

1. Handler returns `nil` → ack.
2. Handler returns `error` and `x-retry-count` < `MaxRetry` (default 5) → publish to `{queue}.retry` with exponential TTL (5s … cap 2 minutes), then ack the original.
3. Retries exhausted → Nack with `requeue=false` → DLQ.
4. Retry publish fails → Nack with requeue, without burning a retry.

Total attempts = 1 original + `MaxRetry` (retry count 0…MaxRetry).

Shutdown does not abort an in-flight handler: the handler context is detached from the signal (`WithoutCancel`). `HandlerTimeout` can be set on `ConsumeOptions` later if needed.

## Add a queue

1. TOML:

```toml
[[queues]]
name = "jobs.queue"
exchange = "jobs.exchange"
binding_key = "jobs"
handler = "jobs"
```

2. Handler, e.g. `internal/consumer/jobs.go`:

```go
func newJobsHandler(deps Deps) rabbitmq.Handler {
    return func(ctx context.Context, msg rabbitmq.Message) error {
        // decode, call service; return error to retry
        return nil
    }
}
```

3. Registry:

```go
var handlerFactories = map[string]factory{
    "common": newCommonHandler,
    "jobs":   newJobsHandler,
}
```

4. If you need logic: add an interface/impl in `internal/service` and inject it via `consumer.Deps`.

Do not change `internal/transport/rabbitmq` unless you are changing broker behavior.

## Producer (another service)

`Publish` **does not** declare topology. The consumer must have run at least once (or the producer must call `SetupTopology` with the same options). If the queue does not exist and `Mandatory=false`, the message is **silently dropped**.

```go
mq, err := rabbitmq.NewRabbitMQAdapter(log, uri, "api")
// ...
err = mq.Publish(ctx, "common.exchange", "common.routing-key", dto.Envelope{
    Kind:    "order.created",
    Payload: payloadJSON,
}, rabbitmq.PublishOptions{Priority: 5})
```

- `exchange == ""` → routing key is the **queue name**.
- Publish waits for a **publisher confirm**.
- `Mandatory: true` → unroutable messages return `ErrUnroutable`.

Do not mix classic + `max_priority` with quorum on the same queue. Quorum: `rabbitmq.NewQuorumConsumeOptions` (not wired in TOML yet).

## Tests

```bash
go test ./internal/config/ ./internal/consumer/ ./internal/transport/rabbitmq/
```

- Config: parse multiple `[[queues]]`; missing `name`/`handler`.
- Registry: unknown handler fails; `"common"` calls the service.
- Adapter: retry delay, payload marshal (no broker required).
