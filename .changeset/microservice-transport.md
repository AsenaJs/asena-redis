---
'@asenajs/asena-redis': minor
---

### Microservice transport over Redis Streams

`RedisMicroserviceTransport` implements Asena 0.8's `MicroserviceTransport` SPI, so `@MessageController` / `@MessagePattern` / `@EventPattern` and `ulak.send` / `ulak.emit` work over Redis:

- **Request/response** on a stream per pattern (`asena:ms:req:<pattern>`), distributed to exactly one replica by consumer group; replies go back over per-instance pub/sub (`asena:ms:reply:<instanceId>`) since the caller is alive and waiting.
- **Events** on a shared stream (`asena:ms:evt`) with one consumer group per service, wildcard patterns matched locally, at-least-once with explicit ACK.
- **Retry and DLQ:** a background `XPENDING` + `XCLAIM` sweep rescues entries from crashed replicas, counts delivery attempts, and moves poison events to `asena:ms:dlq` after `maxRetries`. RPC errors are final — the caller receives them and the entry is ACKed.
- **Backpressure and draining:** `maxInFlight` bounds concurrent handlers, and `destroy()` drains in-flight work within `drainTimeout` before disconnecting.
- Accepts either a `RedisConfig` or an existing `AsenaRedisService`, in which case it reuses that service's connection instead of opening its own.
- Options: `serviceName` (required — the consumer group identity), `streamPrefix`, `requestTimeout`, `maxRetries`, `claimIdleMs`, `maxStreamLength`, `blockMs`, `count`, `maxInFlight`, `handlerTimeout`, `drainTimeout`, `commandTimeout`.

### Connection resilience

- **Wedged-connection recovery.** Blocking reads and publisher commands are now bounded. A blocking read that overruns its `BLOCK` window, or a publisher command that exceeds `commandTimeout`, is treated as a wedged connection: it is replaced rather than left hanging. This works around Bun's `RedisClient` poisoning its reply queue on disconnect, where in-flight commands never settle.
- **Connection lifecycle events** on the adapter seam: `onConnected` / `onConnectionLost`. `NodeRedisAdapter` maps them to node-redis's `ready` / `reconnecting` / `end` events.

Requires `@asenajs/asena` ≥ 0.8.0 (the `@asenajs/asena/microservice` subpath).