# @asenajs/asena-redis

## 3.0.0

### Major Changes

- `@OnStop` releases the connection, and the core peer moves to `^0.10.0`

  Nothing in the framework ever called `AsenaRedisService.disconnect()`, so every `@Redis` service
  held an open socket past `server.stop()` — and `Website/docs/packages/redis.md` claimed the
  opposite. `server.stop()` now closes the service's connection **and** every connection handed out
  by `createSubscriber()`, which were previously untracked and impossible to close.

  `@PostConstruct` on `onStart()` is now `@OnStart` — the same metadata key, renamed with the core.

  **Breaking:**

  - Requires `@asenajs/asena@^0.10.0`. A 0.9.x application cannot use this version.
  - A client supplied through `@Redis({ client })` is now closed on `server.stop()` as well. It was
    always what `disconnect()` did; nothing called `disconnect()` before. If you share one client
    across two services, the first stop hook closes it for both.

## 2.0.0

### Major Changes

- `@Redis` keeps everything the decorated class inherited

  The decorator replaces the class it decorates with a wrapper. The wrapper extended
  `AsenaRedisService` rather than the target, and the copy loops beside it only walked the target's
  _own_ prototype — so every method, getter and static the class inherited from an intermediate
  base class was dropped, `instanceof` against that base was false, and nothing failed until the
  first call.

  The wrapper now extends the target. The member and metadata copy loops are removed: everything is
  reachable through the prototype chain, and the metadata loop was actively harmful — it flattened
  an inherited `NameKey` onto the wrapper, so a `@Redis` class extending another `@Redis` class
  registered under its parent's name and the container promoted the entry to an array.

  The type parameter is now constrained to `AsenaRedisService`, matching `@Database`. Decorating a
  class that does not extend it used to work by accident.

  `duplicate()` on both client adapters preserved the wrong prototype — `Object.create(BunRedisAdapter.prototype)`
  and `new NodeRedisAdapter(...)` rather than the receiver's own constructor — so a user subclass
  added for instrumentation lost its identity on every duplicate. `duplicate()` is on the pub/sub
  and microservice-transport hot path.

  Requires `@asenajs/asena` 0.9.0 or later: the wrapper registers under the target's own name, and
  older versions reported that as a circular dependency.

- Readiness stops reporting healthy while this instance's replies are being dropped

  `RedisMicroserviceTransport.isConnected` was `connected && publisher.isConnected`. It never looked
  at the reply subscriber, and the reply subscriber is what every `send()` depends on: a responder
  answers with `PUBLISH` on a plain pub/sub channel — no replay — and then ACKs the request entry
  unconditionally, so a reply published while the caller's reply channel has no subscriber is dropped
  by Redis and never redelivered. The caller only ever sees a `TIMEOUT`.

  The publisher and the reply subscriber are separate connections that come back separately, and
  Redis drops every subscription with the socket. Bun's `RedisClient` reconnects on its own but does
  **not** restore subscription state (verified: on a raw client the channel stays at zero subscribers
  forever after one blip), so the adapter replays it — one `UNSUBSCRIBE`+`SUBSCRIBE` round trip
  _after_ the socket already reports open. That round trip is the window, and the publisher was
  usually back before it closed.

  Measured over 60 connection outages against a client-only caller (an HTTP gateway that only
  `send()`s) and a separate responder, with 25 requests issued at the instant readiness read true:
  **77 of 1500 requests were lost, every one of them behind a green endpoint.** In all 60 outages the
  endpoint reported ready while `PUBSUB NUMSUB` on its own reply channel was still 0, for 0.5–1.8 ms.
  The loss is bursty rather than spread out — most outages lost one request, four lost 17 or more of
  the 25, because a whole burst can land inside the window. Across 12 restarts of a real Redis
  container the same instance reported ready-but-unsubscribed in 9 of the 10 rounds where the window
  could be sampled, and in 2 of the 12 restarts readiness never reported 503 at all.

  `isConnected` now additionally requires the reply subscription to be live: the subscriber's socket
  open **and** its `SUBSCRIBE` acknowledged since the last reconnect. The reply subscriber is wired
  into the same connection-event handling the publisher already had, with the same identity guard the
  kafka reply consumer uses, so a superseded connection's late event cannot vouch for the connection
  that replaced it and nothing can report ready after `destroy()`. Adapters gained an optional
  `onResubscribed(channel)` — the moment a replay has actually landed, which `onConnected` does not
  mean. `NodeRedisAdapter` maps it to `ready`, which node-redis emits only after its own resubscribe
  resolves.

  Re-running the same 60 outages after the change: **0 of 1500 lost**, and readiness never went green
  before the reply channel had a subscriber, in 60 of 60. All 12 real container restarts now report
  503 during the outage, including the two that previously went unnoticed. Recovery is not slower:
  readiness went green a mean 151 ms after the link returned both before and after.

  **This changes what a 503 from the health endpoint means.** It no longer means only "the publisher
  socket is down"; it also means "this instance's reply channel is not being served, so a `send()`
  issued now cannot complete". The visible consequence is that an instance stays 503 slightly longer
  after an outage — through the resubscribe, not just the reconnect — and that it now reports 503 for
  outages the publisher-only check missed. **Liveness probes must be more forgiving than readiness
  probes**, or an orchestrator will restart instances that were about to recover on their own.

  A replay that fails on a live connection is now retried and, if it still fails, logged instead of
  swallowed. Previously the channel was left unsubscribed on a healthy socket with nothing to retry
  from and no error anywhere.

  **Known limitation — the reply channel is still lossy, by design for now.** Readiness is honest
  about the window; it does not close it.

  - A request already in flight when the connection drops is still lost. The responder publishes the
    reply during the outage, nothing is subscribed, and the entry is ACKed either way — the caller
    gets a `TIMEOUT` and retrying is its decision. RPC remains at-most-once per attempt.
  - Readiness is driven by connection events, so a health check landing between the socket being
    marked open and the connect event being dispatched can still read green for at most one event-loop
    turn.
  - A custom `RedisClientAdapter` that implements `onConnected` but neither `onResubscribed` nor
    subscription restoration is taken at its word on connect. Both built-in adapters report honestly.

  A durable reply channel — replies on a stream instead of pub/sub — is deliberately deferred past
  0.9.0. Until then, treat `send()` across a Redis outage as retryable rather than reliable.

## 1.1.0

### Minor Changes

- 1e3e9d3: ### Microservice transport over Redis Streams

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

### Patch Changes

- 1e3e9d3: Message and event pattern validation errors now mention the `prefix: false` escape hatch, matching Asena 0.8's uniform `@MessageController` prefix rule. Error message text only — no behavior change.
