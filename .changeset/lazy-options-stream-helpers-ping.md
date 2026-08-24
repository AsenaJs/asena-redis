---
"@asenajs/asena-redis": minor
---

The `@Redis` decorator now accepts a thunk — `@Redis(() => ({ config: { url: process.env.REDIS_URL } }))` — so options are resolved when the service is constructed instead of when the class is defined. A service in a shared package can now read environment-dependent values at runtime. The thunk form registers the service under the decorated class's own name; use the object form's `name` field to choose the registration key explicitly. The thunk's return value is what the service stores — nothing outside it is mutated.

The Redis Streams helpers used by the microservice transport are now exported from the package root: `xadd`, `xgroupCreate`, `xgroupDelConsumer`, `xreadgroup`, `xrange`, `xack`, `xpending`, `xpendingConsumer`, `xinfoConsumers`, `xclaim`, `entryTimestamp`, plus the reply normalizers `normalizeStreamsReply`, `normalizeEntries`, `normalizeFields` and the types `StreamEntry`, `PendingEntry`, `ConsumerInfo`. New `xrange(client, key, start = '-', end = '+', count?)` reads a stream range and returns normalized `StreamEntry[]` — consumers no longer need to rewrite the RESP2/RESP3 normalisation themselves.

`AsenaRedisService` gains `ping(timeoutMs = 1000)`, which resolves with `'PONG'` or rejects with `Redis PING timed out after <n>ms`. This matters with the offline queue enabled: against a down Redis a command waits in the queue instead of failing, so an unbounded readiness probe hangs forever. `testConnection()` now goes through `ping()` — behaviour change: it still returns `false` immediately when disconnected, but a probe that never gets a reply now returns `false` after the timeout instead of hanging.
