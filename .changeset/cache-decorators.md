---
'@asenajs/asena-redis': minor
---

Declarative caching: `@Cacheable`, `@CachePut`, `@CacheEvict`, `@CacheConfig`

Activation follows the `@Drizzle` pattern, because AsenaJS never scans `node_modules`:

```typescript
// src/config/AppCache.ts
@RedisCache({ ttl: 300 })
export class AppCache extends CachePostProcessor {}
```

```typescript
@Service()
@CacheConfig({ prefix: 'user' })
export class UserService {
  @Cacheable({ key: (id: string) => id })
  async findById(id: string) { ... }

  @CacheEvict({ key: (id: string) => id })
  async update(id: string, dto: Dto) { ... }

  @CacheEvict({ allEntries: true })
  async purge() { ... }
}
```

Options resolve per field, nearest level first: method > `@CacheConfig` > `@RedisCache`.

**Failure policy.** Runtime Redis failures fail open: a warning on the framework logger and the
call carries on as though there were no cache. A failed read runs the method and skips the write
for that call; a failed write or eviction still returns the method's result.

Each Redis command the cache sends has a time limit, set with the `timeout` option in
milliseconds (default `500`): every `GET`, `SET` and `DEL`, and each `SCAN` round and delete
batch of an `allEntries` eviction. A command past the limit counts as failed, so the call fails
open even when the client queues commands while Redis is down.

**Configuration mistakes fail at bootstrap:** a decorator on something that is not a function,
`@Cacheable` together with `@CachePut`, a `ttl` that is not a positive whole number of seconds
up to `Number.MAX_SAFE_INTEGER` (Redis refuses an `EX` not far above it), a `timeout` that is
not a positive number of milliseconds up to `2147483647` (the largest delay a timer holds), and
`allEntries` without a prefix.

**Wiring mistakes surface on the first call, not at boot**, because a post-processor sees the
component graph while it is still being built:

- no `@Redis` service is registered;
- more than one is registered and the decorator does not name one;
- the named service does not exist;
- the Redis service was replaced by something that is not an `AsenaRedisService`, such as a plain
  test double registered through `overrides`.

The error is thrown before the method for `@Cacheable` and for `@CacheEvict` with
`beforeInvocation`, and after the method has run for `@CachePut` and `@CacheEvict`. Name the
service explicitly whenever there is more than one.

**Limitations:**

- A cached value is whatever survives `JSON.parse(JSON.stringify(x))`: a `Date` returns as a string,
  a `Map` as `{}`. A value that cannot be serialized at all is returned to the caller with the write
  skipped and a warning.
- Wrapped methods always return a Promise.
- The decorators are typed for methods, so TypeScript rejects them on an arrow-function property.
  Applied to one anyway (plain JavaScript, or Bun running without a type check), the property is
  wrapped and cached like a method and keeps its `this`.
- A command that times out is not cancelled. With the client's offline queue on, it is sent when
  the connection returns: a late `SET` stores the value computed during the outage with a fresh
  TTL, and can overwrite a newer entry written after the reconnect, for example by another
  instance of the application.
- `allEntries` uses `SCAN` (non-blocking, not a snapshot) and clears every key under the prefix,
  including keys written by other classes sharing it.
- No stampede protection: concurrent misses on the same key all run the method.

Writes are one atomic `SET key value EX ttl`. `RedisClientAdapter` is unchanged and the
`@asenajs/asena` peer range is unchanged.
