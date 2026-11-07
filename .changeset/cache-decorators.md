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
method runs as though there were no cache. Configuration mistakes fail fast at bootstrap: a
decorator on a non-method, `@Cacheable` together with `@CachePut`, a non-positive `ttl`, or
`allEntries` without a prefix.

**One error surfaces only at runtime.** If the application registers more than one `@Redis` service
and a cache decorator does not name one, the error is thrown on the first cached call, not at boot,
because a post-processor sees the component graph while it is still being built. Name the service
explicitly whenever there is more than one.

**Limitations:**

- A cached value is whatever survives `JSON.parse(JSON.stringify(x))`: a `Date` returns as a string,
  a `Map` as `{}`. A value that cannot be serialized at all is returned to the caller with the write
  skipped and a warning.
- Wrapped methods always return a Promise.
- Arrow-function class properties are not wrapped.
- `allEntries` uses `SCAN` (non-blocking, not a snapshot) and clears every key under the prefix,
  including keys written by other classes sharing it.
- No stampede protection: concurrent misses on the same key all run the method.

Writes are one atomic `SET key value EX ttl`. `RedisClientAdapter` is unchanged and the
`@asenajs/asena` peer range is unchanged.
