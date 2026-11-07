/** Builds the cache key from the wrapped method's arguments. */
export type CacheKeyGenerator = (...args: any[]) => string;

export interface CacheCommonOptions {
  /**
   * Name of the `AsenaRedisService` component to use. Omit it when exactly one is registered;
   * with zero or several, the FIRST CACHE CALL throws, because no post-registration hook exists.
   */
  redis?: string;

  /** Key namespace, joined with `:`. Also the scope of `@CacheEvict({ allEntries: true })`. */
  prefix?: string;

  /** Time to live in SECONDS. Omitted means the entry never expires. */
  ttl?: number;
}

export interface CacheableOptions extends CacheCommonOptions {
  /** Omitted means `${Class.name}.${method}:${JSON.stringify(args)}`. */
  key?: CacheKeyGenerator;

  /** Store `null` / `undefined` results. Default `true`. */
  cacheNull?: boolean;
}

export interface CachePutOptions extends CacheCommonOptions {
  key?: CacheKeyGenerator;
  cacheNull?: boolean;
}

export interface CacheEvictOptions extends Omit<CacheCommonOptions, 'ttl'> {
  key?: CacheKeyGenerator;

  /** Delete every key under the resolved prefix instead of one key. Requires a prefix. */
  allEntries?: boolean;

  /** Evict before the method runs instead of after it succeeds. Default `false`. */
  beforeInvocation?: boolean;
}

export type CacheConfigOptions = CacheCommonOptions;

export type RedisCacheOptions = CacheCommonOptions;

// Symbol.for: with two copies of this module (a linked checkout next to an installed one), a
// unique symbol would make the post-processor read nothing and silently never cache.
export const CACHEABLE_METADATA_KEY = Symbol.for('asena:redis:cache:cacheable');

export const CACHE_PUT_METADATA_KEY = Symbol.for('asena:redis:cache:put');

export const CACHE_EVICT_METADATA_KEY = Symbol.for('asena:redis:cache:evict');

export const CACHE_CONFIG_KEY = Symbol.for('asena:redis:cache:config');

export const REDIS_CACHE_OPTIONS_KEY = Symbol.for('asena:redis:cache:options');
