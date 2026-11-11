import 'reflect-metadata';
import { defineMetadata, getOwnMetadata } from 'reflect-metadata/no-conflict';
import {
  CACHEABLE_METADATA_KEY,
  CACHE_EVICT_METADATA_KEY,
  CACHE_PUT_METADATA_KEY,
  type CacheEvictOptions,
  type CacheableOptions,
  type CachePutOptions,
} from '../cache/CacheOptions';

// getOwnMetadata: reading through the chain would mutate the base class's map in place, making a
// subclass appear to declare all of its parent's methods. The post-processor merges at read time.
const cacheMethodDecorator =
  <T>(key: symbol, options: T): MethodDecorator =>
  (target, propertyKey, descriptor) => {
    const owner = (target as { constructor: Function }).constructor;
    const existing = (getOwnMetadata(key, owner) as Map<string, T> | undefined) ?? new Map<string, T>();

    existing.set(String(propertyKey), options);
    defineMetadata(key, existing, owner);

    return descriptor;
  };

/**
 * Serves the method's result from Redis, running the method only on a miss.
 * Without a `@RedisCache` class in the application it records metadata nobody reads.
 */
export function Cacheable(options: CacheableOptions = {}): MethodDecorator {
  return cacheMethodDecorator(CACHEABLE_METADATA_KEY, options);
}

/** Always runs the method and writes its result to the cache. A method that throws writes nothing. */
export function CachePut(options: CachePutOptions = {}): MethodDecorator {
  return cacheMethodDecorator(CACHE_PUT_METADATA_KEY, options);
}

/** Removes cache entries after the method succeeds, or before it runs with `beforeInvocation`. */
export function CacheEvict(options: CacheEvictOptions = {}): MethodDecorator {
  return cacheMethodDecorator(CACHE_EVICT_METADATA_KEY, options);
}

export type { CacheableOptions, CachePutOptions, CacheEvictOptions, CacheKeyGenerator } from '../cache/CacheOptions';
