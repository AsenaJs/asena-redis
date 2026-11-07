import { PostProcessor } from '@asenajs/asena/decorators';
import { defineTypedMetadata } from '@asenajs/asena/utils';
import { REDIS_CACHE_OPTIONS_KEY, type RedisCacheOptions } from '../cache/CacheOptions';

/**
 * Activates caching: apply it to a `CachePostProcessor` subclass in the source folder, because
 * `node_modules` is never scanned. The options are the application-wide defaults.
 */
export function RedisCache(options: RedisCacheOptions = {}) {
  return function <T extends new (...args: any[]) => any>(target: T): T {
    defineTypedMetadata(REDIS_CACHE_OPTIONS_KEY, options, target);

    return PostProcessor()(target) as unknown as T;
  };
}

export type { RedisCacheOptions } from '../cache/CacheOptions';
