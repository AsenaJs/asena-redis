import 'reflect-metadata';
import { defineTypedMetadata } from '@asenajs/asena/utils';
import { CACHE_CONFIG_KEY, type CacheConfigOptions } from '../cache/CacheOptions';

/**
 * Class-level defaults for every cache decorator in the class. Each field resolves on its own,
 * nearest level first: method option > `@CacheConfig` > `@RedisCache`.
 */
export function CacheConfig(options: CacheConfigOptions = {}): ClassDecorator {
  return (target) => {
    defineTypedMetadata(CACHE_CONFIG_KEY, options, target);
  };
}

export type { CacheConfigOptions } from '../cache/CacheOptions';
