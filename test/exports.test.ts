import { describe, expect, it } from 'bun:test';
import * as pkg from '../index';

describe('package export surface', () => {
  it('should export the stream helpers as functions', () => {
    const names = [
      'xadd',
      'xgroupCreate',
      'xgroupDelConsumer',
      'xreadgroup',
      'xrange',
      'xack',
      'xpending',
      'xpendingConsumer',
      'xinfoConsumers',
      'xclaim',
      'entryTimestamp',
      'normalizeStreamsReply',
      'normalizeEntries',
      'normalizeFields',
    ];

    for (const name of names) {
      expect(typeof (pkg as Record<string, unknown>)[name]).toBe('function');
    }
  });

  it('should export the cache decorators and the post-processor', () => {
    const names = ['Cacheable', 'CachePut', 'CacheEvict', 'CacheConfig', 'RedisCache', 'CachePostProcessor'];

    for (const name of names) {
      expect(typeof (pkg as Record<string, unknown>)[name]).toBe('function');
    }
  });

  it('should export the cache metadata keys', () => {
    const names = [
      'CACHEABLE_METADATA_KEY',
      'CACHE_PUT_METADATA_KEY',
      'CACHE_EVICT_METADATA_KEY',
      'CACHE_CONFIG_KEY',
      'REDIS_CACHE_OPTIONS_KEY',
    ];

    for (const name of names) {
      expect(typeof (pkg as Record<string, unknown>)[name]).toBe('symbol');
    }
  });
});
