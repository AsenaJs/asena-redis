// Core Service
export { AsenaRedisService } from './lib/AsenaRedisService';

// Transport
export { RedisTransport } from './lib/RedisTransport';

// Microservice transport (Redis Streams)
export { RedisMicroserviceTransport } from './lib/microservice';

// Redis Streams helpers
export {
  xadd,
  xgroupCreate,
  xgroupDelConsumer,
  xreadgroup,
  xrange,
  xack,
  xpending,
  xpendingConsumer,
  xinfoConsumers,
  xclaim,
  entryTimestamp,
  normalizeStreamsReply,
  normalizeEntries,
  normalizeFields,
} from './lib/microservice';
export type { StreamEntry, PendingEntry, ConsumerInfo } from './lib/microservice';

// Adapter
export type { RedisClientAdapter } from './lib/adapter';
export { BunRedisAdapter, NodeRedisAdapter, buildRedisUrl } from './lib/adapter';

// Decorators
export { Redis, Cacheable, CachePut, CacheEvict, CacheConfig, RedisCache } from './lib/decorators';
export type { RedisDecoratorOptions } from './lib/decorators';

// Cache
export { CachePostProcessor } from './lib/cache';
export {
  CACHEABLE_METADATA_KEY,
  CACHE_PUT_METADATA_KEY,
  CACHE_EVICT_METADATA_KEY,
  CACHE_CONFIG_KEY,
  REDIS_CACHE_OPTIONS_KEY,
} from './lib/cache';
export type {
  CacheKeyGenerator,
  CacheCommonOptions,
  CacheableOptions,
  CachePutOptions,
  CacheEvictOptions,
  CacheConfigOptions,
  RedisCacheOptions,
} from './lib/cache';

// Types
export type {
  RedisConfig,
  TLSOptions,
  RedisOptions,
  RedisTransportOptions,
  RedisMicroserviceOptions,
} from './lib/types';
