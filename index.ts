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
export { Redis } from './lib/decorators';
export type { RedisDecoratorOptions } from './lib/decorators';

// Types
export type {
  RedisConfig,
  TLSOptions,
  RedisOptions,
  RedisTransportOptions,
  RedisMicroserviceOptions,
} from './lib/types';
