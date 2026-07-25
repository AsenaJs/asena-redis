import type { ServerLogger } from '@asenajs/asena/logger';
import type { RedisClientAdapter } from '../adapter';

export interface RedisConfig {
  url?: string;
  host?: string;
  port?: number;
  username?: string;
  password?: string;
  db?: number;
  connectionTimeout?: number;
  idleTimeout?: number;
  autoReconnect?: boolean;
  maxRetries?: number;
  enableOfflineQueue?: boolean;
  enableAutoPipelining?: boolean;
  tls?: boolean | TLSOptions;
  name?: string;
}

export interface TLSOptions {
  rejectUnauthorized?: boolean;
  ca?: string;
  cert?: string;
  key?: string;
}

export interface RedisOptions {
  config: RedisConfig;
  adapter?: 'bun' | 'node-redis';
  client?: RedisClientAdapter;
  logger?: ServerLogger;
}

export interface RedisDecoratorOptions extends RedisOptions {
  name?: string;
}

export interface RedisTransportOptions {
  channel?: string;
}

/**
 * Options for RedisMicroserviceTransport (Redis Streams based messaging)
 */
export interface RedisMicroserviceOptions {
  /**
   * REQUIRED - consumer group identity. All replicas of the same service must
   * share this name; different services must use different names (each service
   * group receives its own copy of every event).
   */
  serviceName: string;

  /**
   * Stream/channel key prefix
   * @default 'asena:ms'
   */
  streamPrefix?: string;

  /**
   * Default reply timeout for send() in milliseconds
   * @default 30000
   */
  requestTimeout?: number;

  /**
   * Max delivery attempts for EVENT handlers before the entry moves to the DLQ.
   * RPC handlers are never retried (errors are final).
   * @default 3
   */
  maxRetries?: number;

  /**
   * Min idle time (ms) before the sweep reclaims a pending entry from a
   * crashed/stalled replica. Handler duration MUST stay below this value.
   * @default 60000
   */
  claimIdleMs?: number;

  /**
   * Approximate stream trim length (XADD MAXLEN ~). Bounds memory; messages
   * beyond the trim window are lost for services that stay offline too long.
   * @default 100000
   */
  maxStreamLength?: number;

  /**
   * XREADGROUP BLOCK duration in milliseconds
   * @default 5000
   */
  blockMs?: number;

  /**
   * XREADGROUP COUNT - max entries fetched per read
   * @default 16
   */
  count?: number;

  /**
   * Max concurrently running handlers (backpressure limit)
   * @default 32
   */
  maxInFlight?: number;

  /**
   * Per-handler execution timeout in milliseconds. Must not exceed claimIdleMs -
   * a handler outliving claimIdleMs is redelivered to another replica while
   * still running (duplicate processing), so explicit values above claimIdleMs
   * throw at construction. Note: on timeout the dispatch is rejected but the
   * handler itself keeps running (no cancellation).
   * @default min(30000, claimIdleMs)
   */
  handlerTimeout?: number;

  /**
   * Default graceful drain timeout for destroy() in milliseconds
   * @default 10000
   */
  drainTimeout?: number;

  /**
   * Watchdog timeout (ms) for non-blocking publisher commands (XADD, XACK,
   * XCLAIM, ...). A command outliving this bound means the connection died
   * with the command in flight - Bun's RedisClient then neither settles the
   * promise nor realigns its reply queue after reconnecting, so the
   * connection is discarded and replaced. Must comfortably exceed normal
   * command latency; raise it only for extremely slow networks.
   * @default 10000
   */
  commandTimeout?: number;
}
