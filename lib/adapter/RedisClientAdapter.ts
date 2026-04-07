import type { RedisConfig } from '../types';

/**
 * Adapter interface for Redis clients.
 * Normalizes the API across different Redis implementations (Bun native, node-redis, custom).
 */
export interface RedisClientAdapter {
  // Lifecycle
  readonly isConnected: boolean;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  duplicate(): Promise<RedisClientAdapter>;

  // String operations
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<void>;
  del(key: string): Promise<number>;
  exists(key: string): Promise<boolean>;
  incr(key: string): Promise<number>;
  decr(key: string): Promise<number>;

  // Expiration
  expire(key: string, seconds: number): Promise<number>;
  ttl(key: string): Promise<number>;
  keys(pattern: string): Promise<string[]>;

  // Hash operations
  hget(key: string, field: string): Promise<string | null>;
  hmset(key: string, fields: string[]): Promise<void>;
  hmget(key: string, fields: string[]): Promise<(string | null)[]>;

  // Set operations
  sadd(key: string, member: string): Promise<number>;
  srem(key: string, member: string): Promise<number>;
  smembers(key: string): Promise<string[]>;
  sismember(key: string, member: string): Promise<boolean>;

  // Raw command
  send(command: string, args: string[]): Promise<any>;

  // Pub/Sub
  publish(channel: string, message: string): Promise<number>;
  subscribe(channel: string, listener: (message: string) => void): Promise<void>;
  unsubscribe(channel: string): Promise<void>;
}

/**
 * Builds a Redis URL from config fields.
 *
 * Supports multiple scenarios:
 * - `url` provided → use as-is (or inject password if missing from URL)
 * - `host`/`port` provided → construct URL with optional auth and db
 * - Neither → defaults to `redis://localhost:6379`
 */
export function buildRedisUrl(config: RedisConfig): string {
  if (config.url) {
    if (config.password && !config.url.includes('@')) {
      const url = new URL(config.url);

      if (config.username) {
        url.username = config.username;
      }

      url.password = config.password;

      return url.toString();
    }

    return config.url;
  }

  const host = config.host || 'localhost';
  const port = config.port || 6379;
  const protocol = config.tls ? 'rediss' : 'redis';

  let auth = '';

  if (config.password) {
    auth = config.username ? `${config.username}:${config.password}@` : `:${config.password}@`;
  }

  const db = config.db !== undefined ? `/${config.db}` : '';

  return `${protocol}://${auth}${host}:${port}${db}`;
}
