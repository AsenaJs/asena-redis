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