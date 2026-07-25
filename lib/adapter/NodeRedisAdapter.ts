import { createClient } from 'redis';
import type { RedisClientAdapter } from './RedisClientAdapter';
import type { TLSOptions } from '../types';

interface NodeRedisMappedOptions {
  socket?: Record<string, any>;
  disableOfflineQueue?: boolean;
  [key: string]: any;
}

/**
 * Adapter for the official `redis` (node-redis) package.
 *
 * Normalizes the node-redis API to match the RedisClientAdapter interface:
 * - camelCase method names (hGet → hget, sAdd → sadd, etc.)
 * - `exists` returns boolean (node-redis returns number)
 * - `sendCommand([cmd, ...args])` → `send(cmd, args)`
 * - `isReady` → `isConnected`
 *
 * Requires `redis` peer dependency: `bun add redis`
 */
export class NodeRedisAdapter implements RedisClientAdapter {
  private client: any;

  public constructor(client: any) {
    this.client = client;
  }

  /**
   * Factory method: creates a node-redis client and wraps it.
   */
  // eslint-disable-next-line @typescript-eslint/member-ordering
  public static create(url: string, opts?: Record<string, any>): NodeRedisAdapter {
    const mapped = opts ? NodeRedisAdapter.mapOptions(opts) : {};
    const client = createClient({ url, ...mapped });

    return new NodeRedisAdapter(client);
  }

  /**
   * Maps RedisConfig fields to node-redis createClient options.
   *
   * Bun's RedisClient accepts these fields directly, but node-redis uses
   * a different option structure (e.g. `socket.connectTimeout` instead of `connectionTimeout`).
   *
   * Fields with no node-redis equivalent (`enableAutoPipelining`) are silently ignored.
   */
  // eslint-disable-next-line @typescript-eslint/member-ordering
  private static mapOptions(opts: Record<string, any>): NodeRedisMappedOptions {
    const {
      connectionTimeout,
      idleTimeout,
      autoReconnect,
      maxRetries,
      enableOfflineQueue,
      enableAutoPipelining,
      tls,
      ...rest
    } = opts;

    const socket: Record<string, any> = {};
    const mapped: NodeRedisMappedOptions = { ...rest };

    if (connectionTimeout !== undefined) {
      socket['connectTimeout'] = connectionTimeout;
    }

    if (idleTimeout !== undefined) {
      socket['socketTimeout'] = idleTimeout;
    }

    if (autoReconnect === false) {
      socket['reconnectStrategy'] = false;
    } else if (maxRetries !== undefined) {
      socket['reconnectStrategy'] = (retries: number) => {
        if (retries >= maxRetries) return false;

        return Math.min(retries * 100, 3000);
      };
    }

    if (enableOfflineQueue !== undefined) {
      mapped['disableOfflineQueue'] = !enableOfflineQueue;
    }

    if (tls === true) {
      socket['tls'] = true;
    } else if (tls && typeof tls === 'object') {
      socket['tls'] = true;

      const tlsOpts = tls as TLSOptions;

      if (tlsOpts.rejectUnauthorized !== undefined) socket['rejectUnauthorized'] = tlsOpts.rejectUnauthorized;

      if (tlsOpts.ca) socket['ca'] = tlsOpts.ca;

      if (tlsOpts.cert) socket['cert'] = tlsOpts.cert;

      if (tlsOpts.key) socket['key'] = tlsOpts.key;
    }

    if (Object.keys(socket).length > 0) {
      mapped['socket'] = socket;
    }

    return mapped;
  }

  // Lifecycle

  public get isConnected(): boolean {
    return this.client.isReady ?? false;
  }

  public async connect(): Promise<void> {
    await this.client.connect();
  }

  public async disconnect(): Promise<void> {
    await this.client.disconnect();
  }

  public async duplicate(): Promise<RedisClientAdapter> {
    const dup = this.client.duplicate();

    await dup.connect();

    return new NodeRedisAdapter(dup);
  }

  // Connection events - node-redis is an EventEmitter, so these map directly.
  // Note node-redis rejects in-flight commands on disconnect (no reply-queue
  // poisoning like Bun's client), so listeners here rarely have work to do.

  public onConnected(listener: () => void): void {
    this.client.on?.('ready', listener);
  }

  public onConnectionLost(listener: () => void): void {
    this.client.on?.('reconnecting', listener);
    this.client.on?.('end', listener);
  }

  // String operations

  public async get(key: string): Promise<string | null> {
    const result = await this.client.get(key);

    return typeof result === 'string' ? result : null;
  }

  public async set(key: string, value: string): Promise<void> {
    await this.client.set(key, value);
  }

  public async del(key: string): Promise<number> {
    return this.client.del(key);
  }

  public async exists(key: string): Promise<boolean> {
    const result = await this.client.exists(key);

    return result > 0;
  }

  public async incr(key: string): Promise<number> {
    return this.client.incr(key);
  }

  public async decr(key: string): Promise<number> {
    return this.client.decr(key);
  }

  // Expiration

  public async expire(key: string, seconds: number): Promise<number> {
    const result = await this.client.expire(key, seconds);

    return result ? 1 : 0;
  }

  public async ttl(key: string): Promise<number> {
    return this.client.ttl(key);
  }

  public async keys(pattern: string): Promise<string[]> {
    return this.client.keys(pattern);
  }

  // Hash operations — node-redis uses camelCase (hGet, hSet, hmGet)

  public async hget(key: string, field: string): Promise<string | null> {
    const result = await this.client.hGet(key, field);

    return typeof result === 'string' ? result : null;
  }

  public async hmset(key: string, fields: string[]): Promise<void> {
    const obj: Record<string, string> = {};

    for (let i = 0; i < fields.length; i += 2) {
      obj[fields[i]] = fields[i + 1];
    }

    await this.client.hSet(key, obj);
  }

  public async hmget(key: string, fields: string[]): Promise<(string | null)[]> {
    const results = await this.client.hmGet(key, fields);

    return results.map((r: any) => (typeof r === 'string' ? r : null));
  }

  // Set operations — node-redis uses camelCase (sAdd, sRem, sMembers, sIsMember)

  public async sadd(key: string, member: string): Promise<number> {
    return this.client.sAdd(key, member);
  }

  public async srem(key: string, member: string): Promise<number> {
    return this.client.sRem(key, member);
  }

  public async smembers(key: string): Promise<string[]> {
    return this.client.sMembers(key);
  }

  public async sismember(key: string, member: string): Promise<boolean> {
    return Boolean(await this.client.sIsMember(key, member));
  }

  // Raw command — node-redis uses sendCommand([cmd, ...args])

  public async send(command: string, args: string[]): Promise<any> {
    return this.client.sendCommand([command, ...args]);
  }

  // Pub/Sub

  public async publish(channel: string, message: string): Promise<number> {
    return this.client.publish(channel, message);
  }

  public async subscribe(channel: string, listener: (message: string) => void): Promise<void> {
    await this.client.subscribe(channel, listener);
  }

  public async unsubscribe(channel: string): Promise<void> {
    await this.client.unsubscribe(channel);
  }
}
