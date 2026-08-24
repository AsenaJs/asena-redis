import { OnStart, OnStop } from '@asenajs/asena/decorators/ioc';
import type { RedisClientAdapter } from './adapter';
import { BunRedisAdapter } from './adapter';
import { NodeRedisAdapter } from './adapter';
import { buildRedisUrl } from './adapter';
import type { RedisConfig, RedisOptions } from './types';

export abstract class AsenaRedisService {
  protected _client: RedisClientAdapter | null = null;

  protected options: RedisOptions | null = null;

  /**
   * Every connection handed out by {@link createSubscriber}.
   *
   * A duplicate is a second socket that the parent client knows nothing about, so closing the
   * main client leaves it open - and nothing else holds a reference to it either once the
   * caller drops its own. Without this list a subscriber survives the server it belongs to.
   *
   * Entries are never dropped on `isConnected`: a client in the middle of an automatic
   * reconnect reports itself disconnected, and forgetting it there would put us back where we
   * started - an open socket nobody closes. `createSubscriber()` is a setup-time call (one per
   * pub/sub consumer), so the set stays small.
   */
  private readonly subscribers = new Set<RedisClientAdapter>();

  @OnStart()
  public async onStart() {
    if (!this.options) {
      throw new Error('Redis options not initialized. Make sure to use @Redis decorator properly.');
    }

    // If a custom client was provided, use it directly
    if (this.options.client) {
      this._client = this.options.client;

      if (!this._client.isConnected) {
        await this._client.connect();
      }

      this.options.logger?.info(
        `Redis Connected (custom client)${this.options.config.name ? ` - ${this.options.config.name}` : ''}`,
      );

      return;
    }

    try {
      const url = buildRedisUrl(this.options.config);
      const { url: _u, name, host: _h, port: _p, username: _un, password: _pw, db: _d, ...opts } = this.options.config;

      if (this.options.adapter === 'node-redis') {
        this._client = NodeRedisAdapter.create(url, opts);
      } else {
        this._client = new BunRedisAdapter(url, opts);
      }

      await this._client.connect();

      this.options.logger?.info(`Redis Connected${name ? ` - ${name}` : ''}`);
    } catch (error) {
      this.options.logger?.error('Redis connection failed:', error);
      throw new Error(`Redis connection failed: ${error}`);
    }
  }

  // String operations

  public async get(key: string): Promise<string | null> {
    return this.getClient().get(key);
  }

  public async set(key: string, value: string, ttl?: number): Promise<void> {
    const client = this.getClient();

    await client.set(key, value);

    if (ttl !== undefined) {
      await client.expire(key, ttl);
    }
  }

  public async del(...keys: string[]): Promise<number> {
    const client = this.getClient();
    let count = 0;

    for (const key of keys) {
      const result = await client.del(key);

      if (result) count++;
    }

    return count;
  }

  public async exists(key: string): Promise<boolean> {
    return this.getClient().exists(key);
  }

  public async incr(key: string): Promise<number> {
    return this.getClient().incr(key);
  }

  public async decr(key: string): Promise<number> {
    return this.getClient().decr(key);
  }

  public async expire(key: string, seconds: number): Promise<number> {
    return this.getClient().expire(key, seconds);
  }

  public async ttl(key: string): Promise<number> {
    return this.getClient().ttl(key);
  }

  public async keys(pattern: string): Promise<string[]> {
    return this.getClient().keys(pattern);
  }

  // Hash operations

  public async hget(key: string, field: string): Promise<string | null> {
    return this.getClient().hget(key, field);
  }

  public async hmset(key: string, fields: string[]): Promise<void> {
    await this.getClient().hmset(key, fields);
  }

  public async hmget(key: string, fields: string[]): Promise<(string | null)[]> {
    return this.getClient().hmget(key, fields);
  }

  // Set operations

  public async sadd(key: string, member: string): Promise<number> {
    return this.getClient().sadd(key, member);
  }

  public async srem(key: string, member: string): Promise<number> {
    return this.getClient().srem(key, member);
  }

  public async smembers(key: string): Promise<string[]> {
    return this.getClient().smembers(key);
  }

  public async sismember(key: string, member: string): Promise<boolean> {
    return this.getClient().sismember(key, member);
  }

  // Raw command access

  public async send(command: string, args: string[]): Promise<any> {
    return this.getClient().send(command, args);
  }

  // Client access

  public get client(): RedisClientAdapter {
    return this.getClient();
  }

  public get config(): RedisConfig {
    if (!this.options) {
      throw new Error('Redis options not initialized.');
    }

    return this.options.config;
  }

  public async createSubscriber(): Promise<RedisClientAdapter> {
    const subscriber = await this.getClient().duplicate();

    this.subscribers.add(subscriber);

    return subscriber;
  }

  // Lifecycle

  /**
   * Releases every connection this service opened: the subscribers first, then the client
   * they were duplicated from.
   *
   * Runs while the component's own dependencies are still up and the HTTP surface is already
   * down. `disconnect()` stays as it was - a caller that closes the main connection by hand
   * keeps a subscriber it is still reading from.
   */
  @OnStop()
  public async onStop(): Promise<void> {
    await this.closeSubscribers();
    await this.disconnect();
  }

  public async disconnect(): Promise<void> {
    if (this._client) {
      await this._client.disconnect();
      this._client = null;
    }
  }

  /**
   * Sends PING and resolves with the reply, bounded by a timeout.
   *
   * With the offline queue enabled and no connection, a command does not fail - it waits in
   * the queue for a connection that may never come, so an unbounded PING hangs forever. A
   * readiness probe must fail fast instead: on timeout this rejects with
   * `Redis PING timed out after <n>ms`. The timer is cleared in `finally`, so a probe never
   * leaves a live timer behind.
   */
  public async ping(timeoutMs = 1000): Promise<'PONG'> {
    const pong = this.getClient().send('PING', []);

    // If the timeout wins the race, a late failure of the orphaned PING must not surface
    // as an unhandled rejection.
    pong.catch(() => {});

    let timer: ReturnType<typeof setTimeout> | undefined;

    try {
      return await Promise.race([
        pong,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error(`Redis PING timed out after ${timeoutMs}ms`)), timeoutMs);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  public async testConnection(): Promise<boolean> {
    if (!this._client || !this._client.isConnected) {
      return false;
    }

    try {
      await this.ping();

      return true;
    } catch {
      return false;
    }
  }

  protected setRedisOptions(options: RedisOptions): void {
    this.options = options;
  }

  protected setRedisClient(client: RedisClientAdapter): void {
    this._client = client;
  }

  /**
   * Closes every tracked subscriber, all of them regardless of what any single one does.
   *
   * A subscriber is routinely closed by its owner first (the WebSocket transport closes the
   * one it asked for in `destroy()`), and a second close on a dead socket throws on some
   * clients. Neither that nor a genuinely broken connection may strand the subscribers behind
   * it or the main client, which is the connection the whole shutdown is about - so failures
   * are collected, logged and stepped over.
   *
   * The list is cleared up front so a stop that runs twice does not try the same dead
   * connections again.
   */
  private async closeSubscribers(): Promise<void> {
    const subscribers = [...this.subscribers];

    this.subscribers.clear();

    // The async wrapper is what makes a subscriber that throws *synchronously* - a custom
    // adapter, not the ones shipped here - a rejected promise instead of an exception that
    // escapes before allSettled ever sees the batch.
    const results = await Promise.allSettled(subscribers.map(async (subscriber) => subscriber.disconnect()));

    for (const result of results) {
      if (result.status === 'rejected') {
        this.options?.logger?.error('Redis subscriber disconnect failed:', result.reason);
      }
    }
  }

  private getClient(): RedisClientAdapter {
    if (!this._client) {
      throw new Error('Redis client not initialized. Service may not have started properly.');
    }

    return this._client;
  }
}
