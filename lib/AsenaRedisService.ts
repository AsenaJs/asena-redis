import { PostConstruct } from '@asenajs/asena/decorators/ioc';
import type { RedisClientAdapter } from './adapter';
import { BunRedisAdapter } from './adapter';
import { NodeRedisAdapter } from './adapter';
import { buildRedisUrl } from './adapter';
import type { RedisConfig, RedisOptions } from './types';

export abstract class AsenaRedisService {

  protected _client: RedisClientAdapter | null = null;

  protected options: RedisOptions | null = null;

  @PostConstruct()
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

      this.options.logger?.info(`Redis Connected (custom client)${this.options.config.name ? ` - ${this.options.config.name}` : ''}`);

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
    return this.getClient().duplicate();
  }

  // Lifecycle

  public async disconnect(): Promise<void> {
    if (this._client) {
      await this._client.disconnect();
      this._client = null;
    }
  }

  public async testConnection(): Promise<boolean> {
    if (!this._client || !this._client.isConnected) {
      return false;
    }

    try {
      await this._client.send('PING', []);

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

  private getClient(): RedisClientAdapter {
    if (!this._client) {
      throw new Error('Redis client not initialized. Service may not have started properly.');
    }

    return this._client;
  }

}