import { RedisClient } from 'bun';
import type { RedisClientAdapter } from './RedisClientAdapter';

export class BunRedisAdapter implements RedisClientAdapter {
  private client: RedisClient;

  private subscriptions?: Map<string, (message: string) => void>;

  private resubscribeInstalled?: boolean;

  private connectListeners?: Set<() => void>;

  private closeListeners?: Set<() => void>;

  private connectionEventsInstalled?: boolean;

  public constructor(url?: string, opts?: Record<string, any>) {
    this.client = new RedisClient(url, opts);
  }

  // Lifecycle

  public get isConnected(): boolean {
    return this.client.connected;
  }

  public async connect(): Promise<void> {
    await this.client.connect();
  }

  public async disconnect(): Promise<void> {
    this.client.close();
  }

  public async duplicate(): Promise<RedisClientAdapter> {
    const dup = await this.client.duplicate();
    const adapter = Object.create(BunRedisAdapter.prototype) as BunRedisAdapter;

    (adapter as any).client = dup;

    return adapter;
  }

  // String operations

  public async get(key: string): Promise<string | null> {
    return this.client.get(key);
  }

  public async set(key: string, value: string): Promise<void> {
    await this.client.set(key, value);
  }

  public async del(key: string): Promise<number> {
    return this.client.del(key);
  }

  public async exists(key: string): Promise<boolean> {
    return this.client.exists(key);
  }

  public async incr(key: string): Promise<number> {
    return this.client.incr(key);
  }

  public async decr(key: string): Promise<number> {
    return this.client.decr(key);
  }

  // Expiration

  public async expire(key: string, seconds: number): Promise<number> {
    return this.client.expire(key, seconds);
  }

  public async ttl(key: string): Promise<number> {
    return this.client.ttl(key);
  }

  public async keys(pattern: string): Promise<string[]> {
    return this.client.keys(pattern);
  }

  // Hash operations

  public async hget(key: string, field: string): Promise<string | null> {
    return this.client.hget(key, field);
  }

  public async hmset(key: string, fields: string[]): Promise<void> {
    await this.client.hmset(key, fields);
  }

  public async hmget(key: string, fields: string[]): Promise<(string | null)[]> {
    return this.client.hmget(key, fields);
  }

  // Set operations

  public async sadd(key: string, member: string): Promise<number> {
    return this.client.sadd(key, member);
  }

  public async srem(key: string, member: string): Promise<number> {
    return this.client.srem(key, member);
  }

  public async smembers(key: string): Promise<string[]> {
    return this.client.smembers(key);
  }

  public async sismember(key: string, member: string): Promise<boolean> {
    return this.client.sismember(key, member);
  }

  // Raw command

  public async send(command: string, args: string[]): Promise<any> {
    return this.client.send(command, args);
  }

  // Pub/Sub

  public async publish(channel: string, message: string): Promise<number> {
    return this.client.publish(channel, message);
  }

  public async subscribe(channel: string, listener: (message: string) => void): Promise<void> {
    // duplicate() builds instances via Object.create (no constructor run) -
    // initialize the tracking state defensively
    this.subscriptions ??= new Map();
    this.subscriptions.set(channel, listener);
    this.installResubscribe();

    await this.client.subscribe(channel, listener);
  }

  public async unsubscribe(channel: string): Promise<void> {
    this.subscriptions?.delete(channel);
    await this.client.unsubscribe(channel);
  }

  // Connection events

  public onConnected(listener: () => void): void {
    // duplicate() builds instances via Object.create (no constructor run) -
    // initialize the tracking state defensively
    this.connectListeners ??= new Set();
    this.connectListeners.add(listener);
    this.installConnectionEvents();
  }

  public onConnectionLost(listener: () => void): void {
    this.closeListeners ??= new Set();
    this.closeListeners.add(listener);
    this.installConnectionEvents();
  }

  /**
   * Bun's RedisClient exposes onconnect/onclose as single-assignment
   * properties - claim them once and fan out, so resubscribe replay and
   * external listeners (e.g. the transport's poisoning detection) coexist.
   */
  private installConnectionEvents(): void {
    if (this.connectionEventsInstalled) return;

    this.connectionEventsInstalled = true;

    this.client.onconnect = () => {
      for (const listener of this.connectListeners ?? []) {
        try {
          listener();
        } catch {
          // A listener error must not break the client's connect handling
        }
      }
    };

    this.client.onclose = () => {
      for (const listener of this.closeListeners ?? []) {
        try {
          listener();
        } catch {
          // A listener error must not break the client's close handling
        }
      }
    };
  }

  /**
   * Bun's RedisClient reconnects the socket automatically, but server-side
   * subscription state dies with the old connection and the client does NOT
   * replay it. Without this hook a single broker blip silently kills every
   * pub/sub consumer (e.g. the microservice reply channel) forever.
   */
  private installResubscribe(): void {
    if (this.resubscribeInstalled) return;

    this.resubscribeInstalled = true;

    this.onConnected(() => {
      const entries = [...(this.subscriptions ?? new Map())];

      void (async () => {
        for (const [channel, listener] of entries) {
          try {
            // Client-side listener registrations SURVIVE the reconnect, so a
            // bare subscribe would register the listener a second time and
            // deliver every message twice - clear first, then re-subscribe
            await this.client.unsubscribe(channel);
            await this.client.subscribe(channel, listener);
          } catch {
            // Redis went down again mid-replay - the next onconnect retries
          }
        }
      })();
    });
  }
}
