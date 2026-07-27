import { RedisClient } from 'bun';
import type { RedisClientAdapter } from './RedisClientAdapter';

/**
 * Replay attempts per channel before giving up and reporting it. A replay that
 * fails on a live connection leaves the channel unsubscribed with no further
 * connect event to retry from - permanent, silent message loss without this.
 */
const RESUBSCRIBE_ATTEMPTS = 3;

const RESUBSCRIBE_RETRY_MS = 100;

export class BunRedisAdapter implements RedisClientAdapter {
  private client: RedisClient;

  private subscriptions?: Map<string, (message: string) => void>;

  private resubscribeInstalled?: boolean;

  private resubscribeListeners?: Set<(channel: string) => void>;

  /** Bumped per reconnect so a superseded replay cannot touch the current one. */
  private replayGeneration?: number;

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
    // Object.getPrototypeOf(this), not BunRedisAdapter.prototype: a user subclass added for
    // instrumentation would otherwise lose its identity on every duplicate, and duplicate() is
    // on the pub/sub and microservice-transport hot path.
    const adapter = Object.create(Object.getPrototypeOf(this)) as BunRedisAdapter;

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

  public onResubscribed(listener: (channel: string) => void): void {
    // duplicate() builds instances via Object.create (no constructor run) -
    // initialize the tracking state defensively
    this.resubscribeListeners ??= new Set();
    this.resubscribeListeners.add(listener);
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
   * replay it (verified: on a raw RedisClient the channel stays at zero
   * subscribers forever after a single blip). Without this hook one broker
   * blip silently kills every pub/sub consumer - e.g. the microservice reply
   * channel - permanently.
   *
   * The replay costs at least one round trip AFTER the socket reports open,
   * so `connected` is true for a moment while the channel is still dead.
   * Subscribers that must not be trusted during that moment listen for
   * onResubscribed, which fires only once the SUBSCRIBE has been acknowledged.
   */
  private installResubscribe(): void {
    if (this.resubscribeInstalled) return;

    this.resubscribeInstalled = true;

    this.onConnected(() => {
      const generation = (this.replayGeneration = (this.replayGeneration ?? 0) + 1);
      const entries = [...(this.subscriptions ?? new Map())];

      void (async () => {
        for (const [channel, listener] of entries) {
          for (let attempt = 1; attempt <= RESUBSCRIBE_ATTEMPTS; attempt++) {
            // A newer connection is already replaying - this generation must
            // never touch the subscription that one now owns
            if (generation !== this.replayGeneration) return;

            try {
              // Client-side listener registrations SURVIVE the reconnect, so a
              // bare subscribe would register the listener a second time and
              // deliver every message twice - clear first, then re-subscribe
              await this.client.unsubscribe(channel);
              await this.client.subscribe(channel, listener);

              // A newer connection took over while this SUBSCRIBE was in
              // flight: reporting now would vouch for a connection that no
              // longer exists, and the generation that replaced it reports on
              // its own
              if (generation !== this.replayGeneration) return;

              this.emitResubscribed(channel);
              break;
            } catch (error) {
              // Redis went down again mid-replay - the next onconnect retries
              if (!this.client.connected) return;

              // Still connected, so nothing will retry on its own: the channel
              // would stay unsubscribed on a healthy socket and every message
              // published to it would vanish with no error anywhere.
              if (attempt === RESUBSCRIBE_ATTEMPTS) {
                console.error(
                  `BunRedisAdapter: could not restore the subscription to "${channel}" after ${attempt} attempts - messages on this channel are being dropped:`,
                  error,
                );
                break;
              }

              await new Promise<void>((resolve) => {
                setTimeout(resolve, RESUBSCRIBE_RETRY_MS);
              });
            }
          }
        }
      })();
    });
  }

  private emitResubscribed(channel: string): void {
    for (const listener of this.resubscribeListeners ?? []) {
      try {
        listener(channel);
      } catch {
        // A listener error must not abort the replay of the other channels
      }
    }
  }
}
