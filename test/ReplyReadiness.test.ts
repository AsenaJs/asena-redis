import { afterEach, describe, expect, it } from 'bun:test';
import type { Socket, TCPSocketListener } from 'bun';
import { RedisMicroserviceTransport } from '../lib/microservice';
import { BunRedisAdapter } from '../lib/adapter';
import type { RedisClientAdapter } from '../lib/adapter';

/**
 * Readiness must mean "this instance can complete a send()".
 *
 * Replies travel a plain pub/sub channel: no replay, and the request entry is
 * ACKed unconditionally, so a reply published while the caller's reply channel
 * has no subscriber is dropped by Redis and never comes back. Redis drops
 * every subscription with the socket and restoring it costs a round trip
 * AFTER the socket reports open - so a health check that only looks at the
 * publisher reports 200 through a window in which every RPC issued against
 * this instance times out. Measured before the fix over 60 connection
 * outages: the window was 0.5-1.8ms wide in 60 of 60, and 77 of 1500 requests
 * issued at the readiness edge were lost.
 */

const REDIS_URL = 'redis://localhost:6379';
const REDIS_PORT = 6379;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

// --- Offline half: the readiness contract, driven through the adapter seam --

/**
 * A connection whose events the test drives directly. Only the members the
 * transport touches on a client-only instance are implemented.
 */
class FakeConnection implements RedisClientAdapter {
  public isConnected = true;

  public subscriptions: string[] = [];

  public disconnected = false;

  private connectListeners: (() => void)[] = [];

  private lostListeners: (() => void)[] = [];

  private resubscribeListeners: ((channel: string) => void)[] = [];

  public constructor(private readonly supportsResubscribed = true) {}

  public async connect(): Promise<void> {}

  public async disconnect(): Promise<void> {
    this.disconnected = true;
    this.isConnected = false;
  }

  public async duplicate(): Promise<RedisClientAdapter> {
    return new FakeConnection(this.supportsResubscribed);
  }

  public async subscribe(channel: string): Promise<void> {
    this.subscriptions.push(channel);
  }

  public async unsubscribe(channel: string): Promise<void> {
    this.subscriptions = this.subscriptions.filter((c) => c !== channel);
  }

  public onConnected(listener: () => void): void {
    this.connectListeners.push(listener);
  }

  public onConnectionLost(listener: () => void): void {
    this.lostListeners.push(listener);
  }

  public get onResubscribed(): ((listener: (channel: string) => void) => void) | undefined {
    if (!this.supportsResubscribed) return undefined;

    return (listener: (channel: string) => void) => {
      this.resubscribeListeners.push(listener);
    };
  }

  /** The socket died. Bun does not raise onclose for a transient loss. */
  public socketDown(): void {
    this.isConnected = false;
  }

  /** The socket is back - Redis is serving nothing on it yet. */
  public socketUp(): void {
    this.isConnected = true;

    for (const listener of this.connectListeners) listener();
  }

  /** A final close, which adapters that have the event do report. */
  public closed(): void {
    this.isConnected = false;

    for (const listener of this.lostListeners) listener();
  }

  /** The replay landed: Redis is serving this channel again. */
  public resubscribed(channel: string): void {
    for (const listener of this.resubscribeListeners) listener(channel);
  }

  // Unused by a client-only transport
  public async get(): Promise<string | null> {
    return null;
  }

  public async set(): Promise<void> {}

  public async del(): Promise<number> {
    return 0;
  }

  public async exists(): Promise<boolean> {
    return false;
  }

  public async incr(): Promise<number> {
    return 0;
  }

  public async decr(): Promise<number> {
    return 0;
  }

  public async expire(): Promise<number> {
    return 0;
  }

  public async ttl(): Promise<number> {
    return 0;
  }

  public async keys(): Promise<string[]> {
    return [];
  }

  public async hget(): Promise<string | null> {
    return null;
  }

  public async hmset(): Promise<void> {}

  public async hmget(): Promise<(string | null)[]> {
    return [];
  }

  public async sadd(): Promise<number> {
    return 0;
  }

  public async srem(): Promise<number> {
    return 0;
  }

  public async smembers(): Promise<string[]> {
    return [];
  }

  public async sismember(): Promise<boolean> {
    return false;
  }

  public async send(): Promise<any> {
    return null;
  }

  public async publish(): Promise<number> {
    return 0;
  }
}

/**
 * Boots a client-only transport (an HTTP gateway: no handlers, so the reply
 * subscriber is the only thing keeping send() alive) on fake connections.
 * A source carrying createSubscriber() is treated as an AsenaRedisService -
 * the seam these tests ride on.
 */
async function bootFake(supportsResubscribed = true): Promise<{
  transport: RedisMicroserviceTransport;
  subscriber: FakeConnection;
  publisher: FakeConnection;
}> {
  const publisher = new FakeConnection(supportsResubscribed);
  const subscriber = new FakeConnection(supportsResubscribed);
  const source = { client: { duplicate: async () => publisher }, createSubscriber: async () => subscriber } as any;
  const transport = new RedisMicroserviceTransport(source, { serviceName: 'gateway', streamPrefix: 'asena:test:rr' });

  await transport.init();
  await transport.listen();

  return { transport, subscriber, publisher };
}

describe('readiness reflects the reply subscription', () => {
  it('is ready once the reply channel is subscribed', async () => {
    const { transport, subscriber } = await bootFake();

    expect(transport.isConnected).toBe(true);
    expect(subscriber.subscriptions).toHaveLength(1);
  });

  it('is unready while the reply subscriber socket is down, even with a live publisher', async () => {
    const { transport, subscriber, publisher } = await bootFake();

    subscriber.socketDown();

    expect(publisher.isConnected).toBe(true);
    expect(transport.isConnected).toBe(false);
  });

  it('stays unready between the reconnect and the replay landing', async () => {
    // This is the defect: the socket is back and the publisher is fine, so the
    // old check said 200 - but Redis is serving nothing on the reply channel
    // yet, and every reply published in this window is dropped for good.
    const { transport, subscriber } = await bootFake();

    subscriber.socketDown();
    subscriber.socketUp();

    expect(subscriber.isConnected).toBe(true);
    expect(transport.isConnected).toBe(false);

    subscriber.resubscribed('asena:test:rr:reply:nope');
    expect(transport.isConnected).toBe(false);

    subscriber.resubscribed(subscriber.subscriptions[0]!);
    expect(transport.isConnected).toBe(true);
  });

  it('goes unready on a reported connection loss', async () => {
    const { transport, subscriber } = await bootFake();

    subscriber.closed();

    expect(transport.isConnected).toBe(false);
  });

  it('trusts the connect event of an adapter that restores subscriptions itself', async () => {
    // node-redis replays inside its socket initiator and only then emits
    // `ready`, so it has no separate moment to report - and an adapter
    // without the event must not be stuck unready forever.
    const { transport, subscriber } = await bootFake(false);

    expect(transport.isConnected).toBe(true);

    subscriber.socketDown();
    expect(transport.isConnected).toBe(false);

    subscriber.socketUp();
    expect(transport.isConnected).toBe(true);
  });

  it('cannot be talked back into readiness by a detached subscriber after destroy', async () => {
    const { transport, subscriber } = await bootFake();
    const channel = subscriber.subscriptions[0]!;

    await transport.destroy({ drainTimeout: 0 });

    expect(transport.isConnected).toBe(false);

    // The socket keeps emitting through teardown; none of it speaks for a
    // transport that is gone
    subscriber.isConnected = true;
    subscriber.socketUp();
    subscriber.resubscribed(channel);

    expect(transport.isConnected).toBe(false);
  });
});

// --- Online half: the same invariant against a real Redis ------------------

interface LinkData {
  upstream?: Socket<undefined>;
  pending: Uint8Array[];
}

/**
 * TCP proxy in front of Redis. Severing it kills exactly this instance's
 * connections - a real Redis restart also wipes the responder's consumer
 * groups, whose recovery takes seconds and would drown the millisecond-wide
 * window under test.
 */
class TcpProxy {
  public port = 0;

  private server?: TCPSocketListener<LinkData>;

  private links = new Set<Socket<LinkData>>();

  public async start(port = 0): Promise<void> {
    this.server = Bun.listen<LinkData>({
      hostname: '127.0.0.1',
      port,
      socket: {
        open: (client) => {
          client.data = { pending: [] };
          this.links.add(client);

          Bun.connect<undefined>({
            hostname: '127.0.0.1',
            port: REDIS_PORT,
            socket: {
              open: (upstream) => {
                client.data.upstream = upstream;

                for (const chunk of client.data.pending) upstream.write(chunk);

                client.data.pending = [];
              },
              data: (_upstream, chunk) => {
                client.write(chunk);
              },
              close: () => {
                client.end();
              },
              error: () => {
                client.end();
              },
            },
          }).catch(() => client.end());
        },
        data: (client, chunk) => {
          if (client.data.upstream) {
            client.data.upstream.write(chunk);
          } else {
            client.data.pending.push(chunk);
          }
        },
        close: (client) => {
          this.links.delete(client);
          client.data.upstream?.end();
        },
        error: (client) => {
          this.links.delete(client);
          client.data.upstream?.end();
        },
      },
    });

    this.port = this.server.port;
  }

  public stop(): void {
    this.server?.stop(true);
    this.server = undefined;

    for (const client of this.links) {
      client.data.upstream?.end();
      client.end();
    }

    this.links.clear();
  }
}

describe('readiness against a real Redis outage', () => {
  const transports: RedisMicroserviceTransport[] = [];
  const proxies: TcpProxy[] = [];
  const prefixes: string[] = [];

  afterEach(async () => {
    for (const transport of transports) {
      await transport.destroy({ drainTimeout: 500 }).catch(() => {});
    }

    transports.length = 0;

    for (const proxy of proxies) proxy.stop();

    proxies.length = 0;

    const cleanup = new BunRedisAdapter(REDIS_URL);

    await cleanup.connect();

    for (const prefix of prefixes) {
      const keys = await cleanup.send('KEYS', [`${prefix}*`]);

      if (Array.isArray(keys) && keys.length) await cleanup.send('DEL', keys.map(String));
    }

    prefixes.length = 0;
    await cleanup.disconnect();
  });

  it('never reports ready while Redis holds no subscriber on the reply channel', async () => {
    const streamPrefix = `asena:test:rr:${crypto.randomUUID().slice(0, 8)}`;

    prefixes.push(streamPrefix);

    const proxy = new TcpProxy();

    await proxy.start();
    proxies.push(proxy);

    const admin = new BunRedisAdapter(REDIS_URL);

    await admin.connect();

    const caller = new RedisMicroserviceTransport(
      { url: `redis://127.0.0.1:${proxy.port}` },
      { serviceName: 'gateway', streamPrefix, blockMs: 200, commandTimeout: 2_000 },
    );

    transports.push(caller);
    await caller.init();

    const channels = (await admin.send('PUBSUB', ['CHANNELS', `${streamPrefix}:reply:*`])) as string[];
    const replyChannel = channels[0]!;

    expect(replyChannel).toBeDefined();

    const subscriberCount = async (): Promise<number> => {
      const res = (await admin.send('PUBSUB', ['NUMSUB', replyChannel])) as any[];

      return Number(res?.[1] ?? 0);
    };

    try {
      for (let round = 0; round < 5; round++) {
        proxy.stop();
        await sleep(200);
        await proxy.start(proxy.port);

        // Readiness and the SERVER's subscriber count are sampled against
        // each other in one loop, at NUMSUB-round-trip resolution. Two loops
        // would compare two sampling granularities and manufacture a pass.
        const deadline = Date.now() + 30_000;
        let ready = false;

        while (!ready) {
          const live = await subscriberCount();

          ready = caller.isConnected;

          // The invariant: readiness may only go green on an instance whose
          // replies Redis is actually delivering.
          if (ready) expect(live).toBeGreaterThanOrEqual(1);

          if (Date.now() > deadline) throw new Error('transport never became ready again');
        }
      }
    } finally {
      await admin.disconnect();
    }
  }, 60_000);

  it('completes a request issued the instant readiness goes green after an outage', async () => {
    const streamPrefix = `asena:test:rr:${crypto.randomUUID().slice(0, 8)}`;

    prefixes.push(streamPrefix);

    const proxy = new TcpProxy();

    await proxy.start();
    proxies.push(proxy);

    // The responder is on a direct connection: the outage is the caller's
    const responder = new RedisMicroserviceTransport(
      { url: REDIS_URL },
      { serviceName: 'responder', streamPrefix, blockMs: 200, claimIdleMs: 5_000 },
    );

    transports.push(responder);
    responder.registerMessageHandler('probe.echo', (data: any) => ({ echo: data?.i }));
    await responder.init();
    await responder.listen();

    const caller = new RedisMicroserviceTransport(
      { url: `redis://127.0.0.1:${proxy.port}` },
      { serviceName: 'gateway', streamPrefix, blockMs: 200, requestTimeout: 3_000, commandTimeout: 2_000 },
    );

    transports.push(caller);
    await caller.init();

    expect(await caller.send<{ echo: number }>('probe.echo', { i: 0 })).toEqual({ echo: 0 });

    // Ten outages, not one: the window is sub-millisecond, so a burst escapes
    // it outright in most rounds. Measured before the fix, ~37% of outages
    // lost at least one request - one round would let the defect through
    // nearly two thirds of the time, ten rounds ~1%.
    for (let round = 1; round <= 10; round++) {
      proxy.stop();
      await sleep(200);
      await proxy.start(proxy.port);

      const deadline = Date.now() + 30_000;

      // setImmediate, not a 1ms timer: the window is sub-millisecond, so a
      // timer-paced poll steps straight over it and the test would pass
      // against the unfixed transport most of the time. This is the sampling
      // rate the loss measurement used.
      while (!caller.isConnected) {
        if (Date.now() > deadline) throw new Error('transport never became ready again');

        await new Promise((resolve) => {
          setImmediate(resolve);
        });
      }

      // Traffic arrives the moment the endpoint says ready, all at once, the
      // way a load balancer releases queued connections at a pod it has just
      // marked ready. Before the fix a whole burst could land inside the
      // window and every one of them timed out.
      const replies = await Promise.all(
        Array.from({ length: 25 }, (_, i) => caller.send<{ echo: number }>('probe.echo', { i }, { timeout: 3_000 })),
      );

      expect(replies).toHaveLength(25);
      expect(replies[0]).toEqual({ echo: 0 });
    }
  }, 60_000);
});
