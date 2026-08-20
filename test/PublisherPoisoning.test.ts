import { afterEach, describe, expect, it } from 'bun:test';
import type { Socket, TCPSocketListener } from 'bun';
import { RedisMicroserviceTransport } from '../lib/microservice';
import { BunRedisAdapter } from '../lib/adapter';
import type { RedisMicroserviceOptions } from '../lib/types';

const REDIS_URL = 'redis://localhost:6379';
const REDIS_PORT = 6379;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 5000): Promise<void> {
  const start = Date.now();

  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor timed out');
    }

    await sleep(50);
  }
}

interface LinkData {
  upstream?: Socket<undefined>;
  pending: Uint8Array[];
}

/**
 * Minimal TCP proxy in front of Redis. Lets a test kill the exact socket that
 * carries a marker byte-sequence mid-command - the deterministic reproduction
 * of Bun RedisClient's in-flight-loss poisoning: the killed command's promise
 * never settles, and after the automatic reconnect every reply resolves the
 * wrong promise. stop() simulates a full broker outage (refused connections).
 */
class TcpProxy {
  public port = 0;

  public killOnMarker?: string;

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
              // Braces, not an expression body: Socket.end() returns a number and the
              // handler signature is `void | Promise<void>`.
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
          // Marker kill applies to the client->server direction only, so it
          // hits exactly the connection that SENT the marked command
          if (this.killOnMarker && Buffer.from(chunk).includes(this.killOnMarker)) {
            client.data.upstream?.end();
            client.end();

            return;
          }

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

  /** Stops listening AND severs every live link - full outage simulation. */
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

describe('RedisMicroserviceTransport - publisher poisoning & shutdown', () => {
  const transports: RedisMicroserviceTransport[] = [];
  const prefixes: string[] = [];
  const proxies: TcpProxy[] = [];

  function uniquePrefix(): string {
    const prefix = `asena:test:poison:${crypto.randomUUID().slice(0, 8)}`;

    prefixes.push(prefix);

    return prefix;
  }

  async function createProxy(): Promise<TcpProxy> {
    const proxy = new TcpProxy();

    await proxy.start();
    proxies.push(proxy);

    return proxy;
  }

  async function createTransport(
    url: string,
    options: Partial<RedisMicroserviceOptions> & { serviceName: string; streamPrefix: string },
  ) {
    const transport = new RedisMicroserviceTransport({ url }, { blockMs: 200, drainTimeout: 1000, ...options });

    transports.push(transport);
    await transport.init();

    return transport;
  }

  afterEach(async () => {
    for (const transport of transports) {
      await transport.destroy({ drainTimeout: 500 }).catch(() => {});
    }

    transports.length = 0;

    for (const proxy of proxies) {
      proxy.stop();
    }

    proxies.length = 0;

    const cleanup = new BunRedisAdapter(REDIS_URL);

    await cleanup.connect();

    for (const prefix of prefixes) {
      const keys = await cleanup.send('KEYS', [`${prefix}*`]);

      if (Array.isArray(keys) && keys.length) {
        await cleanup.send('DEL', keys.map(String));
      }
    }

    prefixes.length = 0;
    await cleanup.disconnect();
  });

  it('should bound an emit wedged by mid-flight connection loss and keep publishing after the swap', async () => {
    const streamPrefix = uniquePrefix();
    const proxy = await createProxy();

    // Direct-connected consumer proves end-to-end delivery after the swap
    const received: string[] = [];
    const watcher = await createTransport(REDIS_URL, { serviceName: 'watcher', streamPrefix });

    watcher.registerEventHandler('evt.*', (_data, context) => {
      received.push(context.pattern);
    });
    await watcher.listen();

    const producer = await createTransport(`redis://127.0.0.1:${proxy.port}`, {
      serviceName: 'producer',
      streamPrefix,
      commandTimeout: 1500,
    });

    await producer.emit('evt.warmup', {});
    await waitFor(() => received.includes('evt.warmup'));

    // Kill the publisher socket while the XADD is in flight: without the
    // guard this promise would NEVER settle (the original H1 finding).
    // Bun >= 1.4 rejects the in-flight command itself ("Connection closed")
    // instead of leaving it for the watchdog - either way the connection is
    // poisoned and swapped, which evt.after-swap below proves
    proxy.killOnMarker = 'poison-me';
    await expect(producer.emit('evt.poison-me', {})).rejects.toThrow(/wedged|Connection closed/);
    proxy.killOnMarker = undefined;

    // The poisoned connection was replaced - publishing works again
    await producer.emit('evt.after-swap', {});
    await waitFor(() => received.includes('evt.after-swap'), 10_000);
  }, 20_000);

  it('should keep sweep/retry/DLQ alive after publisher poisoning', async () => {
    const streamPrefix = uniquePrefix();
    const proxy = await createProxy();

    const svc = await createTransport(`redis://127.0.0.1:${proxy.port}`, {
      serviceName: 'svc',
      streamPrefix,
      commandTimeout: 1500,
      claimIdleMs: 1000,
      maxRetries: 1,
    });

    svc.registerEventHandler('boom', () => {
      throw new Error('always fails');
    });
    await svc.listen();

    // Poison the service's publisher connection. With the sweep also using
    // the publisher there can be several commands in flight at the kill:
    // the LAST one never settles, the earlier ones may resolve with the
    // WRONG (shifted) replies - so the emit itself may "succeed" or reject,
    // both are the poisoning. The real assertion is the DLQ below.
    proxy.killOnMarker = 'poison-me';
    await svc.emit('evt.poison-me', {}).catch(() => {});
    await sleep(2000); // let the guard/onclose detection finish the swap
    proxy.killOnMarker = undefined;

    // A failing event must still travel initial delivery -> sweep retry ->
    // DLQ, all via the (replaced) publisher. Pre-fix the sweep parsed the
    // desynced replies as empty results and retry/DLQ died silently.
    const client = await createTransport(REDIS_URL, { serviceName: 'client', streamPrefix });

    await client.emit('boom', {});

    const admin = new BunRedisAdapter(REDIS_URL);

    await admin.connect();

    try {
      await waitFor(async () => Number(await admin.send('XLEN', [`${streamPrefix}:dlq`]).catch(() => 0)) >= 1, 15_000);
    } finally {
      await admin.disconnect();
    }
  }, 25_000);

  it('should destroy quickly while stuck in reconnect backoff during a broker outage', async () => {
    const streamPrefix = uniquePrefix();
    const proxy = await createProxy();

    const svc = await createTransport(`redis://127.0.0.1:${proxy.port}`, {
      serviceName: 'svc',
      streamPrefix,
      commandTimeout: 1000,
    });

    svc.registerEventHandler('noop', () => {});
    await svc.listen();

    proxy.stop();

    // Past the consumer wedge watchdog (blockMs 200 + 5s margin) and the
    // first short backoffs, into a multi-second backoff sleep
    await sleep(9500);

    const start = Date.now();

    await svc.destroy({ drainTimeout: 500 });
    // Bounded by the stop signal + capped teardown steps; pre-fix this took
    // the remainder of a capped backoff sleep plus unbounded unsubscribe
    // awaits on the dead connection (>20s observed)
    expect(Date.now() - start).toBeLessThan(2500);
  }, 20_000);

  it('should recover consumption after a failed consumer replacement once the broker returns', async () => {
    const streamPrefix = uniquePrefix();
    const proxy = await createProxy();
    const received: string[] = [];

    const svc = await createTransport(`redis://127.0.0.1:${proxy.port}`, {
      serviceName: 'svc',
      streamPrefix,
      commandTimeout: 1000,
    });

    svc.registerEventHandler('ping', () => {
      received.push('ping');
    });
    await svc.listen();

    const port = proxy.port;

    proxy.stop();

    // The wedge watchdog fires (~5.2s) and its replacement duplicate() then
    // fails against the dead proxy - pre-fix the consumer stayed undefined
    // and the loop threw TypeErrors forever, even after the broker returned
    await sleep(6500);
    await proxy.start(port);

    const client = await createTransport(REDIS_URL, { serviceName: 'client', streamPrefix });

    await client.emit('ping', {});
    await waitFor(() => received.includes('ping'), 20_000);
  }, 40_000);

  it('should leave the borrowed AsenaRedisService client untouched by destroy', async () => {
    const streamPrefix = uniquePrefix();
    const userClient = new BunRedisAdapter(REDIS_URL);

    await userClient.connect();

    // Shape-compatible with AsenaRedisService (isRedisService duck-types on
    // createSubscriber) - the transport must duplicate, never adopt, client
    const service = {
      client: userClient,
      createSubscriber: () => userClient.duplicate(),
    } as any;

    const transport = new RedisMicroserviceTransport(service, { serviceName: 'svc', streamPrefix, blockMs: 200 });

    await transport.init();
    await transport.emit('anything', {});
    await transport.destroy({ drainTimeout: 500 });

    // The user's client must remain fully usable after transport teardown
    expect(await userClient.send('PING', [])).toBe('PONG');
    await userClient.disconnect();
  }, 10_000);
});
