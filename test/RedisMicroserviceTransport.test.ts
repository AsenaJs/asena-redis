import { afterEach, describe, expect, it } from 'bun:test';
import { UlakError, UlakErrorCode } from '@asenajs/asena/messaging';
import { RedisMicroserviceTransport } from '../lib/microservice';
import { xinfoConsumers } from '../lib/microservice/streamCommands';
import { BunRedisAdapter } from '../lib/adapter';
import type { RedisMicroserviceOptions } from '../lib/types';

const REDIS_URL = 'redis://localhost:6379';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitFor(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now();

  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor timed out');
    }

    await sleep(50);
  }
}

describe('RedisMicroserviceTransport', () => {
  const transports: RedisMicroserviceTransport[] = [];
  const prefixes: string[] = [];

  function uniquePrefix(): string {
    const prefix = `asena:test:ms:${crypto.randomUUID().slice(0, 8)}`;

    prefixes.push(prefix);

    return prefix;
  }

  async function createTransport(
    options: Partial<RedisMicroserviceOptions> & { serviceName: string; streamPrefix: string },
  ) {
    const transport = new RedisMicroserviceTransport(
      { url: REDIS_URL },
      { blockMs: 200, drainTimeout: 2000, ...options },
    );

    transports.push(transport);
    await transport.init();

    return transport;
  }

  afterEach(async () => {
    for (const transport of transports) {
      await transport.destroy({ drainTimeout: 500 }).catch(() => {});
    }

    transports.length = 0;

    // Remove all streams created by this test
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

  it('should require a serviceName', () => {
    expect(() => new RedisMicroserviceTransport({ url: REDIS_URL }, {} as any)).toThrow(/serviceName/);
  });

  it('should round-trip request/response between two instances', async () => {
    const streamPrefix = uniquePrefix();

    const responder = await createTransport({ serviceName: 'order-service', streamPrefix });

    responder.registerMessageHandler('order.create', async (data: any) => ({ id: 42, ...data }));
    await responder.listen();

    const caller = await createTransport({ serviceName: 'checkout-service', streamPrefix });

    await caller.listen(); // no handlers - client-only

    const reply = await caller.send<{ id: number; total: number }>('order.create', { total: 7 });

    expect(reply).toEqual({ id: 42, total: 7 });
  });

  it('should reject with TIMEOUT when nobody responds', async () => {
    const streamPrefix = uniquePrefix();
    const caller = await createTransport({ serviceName: 'caller', streamPrefix });

    try {
      await caller.send('nobody.home', {}, { timeout: 300 });
      expect.unreachable();
    } catch (error) {
      expect(error).toBeInstanceOf(UlakError);
      expect((error as UlakError).code).toBe(UlakErrorCode.TIMEOUT);
    }
  });

  it('should propagate handler errors as final REMOTE_ERROR without retry', async () => {
    const streamPrefix = uniquePrefix();
    let attempts = 0;

    const responder = await createTransport({ serviceName: 'order-service', streamPrefix, claimIdleMs: 400 });

    responder.registerMessageHandler('order.fail', async () => {
      attempts++;
      throw new Error('boom');
    });
    await responder.listen();

    const caller = await createTransport({ serviceName: 'caller', streamPrefix });

    try {
      await caller.send('order.fail', {});
      expect.unreachable();
    } catch (error) {
      expect((error as UlakError).code).toBe(UlakErrorCode.REMOTE_ERROR);
      expect((error as UlakError).message).toContain('boom');
    }

    // RPC errors are final: the sweep must NOT redeliver the request
    await sleep(1500);
    expect(attempts).toBe(1);
  });

  it('should fan out events to every service group but once within a group', async () => {
    const streamPrefix = uniquePrefix();

    const orderReceived: string[] = [];
    const billingReceived: string[] = [];

    // Two replicas of order-service (same group)
    const orderA = await createTransport({ serviceName: 'order-service', streamPrefix });
    const orderB = await createTransport({ serviceName: 'order-service', streamPrefix });

    orderA.registerEventHandler('payment.completed', async (data: any) => {
      orderReceived.push(`A:${data.n}`);
    });
    orderB.registerEventHandler('payment.completed', async (data: any) => {
      orderReceived.push(`B:${data.n}`);
    });

    // One replica of billing-service (different group)
    const billing = await createTransport({ serviceName: 'billing-service', streamPrefix });

    billing.registerEventHandler('payment.completed', async (data: any) => {
      billingReceived.push(String(data.n));
    });

    await orderA.listen();
    await orderB.listen();
    await billing.listen();

    const emitter = await createTransport({ serviceName: 'emitter', streamPrefix });

    for (let i = 0; i < 6; i++) {
      await emitter.emit('payment.completed', { n: i });
    }

    await waitFor(() => orderReceived.length >= 6 && billingReceived.length >= 6);
    await sleep(300);

    // billing group: every event exactly once
    expect(billingReceived.length).toBe(6);

    // order group: every event exactly once ACROSS the two replicas
    expect(orderReceived.length).toBe(6);
  });

  it('should match wildcard event patterns and ack non-matching events', async () => {
    const streamPrefix = uniquePrefix();
    const received: string[] = [];

    const listener = await createTransport({ serviceName: 'listener', streamPrefix });

    listener.registerEventHandler('payment.*', async (_data: any, context: any) => {
      received.push(context.pattern);
    });
    await listener.listen();

    const emitter = await createTransport({ serviceName: 'emitter', streamPrefix });

    await emitter.emit('payment.completed', {});
    await emitter.emit('payment.failed', {});
    await emitter.emit('stock.depleted', {}); // no matching handler - ACKed silently

    await waitFor(() => received.length >= 2);
    await sleep(300);

    expect(received.sort()).toEqual(['payment.completed', 'payment.failed']);
  });

  it('should not lose events emitted while the consumer is down (restart-no-loss)', async () => {
    const streamPrefix = uniquePrefix();

    // First boot creates the consumer group, then the service "deploys" (goes down)
    const firstBoot = await createTransport({ serviceName: 'order-service', streamPrefix });

    firstBoot.registerEventHandler('payment.completed', async () => {});
    await firstBoot.listen();
    await firstBoot.destroy();

    // Event is emitted while order-service is offline
    const emitter = await createTransport({ serviceName: 'emitter', streamPrefix });

    await emitter.emit('payment.completed', { orderId: 99 });

    // Service comes back up with the same group identity
    const received: any[] = [];
    const secondBoot = await createTransport({ serviceName: 'order-service', streamPrefix });

    secondBoot.registerEventHandler('payment.completed', async (data: any) => {
      received.push(data);
    });
    await secondBoot.listen();

    await waitFor(() => received.length === 1);
    expect(received[0]).toEqual({ orderId: 99 });
  });

  it('should redeliver failed events to another replica and count attempts', async () => {
    const streamPrefix = uniquePrefix();
    const attempts: number[] = [];

    // claimIdleMs is small so the sweep (min 1s interval) rescues quickly
    const options = { serviceName: 'order-service', streamPrefix, claimIdleMs: 300, maxRetries: 5 };

    const flaky = await createTransport(options);

    flaky.registerEventHandler('payment.completed', async (_data: any, context: any) => {
      attempts.push(context.attempt);

      if (context.attempt < 2) {
        throw new Error('transient failure');
      }
    });
    await flaky.listen();

    const emitter = await createTransport({ serviceName: 'emitter', streamPrefix });

    await emitter.emit('payment.completed', {});

    // First attempt fails, sweep redelivers with attempt > 1
    await waitFor(() => attempts.some((attempt) => attempt >= 2), 8000);

    expect(attempts[0]).toBe(1);
    expect(Math.max(...attempts)).toBeGreaterThanOrEqual(2);
  });

  it('should move poison events to the DLQ after maxRetries', async () => {
    const streamPrefix = uniquePrefix();
    let calls = 0;

    const poisoned = await createTransport({
      serviceName: 'order-service',
      streamPrefix,
      claimIdleMs: 300,
      maxRetries: 1,
    });

    poisoned.registerEventHandler('payment.completed', async () => {
      calls++;
      throw new Error('always fails');
    });
    await poisoned.listen();

    const emitter = await createTransport({ serviceName: 'emitter', streamPrefix });

    await emitter.emit('payment.completed', { orderId: 1 });

    // Wait until the entry lands in the DLQ stream
    const inspector = new BunRedisAdapter(REDIS_URL);

    await inspector.connect();

    let dlqEntries: any = [];

    await waitFor(() => calls >= 1, 8000);

    const start = Date.now();

    while (Date.now() - start < 10000) {
      dlqEntries = await inspector.send('XRANGE', [`${streamPrefix}:dlq`, '-', '+']);
      if (Array.isArray(dlqEntries) && dlqEntries.length) break;
      await sleep(200);
    }

    await inspector.disconnect();

    expect(Array.isArray(dlqEntries) && dlqEntries.length).toBe(1);

    // DLQ entry carries provenance fields
    const fields: string[] = (dlqEntries[0][1] as any[]).map(String);

    expect(fields).toContain('origin_stream');
    expect(fields).toContain('origin_group');
    expect(fields).toContain('p');
  }, 20000);

  it('should drain in-flight handlers on destroy', async () => {
    const streamPrefix = uniquePrefix();
    let completed = false;

    const worker = await createTransport({ serviceName: 'worker', streamPrefix });

    worker.registerEventHandler('slow.task', async () => {
      await sleep(400);
      completed = true;
    });
    await worker.listen();

    const emitter = await createTransport({ serviceName: 'emitter', streamPrefix });

    await emitter.emit('slow.task', {});

    // Give the consumer a moment to pick the entry up, then destroy mid-handling
    await sleep(300);
    await worker.destroy({ drainTimeout: 2000 });

    expect(completed).toBe(true);
  });

  it('should not start a consumer loop in client-only mode', async () => {
    const streamPrefix = uniquePrefix();
    const clientOnly = await createTransport({ serviceName: 'client-only', streamPrefix });

    await clientOnly.listen();

    // No handlers - no dedicated consumer connection, no groups
    expect((clientOnly as any).consumer).toBeUndefined();

    const inspector = new BunRedisAdapter(REDIS_URL);

    await inspector.connect();

    const keys = await inspector.send('KEYS', [`${streamPrefix}*`]);

    await inspector.disconnect();

    // Client-only instances create no streams at all
    expect(Array.isArray(keys) ? keys.length : 0).toBe(0);
  });

  it('should report connection state', async () => {
    const streamPrefix = uniquePrefix();
    const transport = await createTransport({ serviceName: 'reporter', streamPrefix });

    expect(transport.isConnected).toBe(true);

    await transport.destroy();

    expect(transport.isConnected).toBe(false);
  });

  it('should keep a failed pending entry for the next instance across destroy (no message loss)', async () => {
    const streamPrefix = uniquePrefix();
    let called = false;

    // First instance: handler fails, so the entry stays pending (un-ACKed)
    const first = await createTransport({ serviceName: 'order-service', streamPrefix, claimIdleMs: 2000 });

    first.registerEventHandler('payment.completed', async () => {
      called = true;
      throw new Error('crash before ack');
    });
    await first.listen();

    const emitter = await createTransport({ serviceName: 'emitter', streamPrefix });

    await emitter.emit('payment.completed', { orderId: 7 });
    await waitFor(() => called);

    // Rolling deploy: the instance shuts down while the entry is still pending
    await first.destroy({ drainTimeout: 500 });

    // The pending entry must SURVIVE destroy - unconditional DELCONSUMER would
    // have dropped it from the group with no retry, no DLQ, no trace
    const inspector = new BunRedisAdapter(REDIS_URL);

    await inspector.connect();

    const summary: any = await inspector.send('XPENDING', [`${streamPrefix}:evt`, 'order-service']);

    await inspector.disconnect();

    expect(Number(Array.isArray(summary) ? summary[0] : 0)).toBe(1);

    // Second instance with the same group identity rescues it via the sweep
    const received: any[] = [];
    const second = await createTransport({ serviceName: 'order-service', streamPrefix, claimIdleMs: 2000 });

    second.registerEventHandler('payment.completed', async (data: any) => {
      received.push(data);
    });
    await second.listen();

    await waitFor(() => received.length === 1, 10000);
    expect(received[0]).toEqual({ orderId: 7 });
  }, 20000);

  it('should garbage-collect dead consumers once their PEL is empty', async () => {
    const streamPrefix = uniquePrefix();

    // claimIdleMs 500 → cleanup threshold 2s idle, sweep every 1s
    const live = await createTransport({ serviceName: 'order-service', streamPrefix, claimIdleMs: 500 });

    live.registerEventHandler('payment.completed', async () => {});
    await live.listen();

    // A "ghost" consumer: registered in the group, zero pending, never reads again
    const inspector = new BunRedisAdapter(REDIS_URL);

    await inspector.connect();
    await inspector.send('XREADGROUP', [
      'GROUP',
      'order-service',
      'ghost',
      'COUNT',
      '1',
      'STREAMS',
      `${streamPrefix}:evt`,
      '>',
    ]);

    const start = Date.now();
    let names: string[] = [];

    while (Date.now() - start < 10000) {
      const consumers = await xinfoConsumers(inspector, `${streamPrefix}:evt`, 'order-service');

      names = consumers.map((consumer) => consumer.name);
      if (!names.includes('ghost')) break;
      await sleep(200);
    }

    await inspector.disconnect();

    // Ghost is gone, the live instance's consumer survives
    expect(names).not.toContain('ghost');
    expect(names.length).toBeGreaterThanOrEqual(1);
  }, 15000);

  it('should recover after the consumer group is lost (Redis restart/flush)', async () => {
    const streamPrefix = uniquePrefix();
    const received: any[] = [];

    const listener = await createTransport({ serviceName: 'order-service', streamPrefix, claimIdleMs: 1000 });

    listener.registerEventHandler('payment.completed', async (data: any) => {
      received.push(data);
    });
    await listener.listen();

    // Simulate a Redis restart without persistence: the group vanishes
    const inspector = new BunRedisAdapter(REDIS_URL);

    await inspector.connect();
    await inspector.send('XGROUP', ['DESTROY', `${streamPrefix}:evt`, 'order-service']);
    await inspector.disconnect();

    const emitter = await createTransport({ serviceName: 'emitter', streamPrefix });

    await emitter.emit('payment.completed', { n: 1 });

    // The consumer loop recreates the group from '0' and replays the entry
    await waitFor(() => received.length >= 1, 10000);
    expect(listener.isConnected).toBe(true);
  }, 15000);

  it('should reject handlerTimeout above claimIdleMs and derive a safe default', () => {
    expect(
      () =>
        new RedisMicroserviceTransport(
          { url: REDIS_URL },
          { serviceName: 'x', handlerTimeout: 2000, claimIdleMs: 1000 },
        ),
    ).toThrow(/handlerTimeout/);

    const derived = new RedisMicroserviceTransport({ url: REDIS_URL }, { serviceName: 'x', claimIdleMs: 1000 });

    expect((derived as any).handlerTimeout).toBe(1000);
  });

  it('should keep serving RPC after pub/sub connections are killed (reply resubscribe)', async () => {
    const streamPrefix = uniquePrefix();

    const responder = await createTransport({ serviceName: 'order-service', streamPrefix });

    responder.registerMessageHandler('order.echo', async (data: any) => ({ echoed: data.n }));
    await responder.listen();

    const caller = await createTransport({ serviceName: 'caller', streamPrefix });

    expect(await caller.send('order.echo', { n: 1 }, { timeout: 5000 })).toEqual({ echoed: 1 });

    // Sever every pub/sub connection: reply channels die server-side. Bun
    // reconnects the socket but does NOT replay subscriptions - without the
    // adapter-level resubscribe, every send after this times out forever.
    const admin = new BunRedisAdapter(REDIS_URL);

    await admin.connect();
    await admin.send('CLIENT', ['KILL', 'TYPE', 'pubsub', 'SKIPME', 'yes']);
    await admin.disconnect();

    // Reconnect + resubscribe happen within ~1s; retry until the round-trip works
    const start = Date.now();
    let reply: any;

    while (Date.now() - start < 10000) {
      try {
        reply = await caller.send('order.echo', { n: 2 }, { timeout: 1500 });
        break;
      } catch {
        await sleep(200);
      }
    }

    expect(reply).toEqual({ echoed: 2 });
  }, 20000);

  it('should recover the consumer loop after its blocking connection is killed', async () => {
    const streamPrefix = uniquePrefix();
    const received: any[] = [];

    const listener = await createTransport({ serviceName: 'order-service', streamPrefix });

    listener.registerEventHandler('payment.completed', async (data: any) => {
      received.push(data);
    });
    await listener.listen();

    // Kill every normal connection: the in-flight blocking XREADGROUP neither
    // rejects nor resolves (Bun keeps the dead command slot) - without the
    // wedge watchdog the consumer loop hangs forever while healthz lies
    const admin = new BunRedisAdapter(REDIS_URL);

    await admin.connect();
    await admin.send('CLIENT', ['KILL', 'TYPE', 'normal', 'SKIPME', 'yes']);
    await admin.disconnect();

    await sleep(1000);

    const emitter = await createTransport({ serviceName: 'emitter', streamPrefix });

    await emitter.emit('payment.completed', { n: 42 });

    // Watchdog fires at blockMs+5s, connection is replaced, event delivered
    await waitFor(() => received.length >= 1, 15000);
    expect(received[0]).toEqual({ n: 42 });
    expect(listener.isConnected).toBe(true);
  }, 25000);

  it('should reject empty and wildcard final message patterns', () => {
    const transport = new RedisMicroserviceTransport({ url: REDIS_URL }, { serviceName: 'x' });

    expect(() => transport.registerMessageHandler('', async () => null)).toThrow(/empty/);
    expect(() => transport.registerMessageHandler('order.*.create', async () => null)).toThrow(/wildcard/);
    expect(() => transport.registerEventHandler('', async () => {})).toThrow(/empty/);
  });
});
