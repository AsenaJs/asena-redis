import { afterEach, describe, expect, it } from 'bun:test';
import { AsenaServerFactory } from '@asenajs/asena';
import { Config, MessageController } from '@asenajs/asena/decorators';
import { EventPattern, MessagePattern } from '@asenajs/asena/microservice';
import { ICoreServiceNames } from '@asenajs/asena/ioc/types';
import { RedisMicroserviceTransport } from '../lib/microservice';
import { BunRedisAdapter } from '../lib/adapter';

const REDIS_URL = 'redis://localhost:6379';

const quietLogger = { info: () => {}, warn: () => {}, error: () => {}, profile: () => {} } as any;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

/**
 * End-to-end scenario over real Redis: a HEADLESS Asena service (no HTTP adapter)
 * driven purely by microservice messages, talked to by a raw client transport -
 * exercising the full chain factory → config → PrepareMicroserviceService → Ulak
 * → RedisMicroserviceTransport.
 */
describe('Headless E2E over Redis', () => {
  let server: any;
  let client: RedisMicroserviceTransport | undefined;
  const streamPrefix = `asena:test:e2e:${crypto.randomUUID().slice(0, 8)}`;

  afterEach(async () => {
    await client?.destroy({ drainTimeout: 500 }).catch(() => {});
    client = undefined;
    await server?.stop();
    server = undefined;

    const cleanup = new BunRedisAdapter(REDIS_URL);

    await cleanup.connect();

    const keys = await cleanup.send('KEYS', [`${streamPrefix}*`]);

    if (Array.isArray(keys) && keys.length) {
      await cleanup.send('DEL', keys.map(String));
    }

    await cleanup.disconnect();
  });

  it('should run a message-driven headless service end-to-end', async () => {
    const paymentsReceived: any[] = [];

    @Config()
    class HeadlessConfig {
      public transport() {
        return {
          microservice: new RedisMicroserviceTransport(
            { url: REDIS_URL },
            { serviceName: 'order-service', streamPrefix, blockMs: 200 },
          ),
        };
      }
    }

    @MessageController('order')
    class OrderHandler {
      @MessagePattern('create')
      public async create(data: any) {
        return { id: 101, ...data };
      }

      @EventPattern({ pattern: 'payment.completed', prefix: false })
      public async onPayment(data: any) {
        paymentsReceived.push(data);
      }
    }

    // 10000-31999: above the well-known range and below the kernel's ephemeral floor
    // (net.ipv4.ip_local_port_range, 32768-60999). Drawing a *server* port from the
    // ephemeral range collides with the outbound sockets the suite itself holds open -
    // including their 60s TIME_WAIT - and Bun.serve then fails with EADDRINUSE.
    const healthPort = 10000 + Math.floor(Math.random() * 22000);

    server = await AsenaServerFactory.create({
      headless: true,
      logger: quietLogger,
      components: [HeadlessConfig, OrderHandler],
      health: { port: healthPort },
    });

    await server.start();

    // No HTTP adapter was registered - the service is truly headless
    expect(server.coreContainer.container.has(ICoreServiceNames.ASENA_ADAPTER)).toBe(false);

    // Health endpoint reports the transport as connected
    const health = await fetch(`http://localhost:${healthPort}/healthz`);
    const healthBody: any = await health.json();

    expect(health.status).toBe(200);
    expect(healthBody.transports.default).toBe('connected');

    // A separate "service" (raw client transport) does RPC against the headless app
    client = new RedisMicroserviceTransport(
      { url: REDIS_URL },
      { serviceName: 'checkout-service', streamPrefix, blockMs: 200 },
    );
    await client.init();

    const reply = await client.send<{ id: number; total: number }>('order.create', { total: 25 }, { timeout: 8000 });

    expect(reply).toEqual({ id: 101, total: 25 });

    // ...and choreography: emits an event the headless app reacts to
    await client.emit('payment.completed', { orderId: 101 });

    const start = Date.now();

    while (!paymentsReceived.length && Date.now() - start < 5000) {
      await sleep(50);
    }

    expect(paymentsReceived).toEqual([{ orderId: 101 }]);
  }, 20000);
});
