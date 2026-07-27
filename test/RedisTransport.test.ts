import { describe, expect, it, afterEach, mock } from 'bun:test';
import { RedisTransport } from '../lib/RedisTransport';
import { AsenaRedisService } from '../lib/AsenaRedisService';
import type { RedisOptions } from '../lib/types';

const REDIS_URL = 'redis://localhost:6379';

class TestRedisService extends AsenaRedisService {
  public initWithOptions(options: RedisOptions) {
    this.setRedisOptions(options);
  }
}

function createMockServer() {
  return { publish: mock(() => {}) } as any;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

describe('RedisTransport', () => {
  const transports: RedisTransport[] = [];
  const services: TestRedisService[] = [];

  async function createService(): Promise<TestRedisService> {
    const service = new TestRedisService();

    service.initWithOptions({ config: { url: REDIS_URL } });
    await service.onStart();
    services.push(service);

    return service;
  }

  async function createTransport(
    source: TestRedisService | { url: string },
    channel?: string,
  ): Promise<{ transport: RedisTransport; server: ReturnType<typeof createMockServer> }> {
    const transport = new RedisTransport(source, channel ? { channel } : undefined);
    const server = createMockServer();

    await transport.init(server);
    transports.push(transport);

    return { transport, server };
  }

  afterEach(async () => {
    for (const t of transports) {
      await t.destroy();
    }

    transports.length = 0;

    for (const s of services) {
      await s.disconnect();
    }

    services.length = 0;
  });

  // init

  describe('init() with RedisConfig', () => {
    it('should connect and subscribe', async () => {
      const { transport } = await createTransport({ url: REDIS_URL });

      expect(transport).toBeDefined();
    });

    it('should use custom channel', async () => {
      const { transport } = await createTransport({ url: REDIS_URL }, 'custom:channel');

      expect(transport).toBeDefined();
    });
  });

  describe('init() with AsenaRedisService', () => {
    it('should use service client as publisher', async () => {
      const service = await createService();
      const { transport } = await createTransport(service);

      expect(transport).toBeDefined();
    });
  });

  // publish — string data

  describe('publish() — string data', () => {
    it('should deliver locally via server.publish', async () => {
      const { transport, server } = await createTransport({ url: REDIS_URL });

      transport.publish('test-topic', 'hello');

      expect(server.publish).toHaveBeenCalledWith('test-topic', 'hello');
    });
  });

  // publish — binary data

  describe('publish() — binary data', () => {
    it('should handle ArrayBuffer', async () => {
      const { transport, server } = await createTransport({ url: REDIS_URL });
      const data = new TextEncoder().encode('binary-data').buffer;

      transport.publish('bin-topic', data);

      expect(server.publish).toHaveBeenCalledTimes(1);
      expect(server.publish.mock.calls[0][0]).toBe('bin-topic');
    });

    it('should handle Uint8Array', async () => {
      const { transport, server } = await createTransport({ url: REDIS_URL });
      const data = new TextEncoder().encode('uint8-data');

      transport.publish('uint8-topic', data);

      expect(server.publish).toHaveBeenCalledTimes(1);
    });
  });

  // Cross-pod messaging

  describe('Cross-pod messaging', () => {
    it('should deliver string messages to other pods', async () => {
      const channel = `test:crosspod:${Date.now()}`;
      const { transport: transportA, server: serverA } = await createTransport({ url: REDIS_URL }, channel);
      const { server: serverB } = await createTransport({ url: REDIS_URL }, channel);

      // Give subscribers time to settle
      await sleep(100);

      transportA.publish('room-1', 'hello from A');

      // Wait for Redis pub/sub delivery
      await sleep(200);

      // Transport A: local delivery only (1 call from publish, dedup skips Redis echo)
      expect(serverA.publish).toHaveBeenCalledTimes(1);

      // Transport B: received from Redis and delivered locally
      expect(serverB.publish).toHaveBeenCalledTimes(1);
      expect(serverB.publish.mock.calls[0][0]).toBe('room-1');
      expect(serverB.publish.mock.calls[0][1]).toBe('hello from A');
    });

    it('should deliver binary messages to other pods', async () => {
      const channel = `test:binary:${Date.now()}`;
      const { transport: transportA } = await createTransport({ url: REDIS_URL }, channel);
      const { server: serverB } = await createTransport({ url: REDIS_URL }, channel);

      await sleep(100);

      const original = new TextEncoder().encode('binary cross-pod');

      transportA.publish('bin-room', original.buffer);

      await sleep(200);

      expect(serverB.publish).toHaveBeenCalledTimes(1);
      expect(serverB.publish.mock.calls[0][0]).toBe('bin-room');

      // Verify data is an ArrayBuffer and content matches
      const received = serverB.publish.mock.calls[0][1];

      expect(received).toBeInstanceOf(ArrayBuffer);

      const decoded = new TextDecoder().decode(new Uint8Array(received));

      expect(decoded).toBe('binary cross-pod');
    });

    it('should not deliver own messages back', async () => {
      const channel = `test:dedup:${Date.now()}`;
      const { transport, server } = await createTransport({ url: REDIS_URL }, channel);

      await sleep(100);

      transport.publish('topic', 'data');

      await sleep(200);

      // Only 1 call — the direct local delivery in publish(), NOT from Redis echo
      expect(server.publish).toHaveBeenCalledTimes(1);
    });
  });

  // destroy

  describe('destroy()', () => {
    it('should not close publisher when using service', async () => {
      const service = await createService();
      const { transport } = await createTransport(service);

      await transport.destroy();

      // Service client should still be connected
      expect(await service.testConnection()).toBe(true);

      // Remove from cleanup list since already destroyed
      transports.pop();
    });

    it('should close publisher when using RedisConfig', async () => {
      const { transport } = await createTransport({ url: REDIS_URL });

      await transport.destroy();

      // Remove from cleanup list
      transports.pop();
    });
  });
});
