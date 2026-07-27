import { describe, expect, it, mock } from 'bun:test';
import { AsenaRedisService } from '../lib/AsenaRedisService';
import { RedisTransport } from '../lib/RedisTransport';
import type { RedisOptions } from '../lib/types';

const INVALID_URL = 'redis://localhost:19999';

class TestRedisService extends AsenaRedisService {
  public initWithOptions(options: RedisOptions) {
    this.setRedisOptions(options);
  }
}

function createMockServer() {
  return { publish: mock(() => {}) } as any;
}

describe('Connection Failure — AsenaRedisService', () => {
  it('should throw on connection to invalid host', async () => {
    const service = new TestRedisService();

    service.initWithOptions({
      config: { url: INVALID_URL, autoReconnect: false, connectionTimeout: 1000 },
      logger: { info: () => {}, warn: () => {}, error: () => {}, profile: () => {} },
    });

    await expect(service.onStart()).rejects.toThrow('Redis connection failed');
  }, 10000);

  it('should throw on operations after disconnect', async () => {
    const service = new TestRedisService();

    service.initWithOptions({ config: { url: 'redis://localhost:6379' } });
    await service.onStart();
    await service.disconnect();

    await expect(service.get('any-key')).rejects.toThrow('Redis client not initialized');
  });

  it('should throw when accessing client after disconnect', async () => {
    const service = new TestRedisService();

    service.initWithOptions({ config: { url: 'redis://localhost:6379' } });
    await service.onStart();
    await service.disconnect();

    expect(() => service.client).toThrow('Redis client not initialized');
  });
});

describe('Connection Failure — RedisTransport', () => {
  it('should throw on init with invalid config', async () => {
    const transport = new RedisTransport({
      host: 'localhost',
      port: 19999,
      autoReconnect: false,
      connectionTimeout: 1000,
    });
    const server = createMockServer();

    await expect(transport.init(server)).rejects.toThrow();
  }, 10000);

  it('should not throw on double destroy', async () => {
    const transport = new RedisTransport({ url: 'redis://localhost:6379' });
    const server = createMockServer();

    await transport.init(server);
    await transport.destroy();
    await transport.destroy();
  });
});
