import { describe, expect, it, beforeAll, afterAll, afterEach } from 'bun:test';
import { AsenaRedisService } from '../lib/AsenaRedisService';
import type { RedisOptions } from '../lib/types';

const REDIS_URL = 'redis://localhost:6379';
const TEST_PREFIX = 'asena:test:nodesvc:';

class TestRedisService extends AsenaRedisService {
  public initWithOptions(options: RedisOptions) {
    this.setRedisOptions(options);
  }
}

function testKey(name: string): string {
  return `${TEST_PREFIX}${name}`;
}

describe('AsenaRedisService (node-redis adapter)', () => {
  let service: TestRedisService;

  beforeAll(async () => {
    service = new TestRedisService();
    service.initWithOptions({ config: { url: REDIS_URL, name: 'node-test' }, adapter: 'node-redis' });
    await service.onStart();
  });

  afterEach(async () => {
    const keys = await service.keys(`${TEST_PREFIX}*`);

    for (const key of keys) {
      await service.del(key);
    }
  });

  afterAll(async () => {
    await service.disconnect();
  });

  // Lifecycle

  describe('Lifecycle', () => {
    it('should connect via node-redis adapter', async () => {
      expect(await service.testConnection()).toBe(true);
    });

    it('should return false after disconnect', async () => {
      const s = new TestRedisService();

      s.initWithOptions({ config: { url: REDIS_URL }, adapter: 'node-redis' });
      await s.onStart();

      expect(await s.testConnection()).toBe(true);

      await s.disconnect();

      expect(await s.testConnection()).toBe(false);
    });

    it('should expose config', () => {
      expect(service.config.url).toBe(REDIS_URL);
      expect(service.config.name).toBe('node-test');
    });

    it('should expose client', () => {
      expect(service.client).toBeDefined();
      expect(service.client.isConnected).toBe(true);
    });

    it('should create subscriber (duplicate)', async () => {
      const subscriber = await service.createSubscriber();

      expect(subscriber).toBeDefined();
      expect(subscriber.isConnected).toBe(true);

      await subscriber.disconnect();
    });
  });

  // String operations

  describe('String operations', () => {
    it('should set and get', async () => {
      const key = testKey('str');

      await service.set(key, 'world');

      expect(await service.get(key)).toBe('world');
    });

    it('should set with TTL', async () => {
      const key = testKey('str-ttl');

      await service.set(key, 'expiring', 10);
      const ttl = await service.ttl(key);

      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(10);
    });

    it('should delete keys', async () => {
      await service.set(testKey('d1'), 'a');
      await service.set(testKey('d2'), 'b');

      const count = await service.del(testKey('d1'), testKey('d2'));

      expect(count).toBe(2);
    });

    it('should check existence', async () => {
      const key = testKey('ex');

      await service.set(key, 'yes');

      expect(await service.exists(key)).toBe(true);
      expect(await service.exists(testKey('nope'))).toBe(false);
    });

    it('should increment and decrement', async () => {
      const key = testKey('cnt');

      await service.set(key, '0');

      expect(await service.incr(key)).toBe(1);
      expect(await service.decr(key)).toBe(0);
    });
  });

  // Hash operations

  describe('Hash operations', () => {
    it('should set and get hash fields', async () => {
      const key = testKey('hash');

      await service.hmset(key, ['f1', 'v1', 'f2', 'v2']);

      expect(await service.hget(key, 'f1')).toBe('v1');
      expect(await service.hget(key, 'f2')).toBe('v2');
    });

    it('should get multiple hash fields', async () => {
      const key = testKey('hmulti');

      await service.hmset(key, ['a', '1', 'b', '2']);
      const results = await service.hmget(key, ['a', 'b']);

      expect(results).toEqual(['1', '2']);
    });
  });

  // Set operations

  describe('Set operations', () => {
    it('should add, check, and remove members', async () => {
      const key = testKey('set');

      await service.sadd(key, 'x');
      await service.sadd(key, 'y');

      expect(await service.sismember(key, 'x')).toBe(true);
      expect((await service.smembers(key)).sort()).toEqual(['x', 'y']);

      await service.srem(key, 'x');

      expect(await service.sismember(key, 'x')).toBe(false);
    });
  });

  // Raw command

  describe('Raw command', () => {
    it('should send raw PING', async () => {
      const result = await service.send('PING', []);

      expect(result).toBe('PONG');
    });
  });
});
