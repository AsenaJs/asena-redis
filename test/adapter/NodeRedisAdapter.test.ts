import { describe, expect, it, beforeAll, afterAll, afterEach } from 'bun:test';
import { NodeRedisAdapter } from '../../lib/adapter/NodeRedisAdapter';

const REDIS_URL = 'redis://localhost:6379';
const TEST_PREFIX = 'asena:test:node:';

function testKey(name: string): string {
  return `${TEST_PREFIX}${name}`;
}

describe('NodeRedisAdapter', () => {
  let adapter: NodeRedisAdapter;

  beforeAll(async () => {
    adapter = NodeRedisAdapter.create(REDIS_URL);
    await adapter.connect();
  });

  afterEach(async () => {
    const keys = await adapter.keys(`${TEST_PREFIX}*`);

    for (const key of keys) {
      await adapter.del(key);
    }
  });

  afterAll(async () => {
    await adapter.disconnect();
  });

  // Lifecycle

  describe('Lifecycle', () => {
    it('should create and connect', () => {
      expect(adapter.isConnected).toBe(true);
    });

    it('should duplicate and connect', async () => {
      const dup = await adapter.duplicate();

      expect(dup.isConnected).toBe(true);

      await dup.disconnect();
    });

    it('should report disconnected after disconnect', async () => {
      const tmp = NodeRedisAdapter.create(REDIS_URL);

      await tmp.connect();
      expect(tmp.isConnected).toBe(true);

      await tmp.disconnect();
      expect(tmp.isConnected).toBe(false);
    });
  });

  // String operations

  describe('String operations', () => {
    it('should set and get a value', async () => {
      const key = testKey('str1');

      await adapter.set(key, 'hello');
      const result = await adapter.get(key);

      expect(result).toBe('hello');
    });

    it('should return null for non-existent key', async () => {
      const result = await adapter.get(testKey('nonexistent'));

      expect(result).toBeNull();
    });

    it('should delete a key', async () => {
      const key = testKey('del1');

      await adapter.set(key, 'value');
      const count = await adapter.del(key);

      expect(count).toBeGreaterThanOrEqual(1);
      expect(await adapter.get(key)).toBeNull();
    });

    it('should check existence (returns boolean, not number)', async () => {
      const key = testKey('exists');

      await adapter.set(key, 'yes');

      expect(await adapter.exists(key)).toBe(true);
      expect(await adapter.exists(testKey('nope'))).toBe(false);
    });

    it('should increment and decrement', async () => {
      const key = testKey('counter');

      await adapter.set(key, '0');

      expect(await adapter.incr(key)).toBe(1);
      expect(await adapter.incr(key)).toBe(2);
      expect(await adapter.decr(key)).toBe(1);
    });
  });

  // Expiration

  describe('Expiration', () => {
    it('should set and check expire', async () => {
      const key = testKey('expire');

      await adapter.set(key, 'val');
      const result = await adapter.expire(key, 30);

      expect(result).toBe(1);

      const ttl = await adapter.ttl(key);

      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(30);
    });

    it('should find keys by pattern', async () => {
      await adapter.set(testKey('pat-a'), '1');
      await adapter.set(testKey('pat-b'), '2');

      const keys = await adapter.keys(`${TEST_PREFIX}pat-*`);

      expect(keys).toHaveLength(2);
      expect(keys).toContain(testKey('pat-a'));
      expect(keys).toContain(testKey('pat-b'));
    });
  });

  // Hash operations (camelCase normalization: hGet → hget, hSet → hmset)

  describe('Hash operations', () => {
    it('should set and get hash field', async () => {
      const key = testKey('hash');

      await adapter.hmset(key, ['field1', 'value1', 'field2', 'value2']);
      const result = await adapter.hget(key, 'field1');

      expect(result).toBe('value1');
    });

    it('should get multiple hash fields', async () => {
      const key = testKey('hash-multi');

      await adapter.hmset(key, ['a', '1', 'b', '2', 'c', '3']);
      const results = await adapter.hmget(key, ['a', 'c']);

      expect(results).toEqual(['1', '3']);
    });

    it('should return null for non-existent hash field', async () => {
      const key = testKey('hash-miss');

      await adapter.hmset(key, ['x', 'y']);
      const result = await adapter.hget(key, 'missing');

      expect(result).toBeNull();
    });
  });

  // Set operations (camelCase normalization: sAdd → sadd, sRem → srem, etc.)

  describe('Set operations', () => {
    it('should add and list members', async () => {
      const key = testKey('set');

      await adapter.sadd(key, 'a');
      await adapter.sadd(key, 'b');
      await adapter.sadd(key, 'c');

      const members = await adapter.smembers(key);

      expect(members).toHaveLength(3);
      expect(members).toContain('a');
      expect(members).toContain('b');
      expect(members).toContain('c');
    });

    it('should check membership', async () => {
      const key = testKey('set-member');

      await adapter.sadd(key, 'exists');

      expect(await adapter.sismember(key, 'exists')).toBe(true);
      expect(await adapter.sismember(key, 'nope')).toBe(false);
    });

    it('should remove member', async () => {
      const key = testKey('set-rem');

      await adapter.sadd(key, 'x');
      await adapter.sadd(key, 'y');
      await adapter.srem(key, 'x');

      expect(await adapter.sismember(key, 'x')).toBe(false);
      expect(await adapter.sismember(key, 'y')).toBe(true);
    });
  });

  // Raw command (sendCommand normalization)

  describe('Raw command', () => {
    it('should send raw PING via sendCommand mapping', async () => {
      const result = await adapter.send('PING', []);

      expect(result).toBe('PONG');
    });
  });

  // Pub/Sub

  describe('Pub/Sub', () => {
    it('should publish and subscribe', async () => {
      const channel = `${TEST_PREFIX}pubsub:${Date.now()}`;
      const subscriber = await adapter.duplicate();

      const received: string[] = [];

      await subscriber.subscribe(channel, (message: string) => {
        received.push(message);
      });

      // Give subscriber time to settle
      await new Promise((resolve) => setTimeout(resolve, 100));

      await adapter.publish(channel, 'hello-node-redis');

      // Wait for delivery
      await new Promise((resolve) => setTimeout(resolve, 200));

      expect(received).toContain('hello-node-redis');

      await subscriber.unsubscribe(channel);
      await subscriber.disconnect();
    });
  });

  // mapOptions (indirect — via create with options)

  describe('create() with config options', () => {
    it('should connect with connectionTimeout option', async () => {
      const tmp = NodeRedisAdapter.create(REDIS_URL, { connectionTimeout: 5000 });

      await tmp.connect();
      expect(tmp.isConnected).toBe(true);

      await tmp.disconnect();
    });

    it('should connect with enableOfflineQueue option', async () => {
      const tmp = NodeRedisAdapter.create(REDIS_URL, { enableOfflineQueue: false });

      await tmp.connect();
      expect(tmp.isConnected).toBe(true);

      await tmp.disconnect();
    });

    it('should connect with autoReconnect false', async () => {
      const tmp = NodeRedisAdapter.create(REDIS_URL, { autoReconnect: false });

      await tmp.connect();
      expect(tmp.isConnected).toBe(true);

      await tmp.disconnect();
    });

    it('should connect with maxRetries option', async () => {
      const tmp = NodeRedisAdapter.create(REDIS_URL, { maxRetries: 3 });

      await tmp.connect();
      expect(tmp.isConnected).toBe(true);

      await tmp.disconnect();
    });

    it('should silently ignore Bun-only options', async () => {
      const tmp = NodeRedisAdapter.create(REDIS_URL, { idleTimeout: 5000, enableAutoPipelining: true });

      await tmp.connect();
      expect(tmp.isConnected).toBe(true);

      await tmp.disconnect();
    });
  });
});
