import { describe, expect, it, beforeAll, afterAll, afterEach } from 'bun:test';
import { AsenaRedisService } from '../lib/AsenaRedisService';
import type { RedisClientAdapter } from '../lib/adapter';
import type { RedisOptions } from '../lib/types';

const REDIS_URL = 'redis://localhost:6379';
const TEST_PREFIX = 'asena:test:';

class TestRedisService extends AsenaRedisService {
  public initWithOptions(options: RedisOptions) {
    this.setRedisOptions(options);
  }
}

/**
 * A connection whose close path the test dictates. Only the members the service touches are
 * implemented - it never reaches Redis, which is the point: a real duplicate cannot be made
 * to fail on close.
 */
class FakeConnection {
  public isConnected = true;

  /** Handed out by duplicate() in order, so a test picks which subscriber breaks. */
  public readonly duplicates: FakeConnection[] = [];

  private handedOut = 0;

  public constructor(private readonly closeFails = false) {}

  public async connect(): Promise<void> {
    this.isConnected = true;
  }

  public async disconnect(): Promise<void> {
    if (this.closeFails) {
      throw new Error('subscriber close failed');
    }

    this.isConnected = false;
  }

  public async duplicate(): Promise<RedisClientAdapter> {
    const subscriber = this.duplicates[this.handedOut++];

    if (!subscriber) {
      throw new Error('no duplicate prepared for this test');
    }

    return subscriber as unknown as RedisClientAdapter;
  }
}

function testKey(name: string): string {
  return `${TEST_PREFIX}${name}`;
}

describe('AsenaRedisService', () => {
  let service: TestRedisService;

  beforeAll(async () => {
    service = new TestRedisService();
    service.initWithOptions({ config: { url: REDIS_URL, name: 'test' } });
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
    it('should throw if options not set', async () => {
      const s = new TestRedisService();

      await expect(s.onStart()).rejects.toThrow('Redis options not initialized');
    });

    it('should connect to Redis', async () => {
      expect(await service.testConnection()).toBe(true);
    });

    it('should return false after disconnect', async () => {
      const s = new TestRedisService();

      s.initWithOptions({ config: { url: REDIS_URL } });
      await s.onStart();

      expect(await s.testConnection()).toBe(true);

      await s.disconnect();

      expect(await s.testConnection()).toBe(false);
    });

    it('should handle disconnect when no client', async () => {
      const s = new TestRedisService();

      await s.disconnect(); // should not throw
    });
  });

  // Stop hook - the sockets a server.stop() has to leave closed

  describe('Stop hook', () => {
    it('should close the client and every subscriber it handed out', async () => {
      const s = new TestRedisService();

      s.initWithOptions({ config: { url: REDIS_URL } });
      await s.onStart();

      const first = await s.createSubscriber();
      const second = await s.createSubscriber();

      await s.onStop();

      expect(await s.testConnection()).toBe(false);
      expect(first.isConnected).toBe(false);
      expect(second.isConnected).toBe(false);
    });

    it('should tolerate a subscriber its owner already closed', async () => {
      const s = new TestRedisService();

      s.initWithOptions({ config: { url: REDIS_URL } });
      await s.onStart();

      // What RedisTransport.destroy() does with the subscriber it asked for
      const subscriber = await s.createSubscriber();

      await subscriber.disconnect();

      await s.onStop(); // should not throw

      expect(await s.testConnection()).toBe(false);
    });

    it('should close the rest when one subscriber fails to close', async () => {
      const logged: unknown[] = [];
      const client = new FakeConnection();
      const failing = new FakeConnection(true);
      const healthy = new FakeConnection();

      client.duplicates.push(failing, healthy);

      const s = new TestRedisService();

      s.initWithOptions({
        config: { url: REDIS_URL },
        client: client as unknown as RedisClientAdapter,
        logger: { info: () => {}, error: (...args: unknown[]) => logged.push(args) } as any,
      });
      await s.onStart();
      await s.createSubscriber();
      await s.createSubscriber();

      await s.onStop();

      expect(healthy.isConnected).toBe(false);
      expect(client.isConnected).toBe(false);
      expect(logged).toHaveLength(1);
    });

    it('should not close subscribers on a plain disconnect()', async () => {
      const s = new TestRedisService();

      s.initWithOptions({ config: { url: REDIS_URL } });
      await s.onStart();

      const subscriber = await s.createSubscriber();

      // disconnect() drops the main connection only - a caller reading from its own
      // subscriber keeps it
      await s.disconnect();

      expect(subscriber.isConnected).toBe(true);

      await subscriber.disconnect();
    });

    it('should survive a stop that runs twice', async () => {
      const s = new TestRedisService();

      s.initWithOptions({ config: { url: REDIS_URL } });
      await s.onStart();
      await s.createSubscriber();

      await s.onStop();
      await s.onStop(); // should not throw
    });
  });

  // String operations

  describe('String operations', () => {
    it('should set and get a value', async () => {
      const key = testKey('str1');

      await service.set(key, 'hello');
      const result = await service.get(key);

      expect(result).toBe('hello');
    });

    it('should return null for non-existent key', async () => {
      const result = await service.get(testKey('nonexistent'));

      expect(result).toBeNull();
    });

    it('should set with TTL', async () => {
      const key = testKey('str-ttl');

      await service.set(key, 'expiring', 10);
      const ttl = await service.ttl(key);

      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(10);
    });

    it('should delete keys and return count', async () => {
      const k1 = testKey('del1');
      const k2 = testKey('del2');

      await service.set(k1, 'a');
      await service.set(k2, 'b');

      const count = await service.del(k1, k2);

      expect(count).toBe(2);
      expect(await service.get(k1)).toBeNull();
      expect(await service.get(k2)).toBeNull();
    });

    it('should check existence', async () => {
      const key = testKey('exists');

      await service.set(key, 'yes');

      expect(await service.exists(key)).toBe(true);
      expect(await service.exists(testKey('nope'))).toBe(false);
    });

    it('should increment and decrement', async () => {
      const key = testKey('counter');

      await service.set(key, '0');

      expect(await service.incr(key)).toBe(1);
      expect(await service.incr(key)).toBe(2);
      expect(await service.decr(key)).toBe(1);
    });

    it('should set and check expire', async () => {
      const key = testKey('expire');

      await service.set(key, 'val');
      await service.expire(key, 30);

      const ttl = await service.ttl(key);

      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(30);
    });

    it('should find keys by pattern', async () => {
      await service.set(testKey('pattern-a'), '1');
      await service.set(testKey('pattern-b'), '2');

      const keys = await service.keys(`${TEST_PREFIX}pattern-*`);

      expect(keys).toHaveLength(2);
      expect(keys).toContain(testKey('pattern-a'));
      expect(keys).toContain(testKey('pattern-b'));
    });
  });

  // Hash operations

  describe('Hash operations', () => {
    it('should set and get hash field', async () => {
      const key = testKey('hash');

      await service.hmset(key, ['field1', 'value1', 'field2', 'value2']);
      const result = await service.hget(key, 'field1');

      expect(result).toBe('value1');
    });

    it('should get multiple hash fields', async () => {
      const key = testKey('hash-multi');

      await service.hmset(key, ['a', '1', 'b', '2', 'c', '3']);
      const results = await service.hmget(key, ['a', 'c']);

      expect(results).toEqual(['1', '3']);
    });

    it('should return null for non-existent hash field', async () => {
      const key = testKey('hash-miss');

      await service.hmset(key, ['x', 'y']);
      const result = await service.hget(key, 'missing');

      expect(result).toBeNull();
    });
  });

  // Set operations

  describe('Set operations', () => {
    it('should add and list members', async () => {
      const key = testKey('set');

      await service.sadd(key, 'a');
      await service.sadd(key, 'b');
      await service.sadd(key, 'c');

      const members = await service.smembers(key);

      expect(members).toHaveLength(3);
      expect(members).toContain('a');
      expect(members).toContain('b');
      expect(members).toContain('c');
    });

    it('should check membership', async () => {
      const key = testKey('set-member');

      await service.sadd(key, 'exists');

      expect(await service.sismember(key, 'exists')).toBe(true);
      expect(await service.sismember(key, 'nope')).toBe(false);
    });

    it('should remove member', async () => {
      const key = testKey('set-rem');

      await service.sadd(key, 'x');
      await service.sadd(key, 'y');
      await service.srem(key, 'x');

      expect(await service.sismember(key, 'x')).toBe(false);
      expect(await service.sismember(key, 'y')).toBe(true);
    });
  });

  // ping - bounded against a client whose PING behaviour the test dictates

  describe('ping', () => {
    class FakePingClient {
      public isConnected = false;
      public readonly sent: Array<{ command: string; args: string[] }> = [];

      public constructor(private readonly onSend: () => Promise<any> = () => Promise.resolve('PONG')) {}

      public async connect(): Promise<void> {
        this.isConnected = true;
      }

      public async disconnect(): Promise<void> {
        this.isConnected = false;
      }

      public async duplicate(): Promise<RedisClientAdapter> {
        return this as unknown as RedisClientAdapter;
      }

      public async send(command: string, args: string[]): Promise<any> {
        this.sent.push({ command, args });

        return this.onSend();
      }
    }

    async function serviceWith(client: FakePingClient): Promise<TestRedisService> {
      const service = new TestRedisService();

      service.initWithOptions({ config: { url: REDIS_URL }, client: client as unknown as RedisClientAdapter });
      await service.onStart();

      return service;
    }

    it("should resolve with 'PONG'", async () => {
      const client = new FakePingClient(() => Promise.resolve('PONG'));
      const service = await serviceWith(client);

      expect(await service.ping()).toBe('PONG');
      expect(client.sent).toEqual([{ command: 'PING', args: [] }]);
    });

    it('should reject with the timeout message when PING never settles', async () => {
      const client = new FakePingClient(() => new Promise(() => {}));
      const service = await serviceWith(client);

      await expect(service.ping(20)).rejects.toThrow('Redis PING timed out after 20ms');
    });

    it('should return true from testConnection on PONG', async () => {
      const service = await serviceWith(new FakePingClient());

      expect(await service.testConnection()).toBe(true);
    });

    it('should return false from testConnection on timeout', async () => {
      const service = await serviceWith(new FakePingClient(() => new Promise(() => {})));

      expect(await service.testConnection()).toBe(false);
    });

    it('should return false from testConnection without sending when disconnected', async () => {
      const client = new FakePingClient();
      const service = await serviceWith(client);

      client.isConnected = false;

      expect(await service.testConnection()).toBe(false);
      expect(client.sent).toHaveLength(0);
    });
  });

  // Raw command & client access

  describe('Raw command & client access', () => {
    it('should send raw PING', async () => {
      const result = await service.send('PING', []);

      expect(result).toBe('PONG');
    });

    it('should expose client', () => {
      expect(service.client).toBeDefined();
      expect(service.client.isConnected).toBe(true);
    });

    it('should expose config', () => {
      expect(service.config.url).toBe(REDIS_URL);
      expect(service.config.name).toBe('test');
    });

    it('should throw when accessing client before init', () => {
      const s = new TestRedisService();

      expect(() => s.client).toThrow('Redis client not initialized');
    });

    it('should throw when accessing config before init', () => {
      const s = new TestRedisService();

      expect(() => s.config).toThrow('Redis options not initialized');
    });

    it('should create subscriber (duplicate)', async () => {
      const subscriber = await service.createSubscriber();

      expect(subscriber).toBeDefined();
      expect(subscriber.isConnected).toBe(true);

      await subscriber.disconnect();
    });
  });
});
