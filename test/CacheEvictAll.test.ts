import { afterAll, afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { AsenaRedisService } from '../lib/AsenaRedisService';
import { CachePostProcessor } from '../lib/cache/CachePostProcessor';
import { Cacheable, CacheEvict } from '../lib/decorators/CacheDecorators';
import type { Container } from '@asenajs/asena/container';
import type { RedisOptions } from '../lib/types';

// Needs a live Redis: the SCAN reply shape through `send` and the expiry `SET ... EX` applies are
// claims a stub cannot make, and an unrecognized SCAN reply gives up quietly by design.
const REDIS_URL = 'redis://localhost:6379';

class TestRedisService extends AsenaRedisService {
  public initWithOptions(options: RedisOptions): void {
    this.setRedisOptions(options);
  }
}

const buildContainer = (instance: AsenaRedisService) =>
  ({
    services: { AppRedis: { Class: instance.constructor, instance, singleton: true } },
  }) as unknown as Container;

const wrap = async <T extends object>(instance: T, redis: AsenaRedisService): Promise<T> => {
  const processor = new CachePostProcessor();

  (processor as unknown as { container: unknown }).container = buildContainer(redis);
  (processor as unknown as { serverLogger: unknown }).serverLogger = {
    info: () => {},
    warn: () => {},
    error: () => {},
    debug: () => {},
    profile: () => {},
  };

  return processor.postProcess(instance, instance.constructor);
};

for (const adapter of ['bun', 'node-redis'] as const) {
  describe(`cache against a real Redis (${adapter} adapter)`, () => {
    const prefix = `asena:test:cache:${crypto.randomUUID().slice(0, 8)}`;
    const outsider = `asena:test:cache-outsider:${crypto.randomUUID().slice(0, 8)}`;

    let redis: TestRedisService;

    beforeAll(async () => {
      redis = new TestRedisService();
      redis.initWithOptions({ config: { url: REDIS_URL, name: `cache-${adapter}` }, adapter });
      await redis.onStart();
    });

    afterEach(async () => {
      for (const pattern of [`${prefix}*`, `${outsider}*`]) {
        const keys = await redis.keys(pattern);

        for (const key of keys) await redis.del(key);
      }
    });

    afterAll(async () => {
      await redis.disconnect();
    });

    it('applies the TTL in the same command that writes the value', async () => {
      class UserService {
        @Cacheable({ prefix, ttl: 120, key: (id: string) => id })
        public async findById(id: string) {
          return { id };
        }
      }

      const service = await wrap(new UserService(), redis);

      expect(await service.findById('42')).toEqual({ id: '42' });

      const ttl = await redis.ttl(`${prefix}:42`);

      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(120);
    });

    it('stores no expiry when no ttl is configured', async () => {
      class UserService {
        @Cacheable({ prefix, key: (id: string) => id })
        public async findById(id: string) {
          return { id };
        }
      }

      const service = await wrap(new UserService(), redis);

      await service.findById('no-ttl');

      expect(await redis.ttl(`${prefix}:no-ttl`)).toBe(-1);
    });

    it('serves the second call from Redis', async () => {
      let calls = 0;

      class UserService {
        @Cacheable({ prefix, ttl: 60, key: (id: string) => id })
        public async findById(id: string) {
          calls++;

          return { id, calls };
        }
      }

      const service = await wrap(new UserService(), redis);

      expect(await service.findById('hit')).toEqual({ id: 'hit', calls: 1 });
      expect(await service.findById('hit')).toEqual({ id: 'hit', calls: 1 });
      expect(calls).toBe(1);
    });

    it('allEntries clears the prefix through SCAN and leaves everything else alone', async () => {
      await redis.set(`${prefix}:1`, '{"v":1}');
      await redis.set(`${prefix}:2`, '{"v":2}');
      await redis.set(`${outsider}:1`, '{"v":3}');

      class UserService {
        @CacheEvict({ prefix, allEntries: true })
        public async purge() {
          return undefined;
        }
      }

      const service = await wrap(new UserService(), redis);

      await service.purge();

      expect(await redis.exists(`${prefix}:1`)).toBe(false);
      expect(await redis.exists(`${prefix}:2`)).toBe(false);
      expect(await redis.exists(`${outsider}:1`)).toBe(true);
    });

    it('allEntries clears more keys than one SCAN round returns', async () => {
      // COUNT is 500: this crosses the cursor boundary, so a loop that stops early leaves keys.
      const total = 1200;

      for (let index = 0; index < total; index++) {
        await redis.set(`${prefix}:bulk:${index}`, '{"v":1}');
      }

      class UserService {
        @CacheEvict({ prefix, allEntries: true })
        public async purge() {
          return undefined;
        }
      }

      const service = await wrap(new UserService(), redis);

      await service.purge();

      expect(await redis.keys(`${prefix}*`)).toHaveLength(0);
    });

    it('evicts a single key after the method succeeds', async () => {
      await redis.set(`${prefix}:solo`, '{"v":1}');

      class UserService {
        @CacheEvict({ prefix, key: (id: string) => id })
        public async update(id: string) {
          return id;
        }
      }

      const service = await wrap(new UserService(), redis);

      await service.update('solo');

      expect(await redis.exists(`${prefix}:solo`)).toBe(false);
    });
  });
}
