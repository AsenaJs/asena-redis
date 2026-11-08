import { afterEach, describe, expect, it } from 'bun:test';
import { AsenaServerFactory } from '@asenajs/asena';
import { Service } from '@asenajs/asena/decorators';
import { AsenaRedisService } from '../lib/AsenaRedisService';
import { CachePostProcessor } from '../lib/cache/CachePostProcessor';
import { Cacheable, CacheEvict } from '../lib/decorators/CacheDecorators';
import { CacheConfig } from '../lib/decorators/CacheConfig';
import { Redis, RedisCache } from '../lib/decorators';

const REDIS_URL = 'redis://localhost:6379';

const quietLogger = { info: () => {}, warn: () => {}, error: () => {}, debug: () => {}, profile: () => {} } as any;

// A real boot is the only proof of the ordering the design rests on: methods are wrapped before
// the Redis service's own @OnStart has connected, so the client must be resolved on first call.
describe('cache through a real application boot', () => {
  let server: any;

  afterEach(async () => {
    await server?.stop();
    server = undefined;
  });

  it('caches across calls, with the client resolved lazily after start', async () => {
    const prefix = `asena:test:cache-boot:${crypto.randomUUID().slice(0, 8)}`;

    @Redis({ config: { url: REDIS_URL, name: 'BootRedis' }, logger: quietLogger })
    class BootRedis extends AsenaRedisService {}

    @RedisCache({ ttl: 60 })
    class AppCache extends CachePostProcessor {}

    @Service()
    @CacheConfig({ prefix })
    class CountingService {
      public calls = 0;

      @Cacheable({ key: (id: string) => id })
      public async findById(id: string) {
        this.calls++;

        return { id, calls: this.calls };
      }

      @CacheEvict({ key: (id: string) => id })
      public async update(id: string) {
        return id;
      }
    }

    server = await AsenaServerFactory.create({
      headless: true,
      logger: quietLogger,
      components: [BootRedis, AppCache, CountingService],
      keepAlive: false,
    });

    await server.start();

    const service = (await server.coreContainer.container.resolve('CountingService')) as CountingService;
    const redis = (await server.coreContainer.container.resolve('BootRedis')) as BootRedis;

    try {
      // The counter is the assertion: a wrong ordering fails open, so the return value stays right.
      expect(await service.findById('1')).toEqual({ id: '1', calls: 1 });
      expect(await service.findById('1')).toEqual({ id: '1', calls: 1 });
      expect(service.calls).toBe(1);

      expect(await redis.exists(`${prefix}:1`)).toBe(true);
      expect(await redis.ttl(`${prefix}:1`)).toBeGreaterThan(0);

      await service.update('1');
      expect(await redis.exists(`${prefix}:1`)).toBe(false);

      expect(await service.findById('1')).toEqual({ id: '1', calls: 2 });
    } finally {
      for (const key of await redis.keys(`${prefix}*`)) await redis.del(key);
    }
  }, 20000);

  it('injects the framework logger into the post-processor, so contained failures are visible', async () => {
    // Asserted directly: the `?? console` fallback in warn() makes a failed injection look working.
    const lines: string[] = [];
    const capturingLogger = {
      info: () => {},
      warn: (message: string) => lines.push(message),
      error: () => {},
      debug: () => {},
      profile: () => {},
    } as any;

    @Redis({ config: { url: REDIS_URL, name: 'LoggerRedis' }, logger: quietLogger })
    class LoggerRedis extends AsenaRedisService {}

    @RedisCache()
    class AppCache extends CachePostProcessor {}

    @Service()
    class CachedService {
      @Cacheable({ prefix: 'asena:test:cache-logger', key: () => 'k' })
      public async find() {
        return 'live';
      }
    }

    server = await AsenaServerFactory.create({
      headless: true,
      logger: capturingLogger,
      components: [LoggerRedis, AppCache, CachedService],
      keepAlive: false,
    });

    await server.start();

    const processor = (await server.coreContainer.container.resolve('AppCache')) as CachePostProcessor;

    expect((processor as unknown as { serverLogger?: unknown }).serverLogger).toBe(capturingLogger);

    const service = (await server.coreContainer.container.resolve('CachedService')) as CachedService;

    // After stop() the client is gone: the read fails open and the warning must reach this logger.
    await server.stop();
    server = undefined;

    expect(await service.find()).toBe('live');
    expect(lines.some((line) => line.includes('[RedisCache]'))).toBe(true);
  }, 20000);

  it('stores an entry with the largest ttl the bootstrap accepts', async () => {
    const prefix = `asena:test:cache-max-ttl:${crypto.randomUUID().slice(0, 8)}`;
    const warnings: string[] = [];

    @Redis({ config: { url: REDIS_URL, name: 'MaxTtlRedis' }, logger: quietLogger })
    class MaxTtlRedis extends AsenaRedisService {}

    @RedisCache({ ttl: Number.MAX_SAFE_INTEGER })
    class AppCache extends CachePostProcessor {}

    @Service()
    class MaxTtlService {
      @Cacheable({ prefix, key: () => 'k' })
      public async find() {
        return 'live';
      }
    }

    server = await AsenaServerFactory.create({
      headless: true,
      logger: { ...quietLogger, warn: (message: string) => warnings.push(message) },
      components: [MaxTtlRedis, AppCache, MaxTtlService],
      keepAlive: false,
    });

    await server.start();

    const service = (await server.coreContainer.container.resolve('MaxTtlService')) as MaxTtlService;
    const redis = (await server.coreContainer.container.resolve('MaxTtlRedis')) as MaxTtlRedis;

    try {
      expect(await service.find()).toBe('live');
      expect(warnings.filter((message) => message.includes('cache write'))).toEqual([]);
      expect(await redis.ttl(`${prefix}:k`)).toBe(Number.MAX_SAFE_INTEGER);
    } finally {
      for (const key of await redis.keys(`${prefix}*`)) await redis.del(key);
    }
  }, 20000);

  it('leaves a component with no cache metadata untouched', async () => {
    @Redis({ config: { url: REDIS_URL, name: 'PlainRedis' }, logger: quietLogger })
    class PlainRedis extends AsenaRedisService {}

    @RedisCache()
    class AppCache extends CachePostProcessor {}

    @Service()
    class PlainService {
      public async work() {
        return 'done';
      }
    }

    server = await AsenaServerFactory.create({
      headless: true,
      logger: quietLogger,
      components: [PlainRedis, AppCache, PlainService],
      keepAlive: false,
    });

    await server.start();

    const service = (await server.coreContainer.container.resolve('PlainService')) as PlainService;

    expect(await service.work()).toBe('done');
  }, 20000);

  it('caches nothing when the application has no @RedisCache class', async () => {
    const prefix = `asena:test:cache-inactive:${crypto.randomUUID().slice(0, 8)}`;

    @Redis({ config: { url: REDIS_URL, name: 'InactiveRedis' }, logger: quietLogger })
    class InactiveRedis extends AsenaRedisService {}

    @Service()
    class UncachedService {
      public calls = 0;

      @Cacheable({ prefix, key: () => 'k' })
      public async find() {
        this.calls++;

        return this.calls;
      }
    }

    server = await AsenaServerFactory.create({
      headless: true,
      logger: quietLogger,
      components: [InactiveRedis, UncachedService],
      keepAlive: false,
    });

    await server.start();

    const service = (await server.coreContainer.container.resolve('UncachedService')) as UncachedService;
    const redis = (await server.coreContainer.container.resolve('InactiveRedis')) as InactiveRedis;

    expect(await service.find()).toBe(1);
    expect(await service.find()).toBe(2);
    expect(await redis.keys(`${prefix}*`)).toHaveLength(0);
  }, 20000);

  it('boots with two @Redis services and only fails on the first cache call', async () => {
    const prefix = `asena:test:cache-ambiguous:${crypto.randomUUID().slice(0, 8)}`;

    @Redis({ config: { url: REDIS_URL, name: 'FirstRedis' }, logger: quietLogger })
    class FirstRedis extends AsenaRedisService {}

    @Redis({ config: { url: REDIS_URL, name: 'SecondRedis' }, logger: quietLogger })
    class SecondRedis extends AsenaRedisService {}

    @RedisCache()
    class AppCache extends CachePostProcessor {}

    @Service()
    class AmbiguousService {
      @Cacheable({ prefix, key: () => 'k' })
      public async find() {
        return 'live';
      }
    }

    server = await AsenaServerFactory.create({
      headless: true,
      logger: quietLogger,
      components: [FirstRedis, SecondRedis, AppCache, AmbiguousService],
      keepAlive: false,
    });

    await server.start();

    const service = (await server.coreContainer.container.resolve('AmbiguousService')) as AmbiguousService;

    await expect(service.find()).rejects.toThrow(/2 AsenaRedisService components are registered/);
  }, 20000);
});
