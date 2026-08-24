import { describe, expect, it } from 'bun:test';
import { Redis } from '../lib/decorators';
import { AsenaRedisService } from '../lib/AsenaRedisService';

describe('@Redis decorator', () => {
  it('should return a class extending AsenaRedisService', () => {
    @Redis({ config: { url: 'redis://localhost:6379' } })
    class MyRedis extends AsenaRedisService {}

    const instance = new MyRedis();

    expect(instance).toBeInstanceOf(AsenaRedisService);
  });

  it('should preserve original class name', () => {
    @Redis({ config: { url: 'redis://localhost:6379' } })
    class CacheService extends AsenaRedisService {}

    expect(CacheService.name).toBe('CacheService');
  });

  it('should copy prototype methods from target', () => {
    @Redis({ config: { url: 'redis://localhost:6379' } })
    class CustomRedis extends AsenaRedisService {
      public customMethod(): string {
        return 'custom';
      }
    }

    const instance = new CustomRedis();

    expect(instance.customMethod()).toBe('custom');
  });

  it('should copy static properties from target', () => {
    @Redis({ config: { url: 'redis://localhost:6379' } })
    class StaticRedis extends AsenaRedisService {
      public static VERSION = '1.0.0';
    }

    expect(StaticRedis.VERSION).toBe('1.0.0');
  });

  it('should set default logger to console when not provided', () => {
    const options: any = { config: { url: 'redis://localhost:6379' } };

    @Redis(options)
    class LogRedis extends AsenaRedisService {}

    const _ = new LogRedis();

    expect(options.logger).toBe(console);
  });

  it('should not override provided logger', () => {
    const customLogger = { info: () => {}, error: () => {}, warn: () => {}, debug: () => {} };
    const options: any = { config: { url: 'redis://localhost:6379' }, logger: customLogger };

    @Redis(options)
    class LogRedis extends AsenaRedisService {}

    const _ = new LogRedis();

    expect(options.logger).toBe(customLogger);
  });

  describe('options thunk', () => {
    it('should evaluate the thunk at construction, not at decoration', () => {
      let calls = 0;

      @Redis(() => {
        calls++;
        return { config: { url: 'redis://localhost:6379' } };
      })
      class LazyRedis extends AsenaRedisService {}

      expect(calls).toBe(0);

      const instance = new LazyRedis();

      expect(calls).toBe(1);
      expect(instance.config.url).toBe('redis://localhost:6379');

      const second = new LazyRedis();

      expect(second).toBeInstanceOf(AsenaRedisService);
      expect(calls).toBe(2);
    });

    it('should register under the class name', () => {
      @Redis(() => ({ config: { url: 'redis://localhost:6379' } }))
      class ThunkRedis extends AsenaRedisService {}

      expect(ThunkRedis.name).toBe('ThunkRedis');
    });
  });
});
