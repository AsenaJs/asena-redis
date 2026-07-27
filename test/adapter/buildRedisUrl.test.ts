import { describe, expect, it } from 'bun:test';
import { buildRedisUrl } from '../../lib/adapter/RedisClientAdapter';

describe('buildRedisUrl', () => {
  describe('url provided', () => {
    it('should return url as-is when no password override', () => {
      expect(buildRedisUrl({ url: 'redis://localhost:6379' })).toBe('redis://localhost:6379');
    });

    it('should return url as-is when url already has auth', () => {
      expect(buildRedisUrl({ url: 'redis://user:pass@localhost:6379', password: 'other' })).toBe(
        'redis://user:pass@localhost:6379',
      );
    });

    it('should inject password into url when url has no auth', () => {
      const result = buildRedisUrl({ url: 'redis://localhost:6379', password: 'secret' });

      expect(result).toContain(':secret@');
      expect(result).toContain('localhost');
    });

    it('should inject username and password into url', () => {
      const result = buildRedisUrl({
        url: 'redis://localhost:6379',
        username: 'myuser',
        password: 'mypass',
      });

      expect(result).toContain('myuser:mypass@');
    });
  });

  describe('host/port based', () => {
    it('should default to localhost:6379', () => {
      expect(buildRedisUrl({})).toBe('redis://localhost:6379');
    });

    it('should use custom host and port', () => {
      expect(buildRedisUrl({ host: 'redis.example.com', port: 6380 })).toBe('redis://redis.example.com:6380');
    });

    it('should add password auth', () => {
      expect(buildRedisUrl({ password: 'secret' })).toBe('redis://:secret@localhost:6379');
    });

    it('should add username and password auth', () => {
      expect(buildRedisUrl({ username: 'admin', password: 'secret' })).toBe('redis://admin:secret@localhost:6379');
    });

    it('should add database number', () => {
      expect(buildRedisUrl({ db: 2 })).toBe('redis://localhost:6379/2');
    });

    it('should use rediss:// for TLS', () => {
      expect(buildRedisUrl({ tls: true })).toBe('rediss://localhost:6379');
    });

    it('should use rediss:// for TLS options object', () => {
      expect(buildRedisUrl({ tls: { rejectUnauthorized: false } })).toBe('rediss://localhost:6379');
    });

    it('should combine all options', () => {
      expect(
        buildRedisUrl({
          host: 'redis.prod.com',
          port: 6380,
          username: 'app',
          password: 'p@ss',
          db: 1,
          tls: true,
        }),
      ).toBe('rediss://app:p@ss@redis.prod.com:6380/1');
    });

    it('should handle db: 0', () => {
      expect(buildRedisUrl({ db: 0 })).toBe('redis://localhost:6379/0');
    });
  });
});
