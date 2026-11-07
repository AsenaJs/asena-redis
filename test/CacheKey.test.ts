import { describe, expect, it } from 'bun:test';
import { defaultCacheKey, joinPrefix, resolveCacheKey, resolveOption } from '../lib/cache/cacheKey';

describe('resolveOption', () => {
  it('prefers the method option over the class and app levels', () => {
    expect(resolveOption('ttl', [{ ttl: 10 }, { ttl: 20 }, { ttl: 30 }])).toBe(10);
    expect(resolveOption('ttl', [{}, { ttl: 20 }, { ttl: 30 }])).toBe(20);
    expect(resolveOption('ttl', [{}, {}, { ttl: 30 }])).toBe(30);
    expect(resolveOption('ttl', [{}, {}, {}])).toBeUndefined();
  });

  it('does not let an explicit undefined at a nearer level clobber a farther one', () => {
    // A spread would turn a shared 300s TTL into "never expires" for one method, silently.
    expect(resolveOption('ttl', [{ ttl: undefined }, { ttl: 20 }, undefined])).toBe(20);
    expect(resolveOption('prefix', [{ prefix: undefined }, undefined, { prefix: 'app' }])).toBe('app');
  });

  it('resolves each field independently', () => {
    const method = { ttl: 5 };
    const classLevel = { prefix: 'user', redis: 'AppRedis' };

    expect(resolveOption('ttl', [method, classLevel, undefined])).toBe(5);
    expect(resolveOption('prefix', [method, classLevel, undefined])).toBe('user');
    expect(resolveOption('redis', [method, classLevel, undefined])).toBe('AppRedis');
  });
});

describe('joinPrefix', () => {
  it('joins with a colon', () => {
    expect(joinPrefix('user', '42')).toBe('user:42');
  });

  it('does not double a colon the prefix already ends with', () => {
    // Without the trim, 'user' and 'user:' build different keys and the cache just never hits.
    expect(joinPrefix('user:', '42')).toBe('user:42');
  });

  it('trims only one trailing colon', () => {
    expect(joinPrefix('user::', '42')).toBe('user::42');
  });

  it('leaves the key bare when there is no prefix', () => {
    expect(joinPrefix(undefined, '42')).toBe('42');
    expect(joinPrefix('', '42')).toBe('42');
  });
});

describe('defaultCacheKey', () => {
  it('includes the class, the method and the arguments', () => {
    expect(defaultCacheKey('UserService', 'findById', ['abc'])).toBe('UserService.findById:["abc"]');
  });

  it('distinguishes different arguments', () => {
    const a = defaultCacheKey('UserService', 'findById', ['a']);
    const b = defaultCacheKey('UserService', 'findById', ['b']);

    expect(a).not.toBe(b);
  });

  it('distinguishes the same method on different classes', () => {
    expect(defaultCacheKey('AdminService', 'find', [1])).not.toBe(defaultCacheKey('UserService', 'find', [1]));
  });

  it('throws when an argument is not serializable, naming the method', () => {
    // No safe fallback: a key derived from an unreadable value collides across callers.
    const circular: Record<string, unknown> = {};

    circular['self'] = circular;

    expect(() => defaultCacheKey('UserService', 'findById', [circular])).toThrow(
      /UserService\.findById.*not JSON-serializable.*explicit 'key' function/s,
    );
  });
});

describe('resolveCacheKey', () => {
  const findById = { className: 'UserService', methodName: 'findById' };

  it('uses the generator and applies the prefix', () => {
    expect(resolveCacheKey({ ...findById, key: (id: string) => id, prefix: 'user' }, ['42'])).toBe('user:42');
  });

  it('falls back to the derived key when no generator is given', () => {
    expect(resolveCacheKey({ ...findById, prefix: 'user' }, ['42'])).toBe('user:UserService.findById:["42"]');
  });

  it('passes every argument to the generator', () => {
    const key = resolveCacheKey({ className: 'S', methodName: 'm', key: (a: string, b: number) => `${a}-${b}` }, [
      'x',
      7,
    ]);

    expect(key).toBe('x-7');
  });

  it('lets a throwing generator propagate', () => {
    const boom = () => {
      throw new Error('key exploded');
    };

    expect(() => resolveCacheKey({ className: 'S', methodName: 'm', key: boom }, [])).toThrow('key exploded');
  });

  it('rejects a generator that returns an empty string', () => {
    // An empty key is a real Redis key, and every method with a broken generator would share it.
    expect(() => resolveCacheKey({ ...findById, key: () => '' }, [])).toThrow(
      /UserService\.findById.*non-empty string/s,
    );
  });

  it('rejects a generator that returns a non-string', () => {
    expect(() => resolveCacheKey({ ...findById, key: (() => 42) as never }, [])).toThrow(/non-empty string/);
  });
});
