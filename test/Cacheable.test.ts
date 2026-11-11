import { beforeEach, describe, expect, it, mock } from 'bun:test';
import { AsenaRedisService } from '../lib/AsenaRedisService';
import { CachePostProcessor } from '../lib/cache/CachePostProcessor';
import { Cacheable, CacheEvict, CachePut } from '../lib/decorators/CacheDecorators';
import { CacheConfig } from '../lib/decorators/CacheConfig';
import { RedisCache } from '../lib/decorators/RedisCache';
import type { Container } from '@asenajs/asena/container';
import type { RedisClientAdapter } from '../lib/adapter';

// Behind the REAL AsenaRedisService rather than a mock of it, so a change to the `send('SET', ...)`
// arguments or to the service itself shows up here.
class FakeRedisClient {
  public readonly store = new Map<string, string>();

  public readonly commands: Array<{ command: string; args: string[] }> = [];

  public failOn: 'get' | 'send' | 'del' | null = null;

  /** Commands that stay pending until releaseParked(), as commands held in an offline queue do. */
  public readonly hang = new Set<'GET' | 'SET' | 'DEL' | 'SCAN'>();

  public isConnected = true;

  private readonly parked: Array<() => void> = [];

  public async get(key: string): Promise<string | null> {
    if (this.failOn === 'get') throw new Error('redis is down');

    if (this.hang.has('GET')) return this.park(() => this.store.get(key) ?? null);

    return this.store.get(key) ?? null;
  }

  public async del(key: string): Promise<number> {
    if (this.failOn === 'del') throw new Error('redis is down');

    if (this.hang.has('DEL')) return this.park(() => (this.store.delete(key) ? 1 : 0));

    return this.store.delete(key) ? 1 : 0;
  }

  public async send(command: string, args: string[]): Promise<any> {
    this.commands.push({ command, args });

    if (this.failOn === 'send') throw new Error('redis is down');

    if (this.hang.has(command as 'SET')) return this.park(() => this.apply(command, args));

    return this.apply(command, args);
  }

  public releaseParked(): void {
    for (const run of this.parked.splice(0)) run();
  }

  private park<T>(run: () => T): Promise<T> {
    return new Promise((resolve) => {
      this.parked.push(() => resolve(run()));
    });
  }

  private apply(command: string, args: string[]): any {
    if (command === 'SET') {
      this.store.set(args[0]!, args[1]!);

      return 'OK';
    }

    if (command === 'DEL') {
      for (const key of args) this.store.delete(key);

      return args.length;
    }

    if (command === 'SCAN') {
      const pattern = args[2] ?? '*';
      const regex = new RegExp(`^${pattern.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*')}$`);

      return ['0', [...this.store.keys()].filter((key) => regex.test(key))];
    }

    return null;
  }
}

class StubRedisService extends AsenaRedisService {
  public useClient(client: FakeRedisClient): void {
    this.setRedisClient(client as unknown as RedisClientAdapter);
  }
}

const buildContainer = (services: Record<string, AsenaRedisService>) =>
  ({
    services: Object.fromEntries(
      Object.entries(services).map(([name, instance]) => [
        name,
        { Class: instance.constructor, instance, singleton: true },
      ]),
    ),
  }) as unknown as Container;

const warnings: string[] = [];

const wrap = async <T extends object>(
  instance: T,
  container: ReturnType<typeof buildContainer>,
  ProcessorClass: new () => CachePostProcessor = CachePostProcessor,
): Promise<T> => {
  const processor = new ProcessorClass();

  (processor as unknown as { container: unknown }).container = container;
  (processor as unknown as { serverLogger: unknown }).serverLogger = {
    info: () => {},
    warn: (message: string) => warnings.push(message),
    error: () => {},
    debug: () => {},
    profile: () => {},
  };

  return processor.postProcess(instance, instance.constructor);
};

describe('cache decorators', () => {
  let client: FakeRedisClient;
  let redis: StubRedisService;
  let container: ReturnType<typeof buildContainer>;

  beforeEach(() => {
    warnings.length = 0;
    client = new FakeRedisClient();
    redis = new StubRedisService();
    redis.useClient(client);
    container = buildContainer({ AppRedis: redis });
  });

  describe('@Cacheable', () => {
    it('runs the method on a miss and serves the second call from Redis', async () => {
      const body = mock(async (id: string) => ({ id }));

      class UserService {
        @Cacheable({ prefix: 'user', ttl: 300, key: (id: string) => id })
        public async findById(id: string) {
          return body(id);
        }
      }

      const service = await wrap(new UserService(), container);

      expect(await service.findById('42')).toEqual({ id: '42' });
      expect(await service.findById('42')).toEqual({ id: '42' });
      expect(body).toHaveBeenCalledTimes(1);
    });

    it('writes with one atomic SET carrying the TTL', async () => {
      // SET then EXPIRE would leave a window where a crash stores an entry that never expires.
      class UserService {
        @Cacheable({ prefix: 'user', ttl: 300, key: (id: string) => id })
        public async findById(id: string) {
          return { id };
        }
      }

      const service = await wrap(new UserService(), container);

      await service.findById('42');

      expect(client.commands).toEqual([{ command: 'SET', args: ['user:42', '{"v":{"id":"42"}}', 'EX', '300'] }]);
    });

    it('omits EX when no ttl is configured', async () => {
      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return 1;
        }
      }

      const service = await wrap(new UserService(), container);

      await service.find();

      expect(client.commands).toEqual([{ command: 'SET', args: ['k', '{"v":1}'] }]);
    });

    it('caches a null result by default', async () => {
      // Otherwise every request for a key that legitimately has no value reaches the database.
      const body = mock(async () => null);

      class UserService {
        @Cacheable({ key: () => 'missing' })
        public async find() {
          return body();
        }
      }

      const service = await wrap(new UserService(), container);

      expect(await service.find()).toBeNull();
      expect(await service.find()).toBeNull();
      expect(body).toHaveBeenCalledTimes(1);
    });

    it('re-runs the method every time when cacheNull is false', async () => {
      const body = mock(async () => null);

      class UserService {
        @Cacheable({ key: () => 'missing', cacheNull: false })
        public async find() {
          return body();
        }
      }

      const service = await wrap(new UserService(), container);

      await service.find();
      await service.find();

      expect(body).toHaveBeenCalledTimes(2);
      expect(client.store.size).toBe(0);
    });

    it('writes nothing when the method throws, and propagates the error unchanged', async () => {
      const failure = new Error('lookup failed');

      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          throw failure;
        }
      }

      const service = await wrap(new UserService(), container);

      await expect(service.find()).rejects.toBe(failure);
      expect(client.store.size).toBe(0);
    });

    it('caches falsy values as hits', async () => {
      const body = mock(async () => 0);

      class UserService {
        @Cacheable({ key: () => 'zero' })
        public async count() {
          return body();
        }
      }

      const service = await wrap(new UserService(), container);

      expect(await service.count()).toBe(0);
      expect(await service.count()).toBe(0);
      expect(body).toHaveBeenCalledTimes(1);
    });

    it('calls the key function once per invocation', async () => {
      const key = mock((id: string) => id);

      class UserService {
        @Cacheable({ key })
        public async find(id: string) {
          return id;
        }
      }

      const service = await wrap(new UserService(), container);

      await service.find('a');

      expect(key).toHaveBeenCalledTimes(1);
    });
  });

  describe('@CachePut', () => {
    it('always runs the method and always writes, never reading', async () => {
      let version = 0;
      const body = mock(async (id: string) => ({ id, version: ++version }));

      class UserService {
        @CachePut({ prefix: 'user', key: (id: string) => id })
        public async refresh(id: string) {
          return body(id);
        }
      }

      const service = await wrap(new UserService(), container);

      await service.refresh('42');
      await service.refresh('42');

      expect(body).toHaveBeenCalledTimes(2);
      expect(client.commands.filter((c) => c.command === 'SET')).toHaveLength(2);
      expect(client.store.get('user:42')).toBe('{"v":{"id":"42","version":2}}');
    });

    it('writes nothing when the method throws', async () => {
      class UserService {
        @CachePut({ key: () => 'k' })
        public async refresh() {
          throw new Error('nope');
        }
      }

      const service = await wrap(new UserService(), container);

      await expect(service.refresh()).rejects.toThrow('nope');
      expect(client.store.size).toBe(0);
    });
  });

  describe('@CacheEvict', () => {
    it('deletes the key after the method succeeds', async () => {
      client.store.set('user:42', '{"v":1}');

      class UserService {
        @CacheEvict({ prefix: 'user', key: (id: string) => id })
        public async update(id: string) {
          return id;
        }
      }

      const service = await wrap(new UserService(), container);

      await service.update('42');

      expect(client.store.has('user:42')).toBe(false);
    });

    it('does NOT evict when the method throws', async () => {
      // A failed write must not drop an entry that still agrees with the store.
      client.store.set('user:42', '{"v":1}');

      class UserService {
        @CacheEvict({ prefix: 'user', key: (id: string) => id })
        public async update(_id: string) {
          throw new Error('write failed');
        }
      }

      const service = await wrap(new UserService(), container);

      await expect(service.update('42')).rejects.toThrow('write failed');
      expect(client.store.has('user:42')).toBe(true);
    });

    it('evicts before the method with beforeInvocation, even when it throws', async () => {
      client.store.set('user:42', '{"v":1}');

      class UserService {
        @CacheEvict({ prefix: 'user', key: (id: string) => id, beforeInvocation: true })
        public async update(_id: string) {
          throw new Error('write failed halfway');
        }
      }

      const service = await wrap(new UserService(), container);

      await expect(service.update('42')).rejects.toThrow('write failed halfway');
      expect(client.store.has('user:42')).toBe(false);
    });

    it('clears every key under the prefix with allEntries', async () => {
      client.store.set('user:1', '{"v":1}');
      client.store.set('user:2', '{"v":2}');
      client.store.set('session:1', '{"v":3}');

      class UserService {
        @CacheEvict({ prefix: 'user', allEntries: true })
        public async purge() {
          return undefined;
        }
      }

      const service = await wrap(new UserService(), container);

      await service.purge();

      expect([...client.store.keys()]).toEqual(['session:1']);
    });

    it('does not double the colon when the prefix already ends with one', async () => {
      client.store.set('user:1', '{"v":1}');

      class UserService {
        @CacheEvict({ prefix: 'user:', allEntries: true })
        public async purge() {
          return undefined;
        }
      }

      const service = await wrap(new UserService(), container);

      await service.purge();

      expect(client.store.size).toBe(0);
    });
  });

  describe('combined decorators', () => {
    it('reads, runs, writes, then evicts in one wrapper', async () => {
      class UserService {
        @Cacheable({ prefix: 'a', key: () => 'k' })
        @CacheEvict({ prefix: 'b', key: () => 'k' })
        public async both() {
          return 1;
        }
      }

      client.store.set('b:k', '{"v":"stale"}');

      const service = await wrap(new UserService(), container);

      expect(await service.both()).toBe(1);
      expect(client.store.get('a:k')).toBe('{"v":1}');
      expect(client.store.has('b:k')).toBe(false);
    });

    it('rejects @Cacheable and @CachePut on the same method at bootstrap', async () => {
      class UserService {
        @Cacheable({ key: () => 'k' })
        @CachePut({ key: () => 'k' })
        public async both() {
          return 1;
        }
      }

      await expect(wrap(new UserService(), container)).rejects.toThrow(/@Cacheable and @CachePut are both on/);
    });
  });

  describe('configuration errors fail fast at bootstrap', () => {
    it('rejects allEntries without a prefix', async () => {
      // With no prefix the SCAN pattern would be `*`: the whole database.
      class UserService {
        @CacheEvict({ allEntries: true })
        public async purge() {
          return undefined;
        }
      }

      await expect(wrap(new UserService(), container)).rejects.toThrow(/would delete every key in the database/);
    });

    it('rejects a non-positive ttl', async () => {
      class UserService {
        @Cacheable({ key: () => 'k', ttl: 0 })
        public async find() {
          return 1;
        }
      }

      await expect(wrap(new UserService(), container)).rejects.toThrow(/positive whole number of seconds/);
    });

    for (const ttl of [Number.MAX_SAFE_INTEGER + 1, 1e16, 1e21]) {
      it(`rejects ttl=${String(ttl)}, above the largest whole number a number holds exactly`, async () => {
        // Redis's own EX limit (~9.22e15 s, minus the clock) moves with time; above 2^53 literals round.
        @RedisCache({ ttl })
        class AppCache extends CachePostProcessor {}

        class UserService {
          @Cacheable({ key: () => 'k' })
          public async find() {
            return 1;
          }
        }

        await expect(wrap(new UserService(), container, AppCache)).rejects.toThrow(
          /resolved ttl=.*at most 9007199254740991/,
        );
      });
    }

    for (const timeout of [
      0,
      -1,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      '100' as unknown as number,
      2_147_483_648,
      3e9,
    ]) {
      it(`rejects timeout=${String(timeout)} on a read and on an eviction`, async () => {
        class ReadService {
          @Cacheable({ key: () => 'k', timeout })
          public async find() {
            return 1;
          }
        }

        class EvictService {
          @CacheEvict({ key: () => 'k', timeout })
          public async update() {
            return 1;
          }
        }

        await expect(wrap(new ReadService(), container)).rejects.toThrow(
          /ReadService\.find resolved timeout=.*positive number of milliseconds/,
        );
        await expect(wrap(new EvictService(), container)).rejects.toThrow(
          /EvictService\.update resolved timeout=.*positive number of milliseconds/,
        );
      });
    }

    it('rejects a bad application-level timeout', async () => {
      @RedisCache({ timeout: -5 })
      class AppCache extends CachePostProcessor {}

      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return 1;
        }
      }

      await expect(wrap(new UserService(), container, AppCache)).rejects.toThrow(/resolved timeout=-5/);
    });

    it('names the timer ceiling when a timeout is above it', async () => {
      // setTimeout turns a delay above 2^31-1 ms into ~1 ms, so every command would time out at once.
      @RedisCache({ timeout: 3e9 })
      class AppCache extends CachePostProcessor {}

      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return 1;
        }
      }

      await expect(wrap(new UserService(), container, AppCache)).rejects.toThrow(
        /resolved timeout=3000000000\. .*at most 2147483647/,
      );
    });

    it('accepts the largest timeout a timer can hold', async () => {
      class UserService {
        @Cacheable({ key: () => 'k', timeout: 2_147_483_647 })
        public async find() {
          return 1;
        }
      }

      const service = await wrap(new UserService(), container);

      expect(await service.find()).toBe(1);
      expect(client.store.get('k')).toBe('{"v":1}');
    });

    it('rejects a decorator on something that is not a method', async () => {
      class UserService {
        public find = 'not a function';
      }

      // Applied by hand: TypeScript would not let a method decorator sit on a string property.
      Cacheable({ key: () => 'k' })(UserService.prototype, 'find', undefined as never);

      await expect(wrap(new UserService(), container)).rejects.toThrow(
        'A cache decorator on UserService.find found string on the instance instead of a function.',
      );
    });

    it('rejects a decorator on a property that holds no function yet', async () => {
      class UserService {
        public find?: () => Promise<number>;
      }

      Cacheable({ key: () => 'k' })(UserService.prototype, 'find', undefined as never);

      await expect(wrap(new UserService(), container)).rejects.toThrow(/found undefined on the instance/);
    });
  });

  describe('fail-open on Redis failures', () => {
    it('runs the method when the cache read fails', async () => {
      client.failOn = 'get';

      const body = mock(async () => 'live');

      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return body();
        }
      }

      const service = await wrap(new UserService(), container);

      expect(await service.find()).toBe('live');
      expect(body).toHaveBeenCalledTimes(1);
      expect(client.commands).toEqual([]);
    });

    it('returns the method result when the cache write fails', async () => {
      client.failOn = 'send';

      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return 'live';
        }
      }

      const service = await wrap(new UserService(), container);

      expect(await service.find()).toBe('live');
    });

    it('returns the method result when the eviction fails', async () => {
      client.failOn = 'del';

      class UserService {
        @CacheEvict({ key: () => 'k' })
        public async update() {
          return 'done';
        }
      }

      const service = await wrap(new UserService(), container);

      expect(await service.update()).toBe('done');
    });

    it('treats an unreadable entry as a miss and runs the method', async () => {
      client.store.set('k', 'not json at all');

      const body = mock(async () => 'live');

      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return body();
        }
      }

      const service = await wrap(new UserService(), container);

      expect(await service.find()).toBe('live');
      expect(body).toHaveBeenCalledTimes(1);
    });

    it('returns the value but skips the write when the result cannot be serialized', async () => {
      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          const circular: Record<string, unknown> = {};

          circular['self'] = circular;

          return circular;
        }
      }

      const service = await wrap(new UserService(), container);

      expect(await service.find()).toBeDefined();
      expect(client.store.size).toBe(0);
    });

    it('runs the method when the Redis client was never started', async () => {
      const detached = new StubRedisService();

      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return 'live';
        }
      }

      const service = await wrap(new UserService(), buildContainer({ AppRedis: detached }));

      expect(await service.find()).toBe('live');
    });
  });

  describe('time limit on each Redis command', () => {
    const timed = async <T>(run: () => Promise<T>): Promise<{ value: T; ms: number }> => {
      const started = performance.now();
      const value = await run();

      return { value, ms: performance.now() - started };
    };

    // The limit plus scheduling slack under a loaded suite; far below the 500 ms default.
    const WITHIN_50MS_LIMIT = 250;

    it('runs the method when GET does not answer within the limit', async () => {
      client.hang.add('GET');

      const body = mock(async () => 'live');

      class UserService {
        @Cacheable({ key: () => 'k', timeout: 50 })
        public async find() {
          return body();
        }
      }

      const service = await wrap(new UserService(), container);
      const { value, ms } = await timed(() => service.find());

      expect(value).toBe('live');
      expect(body).toHaveBeenCalledTimes(1);
      expect(ms).toBeLessThan(WITHIN_50MS_LIMIT);
      expect(warnings).toEqual([
        "[RedisCache] cache read for 'k' failed, running the method instead: GET timed out after 50ms",
      ]);
    });

    it('skips the write after a read that did not answer, so a miss costs one limit, not two', async () => {
      client.hang.add('GET');
      client.hang.add('SET');

      class UserService {
        @Cacheable({ key: () => 'k', timeout: 150 })
        public async find() {
          return 'live';
        }
      }

      const service = await wrap(new UserService(), container);
      const { value, ms } = await timed(() => service.find());

      expect(value).toBe('live');
      expect(client.commands).toEqual([]);
      expect(ms).toBeLessThan(290);
      expect(warnings).toHaveLength(1);
    });

    it('returns the result when SET does not answer within the limit', async () => {
      client.hang.add('SET');

      class UserService {
        @CachePut({ key: () => 'k', timeout: 50 })
        public async save() {
          return 'saved';
        }
      }

      const service = await wrap(new UserService(), container);
      const { value, ms } = await timed(() => service.save());

      expect(value).toBe('saved');
      expect(ms).toBeLessThan(WITHIN_50MS_LIMIT);
      expect(warnings).toEqual(["[RedisCache] cache write for 'k' failed: SET timed out after 50ms"]);
    });

    it('returns the result when a single-key DEL does not answer within the limit', async () => {
      client.hang.add('DEL');

      class UserService {
        @CacheEvict({ key: () => 'k', timeout: 50 })
        public async update() {
          return 'done';
        }
      }

      const service = await wrap(new UserService(), container);
      const { value, ms } = await timed(() => service.update());

      expect(value).toBe('done');
      expect(ms).toBeLessThan(WITHIN_50MS_LIMIT);
      expect(warnings).toEqual([
        "[RedisCache] cache eviction of 'k' for UserService.update failed: DEL timed out after 50ms",
      ]);
    });

    it('returns the result when a SCAN round of allEntries does not answer within the limit', async () => {
      client.hang.add('SCAN');

      class UserService {
        @CacheEvict({ prefix: 'user', allEntries: true, timeout: 50 })
        public async purge() {
          return 'purged';
        }
      }

      const service = await wrap(new UserService(), container);
      const { value, ms } = await timed(() => service.purge());

      expect(value).toBe('purged');
      expect(ms).toBeLessThan(WITHIN_50MS_LIMIT);
      expect(warnings).toEqual([
        "[RedisCache] cache eviction of 'user:*' for UserService.purge failed: SCAN timed out after 50ms",
      ]);
    });

    it('returns the result when a delete batch of allEntries does not answer within the limit', async () => {
      client.store.set('user:1', '{"v":1}');
      client.hang.add('DEL');

      class UserService {
        @CacheEvict({ prefix: 'user', allEntries: true, timeout: 50 })
        public async purge() {
          return 'purged';
        }
      }

      const service = await wrap(new UserService(), container);
      const { value, ms } = await timed(() => service.purge());

      expect(value).toBe('purged');
      expect(ms).toBeLessThan(WITHIN_50MS_LIMIT);
      expect(warnings).toEqual([
        "[RedisCache] cache eviction of 'user:*' for UserService.purge failed: DEL timed out after 50ms",
      ]);
    });

    it('limits each allEntries command, not the whole sweep', async () => {
      // Three rounds of 30 ms each: 90 ms in all, over a 50 ms limit no single command reaches.
      const rounds = [
        ['10', ['user:1']],
        ['20', ['user:2']],
        ['0', ['user:3']],
      ];
      const send = client.send.bind(client);

      client.send = async (command, args) => {
        if (command !== 'SCAN') return send(command, args);

        await Bun.sleep(30);

        return rounds.shift();
      };

      class UserService {
        @CacheEvict({ prefix: 'user', allEntries: true, timeout: 50 })
        public async purge() {
          return 'purged';
        }
      }

      const service = await wrap(new UserService(), container);

      expect(await service.purge()).toBe('purged');
      expect(rounds).toEqual([]);
      expect(warnings).toEqual([]);
    });

    it('runs the method when a beforeInvocation eviction does not answer within the limit', async () => {
      client.hang.add('DEL');

      const body = mock(async () => 'done');

      class UserService {
        @CacheEvict({ key: () => 'k', beforeInvocation: true, timeout: 50 })
        public async update() {
          return body();
        }
      }

      const service = await wrap(new UserService(), container);
      const { value, ms } = await timed(() => service.update());

      expect(value).toBe('done');
      expect(body).toHaveBeenCalledTimes(1);
      expect(ms).toBeLessThan(WITHIN_50MS_LIMIT);
    });

    it('defaults to 500 ms', async () => {
      client.hang.add('GET');

      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return 'live';
        }
      }

      const service = await wrap(new UserService(), container);
      const { value, ms } = await timed(() => service.find());

      expect(value).toBe('live');
      expect(ms).toBeGreaterThanOrEqual(490);
      expect(ms).toBeLessThan(900);
      expect(warnings[0]).toContain('GET timed out after 500ms');
    });

    it('resolves the limit method > @CacheConfig > @RedisCache', async () => {
      client.hang.add('GET');

      @RedisCache({ timeout: 30 })
      class AppCache extends CachePostProcessor {}

      @CacheConfig({ timeout: 40 })
      class ClassLevel {
        @Cacheable({ key: () => 'a', timeout: 20 })
        public async method() {
          return 1;
        }

        @Cacheable({ key: () => 'b' })
        public async inherited() {
          return 2;
        }
      }

      class AppLevel {
        @Cacheable({ key: () => 'c' })
        public async find() {
          return 3;
        }
      }

      const classLevel = await wrap(new ClassLevel(), container, AppCache);
      const appLevel = await wrap(new AppLevel(), container, AppCache);

      await classLevel.method();
      await classLevel.inherited();
      await appLevel.find();

      expect(warnings.map((warning) => warning.match(/after (\d+)ms/)?.[1])).toEqual(['20', '40', '30']);
    });

    it('serves a hit normally when the command answers in time', async () => {
      client.store.set('k', '{"v":"cached"}');

      const body = mock(async () => 'live');

      class UserService {
        @Cacheable({ key: () => 'k', timeout: 50 })
        public async find() {
          return body();
        }
      }

      const service = await wrap(new UserService(), container);

      expect(await service.find()).toBe('cached');
      expect(body).not.toHaveBeenCalled();
      expect(warnings).toEqual([]);
    });

    it('lets a SET that times out and runs later overwrite a newer entry (documented limitation)', async () => {
      // The command is not cancelled. A client's offline queue can send it after reconnect, over
      // whatever was written in between.
      let version = 0;

      class UserService {
        @CachePut({ key: () => 'k', timeout: 20 })
        public async save() {
          version++;

          return `v${version}`;
        }
      }

      const service = await wrap(new UserService(), container);

      client.hang.add('SET');
      expect(await service.save()).toBe('v1');

      client.hang.delete('SET');
      expect(await service.save()).toBe('v2');
      expect(client.store.get('k')).toBe('{"v":"v2"}');

      client.releaseParked();
      await Bun.sleep(0);

      expect(client.store.get('k')).toBe('{"v":"v1"}');
    });
  });

  describe('@CacheConfig and level precedence', () => {
    it('applies the class-level prefix and ttl', async () => {
      @CacheConfig({ prefix: 'user', ttl: 300 })
      class UserService {
        @Cacheable({ key: (id: string) => id })
        public async findById(id: string) {
          return id;
        }
      }

      const service = await wrap(new UserService(), container);

      await service.findById('42');

      expect(client.commands[0]).toEqual({ command: 'SET', args: ['user:42', '{"v":"42"}', 'EX', '300'] });
    });

    it('lets the method override one field and inherit the rest', async () => {
      @CacheConfig({ prefix: 'user', ttl: 300 })
      class UserService {
        @Cacheable({ key: (id: string) => id, ttl: 30 })
        public async findById(id: string) {
          return id;
        }
      }

      const service = await wrap(new UserService(), container);

      await service.findById('42');

      expect(client.commands[0]!.args).toEqual(['user:42', '{"v":"42"}', 'EX', '30']);
    });

    it('falls back to the @RedisCache application level', async () => {
      @RedisCache({ prefix: 'app', ttl: 60 })
      class AppCache extends CachePostProcessor {}

      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return 1;
        }
      }

      const service = await wrap(new UserService(), container, AppCache);

      await service.find();

      expect(client.commands[0]!.args).toEqual(['app:k', '{"v":1}', 'EX', '60']);
    });

    it('lets @CacheConfig beat @RedisCache', async () => {
      @RedisCache({ prefix: 'app', ttl: 60 })
      class AppCache extends CachePostProcessor {}

      @CacheConfig({ prefix: 'user' })
      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return 1;
        }
      }

      const service = await wrap(new UserService(), container, AppCache);

      await service.find();

      expect(client.commands[0]!.args).toEqual(['user:k', '{"v":1}', 'EX', '60']);
    });
  });

  describe('inheritance', () => {
    it('wraps a @Cacheable declared on a base class', async () => {
      class BaseService {
        @Cacheable({ key: () => 'base' })
        public async shared() {
          return 'from base';
        }
      }

      class ChildService extends BaseService {}

      const service = await wrap(new ChildService(), container);

      await service.shared();

      expect(client.store.get('base')).toBe('{"v":"from base"}');
    });

    it('lets a subclass @CacheConfig override one field of the base config', async () => {
      @CacheConfig({ prefix: 'base', ttl: 100 })
      class BaseService {
        @Cacheable({ key: () => 'k' })
        public async shared() {
          return 1;
        }
      }

      @CacheConfig({ prefix: 'child' })
      class ChildService extends BaseService {}

      const service = await wrap(new ChildService(), container);

      await service.shared();

      expect(client.commands[0]!.args).toEqual(['child:k', '{"v":1}', 'EX', '100']);
    });
  });

  // Each `@ts-expect-error` asserts the decorators stay typed for methods only; the tests show what
  // happens when one reaches an arrow property anyway (JavaScript, or Bun running without tsc).
  describe('arrow-function properties', () => {
    it('wraps and caches an arrow property', async () => {
      const body = mock(async (id: string) => ({ id }));

      class UserService {
        // @ts-expect-error TS1240: a method decorator on a property
        @Cacheable({ prefix: 'user', key: (id: string) => id })
        public findById = async (id: string) => body(id);
      }

      const service = await wrap(new UserService(), container);

      expect(await service.findById('42')).toEqual({ id: '42' });
      expect(await service.findById('42')).toEqual({ id: '42' });
      expect(body).toHaveBeenCalledTimes(1);
      expect(client.store.get('user:42')).toBe('{"v":{"id":"42"}}');
    });

    it('keeps the instance as `this`, also when called detached', async () => {
      class CounterService {
        public calls = 0;

        // @ts-expect-error TS1240: a method decorator on a property
        @Cacheable({ key: (id: string) => id })
        public find = async (id: string) => {
          this.calls++;

          return `${id}:${this.calls}`;
        };
      }

      const service = await wrap(new CounterService(), container);
      const detached = service.find;

      expect(await detached('a')).toBe('a:1');
      expect(await service.find('a')).toBe('a:1');
      expect(await detached('b')).toBe('b:2');
      expect(service.calls).toBe(2);
    });

    it('wraps an arrow property declared on a base class', async () => {
      class BaseService {
        public calls = 0;

        // @ts-expect-error TS1240: a method decorator on a property
        @CacheEvict({ key: () => 'k' })
        public reset = async () => {
          this.calls++;

          return 'reset';
        };
      }

      class ChildService extends BaseService {}

      client.store.set('k', '{"v":1}');

      const service = await wrap(new ChildService(), container);

      expect(await service.reset()).toBe('reset');
      expect(service.calls).toBe(1);
      expect(client.store.has('k')).toBe(false);
    });

    it('shares entries across instances by key, as a method does', async () => {
      class UserService {
        public constructor(private readonly label: string) {}

        // @ts-expect-error TS1240: a method decorator on a property
        @Cacheable({ key: () => 'shared' })
        public viaArrow = async () => this.label;
      }

      const first = await wrap(new UserService('first'), container);
      const second = await wrap(new UserService('second'), container);

      expect(await first.viaArrow()).toBe('first');
      expect(await second.viaArrow()).toBe('first');
    });
  });

  describe('picking the Redis service', () => {
    it('uses the only registered service without being told', async () => {
      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return 1;
        }
      }

      const service = await wrap(new UserService(), container);

      await service.find();

      expect(client.store.get('k')).toBe('{"v":1}');
    });

    it('throws on the FIRST CALL when more than one is registered', async () => {
      const second = new StubRedisService();

      second.useClient(new FakeRedisClient());

      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return 1;
        }
      }

      const service = await wrap(new UserService(), buildContainer({ AppRedis: redis, SessionRedis: second }));

      await expect(service.find()).rejects.toThrow(
        /2 AsenaRedisService components are registered \(AppRedis, SessionRedis\)/,
      );
      await expect(service.find()).rejects.toThrow(/first cache call rather than at boot/);
    });

    it('throws on the first call when none is registered', async () => {
      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return 1;
        }
      }

      const service = await wrap(new UserService(), buildContainer({}));

      await expect(service.find()).rejects.toThrow(/no AsenaRedisService component is registered/);
    });

    it('picks the named service when several are registered', async () => {
      const sessionClient = new FakeRedisClient();
      const session = new StubRedisService();

      session.useClient(sessionClient);

      class UserService {
        @Cacheable({ redis: 'SessionRedis', key: () => 'k' })
        public async find() {
          return 1;
        }
      }

      const service = await wrap(new UserService(), buildContainer({ AppRedis: redis, SessionRedis: session }));

      await service.find();

      expect(sessionClient.store.get('k')).toBe('{"v":1}');
      expect(client.store.size).toBe(0);
    });

    it('throws when the named component is not a Redis service', async () => {
      class UserService {
        @Cacheable({ redis: 'NotRedis', key: () => 'k' })
        public async find() {
          return 1;
        }
      }

      const container2 = {
        services: { NotRedis: { Class: Object, instance: {}, singleton: true } },
      } as unknown as Container;

      const service = await wrap(new UserService(), container2);

      await expect(service.find()).rejects.toThrow(/is not an AsenaRedisService/);
    });

    it('throws when the named component does not exist', async () => {
      class UserService {
        @Cacheable({ redis: 'Missing', key: () => 'k' })
        public async find() {
          return 1;
        }
      }

      const service = await wrap(new UserService(), container);

      await expect(service.find()).rejects.toThrow(/No component named 'Missing' is registered/);
    });
  });

  describe('documented limitations', () => {
    it('returns a Promise even from a synchronous method', async () => {
      class UserService {
        @Cacheable({ key: () => 'k' })
        public find() {
          return 1;
        }
      }

      const service = await wrap(new UserService(), container);
      const result: unknown = service.find();

      expect(result).toBeInstanceOf(Promise);
      expect(await result).toBe(1);
    });

    it('runs the method once per concurrent miss on the same key', async () => {
      const body = mock(async () => 'live');

      class UserService {
        @Cacheable({ key: () => 'k' })
        public async find() {
          return body();
        }
      }

      const service = await wrap(new UserService(), container);

      await Promise.all([service.find(), service.find()]);

      expect(body).toHaveBeenCalledTimes(2);
    });

    it('lets allEntries clear entries another class wrote under the same prefix', async () => {
      class Reader {
        @Cacheable({ prefix: 'shared', key: () => 'k' })
        public async find() {
          return 1;
        }
      }

      class Purger {
        @CacheEvict({ prefix: 'shared', allEntries: true })
        public async purge() {
          return undefined;
        }
      }

      const reader = await wrap(new Reader(), container);
      const purger = await wrap(new Purger(), container);

      await reader.find();
      expect(client.store.has('shared:k')).toBe(true);

      await purger.purge();
      expect(client.store.has('shared:k')).toBe(false);
    });
  });

  describe('wiring mistakes surface on the call, not at boot', () => {
    const notRedis = { get: async () => null, send: async () => 'OK', del: async () => 1 };
    const containerOf = (services: Record<string, object>) =>
      ({
        services: Object.fromEntries(
          Object.entries(services).map(([name, instance]) => [
            name,
            { Class: instance.constructor, instance, singleton: true },
          ]),
        ),
      }) as unknown as Container;

    const mistakes = [
      {
        name: 'no Redis service',
        redis: undefined,
        services: () => ({}),
        error: /no AsenaRedisService component is registered/,
      },
      {
        name: 'two Redis services and no name',
        redis: undefined,
        services: () => {
          const second = new StubRedisService();

          second.useClient(new FakeRedisClient());

          return { AppRedis: redis, SessionRedis: second };
        },
        error: /2 AsenaRedisService components are registered/,
      },
      {
        name: 'a named service that is missing',
        redis: 'Missing',
        services: () => ({ AppRedis: redis }),
        error: /No component named 'Missing' is registered/,
      },
      {
        name: 'a test double that is not an AsenaRedisService, by name',
        redis: 'AppRedis',
        services: () => ({ AppRedis: notRedis }),
        error: /Component 'AppRedis' is not an AsenaRedisService/,
      },
      {
        name: 'a test double that is not an AsenaRedisService, found automatically',
        redis: undefined,
        services: () => ({ AppRedis: notRedis }),
        error: /no AsenaRedisService component is registered/,
      },
    ];

    const decorators = [
      { name: '@Cacheable', bodyRuns: false, decorate: (r?: string) => Cacheable({ redis: r, key: () => 'k' }) },
      { name: '@CachePut', bodyRuns: true, decorate: (r?: string) => CachePut({ redis: r, key: () => 'k' }) },
      { name: '@CacheEvict', bodyRuns: true, decorate: (r?: string) => CacheEvict({ redis: r, key: () => 'k' }) },
      {
        name: '@CacheEvict with beforeInvocation',
        bodyRuns: false,
        decorate: (r?: string) => CacheEvict({ redis: r, key: () => 'k', beforeInvocation: true }),
      },
    ];

    for (const mistake of mistakes) {
      for (const decorator of decorators) {
        const when = decorator.bodyRuns ? 'after the method has run' : 'before the method runs';

        it(`${mistake.name}: ${decorator.name} throws ${when}`, async () => {
          const body = mock(async () => 'ran');

          class Target {
            public async run() {
              return body();
            }
          }

          decorator.decorate(mistake.redis)(
            Target.prototype,
            'run',
            Object.getOwnPropertyDescriptor(Target.prototype, 'run')!,
          );

          const service = await wrap(new Target(), containerOf(mistake.services()));

          await expect(service.run()).rejects.toThrow(mistake.error);
          expect(body).toHaveBeenCalledTimes(decorator.bodyRuns ? 1 : 0);
        });
      }
    }
  });

  describe('components without cache metadata', () => {
    it('is returned untouched', async () => {
      class PlainService {
        public async work() {
          return 'done';
        }
      }

      const instance = new PlainService();
      const original = instance.work;
      const service = await wrap(instance, container);

      expect(service).toBe(instance);
      expect(service.work).toBe(original);
    });
  });
});
