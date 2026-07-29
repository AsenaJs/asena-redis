import { afterEach, describe, expect, it } from 'bun:test';
import { AsenaServerFactory } from '@asenajs/asena';
import { Container } from '@asenajs/asena/container';
import { Redis } from '../lib/decorators';
import { AsenaRedisService } from '../lib/AsenaRedisService';
import type { RedisClientAdapter } from '../lib/adapter';

const REDIS_URL = 'redis://localhost:6379';

const quietLogger = { info: () => {}, warn: () => {}, error: () => {}, debug: () => {}, profile: () => {} } as any;

/**
 * The connections a @Redis service opens have to close with the server that owns it.
 *
 * `disconnect()` existed from the start but nothing ever called it, so every service kept its
 * socket - and every subscriber duplicated off it - open past `server.stop()`. These tests go
 * through the framework rather than calling the hooks directly, because what is being checked
 * is that the lifecycle picks the hooks up through the class the @Redis decorator builds.
 */
describe('@Redis service lifecycle', () => {
  let server: any;

  afterEach(async () => {
    await server?.stop();
    server = undefined;
  });

  it('should register both hooks on the decorated class', () => {
    @Redis({ config: { url: REDIS_URL }, logger: quietLogger })
    class HookCache extends AsenaRedisService {}

    // @Redis wraps the target in a subclass, so the hooks are only found if the reader walks
    // the prototype chain - the same call the lifecycle makes
    const container = new Container();

    expect(container.getStartHooks(HookCache as any)).toContain('onStart');
    expect(container.getStopHooks(HookCache as any)).toContain('onStop');
  });

  it('should close the service connection and its subscribers on server.stop()', async () => {
    @Redis({ config: { url: REDIS_URL, name: 'lifecycle' }, logger: quietLogger })
    class LifecycleCache extends AsenaRedisService {}

    server = await AsenaServerFactory.create({
      headless: true,
      logger: quietLogger,
      components: [LifecycleCache],
      keepAlive: false,
    });

    await server.start();

    const service = (await server.coreContainer.container.resolve('LifecycleCache')) as LifecycleCache;

    expect(await service.testConnection()).toBe(true);

    const subscriber: RedisClientAdapter = await service.createSubscriber();

    expect(subscriber.isConnected).toBe(true);

    await server.stop();
    server = undefined;

    expect(await service.testConnection()).toBe(false);
    expect(subscriber.isConnected).toBe(false);
  }, 20000);
});
