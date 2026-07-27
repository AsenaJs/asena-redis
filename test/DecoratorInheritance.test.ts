import { describe, expect, test } from 'bun:test';
import { ComponentConstants } from '@asenajs/asena/ioc/constants';
import { getOwnTypedMetadata } from '@asenajs/asena/utils';
import { Redis } from '../lib/decorators';
import { AsenaRedisService } from '../lib/AsenaRedisService';

/**
 * @Redis does not augment the decorated class, it returns a replacement. That replacement used
 * to `extend AsenaRedisService` rather than the target, and the copy loops beside it only read
 * the target's *own* prototype - so everything the decorated class inherited from an
 * intermediate base class was dropped, silently, with no error until the first call.
 *
 * Every fixture in the existing suite extends AsenaRedisService directly, which is exactly the
 * one shape that cannot expose this. These use an intermediate base.
 */

abstract class CacheBase extends AsenaRedisService {
  public static readonly kind = 'cache';

  public fromBase(): string {
    return 'base method';
  }

  public overridden(): string {
    return 'base wins';
  }

  public get describeBase(): string {
    return 'base getter';
  }
}

abstract class NamespacedCacheBase extends CacheBase {
  public namespaced(): string {
    return 'namespaced';
  }
}

@Redis({ config: { url: 'redis://localhost:6379' }, name: 'AppCache' })
class AppCache extends NamespacedCacheBase {
  public override overridden(): string {
    return 'subclass wins';
  }

  public own(): string {
    return 'own method';
  }
}

describe('@Redis inheritance', () => {
  test('keeps methods declared on an intermediate base class', () => {
    const instance: any = new (AppCache as any)();

    expect(instance.fromBase()).toBe('base method');
    expect(instance.namespaced()).toBe('namespaced');
    expect(instance.own()).toBe('own method');
  });

  test('the subclass override wins', () => {
    const instance: any = new (AppCache as any)();

    expect(instance.overridden()).toBe('subclass wins');
  });

  test('keeps inherited getters', () => {
    const instance: any = new (AppCache as any)();

    expect(instance.describeBase).toBe('base getter');
  });

  test('keeps inherited statics', () => {
    expect((AppCache as any).kind).toBe('cache');
  });

  test('instances stay instanceof every declared base', () => {
    const instance: any = new (AppCache as any)();

    expect(instance instanceof NamespacedCacheBase).toBe(true);
    expect(instance instanceof CacheBase).toBe(true);
    expect(instance instanceof AsenaRedisService).toBe(true);
  });

  test('a decorated class extending another decorated class keeps its own name', () => {
    @Redis({ config: { url: 'redis://localhost:6379' }, name: 'PrimaryCache' })
    class PrimaryCache extends AsenaRedisService {}

    @Redis({ config: { url: 'redis://localhost:6379' }, name: 'SecondaryCache' })
    class SecondaryCache extends PrimaryCache {}

    // The metadata copy loop used to flatten the parent's NameKey onto the child, so both
    // registered under 'PrimaryCache' and the container promoted the entry to an array.
    expect(getOwnTypedMetadata<string>(ComponentConstants.NameKey, PrimaryCache)).toBe('PrimaryCache');
    expect(getOwnTypedMetadata<string>(ComponentConstants.NameKey, SecondaryCache)).toBe('SecondaryCache');
  });
});
