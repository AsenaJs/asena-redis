import 'reflect-metadata';
import type { RedisDecoratorOptions } from '../types';
import type { AsenaRedisService } from '../AsenaRedisService';
import { Service } from '@asenajs/asena/decorators';

export function Redis(options: RedisDecoratorOptions) {
  return function <T extends new (...args: any[]) => AsenaRedisService>(target: T) {
    // Extend the decorated class itself. Extending AsenaRedisService discarded the target's
    // prototype chain, so anything the service inherited from an intermediate base class -
    // methods, getters, statics, instanceof - was silently dropped.
    //
    // This needs the IocEngine fix in @asenajs/asena 0.9.0: the wrapper registers under the
    // target's own name, and the engine used to treat that parent name as a dependency and
    // report a circular dependency. Hence the peer bump.
    @Service(options.name || target.name)
    class RedisServiceClass extends (target as unknown as typeof AsenaRedisService) {
      public constructor() {
        // `target` is a class at runtime; the `as unknown as` cast above hides that from
        // static analysis, so the rule cannot see that `super` is a constructor.
        // eslint-disable-next-line constructor-super
        super();

        if (!options.logger) {
          options.logger = console;
        }

        this.setRedisOptions(options);

        if (options.client) {
          this.setRedisClient(options.client);
        }
      }
    }

    // No member or metadata copying. Everything on the target - and on anything the target
    // itself extends - is reachable through the prototype chain now, and every reader walks it.
    //
    // The metadata loop was actively harmful: `getMetadataKeys` walks the chain while
    // `getMetadata` returns only the nearest value, so it flattened inherited records onto the
    // wrapper as own properties. A decorated class extending another decorated class inherited
    // its parent's NameKey and registered under the parent's name.

    // Override class name to match original target
    Object.defineProperty(RedisServiceClass, 'name', {
      value: target.name,
      writable: false,
      configurable: true,
    });

    return RedisServiceClass as any;
  };
}
