import 'reflect-metadata';
import { getOwnMetadata } from 'reflect-metadata/no-conflict';
import { Inject } from '@asenajs/asena/decorators/ioc';
import type { ComponentPostProcessor } from '@asenajs/asena/ioc/types';
import { ICoreServiceNames } from '@asenajs/asena/ioc/types';
import type { Container } from '@asenajs/asena/container';
import type { ServerLogger } from '@asenajs/asena/logger';
import { getChainedTypedMetadata, getPrototypeChainOf } from '@asenajs/asena/utils';
import { AsenaRedisService } from '../AsenaRedisService';
import {
  CACHEABLE_METADATA_KEY,
  CACHE_CONFIG_KEY,
  CACHE_EVICT_METADATA_KEY,
  CACHE_PUT_METADATA_KEY,
  REDIS_CACHE_OPTIONS_KEY,
  type CacheCommonOptions,
  type CacheConfigOptions,
  type CacheEvictOptions,
  type CacheableOptions,
  type CachePutOptions,
  type RedisCacheOptions,
} from './CacheOptions';
import { decodeCacheValue, encodeCacheValue, type DecodeResult } from './cacheCodec';
import { prefixScanPattern, resolveCacheKey, resolveOption, type CacheKeySource } from './cacheKey';
import { DEFAULT_CACHE_TIMEOUT_MS, MAX_CACHE_TIMEOUT_MS, withTimeout } from './commandTimeout';
import { deleteByPattern } from './scanDelete';

const AUTO_DETECTED = Symbol.for('asena:redis:cache:auto');

const MISS: DecodeResult = { ok: false };

const READ_FAILED = Symbol('read failed');

/** One decorated method, with the class and application levels its options fall back to. */
interface MethodScope {
  className: string;
  methodName: string;
  levels: readonly [CacheConfigOptions, RedisCacheOptions];
}

interface CommandPlan extends CacheKeySource {
  redis?: string;
  timeout: number;
}

interface WritePlan extends CommandPlan {
  ttl?: number;
  cacheNull: boolean;
}

interface EvictPlan extends CommandPlan {
  allEntries: boolean;
  beforeInvocation: boolean;
}

/**
 * Wraps every `@Cacheable` / `@CachePut` / `@CacheEvict` method so each call goes through Redis.
 *
 * Carries no `@PostProcessor()` because `node_modules` is never scanned: subclass it in the source
 * folder under `@RedisCache`. Redis failures fail open; configuration mistakes fail fast.
 */
export class CachePostProcessor implements ComponentPostProcessor {
  @Inject(ICoreServiceNames.CONTAINER)
  private container!: Container;

  @Inject(ICoreServiceNames.SERVER_LOGGER)
  private serverLogger?: ServerLogger;

  private readonly serviceCache = new Map<string | symbol, AsenaRedisService>();

  public postProcess<T>(instance: T, Class: any): T {
    const cacheables = this.collect<CacheableOptions>(CACHEABLE_METADATA_KEY, Class);
    const puts = this.collect<CachePutOptions>(CACHE_PUT_METADATA_KEY, Class);
    const evicts = this.collect<CacheEvictOptions>(CACHE_EVICT_METADATA_KEY, Class);

    if (cacheables.size === 0 && puts.size === 0 && evicts.size === 0) {
      return instance;
    }

    const levels = [
      getChainedTypedMetadata<CacheConfigOptions>(CACHE_CONFIG_KEY, Class),
      getChainedTypedMetadata<RedisCacheOptions>(REDIS_CACHE_OPTIONS_KEY, this.constructor),
    ] as const;
    const className = Class.name;

    for (const methodName of new Set([...cacheables.keys(), ...puts.keys(), ...evicts.keys()])) {
      const cacheable = cacheables.get(methodName);
      const put = puts.get(methodName);
      const evict = evicts.get(methodName);

      if (cacheable && put) {
        throw new Error(
          `@Cacheable and @CachePut are both on ${className}.${methodName}. They contradict: ` +
            `@Cacheable skips the method on a hit, @CachePut requires it to run every time.`,
        );
      }

      const original = (instance as Record<string, unknown>)[methodName];

      if (typeof original !== 'function') {
        throw new Error(
          `A cache decorator on ${className}.${methodName} found ${typeof original} on the instance ` +
            `instead of a function. Cache decorators go on methods.`,
        );
      }

      const scope: MethodScope = { className, methodName, levels };
      const writePlan = this.buildWritePlan(cacheable ?? put, scope);
      const evictPlan = this.buildEvictPlan(evict, scope);

      const readFor = cacheable && writePlan ? (key: string) => this.readCache(writePlan, key) : null;
      const writeFor = writePlan ? (key: string, value: unknown) => this.writeCache(writePlan, key, value) : null;
      const keyFor = writePlan ? (args: unknown[]) => resolveCacheKey(writePlan, args) : null;
      const evictFor = evictPlan ? (args: unknown[]) => this.runEvict(evictPlan, args) : null;
      const evictBefore = evictPlan?.beforeInvocation === true;

      const invoke = original as (...args: unknown[]) => unknown;

      // Own property rather than a Proxy: the prototype chain the @Redis and @Service wrappers
      // rely on stays exactly as it was.
      (instance as Record<string, unknown>)[methodName] = async function wrapped(this: unknown, ...args: unknown[]) {
        if (evictFor && evictBefore) {
          await evictFor(args);
        }

        const key = keyFor ? keyFor(args) : undefined;
        let writable = true;

        if (readFor && key !== undefined) {
          const hit = await readFor(key);

          if (hit === READ_FAILED) {
            // Redis just failed the read; a write would most likely wait out the limit a second time.
            writable = false;
          } else if (hit.ok) {
            return hit.value;
          }
        }

        // Not caught: the method's own error must reach the caller, skipping write and eviction.
        const result = await invoke.apply(this, args);

        if (writeFor && key !== undefined && writable) {
          await writeFor(key, result);
        }

        if (evictFor && !evictBefore) {
          await evictFor(args);
        }

        return result;
      };
    }

    return instance;
  }

  // Reading the nearest ancestor's map would let one decorator on a subclass shadow all the base's.
  private collect<T>(key: symbol, Class: any): Map<string, T> {
    const merged = new Map<string, T>();

    for (const link of getPrototypeChainOf(Class)) {
      const own = getOwnMetadata(key, link) as Map<string, T> | undefined;

      if (!own) continue;

      for (const [methodName, options] of own.entries()) {
        merged.set(methodName, options);
      }
    }

    return merged;
  }

  private buildWritePlan(
    options: CacheableOptions | CachePutOptions | undefined,
    scope: MethodScope,
  ): WritePlan | null {
    if (!options) return null;

    const { className, methodName } = scope;
    const levels = [options, ...scope.levels];
    const ttl = resolveOption('ttl', levels);

    if (ttl !== undefined && (!Number.isInteger(ttl) || ttl <= 0)) {
      throw new Error(
        `The cache decorator on ${className}.${methodName} resolved ttl=${String(ttl)}. ` +
          `It must be a positive whole number of seconds, or omitted for no expiry.`,
      );
    }

    return {
      className,
      methodName,
      redis: resolveOption('redis', levels),
      prefix: resolveOption('prefix', levels),
      timeout: this.resolveTimeout(levels, scope),
      ttl,
      key: options.key,
      cacheNull: options.cacheNull ?? true,
    };
  }

  private buildEvictPlan(options: CacheEvictOptions | undefined, scope: MethodScope): EvictPlan | null {
    if (!options) return null;

    const { className, methodName } = scope;
    const levels = [options, ...scope.levels];
    const prefix = resolveOption('prefix', levels);
    const allEntries = options.allEntries === true;

    if (allEntries && !prefix) {
      throw new Error(
        `@CacheEvict({ allEntries: true }) on ${className}.${methodName} has no prefix to scope it to, ` +
          `so it would delete every key in the database. Set 'prefix' on the decorator or on a ` +
          `class-level @CacheConfig.`,
      );
    }

    return {
      className,
      methodName,
      redis: resolveOption('redis', levels),
      prefix,
      timeout: this.resolveTimeout(levels, scope),
      key: options.key,
      allEntries,
      beforeInvocation: options.beforeInvocation === true,
    };
  }

  private resolveTimeout(levels: ReadonlyArray<CacheCommonOptions | undefined>, scope: MethodScope): number {
    const timeout = resolveOption('timeout', levels) ?? DEFAULT_CACHE_TIMEOUT_MS;

    if (typeof timeout !== 'number' || !(timeout > 0 && timeout <= MAX_CACHE_TIMEOUT_MS)) {
      throw new Error(
        `The cache decorator on ${scope.className}.${scope.methodName} resolved timeout=${String(timeout)}. ` +
          `It must be a positive number of milliseconds, at most ${MAX_CACHE_TIMEOUT_MS}, or omitted for the ` +
          `${DEFAULT_CACHE_TIMEOUT_MS} ms default.`,
      );
    }

    return timeout;
  }

  private async readCache(plan: WritePlan, key: string): Promise<DecodeResult | typeof READ_FAILED> {
    // Outside the try: a misconfigured graph must propagate, a failed Redis command must not.
    const service = this.resolveService(plan.redis);

    try {
      const raw = await withTimeout(service.get(key), plan.timeout, 'GET');

      if (raw === null) return MISS;

      const decoded = decodeCacheValue(raw);

      if (!decoded.ok) {
        this.warn(`unreadable cache entry at '${key}', treating it as a miss`);
      }

      return decoded;
    } catch (error) {
      this.warn(`cache read for '${key}' failed, running the method instead: ${this.reason(error)}`);

      return READ_FAILED;
    }
  }

  private async writeCache(plan: WritePlan, key: string, value: unknown): Promise<void> {
    if ((value === null || value === undefined) && !plan.cacheNull) {
      return;
    }

    const payload = encodeCacheValue(value);

    if (payload === null) {
      this.warn(
        `${plan.className}.${plan.methodName} returned a value that cannot be JSON-serialized; skipping the ` +
          `cache write for '${key}'`,
      );

      return;
    }

    const service = this.resolveService(plan.redis);

    try {
      // One command: a crash between SET and EXPIRE would store an entry that never expires.
      const args = plan.ttl === undefined ? [key, payload] : [key, payload, 'EX', String(plan.ttl)];

      await withTimeout(service.send('SET', args), plan.timeout, 'SET');
    } catch (error) {
      this.warn(`cache write for '${key}' failed: ${this.reason(error)}`);
    }
  }

  private async runEvict(plan: EvictPlan, args: unknown[]): Promise<void> {
    const service = this.resolveService(plan.redis);

    // Outside the try: a throwing `key` function is a bug, and swallowing it hides a lost eviction.
    const key = plan.allEntries ? null : resolveCacheKey(plan, args);
    const target = key ?? prefixScanPattern(plan.prefix!);

    try {
      if (key === null) {
        const send = (command: string, commandArgs: string[]) =>
          withTimeout(service.send(command, commandArgs), plan.timeout, command);

        await deleteByPattern(send, target, (message) => this.warn(message));
      } else {
        await withTimeout(service.del(key), plan.timeout, 'DEL');
      }
    } catch (error) {
      this.warn(`cache eviction of '${target}' for ${plan.className}.${plan.methodName} failed: ${this.reason(error)}`);
    }
  }

  // Lazy on purpose: AsenaRedisService has no client until its @OnStart, which runs after every
  // postProcess() call, so nothing can be captured at wrap time.
  private resolveService(name: string | undefined): AsenaRedisService {
    const slot = name ?? AUTO_DETECTED;
    const cached = this.serviceCache.get(slot);

    if (cached) return cached;

    const resolved = name === undefined ? this.autoDetectService() : this.resolveNamedService(name);

    this.serviceCache.set(slot, resolved);

    return resolved;
  }

  private resolveNamedService(name: string): AsenaRedisService {
    const registry = this.container.services?.[name];

    if (!registry) {
      throw new Error(
        `No component named '${name}' is registered, so the cache decorator naming it cannot run. ` +
          `Make sure a @Redis service with that name is exported and picked up by component scanning.`,
      );
    }

    const entry = Array.isArray(registry) ? registry[0] : registry;
    const instance = entry?.instance;

    if (!(instance instanceof AsenaRedisService)) {
      throw new Error(
        `Component '${name}' is not an AsenaRedisService, so it cannot back a cache decorator. ` +
          `Point 'redis' at a @Redis-decorated service. (If it is one, check for two copies of ` +
          `@asenajs/asena-redis in node_modules.)`,
      );
    }

    return instance;
  }

  // Not checkable at boot: a post-processor sees the graph mid-build and core has no later hook.
  private autoDetectService(): AsenaRedisService {
    const found = new Map<AsenaRedisService, string>();

    for (const [name, registry] of Object.entries(this.container.services ?? {})) {
      for (const entry of Array.isArray(registry) ? registry : [registry]) {
        const instance = entry?.instance;

        if (instance instanceof AsenaRedisService && !found.has(instance)) {
          found.set(instance, name);
        }
      }
    }

    if (found.size === 1) {
      return [...found.keys()][0]!;
    }

    const names = [...found.values()].sort();
    const detail =
      found.size === 0
        ? 'no AsenaRedisService component is registered'
        : `${found.size} AsenaRedisService components are registered (${names.join(', ')})`;

    throw new Error(
      `A cache decorator could not pick a Redis service: ${detail}. Name one explicitly - ` +
        `@Cacheable({ redis: 'AppRedis' }), or @CacheConfig({ redis: 'AppRedis' }) on the class, or ` +
        `@RedisCache({ redis: 'AppRedis' }) on your post-processor. Note: this is checked on the ` +
        `first cache call rather than at boot, because AsenaJS has no post-registration hook for ` +
        `post-processors.`,
    );
  }

  private reason(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }

  private warn(message: string): void {
    (this.serverLogger ?? console).warn(`[RedisCache] ${message}`);
  }
}
