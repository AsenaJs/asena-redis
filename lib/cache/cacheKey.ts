import type { CacheCommonOptions, CacheKeyGenerator } from './CacheOptions';

// `??` rather than a spread: an explicit `ttl: undefined` must not clobber the class-level value.
export const resolveOption = <K extends keyof CacheCommonOptions>(
  field: K,
  method: CacheCommonOptions | undefined,
  classLevel: CacheCommonOptions | undefined,
  app: CacheCommonOptions | undefined,
): CacheCommonOptions[K] => method?.[field] ?? classLevel?.[field] ?? app?.[field];

export const joinPrefix = (prefix: string | undefined, key: string): string => {
  if (!prefix) return key;

  const trimmed = prefix.endsWith(':') ? prefix.slice(0, -1) : prefix;

  return `${trimmed}:${key}`;
};

/**
 * @throws When an argument cannot be serialized: a key derived from a value we cannot read is a
 *   key that collides, and a colliding cache returns one caller's data to another.
 */
export const defaultCacheKey = (className: string, methodName: string, args: unknown[]): string => {
  let serializedArgs: string;

  try {
    serializedArgs = JSON.stringify(args);
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);

    throw new Error(
      `Cache key for ${className}.${methodName} could not be derived: its arguments are not ` +
        `JSON-serializable (${reason}). Pass an explicit 'key' function to the decorator.`,
    );
  }

  return `${className}.${methodName}:${serializedArgs}`;
};

/** @throws When the generator returns anything but a non-empty string, or itself throws. */
export const resolveCacheKey = (
  generator: CacheKeyGenerator | undefined,
  prefix: string | undefined,
  className: string,
  methodName: string,
  args: unknown[],
): string => {
  const raw = generator ? generator(...args) : defaultCacheKey(className, methodName, args);

  if (typeof raw !== 'string' || raw.length === 0) {
    throw new Error(
      `The 'key' function on ${className}.${methodName} returned ${JSON.stringify(raw)}; ` +
        `it must return a non-empty string.`,
    );
  }

  return joinPrefix(prefix, raw);
};
