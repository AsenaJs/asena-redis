// The envelope exists because a bare JSON.stringify cannot store `undefined`, and a stored `null`
// has to differ from the JS null AsenaRedisService.get() answers on a miss.
interface CacheEnvelope {
  v?: unknown;
}

export type DecodeResult = { ok: true; value: unknown } | { ok: false };

/** @returns The payload, or `null` when the value cannot be serialized (circular, `BigInt`). */
export const encodeCacheValue = (value: unknown): string | null => {
  try {
    return JSON.stringify({ v: value } satisfies CacheEnvelope);
  } catch {
    return null;
  }
};

/** Anything that is not one of our envelopes reads as a miss, never as an error. */
export const decodeCacheValue = (raw: string): DecodeResult => {
  let parsed: unknown;

  try {
    parsed = JSON.parse(raw);
  } catch {
    return { ok: false };
  }

  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return { ok: false };
  }

  // `{}` is the stored form of `undefined`: an absent 'v' is a hit carrying undefined.
  return { ok: true, value: (parsed as CacheEnvelope).v };
};
