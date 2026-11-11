import { describe, expect, it } from 'bun:test';
import { decodeCacheValue, encodeCacheValue } from '../lib/cache/cacheCodec';

const roundTrip = (value: unknown) => {
  const encoded = encodeCacheValue(value);

  if (encoded === null) throw new Error('expected the value to encode');

  return decodeCacheValue(encoded);
};

describe('cache codec', () => {
  it('round-trips ordinary values', () => {
    expect(roundTrip(42)).toEqual({ ok: true, value: 42 });
    expect(roundTrip('hello')).toEqual({ ok: true, value: 'hello' });
    expect(roundTrip({ id: 1, tags: ['a'] })).toEqual({ ok: true, value: { id: 1, tags: ['a'] } });
    expect(roundTrip([1, 2, 3])).toEqual({ ok: true, value: [1, 2, 3] });
  });

  it('round-trips falsy values as hits, not misses', () => {
    expect(roundTrip(0)).toEqual({ ok: true, value: 0 });
    expect(roundTrip('')).toEqual({ ok: true, value: '' });
    expect(roundTrip(false)).toEqual({ ok: true, value: false });
  });

  it('round-trips null as a stored null', () => {
    expect(roundTrip(null)).toEqual({ ok: true, value: null });
  });

  it('round-trips undefined', () => {
    expect(encodeCacheValue(undefined)).toBe('{}');
    expect(roundTrip(undefined)).toEqual({ ok: true, value: undefined });
  });

  it('reports failure instead of throwing for circular values', () => {
    const circular: Record<string, unknown> = {};

    circular['self'] = circular;

    expect(encodeCacheValue(circular)).toBeNull();
  });

  it('reports failure instead of throwing for BigInt', () => {
    expect(encodeCacheValue({ n: 1n })).toBeNull();
  });

  it('loses the prototype of a Date - this is the documented limitation', () => {
    // If a real serializer ever arrives, this test should fail and force the docs to change.
    const date = new Date('2020-01-01T00:00:00.000Z');
    const result = roundTrip(date);

    expect(result).toEqual({ ok: true, value: '2020-01-01T00:00:00.000Z' });
    expect((result as { value: unknown }).value).not.toBeInstanceOf(Date);
  });

  it('degrades a Map to an empty object - same limitation', () => {
    expect(roundTrip(new Map([['a', 1]]))).toEqual({ ok: true, value: {} });
  });

  it('treats a corrupt payload as a miss', () => {
    expect(decodeCacheValue('not json')).toEqual({ ok: false });
  });

  it('treats a foreign payload as a miss', () => {
    expect(decodeCacheValue('42')).toEqual({ ok: false });
    expect(decodeCacheValue('"hello"')).toEqual({ ok: false });
    expect(decodeCacheValue('null')).toEqual({ ok: false });
    expect(decodeCacheValue('[1,2]')).toEqual({ ok: false });
  });

  it('treats a foreign object as a miss, not as a hit carrying undefined', () => {
    // A key collision with some other writer's JSON would otherwise answer callers with undefined.
    expect(decodeCacheValue('{"name":"x"}')).toEqual({ ok: false });
    expect(decodeCacheValue('{"v":1,"name":"x"}')).toEqual({ ok: false });
  });
});
