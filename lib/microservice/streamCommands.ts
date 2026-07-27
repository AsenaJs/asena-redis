import type { RedisClientAdapter } from '../adapter';

/**
 * A single stream entry: id + flattened field/value pairs parsed into a record.
 */
export interface StreamEntry {
  id: string;
  fields: Record<string, string>;
}

/**
 * A pending-entry summary row from XPENDING (extended form).
 */
export interface PendingEntry {
  id: string;
  consumer: string;
  idleMs: number;
  deliveryCount: number;
}

/**
 * Thin, client-agnostic wrappers over raw Redis Stream commands.
 *
 * All calls go through RedisClientAdapter.send() so they work with both
 * BunRedisAdapter (RESP3 - may return maps/objects) and NodeRedisAdapter
 * (RESP2 - returns nested arrays). Reply-shape normalization lives HERE,
 * in one place, instead of being duplicated per client implementation.
 */

/**
 * XADD with approximate MAXLEN trimming. Returns the new entry id.
 */
export async function xadd(
  client: RedisClientAdapter,
  key: string,
  fields: Record<string, string>,
  maxLen?: number,
): Promise<string> {
  const args = [key];

  if (maxLen && maxLen > 0) {
    args.push('MAXLEN', '~', String(maxLen));
  }

  args.push('*');

  for (const [field, value] of Object.entries(fields)) {
    args.push(field, value);
  }

  return client.send('XADD', args);
}

/**
 * XGROUP CREATE with MKSTREAM. Safe to call repeatedly - BUSYGROUP errors
 * (group already exists) are swallowed.
 *
 * `start` defaults to '$' (only new entries). NOGROUP recovery passes '0' so
 * entries added between the group loss and re-creation are replayed, not skipped.
 */
export async function xgroupCreate(client: RedisClientAdapter, key: string, group: string, start = '$'): Promise<void> {
  try {
    await client.send('XGROUP', ['CREATE', key, group, start, 'MKSTREAM']);
  } catch (error) {
    if (!String((error as Error).message).includes('BUSYGROUP')) {
      throw error;
    }
  }
}

/**
 * XGROUP DELCONSUMER - removes this consumer's entry from the group
 * (prevents unbounded dead-consumer accumulation across restarts).
 */
export async function xgroupDelConsumer(
  client: RedisClientAdapter,
  key: string,
  group: string,
  consumer: string,
): Promise<void> {
  await client.send('XGROUP', ['DELCONSUMER', key, group, consumer]);
}

/**
 * XREADGROUP over multiple streams with COUNT + BLOCK.
 * Returns a map of stream key → entries (empty map on timeout).
 */
export async function xreadgroup(
  client: RedisClientAdapter,
  group: string,
  consumer: string,
  keys: string[],
  count: number,
  blockMs: number,
): Promise<Map<string, StreamEntry[]>> {
  const args = ['GROUP', group, consumer, 'COUNT', String(count), 'BLOCK', String(blockMs), 'STREAMS', ...keys];

  for (let i = 0; i < keys.length; i++) {
    args.push('>');
  }

  const reply = await client.send('XREADGROUP', args);

  return normalizeStreamsReply(reply);
}

/**
 * XACK one or more entry ids.
 */
export async function xack(client: RedisClientAdapter, key: string, group: string, ids: string[]): Promise<void> {
  if (!ids.length) return;

  await client.send('XACK', [key, group, ...ids]);
}

/**
 * XPENDING extended form with IDLE filter. Returns pending rows with delivery counts.
 */
export async function xpending(
  client: RedisClientAdapter,
  key: string,
  group: string,
  minIdleMs: number,
  count: number,
): Promise<PendingEntry[]> {
  const reply = await client.send('XPENDING', [key, group, 'IDLE', String(minIdleMs), '-', '+', String(count)]);

  return parsePendingRows(reply);
}

/**
 * XPENDING extended form scoped to a single consumer (no IDLE filter).
 * Used to check whether a consumer's PEL is empty before deleting it -
 * XGROUP DELCONSUMER drops the consumer's pending entries from the group.
 */
export async function xpendingConsumer(
  client: RedisClientAdapter,
  key: string,
  group: string,
  consumer: string,
  count: number,
): Promise<PendingEntry[]> {
  const reply = await client.send('XPENDING', [key, group, '-', '+', String(count), consumer]);

  return parsePendingRows(reply);
}

/**
 * A consumer summary row from XINFO CONSUMERS.
 */
export interface ConsumerInfo {
  name: string;
  pending: number;
  idleMs: number;
}

/**
 * XINFO CONSUMERS - lists every consumer in the group with its pending count
 * and idle time. Used by the sweep to garbage-collect dead consumers.
 */
export async function xinfoConsumers(client: RedisClientAdapter, key: string, group: string): Promise<ConsumerInfo[]> {
  const reply = await client.send('XINFO', ['CONSUMERS', key, group]);

  const result: ConsumerInfo[] = [];

  for (const row of toArray(reply)) {
    // RESP2: flat ['name', n, 'pending', p, 'idle', ms, ...]; RESP3: map/object
    const fields = normalizeFields(row);

    if (fields['name'] !== undefined) {
      result.push({
        name: fields['name'],
        pending: Number(fields['pending'] ?? 0),
        idleMs: Number(fields['idle'] ?? 0),
      });
    }
  }

  return result;
}

/**
 * XCLAIM - takes ownership of pending entries and returns them with their payloads.
 */
export async function xclaim(
  client: RedisClientAdapter,
  key: string,
  group: string,
  consumer: string,
  minIdleMs: number,
  ids: string[],
): Promise<StreamEntry[]> {
  if (!ids.length) return [];

  const reply = await client.send('XCLAIM', [key, group, consumer, String(minIdleMs), ...ids]);

  return normalizeEntries(reply);
}

/**
 * Extract the creation timestamp (epoch millis) from a stream entry id ('<ms>-<seq>').
 */
export function entryTimestamp(id: string): number {
  return Number(id.split('-')[0]);
}

/**
 * Normalize an XREADGROUP reply into stream key → entries.
 *
 * RESP2 (node-redis): [[key, [[id, [f, v, ...]], ...]], ...]
 * RESP3 (Bun): Map/object keyed by stream name, values = entry arrays
 */
function normalizeStreamsReply(reply: any): Map<string, StreamEntry[]> {
  const result = new Map<string, StreamEntry[]>();

  if (!reply) return result;

  if (reply instanceof Map) {
    for (const [key, entries] of reply) {
      result.set(String(key), normalizeEntries(entries));
    }

    return result;
  }

  if (Array.isArray(reply)) {
    for (const row of reply) {
      const cells = toArray(row);

      if (cells.length >= 2) {
        result.set(String(cells[0]), normalizeEntries(cells[1]));
      }
    }

    return result;
  }

  if (typeof reply === 'object') {
    for (const [key, entries] of Object.entries(reply)) {
      result.set(key, normalizeEntries(entries));
    }
  }

  return result;
}

/**
 * Normalize an entry list ([[id, [f, v, ...]], ...]) into StreamEntry objects.
 */
function normalizeEntries(entries: any): StreamEntry[] {
  const result: StreamEntry[] = [];

  for (const entry of toArray(entries)) {
    const cells = toArray(entry);

    if (cells.length >= 2) {
      result.push({ id: String(cells[0]), fields: normalizeFields(cells[1]) });
    }
  }

  return result;
}

/**
 * Normalize a field list into a record.
 * RESP2: flat [field, value, field, value] array; RESP3 may already be a map/object.
 */
function normalizeFields(fields: any): Record<string, string> {
  const result: Record<string, string> = {};

  if (fields instanceof Map) {
    for (const [key, value] of fields) {
      result[String(key)] = String(value);
    }

    return result;
  }

  if (Array.isArray(fields)) {
    for (let i = 0; i + 1 < fields.length; i += 2) {
      result[String(fields[i])] = String(fields[i + 1]);
    }

    return result;
  }

  if (fields && typeof fields === 'object') {
    for (const [key, value] of Object.entries(fields)) {
      result[key] = String(value);
    }
  }

  return result;
}

/**
 * Parse XPENDING extended-form rows ([[id, consumer, idleMs, deliveryCount], ...]).
 * Shared by the IDLE-filtered and per-consumer variants - both return this shape.
 */
function parsePendingRows(reply: any): PendingEntry[] {
  if (!reply) return [];

  const rows: PendingEntry[] = [];

  for (const row of toArray(reply)) {
    const cells = toArray(row);

    if (cells.length >= 4) {
      rows.push({
        id: String(cells[0]),
        consumer: String(cells[1]),
        idleMs: Number(cells[2]),
        deliveryCount: Number(cells[3]),
      });
    }
  }

  return rows;
}

function toArray(value: any): any[] {
  if (Array.isArray(value)) return value;

  if (value === null || value === undefined) return [];

  if (value instanceof Map) return Array.from(value.entries()).flat();

  return [value];
}
