import { describe, expect, it } from 'bun:test';
import type { RedisClientAdapter } from '../lib/adapter';
import { BunRedisAdapter } from '../lib/adapter';
import {
  entryTimestamp,
  normalizeEntries,
  normalizeFields,
  normalizeStreamsReply,
  xack,
  xadd,
  xclaim,
  xgroupCreate,
  xgroupDelConsumer,
  xpending,
  xpendingConsumer,
  xrange,
  xreadgroup,
} from '../lib/microservice/streamCommands';

const REDIS_URL = 'redis://localhost:6379';

/**
 * Records every send() call and returns a canned reply - the commands under test only
 * need a client that talks back, never a real socket.
 */
class RecordingClient {
  public readonly calls: Array<{ command: string; args: string[] }> = [];
  public reply: unknown = [];

  public async send(command: string, args: string[]): Promise<unknown> {
    this.calls.push({ command, args });

    return this.reply;
  }
}

function fakeClient(): RecordingClient & RedisClientAdapter {
  return new RecordingClient() as unknown as RecordingClient & RedisClientAdapter;
}

describe('streamCommands', () => {
  describe('xrange', () => {
    it('should send XRANGE with the full range by default', async () => {
      const client = fakeClient();

      await xrange(client, 'stream:1');

      expect(client.calls).toEqual([{ command: 'XRANGE', args: ['stream:1', '-', '+'] }]);
    });

    it('should append COUNT when given', async () => {
      const client = fakeClient();

      await xrange(client, 'stream:1', '0-1', '0-5', 10);

      expect(client.calls).toEqual([{ command: 'XRANGE', args: ['stream:1', '0-1', '0-5', 'COUNT', '10'] }]);
    });

    it('should normalize a nested-array reply into StreamEntry[]', async () => {
      const client = fakeClient();

      client.reply = [
        ['1-1', ['a', '1', 'b', '2']],
        ['2-1', ['c', '3']],
      ];

      const entries = await xrange(client, 'stream:1');

      expect(entries).toEqual([
        { id: '1-1', fields: { a: '1', b: '2' } },
        { id: '2-1', fields: { c: '3' } },
      ]);
    });

    it('should round-trip xadd → xrange against a live Redis', async () => {
      const client = new BunRedisAdapter(REDIS_URL);

      await client.connect();

      const key = `asena:test:stream:${crypto.randomUUID().slice(0, 8)}`;

      try {
        await xadd(client, key, { hello: 'world' });
        await xadd(client, key, { second: 'entry' });

        const entries = await xrange(client, key);

        expect(entries).toHaveLength(2);
        expect(entries[0]!.fields).toEqual({ hello: 'world' });
        expect(entries[1]!.fields).toEqual({ second: 'entry' });

        const limited = await xrange(client, key, '-', '+', 1);

        expect(limited).toHaveLength(1);
        expect(limited[0]!.fields).toEqual({ hello: 'world' });
      } finally {
        await client.send('DEL', [key]);
        await client.disconnect();
      }
    });
  });

  describe('normalizeFields', () => {
    it('should parse a RESP2 flat field array', () => {
      expect(normalizeFields(['a', '1', 'b', '2'])).toEqual({ a: '1', b: '2' });
    });

    it('should parse a RESP3 Map', () => {
      expect(
        normalizeFields(
          new Map<string, string>([
            ['a', '1'],
            ['b', '2'],
          ]),
        ),
      ).toEqual({ a: '1', b: '2' });
    });
  });

  describe('normalizeEntries', () => {
    it('should parse a nested RESP2 entry array', () => {
      expect(normalizeEntries([['1-1', ['a', '1']]])).toEqual([{ id: '1-1', fields: { a: '1' } }]);
    });
  });

  describe('normalizeStreamsReply', () => {
    it('should parse a RESP2 array reply into a Map', () => {
      const reply = [['stream:1', [['1-1', ['a', '1']]]]];

      expect(normalizeStreamsReply(reply)).toEqual(new Map([['stream:1', [{ id: '1-1', fields: { a: '1' } }]]]));
    });

    it('should parse a RESP3 Map reply', () => {
      const reply = new Map([['stream:1', [['1-1', ['a', '1']]]]]);

      expect(normalizeStreamsReply(reply)).toEqual(new Map([['stream:1', [{ id: '1-1', fields: { a: '1' } }]]]));
    });
  });
});

describe('streamCommands argument construction', () => {
  it('xadd trims with MAXLEN ~ only when a limit is given', async () => {
    const client = fakeClient();

    client.reply = '1-0';

    await xadd(client, 's', { a: '1', b: '2' });
    await xadd(client, 's', { a: '1' }, 100);

    expect(client.calls).toEqual([
      { command: 'XADD', args: ['s', '*', 'a', '1', 'b', '2'] },
      { command: 'XADD', args: ['s', 'MAXLEN', '~', '100', '*', 'a', '1'] },
    ]);
  });

  it('xgroupCreate uses MKSTREAM, defaults to $, and swallows BUSYGROUP only', async () => {
    const client = fakeClient();

    await xgroupCreate(client, 's', 'g');
    await xgroupCreate(client, 's', 'g', '0');

    expect(client.calls).toEqual([
      { command: 'XGROUP', args: ['CREATE', 's', 'g', '$', 'MKSTREAM'] },
      { command: 'XGROUP', args: ['CREATE', 's', 'g', '0', 'MKSTREAM'] },
    ]);

    const busy = fakeClient();

    busy.send = async () => {
      throw new Error('BUSYGROUP Consumer Group name already exists');
    };

    await expect(xgroupCreate(busy, 's', 'g')).resolves.toBeUndefined();

    const broken = fakeClient();

    broken.send = async () => {
      throw new Error('NOGROUP something else');
    };

    await expect(xgroupCreate(broken, 's', 'g')).rejects.toThrow('NOGROUP');
  });

  it('xgroupDelConsumer names the consumer', async () => {
    const client = fakeClient();

    await xgroupDelConsumer(client, 's', 'g', 'c');

    expect(client.calls).toEqual([{ command: 'XGROUP', args: ['DELCONSUMER', 's', 'g', 'c'] }]);
  });

  it('xreadgroup reads new entries from every stream with COUNT and BLOCK', async () => {
    const client = fakeClient();

    client.reply = [['s1', [['1-0', ['k', 'v']]]]];

    const result = await xreadgroup(client, 'g', 'c', ['s1', 's2'], 16, 5000);

    expect(client.calls).toEqual([
      {
        command: 'XREADGROUP',
        args: ['GROUP', 'g', 'c', 'COUNT', '16', 'BLOCK', '5000', 'STREAMS', 's1', 's2', '>', '>'],
      },
    ]);
    expect(result.get('s1')).toEqual([{ id: '1-0', fields: { k: 'v' } }]);
  });

  it('xack sends every id and nothing for an empty list', async () => {
    const client = fakeClient();

    await xack(client, 's', 'g', []);
    await xack(client, 's', 'g', ['1-0', '2-0']);

    expect(client.calls).toEqual([{ command: 'XACK', args: ['s', 'g', '1-0', '2-0'] }]);
  });

  it('xpending filters by IDLE and parses the extended rows', async () => {
    const client = fakeClient();

    client.reply = [['1-0', 'c', 1500, 2]];

    const rows = await xpending(client, 's', 'g', 60000, 10);

    expect(client.calls).toEqual([{ command: 'XPENDING', args: ['s', 'g', 'IDLE', '60000', '-', '+', '10'] }]);
    expect(rows).toEqual([{ id: '1-0', consumer: 'c', idleMs: 1500, deliveryCount: 2 }]);
  });

  it('xpendingConsumer scopes to one consumer without an IDLE filter', async () => {
    const client = fakeClient();

    await xpendingConsumer(client, 's', 'g', 'c', 10);

    expect(client.calls).toEqual([{ command: 'XPENDING', args: ['s', 'g', '-', '+', '10', 'c'] }]);
  });

  it('xclaim takes ownership of the given ids and returns their payloads', async () => {
    const client = fakeClient();

    expect(await xclaim(client, 's', 'g', 'c', 60000, [])).toEqual([]);
    expect(client.calls).toEqual([]);

    client.reply = [['1-0', ['k', 'v']]];

    const entries = await xclaim(client, 's', 'g', 'c', 60000, ['1-0']);

    expect(client.calls).toEqual([{ command: 'XCLAIM', args: ['s', 'g', 'c', '60000', '1-0'] }]);
    expect(entries).toEqual([{ id: '1-0', fields: { k: 'v' } }]);
  });

  it('entryTimestamp reads the millisecond half of an id', () => {
    expect(entryTimestamp('1700000000000-3')).toBe(1700000000000);
  });
});
