import { describe, expect, it } from 'bun:test';
import type { RedisClientAdapter } from '../lib/adapter';
import { BunRedisAdapter } from '../lib/adapter';
import {
  normalizeEntries,
  normalizeFields,
  normalizeStreamsReply,
  xadd,
  xrange,
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
