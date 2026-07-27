import { describe, expect, it, spyOn } from 'bun:test';
import { BunRedisAdapter } from '../../lib/adapter';

/**
 * Offline tests for the subscription replay. Bun's RedisClient reconnects the
 * socket by itself but never restores server-side subscription state, so the
 * adapter replays it - and everything that depends on a pub/sub channel being
 * served again (the microservice transport's reply channel) needs to know
 * WHEN that replay actually landed, not when the socket came back. Those two
 * moments are a round trip apart, and a message published in between is
 * dropped by Redis with no error anywhere.
 *
 * The fake rides the same `client` slot duplicate() writes through, so the
 * replay is exercised without a broker.
 */

interface Call {
  op: 'subscribe' | 'unsubscribe';
  channel: string;
}

class FakeRedisClient {
  public connected = true;

  public onconnect?: () => void;

  public onclose?: () => void;

  public readonly calls: Call[] = [];

  /** Number of subscribe() calls to fail before letting one through. */
  public failSubscribes = 0;

  /** Resolves the next subscribe() only when released, if set. */
  public gate?: { promise: Promise<void>; release: () => void };

  public async subscribe(channel: string, _listener: (message: string) => void): Promise<void> {
    if (this.failSubscribes > 0) {
      this.failSubscribes--;
      throw new Error('SUBSCRIBE failed');
    }

    if (this.gate) await this.gate.promise;

    this.calls.push({ op: 'subscribe', channel });
  }

  public async unsubscribe(channel: string): Promise<void> {
    this.calls.push({ op: 'unsubscribe', channel });
  }

  /** Simulates the automatic reconnect: the socket is back, nothing is subscribed. */
  public reconnect(): void {
    this.calls.length = 0;
    this.onconnect?.();
  }
}

function buildAdapter(): { adapter: BunRedisAdapter; client: FakeRedisClient } {
  // Object.create + client assignment is exactly how duplicate() builds an
  // adapter, so nothing here is a shape the production code never sees.
  const adapter = Object.create(BunRedisAdapter.prototype) as BunRedisAdapter;
  const client = new FakeRedisClient();

  (adapter as any).client = client;

  return { adapter, client };
}

/** Lets the unawaited replay task make progress. */
const flush = async (turns = 6): Promise<void> => {
  for (let i = 0; i < turns; i++)
    await new Promise((resolve) => {
      setTimeout(resolve, 1);
    });
};

/**
 * Waits for a replay outcome instead of guessing how long it takes. The
 * retry backoff is deliberately longer than a flush, so a fixed turn count
 * would only be measuring the backoff.
 */
async function waitFor(predicate: () => boolean, label: string, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);

    await new Promise((resolve) => {
      setTimeout(resolve, 5);
    });
  }
}

function gate(): { promise: Promise<void>; release: () => void } {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });

  return { promise, release };
}

describe('BunRedisAdapter subscription replay', () => {
  it('reports a channel resubscribed only after the SUBSCRIBE has landed', async () => {
    const { adapter, client } = buildAdapter();
    const events: string[] = [];

    adapter.onConnected(() => events.push('connected'));
    adapter.onResubscribed((channel) => events.push(`resubscribed:${channel}`));

    await adapter.subscribe('replies', () => {});

    // The replay is held at the SUBSCRIBE: the socket is open and the client
    // says connected, but Redis is not serving the channel yet. This is the
    // window a naive readiness check reports as healthy.
    client.gate = gate();
    client.reconnect();
    await flush();

    expect(events).toEqual(['connected']);

    client.gate.release();
    await flush();

    expect(events).toEqual(['connected', 'resubscribed:replies']);
    // Clear first, then re-subscribe: the client-side listener registration
    // survives the reconnect, so a bare subscribe would double every message
    expect(client.calls).toEqual([
      { op: 'unsubscribe', channel: 'replies' },
      { op: 'subscribe', channel: 'replies' },
    ]);
  });

  it('retries a replay that failed on a live connection', async () => {
    // Nothing else will retry: the socket stays up, so there is no further
    // connect event. Without this the channel is unsubscribed for good and
    // every message published to it disappears silently.
    const { adapter, client } = buildAdapter();
    const resubscribed: string[] = [];

    adapter.onResubscribed((channel) => resubscribed.push(channel));

    await adapter.subscribe('replies', () => {});

    client.failSubscribes = 2;
    client.reconnect();

    await waitFor(() => resubscribed.length > 0, 'the retried replay to land');

    expect(resubscribed).toEqual(['replies']);
  });

  it('reports a replay it could not complete instead of swallowing it', async () => {
    const { adapter, client } = buildAdapter();
    const resubscribed: string[] = [];
    const errors: unknown[][] = [];
    const spy = spyOn(console, 'error').mockImplementation((...args: unknown[]) => {
      errors.push(args);
    });

    adapter.onResubscribed((channel) => resubscribed.push(channel));

    await adapter.subscribe('replies', () => {});

    client.failSubscribes = Number.MAX_SAFE_INTEGER;
    client.reconnect();

    try {
      await waitFor(
        () => errors.some((args) => String(args[0]).includes('could not restore the subscription to "replies"')),
        'the failed replay to be reported',
      );
    } finally {
      spy.mockRestore();
    }

    expect(resubscribed).toEqual([]);
  });

  it('lets a newer connection supersede a replay that is still in flight', async () => {
    // The first replay resumes against a connection that is two generations
    // old; re-subscribing there would fight the replay that now owns the
    // channel, and its late "resubscribed" would vouch for a connection that
    // is gone.
    const { adapter, client } = buildAdapter();
    const resubscribed: string[] = [];

    adapter.onResubscribed((channel) => resubscribed.push(channel));

    await adapter.subscribe('replies', () => {});

    const held = gate();

    client.gate = held;
    client.reconnect();
    await flush();

    // Second reconnect while the first replay is still stuck
    client.gate = undefined;
    client.reconnect();
    await flush();

    expect(resubscribed).toEqual(['replies']);

    held.release();
    await flush();

    // The superseded replay must not report a second time
    expect(resubscribed).toEqual(['replies']);
  });

  it('stops replaying when the socket dies again mid-replay', async () => {
    const { adapter, client } = buildAdapter();
    const resubscribed: string[] = [];

    adapter.onResubscribed((channel) => resubscribed.push(channel));

    await adapter.subscribe('replies', () => {});

    client.failSubscribes = 1;
    client.connected = false;
    client.reconnect();

    await flush(30);

    // No retry storm against a dead socket - the next connect event replays
    expect(resubscribed).toEqual([]);

    client.connected = true;
    client.reconnect();
    await waitFor(() => resubscribed.length > 0, 'the replay after the socket came back');

    expect(resubscribed).toEqual(['replies']);
  });
});
