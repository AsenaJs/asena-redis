import { UlakError, UlakErrorCode } from '@asenajs/asena/messaging';
import { PatternHandlerIndex } from '@asenajs/asena/event';
import type {
  DestroyOptions,
  EmitOptions,
  EventPatternHandler,
  MessageContext,
  MessageHandler,
  MicroserviceTransport,
  SendOptions,
} from '@asenajs/asena/microservice';
import type { RedisClientAdapter } from '../adapter';
import { BunRedisAdapter, buildRedisUrl } from '../adapter';
import type { AsenaRedisService } from '../AsenaRedisService';
import type { RedisConfig, RedisMicroserviceOptions } from '../types';
import {
  entryTimestamp,
  xack,
  xadd,
  xclaim,
  xgroupCreate,
  xgroupDelConsumer,
  xinfoConsumers,
  xpending,
  xpendingConsumer,
  xreadgroup,
} from './streamCommands';
import type { StreamEntry } from './streamCommands';

const DEFAULT_STREAM_PREFIX = 'asena:ms';
const DEFAULT_REQUEST_TIMEOUT = 30_000;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_CLAIM_IDLE_MS = 60_000;
const DEFAULT_MAX_STREAM_LENGTH = 100_000;
const DEFAULT_BLOCK_MS = 5_000;
const DEFAULT_COUNT = 16;
const DEFAULT_MAX_IN_FLIGHT = 32;
const DEFAULT_HANDLER_TIMEOUT = 30_000;
const DEFAULT_DRAIN_TIMEOUT = 10_000;
const RECONNECT_BACKOFF_START = 1_000;
const RECONNECT_BACKOFF_CAP = 30_000;
const WEDGE_MARGIN_MS = 5_000;
const DEFAULT_COMMAND_TIMEOUT = 10_000;
const SHUTDOWN_STEP_TIMEOUT = 1_000;

interface PendingRequest {
  resolve: (value: any) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

/**
 * Per-connection bookkeeping for the publisher. A fresh state object is
 * created for every replacement connection, so orphaned promises and stale
 * in-flight counters die with the connection they belong to.
 */
interface PublisherState {
  client: RedisClientAdapter;
  inFlight: number;
  poisoned: boolean;
}

/**
 * @description Production-grade Redis Streams microservice transport.
 *
 * Delivery model:
 * - Events: single shared stream `{prefix}:evt`, one consumer group per service
 *   (serviceName). Every service group receives a copy; inside a group exactly one
 *   replica processes each entry. Wildcard patterns are matched locally; entries
 *   matching no local handler are ACKed immediately. At-least-once: handler errors
 *   leave the entry pending, the sweep redelivers up to maxRetries, then moves it
 *   to the `{prefix}:dlq` stream.
 * - Requests: one stream per exact pattern `{prefix}:req:{pattern}`, consumer group
 *   per responding service - natural single-handler distribution across replicas.
 *   RPC errors are FINAL: the caller gets an `ok:false` reply and the entry is ACKed
 *   (no broker retry - retrying is the caller's decision). The sweep only rescues
 *   entries whose claiming replica crashed; entries older than requestTimeout are
 *   dropped (the caller has already timed out).
 * - Replies: plain pub/sub channel per transport instance - the caller is alive and
 *   waiting, so persistence is unnecessary.
 *
 * Operational notes (see docs for details):
 * - Connection poisoning defense: Bun's RedisClient loses in-flight commands
 *   when the socket dies and afterwards resolves every reply against the
 *   wrong promise, permanently. BOTH transport-owned command connections are
 *   guarded against this: the consumer via the blocking-read wedge watchdog,
 *   the publisher via onConnectionLost detection plus a per-command timeout
 *   (commandTimeout). Poisoned connections are discarded and replaced; the
 *   publisher is always an owned duplicate, never the user's client.
 * - Handlers should be idempotent: duplicate delivery is possible (at-least-once).
 *   Use context.messageId for deduplication.
 * - Handler duration must stay below claimIdleMs, otherwise the sweep may deliver
 *   the same entry to another replica concurrently.
 * - maxStreamLength bounds memory: messages older than the trim window are LOST
 *   for services that stay offline too long.
 */
export class RedisMicroserviceTransport implements MicroserviceTransport {
  public readonly name = 'redis-streams';

  private readonly instanceId = crypto.randomUUID();

  private readonly source: AsenaRedisService | RedisConfig;

  private readonly serviceName: string;

  private readonly streamPrefix: string;

  private readonly requestTimeout: number;

  private readonly maxRetries: number;

  private readonly claimIdleMs: number;

  private readonly maxStreamLength: number;

  private readonly blockMs: number;

  private readonly count: number;

  private readonly maxInFlight: number;

  private readonly handlerTimeout: number;

  private readonly defaultDrainTimeout: number;

  private readonly commandTimeout: number;

  private publisher!: RedisClientAdapter;

  private publisherState!: PublisherState;

  private publisherSwap?: Promise<void>;

  private connectionFactory!: () => Promise<RedisClientAdapter>;

  private consumer?: RedisClientAdapter;

  private replySubscriber?: RedisClientAdapter;

  /**
   * Whether Redis is currently serving this instance's reply channel. Not the
   * same fact as the subscriber socket being open - see installReplySubscriber.
   */
  private replySubscribed = false;

  private messageHandlers = new Map<string, MessageHandler>();

  private eventHandlers = new PatternHandlerIndex<EventPatternHandler>();

  private pendingRequests = new Map<string, PendingRequest>();

  private inFlight = new Set<Promise<void>>();

  private running = false;

  private connected = false;

  private destroyed = false;

  private destroyPromise?: Promise<void>;

  private readonly stopPromise: Promise<void>;

  private stopResolve!: () => void;

  private consumerLoop?: Promise<void>;

  private sweepTimer?: ReturnType<typeof setInterval>;

  private sweepPromise?: Promise<void>;

  private sweeping = false;

  public constructor(source: AsenaRedisService | RedisConfig, options: RedisMicroserviceOptions) {
    if (!options?.serviceName) {
      throw new Error(
        'RedisMicroserviceTransport requires a serviceName - it is the consumer group identity shared by all replicas of this service',
      );
    }

    this.source = source;
    this.serviceName = options.serviceName;
    this.streamPrefix = options.streamPrefix ?? DEFAULT_STREAM_PREFIX;
    this.requestTimeout = options.requestTimeout ?? DEFAULT_REQUEST_TIMEOUT;
    this.maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;
    this.claimIdleMs = options.claimIdleMs ?? DEFAULT_CLAIM_IDLE_MS;
    this.maxStreamLength = options.maxStreamLength ?? DEFAULT_MAX_STREAM_LENGTH;
    this.blockMs = options.blockMs ?? DEFAULT_BLOCK_MS;
    this.count = options.count ?? DEFAULT_COUNT;
    this.maxInFlight = options.maxInFlight ?? DEFAULT_MAX_IN_FLIGHT;
    this.defaultDrainTimeout = options.drainTimeout ?? DEFAULT_DRAIN_TIMEOUT;
    this.commandTimeout = options.commandTimeout ?? DEFAULT_COMMAND_TIMEOUT;
    this.stopPromise = new Promise<void>((resolve) => {
      this.stopResolve = resolve;
    });

    // A handler outliving claimIdleMs looks stalled to the sweep, which then
    // redelivers the entry to another replica while the original handler is
    // still running - systematic duplicate processing. The derived default
    // keeps the invariant when only claimIdleMs is lowered; an explicit
    // conflicting value is a config error. Note: handlerTimeout rejects the
    // dispatch but does NOT cancel the running handler.
    if (options.handlerTimeout !== undefined && options.handlerTimeout > this.claimIdleMs) {
      throw new Error(
        `handlerTimeout (${options.handlerTimeout}ms) must not exceed claimIdleMs (${this.claimIdleMs}ms) - ` +
          'a handler outliving claimIdleMs is redelivered to another replica while it is still running',
      );
    }

    this.handlerTimeout = options.handlerTimeout ?? Math.min(DEFAULT_HANDLER_TIMEOUT, this.claimIdleMs);
  }

  /**
   * Readiness = "this instance can complete a send()", not "a socket is open".
   *
   * The publisher being connected is not that. Replies travel a plain pub/sub
   * channel with no replay and the request is ACKed unconditionally, so a
   * reply published while this instance's reply subscription is not live is
   * dropped by Redis and lost - the caller only ever sees a timeout. The
   * publisher and the reply subscriber are separate connections that come
   * back separately, and the reply subscription costs a further round trip
   * after its socket reports open. Measured over 60 connection outages, the
   * publisher was green 0.5-1.8ms before the reply channel had a subscriber
   * again, and 5% of requests issued in that window were lost behind a
   * healthy-looking endpoint.
   *
   * This holds for a client-only instance as well - an HTTP gateway never
   * sets `running`, and it is the instance that depends on the reply
   * subscriber most.
   */
  public get isConnected(): boolean {
    return this.connected && this.publisher?.isConnected === true && this.replyServing;
  }

  /** True while Redis actually holds a subscription on this reply channel. */
  private get replyServing(): boolean {
    return this.replySubscriber?.isConnected === true && this.replySubscribed;
  }

  public async init(): Promise<void> {
    if (this.isRedisService(this.source)) {
      const service = this.source;

      // Own duplicate instead of the borrowed client itself: poisoning
      // recovery discards and replaces the publisher connection, which is
      // only legal on a connection the transport owns. The user's client is
      // never touched.
      this.connectionFactory = () => service.client.duplicate();
      this.installPublisher(await this.connectionFactory());
      this.installReplySubscriber(await service.createSubscriber());
    } else {
      const url = buildRedisUrl(this.source);
      const { url: _u, name: _n, host: _h, port: _p, username: _un, password: _pw, db: _d, ...opts } = this.source;

      this.connectionFactory = async () => {
        const client = new BunRedisAdapter(url, opts);

        await client.connect();

        return client;
      };

      this.installPublisher(await this.connectionFactory());
      this.installReplySubscriber(await this.publisher.duplicate());
    }

    // Reply channel is live from init() so client-only send() works before listen()
    await this.replySubscriber!.subscribe(this.replyChannel, (message: string) => this.handleReply(message));

    this.replySubscribed = true;
    this.connected = true;
  }

  public registerMessageHandler(pattern: string, handler: MessageHandler): void {
    // Validate the FINAL pattern: the decorator only sees the raw method
    // pattern, so a wildcard or emptiness introduced by the @MessageController
    // prefix would otherwise slip through
    if (!pattern) {
      throw new Error('Message pattern cannot be empty - check @MessagePattern and the @MessageController prefix');
    }

    if (pattern.includes('*')) {
      throw new Error(
        `Message pattern "${pattern}" cannot contain wildcards - request/response requires exact routing ` +
          '(a wildcard likely leaked in via the @MessageController prefix - remove it, or set ' +
          'prefix: false on the @MessagePattern)',
      );
    }

    if (this.messageHandlers.has(pattern)) {
      throw new Error(`Duplicate @MessagePattern('${pattern}') - a message pattern can only have one handler`);
    }

    this.messageHandlers.set(pattern, handler);
  }

  public registerEventHandler(pattern: string, handler: EventPatternHandler): void {
    if (!pattern) {
      throw new Error('Event pattern cannot be empty - check @EventPattern and the @MessageController prefix');
    }

    this.eventHandlers.add(pattern, handler);
  }

  public async listen(): Promise<void> {
    const streams = this.consumedStreams();

    // Contract: zero handlers → no consumer loop, no groups (client-only mode)
    if (!streams.length) {
      return;
    }

    for (const stream of streams) {
      await this.guardedPublisherCall('XGROUP CREATE', (client) => xgroupCreate(client, stream, this.serviceName));
    }

    // Dedicated blocking connection - XREADGROUP BLOCK would freeze the publisher
    this.consumer = await this.connectionFactory();

    this.running = true;
    this.consumerLoop = this.runConsumerLoop();
    // Keep a handle on the in-flight sweep so destroy() can await it
    this.sweepTimer = setInterval(
      () => {
        this.sweepPromise = this.sweep();
      },
      Math.max(1_000, Math.floor(this.claimIdleMs / 2)),
    );
  }

  public async send<T = unknown>(pattern: string, data?: unknown, options?: SendOptions): Promise<T> {
    const correlationId = crypto.randomUUID();
    const timeout = options?.timeout ?? this.requestTimeout;

    const fields: Record<string, string> = {
      p: pattern,
      d: JSON.stringify(data ?? null),
      h: JSON.stringify(options?.headers ?? {}),
      ts: String(Date.now()),
      c: correlationId,
      r: this.replyChannel,
      // Caller's own timeout, so the sweep can judge staleness per entry
      // instead of assuming the transport-wide default
      to: String(timeout),
    };

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingRequests.delete(correlationId);
        reject(new UlakError(`Request "${pattern}" timed out after ${timeout}ms`, UlakErrorCode.TIMEOUT));
      }, timeout);

      this.pendingRequests.set(correlationId, { resolve, reject, timer });

      this.guardedPublisherCall('XADD', (client) =>
        xadd(client, this.requestStream(pattern), fields, this.maxStreamLength),
      ).catch((error) => {
        const pending = this.pendingRequests.get(correlationId);

        if (pending) {
          this.pendingRequests.delete(correlationId);
          clearTimeout(pending.timer);
          pending.reject(
            new UlakError(
              `Failed to publish request "${pattern}": ${(error as Error).message}`,
              UlakErrorCode.SEND_FAILED,
              undefined,
              error as Error,
            ),
          );
        }
      });
    });
  }

  public async emit(pattern: string, data?: unknown, options?: EmitOptions): Promise<void> {
    const fields: Record<string, string> = {
      p: pattern,
      d: JSON.stringify(data ?? null),
      h: JSON.stringify(options?.headers ?? {}),
      ts: String(Date.now()),
    };

    await this.guardedPublisherCall('XADD', (client) => xadd(client, this.eventStream, fields, this.maxStreamLength));
  }

  public destroy(options?: DestroyOptions): Promise<void> {
    // Idempotent: concurrent/repeated calls join the first teardown
    this.destroyPromise ??= this.doDestroy(options);

    return this.destroyPromise;
  }

  private async doDestroy(options?: DestroyOptions): Promise<void> {
    const drainTimeout = options?.drainTimeout ?? this.defaultDrainTimeout;

    // 1. Stop consuming new messages. The stop signal wakes the consumer
    //    loop out of a reconnect backoff sleep - without it a SIGTERM during
    //    a broker outage would wait out up to RECONNECT_BACKOFF_CAP here.
    this.running = false;
    this.destroyed = true;
    this.stopResolve();

    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = undefined;
    }

    // The blocking read returns within blockMs, then the loop exits
    if (this.consumerLoop) {
      await this.consumerLoop.catch(() => {});
    }

    // A sweep started before running=false may still be claiming/dispatching.
    // Wait for it here so its dispatches land in the in-flight snapshot below
    // and it cannot resurrect our consumer after the DELCONSUMER step.
    if (this.sweepPromise) {
      await this.sweepPromise.catch(() => {});
    }

    // 2. Drain in-flight handlers (finished ones ACK; unfinished stay pending
    //    for another replica - at-least-once tolerates this)
    if (this.inFlight.size) {
      await Promise.race([
        Promise.allSettled([...this.inFlight]),
        new Promise((resolve) => {
          setTimeout(resolve, drainTimeout);
        }),
      ]);
    }

    // 3. Reject pending sends
    for (const [correlationId, pending] of this.pendingRequests) {
      clearTimeout(pending.timer);
      pending.reject(new UlakError('Transport destroyed', UlakErrorCode.SEND_FAILED));
      this.pendingRequests.delete(correlationId);
    }

    // 4. Dead-consumer hygiene: delete our consumer ONLY when its PEL is
    //    empty. XGROUP DELCONSUMER drops the consumer's pending entries from
    //    the group - a non-empty PEL must survive so another replica's sweep
    //    can XCLAIM the entries. The leftover consumer name is then
    //    garbage-collected by the sweep once drained. Skipped entirely when
    //    the publisher socket is down: commands on a dead connection may
    //    never settle, and hygiene is not worth stalling shutdown for.
    if (this.publisher?.isConnected) {
      for (const stream of this.consumedStreams()) {
        try {
          const own = await this.guardedPublisherCall('XPENDING', (client) =>
            xpendingConsumer(client, stream, this.serviceName, this.instanceId, 1),
          );

          if (own.length === 0) {
            await this.guardedPublisherCall('XGROUP DELCONSUMER', (client) =>
              xgroupDelConsumer(client, stream, this.serviceName, this.instanceId),
            );
          }
        } catch {
          // Best effort - never block shutdown
        }
      }
    }

    // 5. Release connections. Every step is bounded: commands issued on a
    //    dead connection may NEVER settle (Bun keeps them pending across
    //    reconnect attempts), and shutdown must not hang behind them.
    this.connected = false;

    if (this.consumer) {
      await this.bounded(this.consumer.disconnect());
      this.consumer = undefined;
    }

    if (this.replySubscriber) {
      // Detach first: the connection's own events are identity-guarded
      // against this field, so clearing it makes anything the socket still
      // emits during teardown inert instead of flipping readiness back green
      const subscriber = this.replySubscriber;

      this.replySubscriber = undefined;
      this.replySubscribed = false;

      await this.bounded(subscriber.unsubscribe(this.replyChannel));
      await this.bounded(subscriber.disconnect());
    }

    // The publisher is always transport-owned (a duplicate even when built
    // from an AsenaRedisService). Wait for an in-progress poisoning swap so
    // its replacement connection cannot leak past shutdown.
    if (this.publisherSwap) {
      await this.publisherSwap.catch(() => {});
    }

    if (this.publisher) {
      await this.bounded(this.publisher.disconnect());
    }
  }

  /**
   * Best-effort await with an upper bound - teardown steps must never hang
   * shutdown behind a dead connection whose commands may never settle.
   */
  private async bounded(work: Promise<unknown>): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;

    await Promise.race([
      work.catch(() => {}),
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, SHUTDOWN_STEP_TIMEOUT);
      }),
    ]).finally(() => clearTimeout(timer));
  }

  // --- Publisher guard (Bun in-flight-loss poisoning defense) --------------

  /**
   * Attaches a replacement-capable publisher connection. When the socket dies
   * while commands are in flight, Bun's RedisClient neither settles those
   * promises nor realigns its reply queue after the automatic reconnect:
   * every later reply resolves the WRONG promise, permanently (observed
   * empirically - see readBlockingWithWatchdog for the consumer-side twin of
   * this defense). The onConnectionLost hook detects exactly that condition
   * (in-flight > 0 at close) and replaces the connection immediately;
   * guardedPublisherCall's timeout is the backstop for adapters without
   * connection events.
   */
  private installPublisher(client: RedisClientAdapter): void {
    const state: PublisherState = { client, inFlight: 0, poisoned: false };

    this.publisherState = state;
    this.publisher = client;

    client.onConnectionLost?.(() => {
      if (state.poisoned || state.inFlight === 0) return;

      state.poisoned = true;
      console.error(
        `RedisMicroserviceTransport(${this.serviceName}): publisher connection lost with ${state.inFlight} command(s) in flight - reply queue is desynced, replacing connection`,
      );

      if (!this.destroyed) {
        void this.swapPublisher(state).catch(() => {});
      }
    });
  }

  // --- Reply subscriber (readiness) ----------------------------------------

  /**
   * Attaches the reply subscriber and wires its connection events into
   * readiness, the same way installPublisher wires the publisher's.
   *
   * A reconnect and a live reply channel are two different facts. Redis drops
   * every subscription with the socket; the adapter replays it, but only
   * AFTER the socket reports open, so `isConnected` on that connection turns
   * true while the channel still has no subscriber. Anything the responder
   * publishes in between is dropped - plain pub/sub, no replay, and the
   * request was ACKed - so readiness must not go green on the socket alone.
   *
   * The down edge comes from the socket state rather than an event on
   * purpose: Bun's RedisClient does not raise `onclose` for a transient loss
   * (verified - only `onconnect` fires on the way back), so `onConnected`
   * clearing the flag is what actually reports the replay window, and
   * `onConnectionLost` covers a final close on adapters that do raise it.
   *
   * The listeners are identity-guarded, like the kafka reply consumer's: a
   * superseded connection's late event must never touch the readiness of the
   * connection that replaced it, and destroy() clears the field so nothing
   * can report ready after teardown.
   */
  private installReplySubscriber(client: RedisClientAdapter): void {
    this.replySubscriber = client;
    this.replySubscribed = false;

    const isCurrent = (): boolean => this.replySubscriber === client;

    client.onConnectionLost?.(() => {
      if (!isCurrent()) return;

      this.replySubscribed = false;
    });

    if (client.onResubscribed) {
      client.onConnected?.(() => {
        if (!isCurrent()) return;

        // Connected again, but the channel is not served again until the
        // replay lands - that gap is the whole point of this flag
        this.replySubscribed = false;
      });

      client.onResubscribed((channel) => {
        if (!isCurrent() || channel !== this.replyChannel) return;

        this.replySubscribed = true;
      });
    } else {
      // Adapters whose client restores subscriptions inside its own reconnect
      // handshake before signalling ready (node-redis does) have no separate
      // moment to report - their connect event already carries it.
      client.onConnected?.(() => {
        if (!isCurrent()) return;

        this.replySubscribed = true;
      });
    }
  }

  /**
   * Runs a publisher command with a wedge watchdog: the returned promise is
   * guaranteed to settle within commandTimeout. A non-blocking command
   * outliving that bound means the connection died with the command in
   * flight, so the connection is marked poisoned and replaced. The orphaned
   * promise is detached - it must never be processed and never go unhandled.
   */
  private async guardedPublisherCall<T>(label: string, fn: (client: RedisClientAdapter) => Promise<T>): Promise<T> {
    const WEDGED = Symbol('wedged');
    const used: { state?: PublisherState } = {};
    const call = this.publisherCallOnce(fn, used);

    let timer: ReturnType<typeof setTimeout> | undefined;

    const winner = await Promise.race([
      call,
      new Promise<typeof WEDGED>((resolve) => {
        timer = setTimeout(() => resolve(WEDGED), this.commandTimeout);
      }),
    ]).finally(() => clearTimeout(timer));

    if (winner === WEDGED) {
      call.catch(() => {});

      const state = used.state;

      // Poison only the connection the call actually ran on - a call that was
      // still waiting for an in-progress swap has no connection to blame
      if (state && !state.poisoned && this.publisherState === state && !this.destroyed) {
        state.poisoned = true;
        console.error(
          `RedisMicroserviceTransport(${this.serviceName}): publisher command ${label} exceeded ${this.commandTimeout}ms - connection wedged, replacing`,
        );
        void this.swapPublisher(state).catch(() => {});
      }

      throw new Error(
        `publisher command ${label} exceeded commandTimeout (${this.commandTimeout}ms) - connection wedged, replaced`,
      );
    }

    return winner as T;
  }

  private async publisherCallOnce<T>(
    fn: (client: RedisClientAdapter) => Promise<T>,
    used: { state?: PublisherState },
  ): Promise<T> {
    if (this.publisherSwap) {
      await this.publisherSwap;
    }

    if (this.publisherState.poisoned) {
      await this.swapPublisher(this.publisherState);
    }

    const state = this.publisherState;

    used.state = state;
    state.inFlight++;

    try {
      return await fn(state.client);
    } finally {
      state.inFlight--;
    }
  }

  /**
   * Bounds a connection attempt by the transport's stop signal so shutdown
   * never waits behind a broker that is still down. A connection that
   * completes after the stop is disposed of instead of leaking.
   */
  private raceStop(work: Promise<RedisClientAdapter>): Promise<RedisClientAdapter> {
    const STOPPED = Symbol('stopped');

    return Promise.race([work, this.stopPromise.then(() => STOPPED)]).then((winner) => {
      if (winner === STOPPED) {
        work.then((client) => client.disconnect().catch(() => {})).catch(() => {});
        throw new Error('transport stopped during connection attempt');
      }

      return winner as RedisClientAdapter;
    });
  }

  /**
   * Single-flight replacement of a poisoned publisher connection: concurrent
   * detections join the same swap, and a swap requested for an already
   * replaced state is a no-op. A failed swap (broker still down) clears
   * itself so the next caller retries.
   */
  private swapPublisher(poisoned: PublisherState): Promise<void> {
    if (this.publisherState !== poisoned) {
      return this.publisherSwap ?? Promise.resolve();
    }

    this.publisherSwap ??= (async () => {
      const fresh = await this.raceStop(this.connectionFactory());

      // destroy() ran while the replacement was connecting - do not leak it
      if (this.destroyed) {
        await fresh.disconnect().catch(() => {});

        return;
      }

      this.installPublisher(fresh);
      await poisoned.client.disconnect().catch(() => {});
    })().finally(() => {
      this.publisherSwap = undefined;
    });

    return this.publisherSwap;
  }

  // --- Consumer loop -------------------------------------------------------

  /**
   * Blocking XREADGROUP guarded by a wedge watchdog. When the connection dies
   * mid-command, Bun's RedisClient neither rejects the in-flight promise nor
   * clears its command slot after reconnecting: the read hangs forever, and a
   * later reply on that connection would resolve the WRONG promise (observed
   * empirically - an orphaned XREADGROUP resolving with "PONG"). If the read
   * outlives BLOCK by a wide margin, the connection is poisoned: discard it
   * entirely, attach a fresh duplicate, and route to the reconnect path.
   */
  private async readBlockingWithWatchdog(): Promise<Map<string, StreamEntry[]>> {
    const WEDGED = Symbol('wedged');

    // A previous replacement may have failed (broker down mid-swap) - the
    // consumer is then recreated lazily here instead of looping on a
    // TypeError against undefined forever
    if (!this.consumer) {
      this.consumer = await this.raceStop(this.connectionFactory());
    }

    const read = xreadgroup(
      this.consumer,
      this.serviceName,
      this.instanceId,
      this.consumedStreams(),
      this.count,
      this.blockMs,
    );

    let timer: ReturnType<typeof setTimeout> | undefined;

    const winner = await Promise.race([
      read,
      new Promise<typeof WEDGED>((resolve) => {
        timer = setTimeout(() => resolve(WEDGED), this.blockMs + WEDGE_MARGIN_MS);
      }),
    ]).finally(() => clearTimeout(timer));

    if (winner === WEDGED) {
      // The orphaned promise may still settle with a shifted reply or reject
      // later - it must never be processed and never go unhandled
      read.catch(() => {});

      const poisoned = this.consumer;

      this.consumer = undefined;
      await poisoned?.disconnect().catch(() => {});
      // If this rejects (broker down), consumer stays undefined and the lazy
      // recreation above recovers it on the next loop iteration
      this.consumer = await this.raceStop(this.connectionFactory());

      throw new Error(`blocking read exceeded BLOCK by ${WEDGE_MARGIN_MS}ms - consumer connection wedged, replaced`);
    }

    return winner;
  }

  private async runConsumerLoop(): Promise<void> {
    let backoff = RECONNECT_BACKOFF_START;

    while (this.running) {
      try {
        // Backpressure: wait for a slot before reading more
        while (this.running && this.inFlight.size >= this.maxInFlight) {
          await Promise.race(this.inFlight);
        }

        if (!this.running) break;

        const batches = await this.readBlockingWithWatchdog();

        this.connected = true;
        backoff = RECONNECT_BACKOFF_START;

        for (const [stream, entries] of batches) {
          for (const entry of entries) {
            this.track(this.dispatch(stream, entry, 1));
          }
        }
      } catch (error) {
        if (!this.running) break;

        // NOGROUP is permanent, not transient: the group is gone (Redis
        // restarted without persistence / FLUSHALL) and no amount of backoff
        // brings it back - recreate it and resume. '0' instead of '$' so
        // entries XADDed between the loss and this recovery are replayed.
        if (String((error as Error)?.message ?? error).includes('NOGROUP')) {
          console.error(
            `RedisMicroserviceTransport(${this.serviceName}): consumer group lost (Redis restart/flush) - recreating`,
          );

          try {
            for (const stream of this.consumedStreams()) {
              await this.guardedPublisherCall('XGROUP CREATE', (client) =>
                xgroupCreate(client, stream, this.serviceName, '0'),
              );
            }

            continue; // Group restored - resume reading without backoff
          } catch {
            // Redis still down - fall through to the normal backoff below
          }
        }

        // Connection trouble: report degraded, retry with capped backoff.
        // Streams keep the messages - nothing is lost within the trim window.
        this.connected = false;
        console.error(`RedisMicroserviceTransport(${this.serviceName}): consumer loop error:`, error);

        // Raced against the stop signal so destroy() never has to wait out
        // a capped (up to 30s) backoff sleep
        let timer: ReturnType<typeof setTimeout> | undefined;

        await Promise.race([
          new Promise<void>((resolve) => {
            timer = setTimeout(resolve, backoff);
          }),
          this.stopPromise,
        ]).finally(() => clearTimeout(timer));

        backoff = Math.min(backoff * 2, RECONNECT_BACKOFF_CAP);
      }
    }
  }

  private track(work: Promise<void>): void {
    const tracked: Promise<void> = work.finally(() => {
      this.inFlight.delete(tracked);
    });

    this.inFlight.add(tracked);
  }

  private async dispatch(stream: string, entry: StreamEntry, attempt: number): Promise<void> {
    if (stream === this.eventStream) {
      await this.dispatchEvent(entry, attempt);
    } else {
      await this.dispatchRequest(stream, entry, attempt);
    }
  }

  // --- Event path (at-least-once, retry + DLQ) -----------------------------

  private async dispatchEvent(entry: StreamEntry, attempt: number): Promise<void> {
    const context = this.buildContext(entry, attempt);

    const handlers = this.eventHandlers.collect(context.pattern);

    // No local handler for this pattern - ACK immediately, nothing to do
    if (!handlers.length) {
      await this.guardedPublisherCall('XACK', (client) =>
        xack(client, this.eventStream, this.serviceName, [entry.id]),
      ).catch(() => {});
      return;
    }

    const data = this.parsePayload(entry);

    try {
      await this.withHandlerTimeout(
        Promise.all(handlers.map((handler) => Promise.resolve(handler(data, context)))),
        context.pattern,
      );

      await this.guardedPublisherCall('XACK', (client) => xack(client, this.eventStream, this.serviceName, [entry.id]));
    } catch (error) {
      // NO ACK: the entry stays pending, the sweep redelivers it (up to maxRetries)
      console.error(
        `RedisMicroserviceTransport(${this.serviceName}): event handler failed for "${context.pattern}" (attempt ${attempt}):`,
        error,
      );
    }
  }

  // --- Request path (RPC errors are final, no broker retry) ----------------

  private async dispatchRequest(stream: string, entry: StreamEntry, attempt: number): Promise<void> {
    const context = this.buildContext(entry, attempt);
    const handler = this.messageHandlers.get(context.pattern);
    const replyChannel = entry.fields['r'];

    let reply: { c: string; ok: boolean; d?: unknown; e?: { name: string; message: string } };

    if (!handler) {
      // Stream exists but no local handler (should not happen - streams are derived from handlers)
      reply = {
        c: context.correlationId,
        ok: false,
        e: { name: 'UlakError', message: `No handler for pattern "${context.pattern}"` },
      };
    } else {
      try {
        const result = await this.withHandlerTimeout(
          Promise.resolve(handler(this.parsePayload(entry), context)),
          context.pattern,
        );

        reply = { c: context.correlationId, ok: true, d: result ?? null };
      } catch (error) {
        reply = {
          c: context.correlationId,
          ok: false,
          e: { name: (error as Error).name || 'Error', message: (error as Error).message },
        };
      }
    }

    if (replyChannel) {
      await this.guardedPublisherCall('PUBLISH', (client) => client.publish(replyChannel, JSON.stringify(reply))).catch(
        (error) => {
          console.error(`RedisMicroserviceTransport(${this.serviceName}): failed to publish reply:`, error);
        },
      );
    }

    // RPC is final either way - ACK success AND error (no broker retry)
    await this.guardedPublisherCall('XACK', (client) => xack(client, stream, this.serviceName, [entry.id])).catch(
      () => {},
    );
  }

  private handleReply(message: string): void {
    try {
      const reply = JSON.parse(message);
      const pending = this.pendingRequests.get(reply.c);

      // Late/duplicate replies are ignored - the entry is already settled
      if (!pending) return;

      this.pendingRequests.delete(reply.c);
      clearTimeout(pending.timer);

      if (reply.ok) {
        pending.resolve(reply.d);
      } else {
        pending.reject(
          new UlakError(`Remote handler failed: ${reply.e?.message ?? 'unknown error'}`, UlakErrorCode.REMOTE_ERROR),
        );
      }
    } catch (error) {
      console.error(`RedisMicroserviceTransport(${this.serviceName}): failed to handle reply:`, error);
    }
  }

  // --- Sweep: crash recovery, retry counting, DLQ --------------------------

  private async sweep(): Promise<void> {
    if (this.sweeping || !this.running) return;

    this.sweeping = true;

    try {
      // running is re-checked after every await: destroy() may flip it to
      // false while we are suspended on a Redis call - bailing early keeps
      // shutdown latency at one command and prevents dispatching after the
      // drain snapshot
      for (const stream of this.consumedStreams()) {
        if (!this.running) return;

        const pending = await this.guardedPublisherCall('XPENDING', (client) =>
          xpending(client, stream, this.serviceName, this.claimIdleMs, 100),
        );

        for (const row of pending) {
          if (!this.running) return;

          if (stream === this.eventStream && row.deliveryCount > this.maxRetries) {
            await this.moveToDlq(stream, row.id, row.deliveryCount);
            continue;
          }

          if (stream !== this.eventStream) {
            // XPENDING rows carry no payload, so claim first, then judge
            // staleness against the caller's own timeout (envelope field
            // 'to'). An empty claim means another replica won the race.
            const claimed = await this.guardedPublisherCall('XCLAIM', (client) =>
              xclaim(client, stream, this.serviceName, this.instanceId, this.claimIdleMs, [row.id]),
            );

            for (const entry of claimed) {
              const callerTimeout = Number(entry.fields['to']) || this.requestTimeout;
              const age = Date.now() - (Number(entry.fields['ts']) || entryTimestamp(entry.id));

              if (age > callerTimeout) {
                // The caller has already timed out - re-executing is wasted work
                await this.guardedPublisherCall('XACK', (client) => xack(client, stream, this.serviceName, [entry.id]));
              } else {
                // The claiming replica crashed mid-request and the caller is
                // still waiting - take the entry over and process it here
                if (!(await this.gateSweepDispatch())) return;

                this.track(this.dispatch(stream, entry, row.deliveryCount + 1));
              }
            }

            continue;
          }

          // Take over the crashed/stalled replica's entry and process it here
          const claimed = await this.guardedPublisherCall('XCLAIM', (client) =>
            xclaim(client, stream, this.serviceName, this.instanceId, this.claimIdleMs, [row.id]),
          );

          for (const entry of claimed) {
            if (!(await this.gateSweepDispatch())) return;

            this.track(this.dispatch(stream, entry, row.deliveryCount + 1));
          }
        }

        await this.cleanupDeadConsumers(stream);
      }
    } catch (error) {
      // NOGROUP after a Redis flush also lands here - the consumer loop owns
      // group re-creation, the sweep just skips the cycle
      console.error(`RedisMicroserviceTransport(${this.serviceName}): sweep error:`, error);
    } finally {
      this.sweeping = false;
    }
  }

  /**
   * Backpressure gate for sweep-claimed dispatches - the consumer loop's
   * maxInFlight check does not cover the sweep, so without this a large
   * backlog of reclaimed entries could exceed the in-flight budget.
   * Returns false when the transport is shutting down.
   */
  private async gateSweepDispatch(): Promise<boolean> {
    while (this.running && this.inFlight.size >= this.maxInFlight) {
      await Promise.race(this.inFlight);
    }

    return this.running;
  }

  /**
   * Remove consumers that are gone for good: zero pending entries and idle
   * well beyond claimIdleMs. Deleting a 0-pending consumer never loses data,
   * and a live consumer transparently re-registers on its next XREADGROUP -
   * the idle threshold only limits delete/recreate churn. Without this,
   * every replica that shut down with a non-empty PEL (destroy() skips
   * DELCONSUMER then) would leave its consumer name in the group forever.
   */
  private async cleanupDeadConsumers(stream: string): Promise<void> {
    const consumers = await this.guardedPublisherCall('XINFO CONSUMERS', (client) =>
      xinfoConsumers(client, stream, this.serviceName),
    ).catch(() => []);

    for (const consumer of consumers) {
      if (!this.running) return;

      if (consumer.name === this.instanceId) continue;

      if (consumer.pending > 0) continue;

      if (consumer.idleMs < this.claimIdleMs * 4) continue;

      await this.guardedPublisherCall('XGROUP DELCONSUMER', (client) =>
        xgroupDelConsumer(client, stream, this.serviceName, consumer.name),
      ).catch(() => {});
    }
  }

  private async moveToDlq(stream: string, id: string, deliveryCount: number): Promise<void> {
    // Claim to obtain the payload, then park it in the DLQ stream with provenance
    const claimed = await this.guardedPublisherCall('XCLAIM', (client) =>
      xclaim(client, stream, this.serviceName, this.instanceId, this.claimIdleMs, [id]),
    );

    for (const entry of claimed) {
      await this.guardedPublisherCall('XADD', (client) =>
        xadd(
          client,
          this.dlqStream,
          {
            ...entry.fields,
            origin_stream: stream,
            origin_group: this.serviceName,
            origin_id: entry.id,
            delivery_count: String(deliveryCount),
            dlq_ts: String(Date.now()),
          },
          this.maxStreamLength,
        ),
      );

      console.error(
        `RedisMicroserviceTransport(${this.serviceName}): entry ${entry.id} (pattern "${entry.fields['p']}") moved to DLQ after ${deliveryCount} deliveries`,
      );
    }

    await this.guardedPublisherCall('XACK', (client) =>
      xack(
        client,
        stream,
        this.serviceName,
        claimed.map((e) => e.id),
      ),
    );
  }

  // --- Helpers -------------------------------------------------------------

  private get eventStream(): string {
    return `${this.streamPrefix}:evt`;
  }

  private get dlqStream(): string {
    return `${this.streamPrefix}:dlq`;
  }

  private get replyChannel(): string {
    return `${this.streamPrefix}:reply:${this.instanceId}`;
  }

  private requestStream(pattern: string): string {
    return `${this.streamPrefix}:req:${pattern}`;
  }

  /**
   * Streams this instance consumes: the shared event stream (only when event
   * handlers exist) plus one request stream per registered message pattern.
   */
  private consumedStreams(): string[] {
    const streams: string[] = [];

    if (!this.eventHandlers.isEmpty) {
      streams.push(this.eventStream);
    }

    for (const pattern of this.messageHandlers.keys()) {
      streams.push(this.requestStream(pattern));
    }

    return streams;
  }

  private buildContext(entry: StreamEntry, attempt: number): MessageContext {
    let headers: Record<string, string> = {};

    try {
      headers = JSON.parse(entry.fields['h'] || '{}');
    } catch {
      // Malformed headers - continue with empty headers
    }

    return {
      pattern: entry.fields['p'],
      messageId: entry.id,
      correlationId: entry.fields['c'],
      headers,
      timestamp: Number(entry.fields['ts']) || entryTimestamp(entry.id),
      attempt,
    };
  }

  private parsePayload(entry: StreamEntry): unknown {
    try {
      return JSON.parse(entry.fields['d'] ?? 'null');
    } catch {
      return entry.fields['d'];
    }
  }

  private withHandlerTimeout<T>(promise: Promise<T>, pattern: string): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(
          new UlakError(
            `Handler for "${pattern}" exceeded handlerTimeout (${this.handlerTimeout}ms)`,
            UlakErrorCode.TIMEOUT,
          ),
        );
      }, this.handlerTimeout);

      promise
        .then((value) => {
          clearTimeout(timer);
          resolve(value);
        })
        .catch((error) => {
          clearTimeout(timer);
          reject(error);
        });
    });
  }

  private isRedisService(source: AsenaRedisService | RedisConfig): source is AsenaRedisService {
    return typeof (source as AsenaRedisService).createSubscriber === 'function';
  }
}
