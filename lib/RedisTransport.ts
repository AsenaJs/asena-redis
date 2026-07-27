import type { Server } from 'bun';
import type { WebSocketData, WebSocketTransport } from '@asenajs/asena/web-socket';
import type { RedisClientAdapter } from './adapter';
import { BunRedisAdapter } from './adapter';
import { buildRedisUrl } from './adapter';
import type { AsenaRedisService } from './AsenaRedisService';
import type { RedisConfig, RedisTransportOptions } from './types';

const DEFAULT_CHANNEL = 'asena:ws:transport';

interface TransportEnvelope {
  d: string;
  t: string;
  o: string;
  b?: 1;
}

export class RedisTransport implements WebSocketTransport {
  private server!: Server<WebSocketData>;

  private readonly podId = crypto.randomUUID();

  private readonly channel: string;

  private publisher!: RedisClientAdapter;

  private subscriber!: RedisClientAdapter;

  private readonly source: AsenaRedisService | RedisConfig;

  private ownsPublisher = false;

  public constructor(source: AsenaRedisService | RedisConfig, options?: RedisTransportOptions) {
    this.source = source;
    this.channel = options?.channel ?? DEFAULT_CHANNEL;
  }

  public async init(server: Server<WebSocketData>): Promise<void> {
    this.server = server;

    if (this.isRedisService(this.source)) {
      this.publisher = this.source.client;
      this.subscriber = await this.source.createSubscriber();
    } else {
      const url = buildRedisUrl(this.source);
      const { url: _u, name: _n, host: _h, port: _p, username: _un, password: _pw, db: _d, ...opts } = this.source;

      this.publisher = new BunRedisAdapter(url, opts);
      await this.publisher.connect();
      this.ownsPublisher = true;

      this.subscriber = await this.publisher.duplicate();
    }

    await this.subscriber.subscribe(this.channel, (message: string) => {
      this.handleMessage(message);
    });
  }

  public publish(topic: string, data: string | ArrayBuffer | ArrayBufferView): void {
    // Local delivery
    this.server.publish(topic, data as string | ArrayBuffer);

    // Remote delivery via Redis
    let envelope: TransportEnvelope;

    if (typeof data === 'string') {
      envelope = { d: data, t: topic, o: this.podId };
    } else {
      const buffer = Buffer.from(data instanceof ArrayBuffer ? data : data.buffer);

      envelope = { d: buffer.toString('base64'), t: topic, o: this.podId, b: 1 };
    }

    this.publisher.publish(this.channel, JSON.stringify(envelope)).catch((err) => {
      console.error('RedisTransport: Failed to publish to Redis:', err);
    });
  }

  public async destroy(): Promise<void> {
    if (this.subscriber) {
      await this.subscriber.unsubscribe(this.channel);
      await this.subscriber.disconnect();
      this.subscriber = null!;
    }

    if (this.ownsPublisher && this.publisher) {
      await this.publisher.disconnect();
      this.publisher = null!;
    }
  }

  private handleMessage(message: string): void {
    try {
      const envelope: TransportEnvelope = JSON.parse(message);

      // Deduplication: skip messages from this pod
      if (envelope.o === this.podId) return;

      let data: string | ArrayBuffer;

      if (envelope.b) {
        const buffer = Buffer.from(envelope.d, 'base64');

        data = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
      } else {
        data = envelope.d;
      }

      // Deliver to local sockets
      this.server.publish(envelope.t, data as string | ArrayBuffer);
    } catch (err) {
      console.error('RedisTransport: Failed to handle message:', err);
    }
  }

  private isRedisService(source: AsenaRedisService | RedisConfig): source is AsenaRedisService {
    return typeof (source as AsenaRedisService).createSubscriber === 'function';
  }
}
