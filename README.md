<p width="%100" align="center">
  <img src="https://avatars.githubusercontent.com/u/179836938?s=200&v=4" width="150" align="center"/>
</p>

# @asenajs/asena-redis

[![Version](https://img.shields.io/badge/version-3.1.1-blue.svg)](https://github.com/AsenaJs/asena-redis)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Bun Version](https://img.shields.io/badge/Bun-1.4%2B-blueviolet)](https://bun.sh)

Redis integration for AsenaJS — service client with built-in multi-pod WebSocket transport.

Your existing `@Redis` decorated service gives you full Redis operations with automatic IoC registration. For multi-pod deployments, `RedisTransport` synchronizes WebSocket messages across instances via Redis pub/sub.

## Features

- **Decorator-Based Setup** - `@Redis` decorator handles IoC registration and connection lifecycle
- **Dual Adapter Support** - Bun native `RedisClient` (default) and `redis` (node-redis) package
- **Multi-Pod WebSocket Transport** - Synchronize WebSocket messages across pods via Redis pub/sub
- **Full Redis Operations** - String, Hash, Set, Key, and raw command support
- **Binary Data Support** - ArrayBuffer and Uint8Array transport with Base64 encoding
- **Zero Runtime Dependencies** - Only peer deps (asena, reflect-metadata)

## Requirements

- [Bun](https://bun.sh) v1.4 or higher
- [@asenajs/asena](https://github.com/AsenaJs/Asena) v0.10.1 or higher

## Installation

```bash
bun add @asenajs/asena-redis
```

For node-redis adapter (optional):
```bash
bun add @asenajs/asena-redis redis
```

## Quick Start

```typescript
import { Redis, AsenaRedisService } from '@asenajs/asena-redis';

@Redis({
  config: { url: 'redis://localhost:6379' },
  name: 'AppRedis',
})
export class AppRedis extends AsenaRedisService {

  async getOrSet(key: string, factory: () => Promise<string>, ttl?: number): Promise<string> {
    const cached = await this.get(key);

    if (cached) return cached;

    const value = await factory();
    await this.set(key, value, ttl);

    return value;
  }

}
```

Asena automatically discovers it — that's it.

Now inject and use:

```typescript
import { Service } from '@asenajs/asena/decorators';
import { Inject } from '@asenajs/asena/decorators/ioc';

@Service('CacheService')
export class CacheService {

  @Inject('AppRedis')
  private redis: AppRedis;

  async getUserName(id: string): Promise<string> {
    return this.redis.getOrSet(`user:${id}`, async () => {
      // fetch from database...
      return 'John';
    }, 60);
  }

}
```

## Adapter Selection

By default, `@asenajs/asena-redis` uses Bun's native `RedisClient`. For environments requiring the `redis` (node-redis) package:

```typescript
@Redis({
  config: { url: 'redis://localhost:6379' },
  adapter: 'node-redis',
})
export class AppRedis extends AsenaRedisService {}
```

| Adapter | Package | Best For |
|---------|---------|----------|
| `'bun'` (default) | None (Bun built-in) | Bun runtime, maximum performance |
| `'node-redis'` | `redis` ^5.11.0 | Node.js compatibility, Redis modules |

## Multi-Pod WebSocket Transport

`RedisTransport` synchronizes WebSocket messages across multiple server instances using Redis pub/sub.

### Setup

```typescript
import { Config } from '@asenajs/asena/decorators';
import { Inject } from '@asenajs/asena/decorators/ioc';
import { RedisTransport } from '@asenajs/asena-redis';

@Config()
export class AppConfig {

  @Inject('AppRedis')
  private redis: AppRedis;

  transport() {
    return new RedisTransport(this.redis);
  }

}
```

Or without an existing Redis service:

```typescript
transport() {
  return new RedisTransport({ url: 'redis://localhost:6379' });
}
```

### How It Works

Each server instance gets a unique pod ID. When a message is published through the server
(`socket.to()` / `socket.in()`):

1. The message is delivered locally via `server.publish()`
2. The message is sent to Redis pub/sub with the originating pod ID
3. Other pods receive the message and deliver it to their local sockets
4. Messages from the same pod are deduplicated automatically

`socket.publish()` takes a different route: it excludes the sender, so it does local delivery
itself with `ws.publish()` and asks the transport only for step 2 onwards via `publishRemote()`.
Steps 3 and 4 are unchanged. This requires `@asenajs/asena` 0.10.1 or newer.

### Options

```typescript
new RedisTransport(source, {
  channel: 'asena:ws:transport', // Redis pub/sub channel (default)
});
```

## Configuration

```typescript
interface RedisConfig {
  // Connection
  url?: string;               // redis[s]://[[username][:password]@][host][:port][/db]
  host?: string;               // default: 'localhost'
  port?: number;               // default: 6379
  username?: string;
  password?: string;
  db?: number;

  // Timeouts & Reconnection
  connectionTimeout?: number;  // Connection timeout in ms (default: 10000)
  idleTimeout?: number;        // Idle timeout in ms (Bun only, default: 0)
  autoReconnect?: boolean;     // Auto-reconnect on disconnect (default: true)
  maxRetries?: number;         // Max reconnection attempts (default: 10)

  // Behavior
  enableOfflineQueue?: boolean;     // Queue commands when disconnected (default: true)
  enableAutoPipelining?: boolean;   // Automatic command pipelining (Bun only, default: true)

  // TLS
  tls?: boolean | TLSOptions;

  // Identification
  name?: string;               // Service name for logging
}
```

> **Note:** `idleTimeout` and `enableAutoPipelining` are Bun-only features and are silently ignored when using the `node-redis` adapter.

## API Reference

### Service Methods

#### String Operations
- `get(key)` - Get string value
- `set(key, value, ttl?)` - Set string value with optional TTL (seconds)
- `del(...keys)` - Delete keys, returns count
- `exists(key)` - Check if key exists
- `incr(key)` / `decr(key)` - Increment/decrement counter
- `expire(key, seconds)` - Set expiration
- `ttl(key)` - Get remaining TTL
- `keys(pattern)` - Find keys by pattern

#### Hash Operations
- `hget(key, field)` - Get hash field
- `hmset(key, fields)` - Set multiple hash fields (`['f1', 'v1', 'f2', 'v2']`)
- `hmget(key, fields)` - Get multiple hash fields

#### Set Operations
- `sadd(key, member)` - Add member to set
- `srem(key, member)` - Remove member from set
- `smembers(key)` - Get all members
- `sismember(key, member)` - Check membership

#### Raw & Lifecycle
- `send(command, args)` - Execute raw Redis command
- `client` - Access underlying `RedisClientAdapter`
- `createSubscriber()` - Create a duplicate connection for pub/sub
- `testConnection()` - Returns `true` if connected
- `disconnect()` - Close the main connection (subscribers stay open)

`server.stop()` closes the service connection **and** every subscriber `createSubscriber()` handed
out - no manual teardown needed.

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Maintain test coverage for critical paths
2. Follow existing code style and linting rules
3. Test with both Bun and node-redis adapters

Submit a Pull Request on [GitHub](https://github.com/AsenaJs/asena-redis).

## License

MIT

## Support

Issues or questions? Open an issue on [GitHub](https://github.com/AsenaJs/asena-redis/issues).