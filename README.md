<p width="%100" align="center">
  <img src="https://avatars.githubusercontent.com/u/179836938?s=200&v=4" width="150" align="center"/>
</p>

# @asenajs/asena-redis

[![Version](https://img.shields.io/npm/v/@asenajs/asena-redis.svg)](https://github.com/AsenaJs/asena-redis)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Bun Version](https://img.shields.io/badge/Bun-1.4%2B-blueviolet)](https://bun.sh)

Redis integration for AsenaJS — a `@Redis` service client, a multi-pod WebSocket transport and a Redis Streams microservice transport.

**Requirements:** Bun 1.4+, `@asenajs/asena` 0.11+.

## Installation

```bash
bun add @asenajs/asena-redis
```

## Quick Start

```typescript
import { Redis, AsenaRedisService } from '@asenajs/asena-redis';

@Redis({ config: { url: 'redis://localhost:6379' }, name: 'AppRedis' })
export class AppRedis extends AsenaRedisService {}
```

Asena discovers it, connects on start and disconnects on stop. Inject it anywhere:

```typescript
import { Service } from '@asenajs/asena/decorators';
import { Inject } from '@asenajs/asena/decorators/ioc';

@Service()
export class GreetingService {

  @Inject('AppRedis')
  private redis: AppRedis;

  async greet(name: string): Promise<string> {
    await this.redis.set(`greeting:${name}`, `Hello, ${name}`, 60);

    return (await this.redis.get(`greeting:${name}`)) ?? '';
  }

}
```

## Documentation

Full documentation: [asena.sh/docs/packages/redis](https://asena.sh/docs/packages/redis)

## Contributing

Contributions are welcome! Submit a Pull Request on [GitHub](https://github.com/AsenaJs/asena-redis).

## License

MIT

## Support

Issues or questions? Open an issue on [GitHub](https://github.com/AsenaJs/asena-redis/issues).
