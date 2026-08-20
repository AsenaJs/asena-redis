---
"@asenajs/asena-redis": patch
---

Swap the publisher connection when bun rejects an in-flight command

- Bun >= 1.4 rejects in-flight Redis commands itself (`ERR_REDIS_CONNECTION_CLOSED`) when the connection drops instead of leaving them pending, so the wedge watchdog never fired and the poisoned connection was never replaced
- The publisher guard now runs the same poison-and-swap on that rejection path and rethrows the original error
