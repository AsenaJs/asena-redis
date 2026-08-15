---
'@asenajs/asena-redis': minor
---

`RedisTransport` implements `publishRemote()`

The wire half of `publish()`: the Redis publish alone, with no `server.publish()`. `AsenaSocket`
calls it after doing local delivery itself through Bun's socket-level `ws.publish()` — the only
primitive that leaves the publishing socket out — so configuring this transport no longer starts
echoing every `socket.publish()` back to its sender.

`publish()` is unchanged and still does both halves; it is what the service-level `this.to()` uses,
which is meant to reach everyone. The `podId` deduplication in `handleMessage()` covers the new path
unmodified, so the publishing pod does not pick its own envelope back up off the wire either.

Pairs with `@asenajs/asena` 0.10.1, which is what calls it. Against `0.10.0` — which the `^0.10.0`
peer range still allows — the method is simply never called and behaviour is what it was.
