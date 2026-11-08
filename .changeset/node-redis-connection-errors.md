---
'@asenajs/asena-redis': patch
---

Fix `NodeRedisAdapter` on a dropped connection: the connection error escaped as an uncaught
exception and node-redis never reconnected. The adapter now listens for `'error'`, logs one
`console.error` line per outage, and the client reconnects when Redis is back. Clients made with
`duplicate()` get the same listener before they connect.
