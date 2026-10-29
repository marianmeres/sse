# `@marianmeres/sse` vs `@marianmeres/ws`

Two packages, deliberately similar APIs, deliberately different designs. They
are **not** drop-in replacements for one another, and no effort is spent
pretending otherwise — each leans into what its transport is good at.

## Pick one

**Reach for `sse` when:**

- Traffic is mostly **server → client** — notifications, live dashboards,
  progress, activity feeds, log tails
- **Losing messages across a reconnect is not acceptable.** This is the big one:
  SSE resumes, WebSocket does not
- You want something you can **debug with `curl`**, read in devtools' network
  tab, and reason about as plain HTTP
- You are behind proxies or corporate networks that mangle WebSocket upgrades

**Reach for `ws` when:**

- Traffic is genuinely **bidirectional and chatty** — collaborative editing,
  games, chat with typing indicators. Every upstream message here is a full
  HTTP request, which is fine at a message a second and wrong at fifty
- You need **presence** (who else is in this room?)
- You need **binary frames**
- You open **many streams per origin over HTTP/1.1** — an SSE stream consumes
  one of the browser's ~6 per-origin connections; a WebSocket does not. Over
  HTTP/2 this is a non-issue

## The differences

|                           | `@marianmeres/ws`                                          | `@marianmeres/sse`                                                                                                  |
| ------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| **Transport**             | One WebSocket, bidirectional                               | SSE stream down, HTTP `POST` up                                                                                     |
| **Delivery guarantee**    | At-most-once, always                                       | Nothing lost on a reconnect inside the replay window, on one instance; at-most-once otherwise                       |
| **Resume**                | None                                                       | `Last-Event-ID`, resent by the browser for free                                                                     |
| **Reconnect loop**        | Entirely the client's, exponential backoff with jitter     | `EventSource`'s own, paced by a server-sent jittered `retry:`. The client takes over only after a permanent failure |
| **Presence**              | Yes — `sync`/`join`/`leave`, opt-in per room               | No                                                                                                                  |
| **Offline send buffer**   | `Outbox` — capped, flushed after re-subscribe              | None. Publishing is an independent `POST` that works while the stream is down                                       |
| **Auth**                  | One `auth` frame per connection                            | Every request, stream and upstream alike. `verify()` must be deterministic                                          |
| **Auth transport**        | Anything — it is a frame payload                           | Cookie or query string for the stream (`EventSource` cannot set headers); `x-sse-auth` header upstream              |
| **Failure vocabulary**    | Synthetic close codes (`4001`, `4003`, …)                  | HTTP statuses (`401`, `403`, …)                                                                                     |
| **Namespace default**     | `"default"`                                                | **Required** — no default                                                                                           |
| **Liveness**              | Application-level ping/pong, both directions               | Server heartbeat event + client staleness watchdog. Receive-side only; there is no upstream channel to ping on      |
| **Idle reaping**          | Server sweeps silent sockets                               | Not needed — `request.signal` and a failing write cover it                                                          |
| **Binary payloads**       | Yes, via `encode`/`decode` hooks                           | No. JSON text only                                                                                                  |
| **Re-subscribe ordering** | Load-bearing: re-subscribe must precede the outbox flush   | Not a thing. Rooms ride the connect URL, so they are registered before the first byte                               |
| **Server runtime**        | Any demino 2.0 adapter (Deno; Node + `ws`) since ws 0.8.0  | Any demino 2.0 adapter; the core is standard Web APIs                                                               |
| **Client runtime**        | Everywhere (`WebSocket` is global)                         | Everywhere (`EventSource` is global)                                                                                |
| **npm**                   | Client and server since ws 0.8.0 (demino an optional peer) | Client only; the server is on JSR                                                                                   |
| **Source size**           | ~1800 lines of code                                        | ~1690 lines of code                                                                                                 |

## Where the APIs differ

Both look like this:

```typescript
const client = createXClient({ url, namespace });
const unsub = await client.subscribe("room", (msg) => {});
await client.publish("room", payload);
await client.broadcast("room", payload);
client.on("connected", …); client.disconnect(); client.dispose();
client.state; // Svelte store
```

The differences you would actually trip over:

|                                      | `ws`                                           | `sse`                               |
| ------------------------------------ | ---------------------------------------------- | ----------------------------------- |
| Namespace                            | optional                                       | **required**, throws without it     |
| `subscribe()` third argument         | `{ presence }`                                 | —                                   |
| `members(room)`                      | present                                        | —                                   |
| `publish()` with `autoConnect`       | opens the socket                               | does **not** open the stream        |
| `publish(room, payload, namespace?)` | can target a namespace                         | always the client's own             |
| Terminal failures                    | `terminalCloseCodes: [4001, 4003]`             | `terminalStatusCodes: [401, 403]`   |
| Liveness options                     | `pingInterval`, `pongTimeout`                  | `staleTimeout`                      |
| Send bounding                        | `sendTimeout`, `outboxMaxSize`, `onOutboxDrop` | `fetchTimeout`                      |
| `connected` event payload            | `{ clientId, namespace }`                      | `SSEHello`, incl. `resumed` / `gap` |
| Extra state                          | `"authenticating"`                             | —                                   |

## What "replay is instance-local" means

Event ids are `{instanceId}:{seq}`, where `instanceId` is minted per server
process. A client reconnecting to a _different_ instance presents a resume
point that instance cannot interpret, so it is ignored and the stream starts
live — `hello.resumed` is `false`.

That means: **behind a load balancer without sticky sessions, replay silently
does nothing.** It is not wrong, it just is not helping. Making it work across
instances needs a globally ordered log (Redis Streams or similar) behind the
`SSEPubSubAdapter` seam, which is not implemented.

When the resume point is understood but has aged out of the buffer,
`hello.gap` is `true` — the application knows messages were missed and can
refetch instead of assuming it is current.

## Shared conventions

Both packages agree on the things worth agreeing on:

- **Payload is opaque.** The protocol owns a closed set of event/frame types;
  `payload` is never inspected or mutated. Your payload may have its own `type`
  field and nothing collides.
- **`protocol/` is a dependency-free module** both sides import, exported as a
  subpath so a third-party implementation can use it.
- **Safe defaults are deny.** `allowBroadcast` denies; the HTTP injection
  routes are not mounted without `httpAuth`.
- **`from: null` means server-injected**, which is how a client tells a server
  push from peer traffic.
- **`disconnect()` is resumable, `dispose()` is terminal.**
- **Reactive state** through the Svelte store contract.
- **`recipients` is instance-local telemetry**, never a delivery guarantee.
