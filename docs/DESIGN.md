# @marianmeres/sse — design decisions

Why this package is shaped the way it is. Extracted from the pre-implementation
spec and kept as a decision log.

This is **not** a reference. `API.md` documents the surface, `AGENTS.md` states
the conventions, `COMPARISON.md` covers the differences from `@marianmeres/ws`.
This file only records the reasoning behind the choices those documents assume,
so that a future change can be judged against the argument rather than against
a bare rule. Where it disagrees with the code, the code wins.

## Goal, stated precisely

Not parity with `@marianmeres/ws`. The goals, in priority order:

1. **Long-term maintainability.** Fewer moving parts wins over symmetry.
2. **Play to SSE's strengths.** SSE is HTTP, it resumes, and the browser owns
   the transport. Exploit all three rather than papering over them.
3. **Familiar API.** Someone who knows `ws` should recognise this in seconds.
   They should not be able to swap one for the other blindly — that is fine,
   nobody needs to.
4. **Document the differences loudly.** `COMPARISON.md` is a deliverable, not an
   afterthought.

## Decision log

### Native `EventSource`, not `fetch` + a hand-rolled parser

The parser is the single largest chunk of avoidable code in a fetch-based
design: chunk boundaries splitting a field mid-line, multi-line `data:`, CRLF
vs LF, `retry:`, comment lines, BOM. All spec-fiddly, all permanent.

The objection to `EventSource` — that it hides the HTTP status, so you cannot
tell a terminal failure from a recoverable one — mostly dissolves on reading the
spec. Per WHATWG HTML, a non-200 response or a `Content-Type` other than
`text/event-stream` makes `EventSource` **fail the connection permanently**:
`readyState` goes to `CLOSED` and it never retries. A mid-stream drop instead
"reestablishes the connection". So `onerror` + `readyState` already separates
the two cases, and we get the reconnect loop and `Last-Event-ID` resend free.

Three costs, all accepted:

1. **No custom request headers.** Stream auth travels by cookie
   (`withCredentials`) or query string. Upstream RPC is ours, so it uses a
   header. Asymmetric but honest.
2. **Fixed retry interval, no jitter.** Fixed server-side by emitting a
   per-connection jittered `retry:` field. Server-controlled backoff is better
   than client-side jitter anyway: the server can widen it under load.
3. **A 503 during a rolling deploy also lands in `CLOSED`.** Handled by the
   probe shim (below), the one place we override the built-in behaviour.

### The probe shim

`onerror` fires with no status. Classification:

- `readyState === CONNECTING` — `EventSource` is retrying on its own. Emit
  `reconnecting`, bump the attempt counter, otherwise stay out of the way.
- `readyState === CLOSED` — it gave up. `GET {mount}/ping` (same `verify`, no
  stream) tells us why:
  - status in `terminalStatusCodes` (default `[401, 403]`) → emit `terminated`,
    stop for good.
  - anything else, including a network error → schedule our own reconnect on
    `backoffDelay` and construct a fresh `EventSource`.

`/ping` exists solely so this probe is cheap and unambiguous. It doubles as a
health check.

### Client mints its own id

The client generates `clientId` at construction. Consequences:

- Upstream RPC works before the stream is open. No wait-for-`hello` dance.
- **No outbox.** In `ws` the outbox exists because sends ride the socket; a
  dead socket means a queue. Here `publish()` is an independent `POST` that
  succeeds whether or not the stream is up. Buffering it would be cargo cult.
  That deletes `outbox.ts` (184 LOC in `ws`) and its resilience tests, plus the
  `sendTimeout` / `outboxMaxSize` / `onOutboxDrop` options and the
  `WSOutboxDropError` / `WSNotConnectedError` errors.
- `subscribe()` before connect registers the handler and room locally and
  resolves. The room then rides the connect URL.

`verify()` may override the id (pin it to the authenticated user). **This makes
`verify()` required to be deterministic** for a given auth payload: the stream
request and every RPC request run it independently, and `sub`/`unsub` resolve
the connection by id. A `verify()` that returns a different id per call breaks
subscription routing.

### Rooms ride the connect URL

`GET /events?rooms=a,b,c`. The server registers them synchronously while
building the stream, before the first byte goes out.

This deletes `ws`'s single most load-bearing ordering rule — "`#onHello` must
re-subscribe _before_ flushing the outbox, and the server must handle
`sub`/`unsub` synchronously" — because there is no window in which a message can
land in a room the server has not registered yet. There is nothing to get wrong.

### Replay is on by default

The strength that justifies this package existing next to `ws`.

- Per-instance `instanceId` (base36) and a monotonic `seq`.
- A bounded ring buffer of the last `replayBufferSize` messages (default 500).
- Only `message` events carry `id:`, formatted `{instanceId}:{seq}`. `hello`
  and `heartbeat` carry none, so `Last-Event-ID` always points at the last real
  message.
- On reconnect the browser resends `Last-Event-ID` as a header. Our own manual
  reconnect cannot set headers, so it appends `?lastEventId=`. Server reads the
  header first, then the query param.
- Instance-id mismatch → ignore and go live. Multi-instance without a shared
  adapter therefore degrades to at-most-once, silently and safely.

**The duplicate-vs-gap race.** Snapshot `const upTo = this.#seq` _before_
registering the connection, then register, then replay `(lastSeq, upTo]`.
Anything published after the snapshot arrives live instead. Registration and
replay both run synchronously inside the `ReadableStream` `start()` callback, so
nothing interleaves. Getting this backwards yields either a duplicate (register
first, then drain to current seq) or a gap.

**Eviction.** If `lastSeq` is older than the oldest buffered entry, messages are
genuinely gone. `hello` carries `resumed: boolean` and `gap: boolean` so the app
can refetch instead of silently believing it is up to date.

### HTTP status codes, not synthetic close codes

SSE is HTTP. Inventing a 4xxx table on top of it means every debugging session
translates between two vocabularies. `terminalStatusCodes: [401, 403]` replaces
`terminalCloseCodes: [4001, 4003]`.

### Heartbeats are real events, not comments

`EventSource` does not expose `:` comment lines to JS, so a comment heartbeat
cannot drive staleness detection — the client would have no way to notice a
silently dead stream. We send `event: heartbeat`.

### Named events, not a discriminator inside `data`

`EventSource` dispatches by event name natively, so the client is three
`addEventListener` calls instead of a parse-and-switch.

`payload` stays opaque — never inspected, never mutated. The protocol owns the
event names; the application owns everything inside `payload`, including its own
`type` field if it wants one. Conflating the two forces the server to strip
application fields back out, which is a mistake worth not repeating.

### No presence

Dropped by decision. It is the largest optional feature (~150 LOC across both
sides plus tests) and not wanted here. `ws` keeps it; `COMPARISON.md` says so.

### No rate limiting in the library

A per-`clientId` cap is trivially bypassed when the client mints its own id, so
it would be theatre. Rate limiting belongs in a demino middleware, keyed on
something authenticated. Documented, not implemented.

### No idle sweeper

`ws` reaps silent sockets because half-open TCP leaves a socket that looks
alive. Here, client disconnect fires `request.signal`'s `abort`, and a write to
a dead stream throws — both are handled, and between them there is nothing left
for a sweeper to find.

### Adapters: local by default, Redis when there are peers

`SSEPubSubAdapter` is the cross-instance seam; `SSEPubSubLocal` (a no-op) is
the default and `SSEPubSubRedis` ships as its own entry point,
`@marianmeres/sse/adapters/redis` — a separate entry so a single-instance
server never carries Redis-flavoured code, and structural over `RedisLike` so
the package still depends on no concrete Redis library. The seam's division of
labour holds under both: local delivery is always the service's job, an adapter
only gossips to peers and must suppress its own echo. A Deno-KV adapter for
the same seam remains unimplemented.
