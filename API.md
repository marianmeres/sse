# API

## Entry points

Three of them. Which one you import from decides what lands in your bundle.

| Import                      | Contains                                           | Runtime               |
| --------------------------- | -------------------------------------------------- | --------------------- |
| `@marianmeres/sse`          | The client, plus everything in `/protocol`         | Anywhere; also on npm |
| `@marianmeres/sse/server`   | The reference server                               | Deno only, JSR only   |
| `@marianmeres/sse/protocol` | Wire definitions and typed errors, dependency-free | Anywhere; also on npm |

`/server` pulls in `@marianmeres/demino`, which is JSR-only — that is the whole
reason it is a separate entry point rather than part of the root. `/protocol` is
a strict subset of the root, exported separately for third-party implementations
that want the wire types without the client.

## Functions

### `createSSEClient(options)`

Creates a client. `SSEClient` is also exported for `new SSEClient(options)` —
same thing, following the `PubSub` / `createPubSub` precedent.

**Parameters**

| Name                  | Type                                | Default             | Description                                                       |
| --------------------- | ----------------------------------- | ------------------- | ----------------------------------------------------------------- |
| `url`                 | `string \| URL`                     | `"/sse"`            | Absolute `http(s)://`, or a path resolved against `location`      |
| `namespace`           | `string`                            | — **required**      | Isolation boundary. Throws when absent                            |
| `clientId`            | `string`                            | generated           | Preferred id; `verify()` may override it                          |
| `rooms`               | `string[]`                          | `[]`                | Rooms joined on every (re)connect                                 |
| `auth`                | `() => unknown \| Promise<unknown>` | —                   | Auth payload; called before _every_ (re)connect and upstream call |
| `autoConnect`         | `boolean`                           | `true`              | First `subscribe()` starts the stream                             |
| `withCredentials`     | `boolean`                           | `false`             | Send cookies with the stream and upstream calls                   |
| `logger`              | `Logger \| null`                    | `createClog("sse")` | `null` silences                                                   |
| `reconnectDelay`      | `number`                            | `500`               | Initial backoff for self-driven reconnects, ms                    |
| `reconnectDelayMax`   | `number`                            | `30_000`            | Backoff ceiling, ms                                               |
| `terminalStatusCodes` | `number[]`                          | `[401, 403]`        | Statuses after which retrying stops                               |
| `staleTimeout`        | `number`                            | `45_000`            | Silence deadline. `0` disables                                    |
| `connectTimeout`      | `number`                            | `0`                 | Bounds the first `connect()` await. `0` waits indefinitely        |
| `fetchTimeout`        | `number`                            | `30_000`            | Per-request deadline for upstream calls                           |

**Returns** `SSEClient`

---

### `createSSEApp(mountPath?, middlewares?, options?)`

Creates the reference server. Import from `@marianmeres/sse/server` (JSR only).

**Parameters**

| Name                        | Type                                   | Default                    | Description                                                                   |
| --------------------------- | -------------------------------------- | -------------------------- | ----------------------------------------------------------------------------- |
| `mountPath`                 | `string`                               | `"/sse"`                   | Demino mount path                                                             |
| `middlewares`               | `DeminoHandler[]`                      | `[]`                       | Applied to all routes                                                         |
| `options.verify`            | `(payload, req) => AuthResult \| null` | —                          | Return `null` (or throw) to reject with `401`. Absent means no authentication |
| `options.allowBroadcast`    | `(ctx, room) => boolean`               | **deny**                   | Gate for cross-namespace broadcast                                            |
| `options.httpAuth`          | `DeminoHandler`                        | —                          | Guards the injection routes. **Without it they are not mounted**              |
| `options.deminoOptions`     | `DeminoOptions`                        | —                          | Passed through to `demino()`                                                  |
| `options.heartbeatInterval` | `number`                               | `15_000`                   | Liveness beat cadence. `0` disables                                           |
| `options.retryInterval`     | `number`                               | `3_000`                    | Base for the SSE `retry:` field, jittered ±50% per connection                 |
| `options.replayBufferSize`  | `number`                               | `500`                      | Messages retained for replay. `0` disables replay                             |
| `options.maxRoomNameLength` | `number`                               | `256`                      | Reject longer room names                                                      |
| `options.maxRoomsPerClient` | `number`                               | `100`                      | Reject subscriptions beyond this many rooms                                   |
| `options.adapter`           | `SSEPubSubAdapter`                     | `SSEPubSubLocal`           | Cross-instance fan-out                                                        |
| `options.logger`            | `Logger \| null`                       | `createClog("sse:server")` | `null` silences                                                               |

**Returns** `{ app: Demino, service: SSEService }`

---

### `backoffDelay(attempt, base, max, rnd?)`

Exponential backoff with equal jitter. Returns a delay in `[d/2, d]` where
`d = min(max, base * 2^(attempt-1))`. Governs only the reconnects the client
drives itself; `EventSource`'s own loop is paced by the server's `retry:`.

---

### `sseFrame(event, data, id?)`

Serialises one SSE event. Exported from `@marianmeres/sse/server` mainly so the
wire format is testable.

---

### `parseEventId(raw)` / `formatEventId(instanceId, seq)`

Event ids are `{instanceId}:{seq}`. `parseEventId` returns
`{ instanceId, seq }` or `null` for anything malformed — a bad resume point is
not an error, it just means starting live.

## Types

### `SSEClient`

```typescript
connect(): Promise<void>
disconnect(): void
dispose(): void

subscribe<T>(room, handler): Promise<Unsubscriber>
unsubscribe(room): Promise<void>
isSubscribed(room): boolean

publish<T>(room, payload): Promise<SSEPublishResult>
broadcast<T>(room, payload): Promise<SSEPublishResult>

on<K>(event, cb): Unsubscriber
once<K>(event, cb): Unsubscriber

get state(): { subscribe(cb): Unsubscriber }   // Svelte store contract
get connected(): boolean
get connectionState(): SSEConnectionState
get clientId(): string
get namespace(): string
get rooms(): string[]
get eventSource(): EventSource | null
get url(): URL
get lastEventId(): string | null
dump(): Record<string, unknown>

logger: Logger | null      // assignable; set to null to silence
```

`connect()` is idempotent — concurrent calls share one promise, and it resolves
immediately when already connected. It rejects **only** where retrying cannot
help: `SSETerminatedError` (a terminal status) or `SSEConnectTimeoutError`
(`connectTimeout` elapsed; retrying continues in the background). Ordinary
network failure never rejects.

`subscribe()` attaches the handler **synchronously**, before anything goes out,
so nothing arriving between the request and its acknowledgement is lost. When
connected it awaits the server's answer, so a refused subscription rejects
here; when not connected it resolves once registered, and the subscription is
established by the connect URL of the next stream.

`publish()` and `broadcast()` do **not** trigger `autoConnect`. They are
ordinary HTTP requests with no dependence on the stream.

### `SSEEvents`

| Event          | Payload                                             |
| -------------- | --------------------------------------------------- |
| `open`         | `void` — stream connected, `hello` not yet seen     |
| `connected`    | `SSEHello`                                          |
| `message`      | `SSEMessage` — firehose, every room                 |
| `heartbeat`    | `{ timestamp }`                                     |
| `close`        | `{ status: number \| null, reason, willReconnect }` |
| `reconnecting` | `{ attempt, delay: number \| null }`                |
| `terminated`   | `{ status, reason }` — gave up                      |
| `error`        | `Error`                                             |

`close.status` is `null` when the drop produced no HTTP response to read a
status from. `reconnecting.delay` is `null` when `EventSource` owns the retry
schedule rather than this client.

### `SSEState`

```typescript
{
	state: SSEConnectionState;
	connected: boolean;
	connecting: boolean;
	attempt: number;
	lastError: Error | null;
}
```

### `SSEConnectionState`

`"idle" | "connecting" | "open" | "reconnecting" | "terminated" | "disposed"`

There is no `"authenticating"` state, unlike `@marianmeres/ws`: auth is part of
the HTTP request rather than a post-connect handshake, so it has no observable
phase of its own.

### `SSEMessage<T>`

```typescript
{
	room: string;
	namespace: string;
	from: string | null;
	payload: T;
	timestamp: number;
}
```

`from` is `null` when the message was injected server-side, which is how a
client distinguishes server pushes from peer traffic. `namespace` is always the
one the _receiver_ lives in, even for a broadcast that originated elsewhere.

`payload` is **opaque** — never inspected, never mutated. It may carry its own
`type` field and nothing collides with the protocol's event names.

### `MessageHandler<T>`

```typescript
type MessageHandler<T = unknown> = (msg: SSEMessage<T>) => void;
```

What `subscribe()` takes. A handler that throws is caught and surfaced as an
`error` event rather than tearing down delivery to the other handlers.

### `SSEPublishResult`

```typescript
{
	recipients: number;
}
```

What `publish()` and `broadcast()` resolve with. `recipients` counts streams the
message was handed to **on the answering server instance** — best-effort
telemetry, never a delivery guarantee, and it stays instance-local under a
distributed adapter.

### `SSEHeartbeat`

```typescript
{
	timestamp: number;
}
```

Payload of the `heartbeat` event. Carries no `id:`, so it never moves the resume
point.

### `SSEHello`

```typescript
{
	clientId: string;
	namespace: string;
	protocol: number;
	heartbeatInterval: number;
	resumed: boolean;
	gap: boolean;
}
```

`gap` is `true` when the resume point had already been evicted from the replay
buffer — messages are genuinely gone and the application should refetch rather
than assume it is up to date.

### `SSEService`

```typescript
handleStream(request, extraHeaders?): Promise<Response>
handleRpc(request): Promise<Response>
handlePing(request): Promise<Response>
publish(room, payload, namespace, from?): Promise<number>
broadcast(room, payload, from?): Promise<number>
members(room, namespace): string[]
stats(): SSEStats
close(): Promise<void>

readonly instanceId: string   // prefix on every event id
logger: Logger | null         // assignable; set to null to silence
```

Usable standalone — drive it from any `Deno.serve` handler — or through
`createSSEApp`, which mounts it as a demino app. `close()` releases the
heartbeat timer; without it the process will not exit.

### `SSEStats`

```typescript
{
	instanceId: string;
	connections: number;
	rooms: number;
	namespaces: Record<string, number>;
	seq: number;
	buffered: number;
}
```

| Field         | Meaning                                           |
| ------------- | ------------------------------------------------- |
| `instanceId`  | This instance's id — the prefix on every event id |
| `connections` | Open streams                                      |
| `rooms`       | Distinct room names in use                        |
| `namespaces`  | Stream count per namespace                        |
| `seq`         | Most recently stamped sequence number             |
| `buffered`    | Messages currently retained for replay            |

Counts only, never client ids — which is why `GET /stats` is safe to leave
unguarded in development.

### `SSEConnectionContext`

```typescript
{
	clientId: string;
	namespace: string;
	meta: Record<string, unknown>;
	request: Request;
}
```

What `allowBroadcast` receives as its first argument. `meta` is whatever
`verify()` returned, or `{}` when it returned none.

### `AuthResult`

```typescript
{ clientId?: string; namespace?: string; meta?: Record<string, unknown> }
```

**`verify()` must be deterministic for a given payload.** The stream request
and every upstream call run it independently, and `sub`/`unsub` resolve the
target stream by client id — a hook that returns a different id per call
silently misroutes subscriptions.

### `SSEPubSubAdapter`

```typescript
publish(envelope: SSEBroadcastEnvelope): Promise<void>
onRemote(cb: (envelope: SSEBroadcastEnvelope) => void): () => void
close(): Promise<void>
```

Local delivery is always the service's job; an adapter only propagates to peer
instances. Only `SSEPubSubLocal` (a no-op — no peers to tell, nothing ever
arrives) ships today, and it is the default. Because a peer's message enters the
local replay buffer with _this_ instance's sequence numbers, replay stays
instance-local under any adapter.

### `SSEBroadcastEnvelope`

```typescript
{
	namespace: string | null;
	message: SSEMessage;
}
```

What crosses the instance boundary, in both directions. `namespace` is `null`
for a cross-namespace broadcast.

### `ReplayBuffer`

```typescript
new ReplayBuffer(capacity: number)   // 0 disables replay

add(namespace: string | null, message: SSEMessage): number   // returns the seq
since(after, upTo, namespace, rooms: ReadonlySet<string>): ReplayEntry[]
clear(): void

get enabled(): boolean      // capacity > 0
get currentSeq(): number    // most recently stamped
get oldestSeq(): number | null
get size(): number
```

`SSEService` owns one of these; it is exported because the resume semantics are
worth being able to test and reason about directly. `add()` always increments
the sequence, even when disabled, so event ids stay monotonic either way.

### `ReplayEntry` / `ResumePoint`

```typescript
interface ReplayEntry {
	seq: number;
	namespace: string | null;
	message: SSEMessage;
}

interface ResumePoint {
	instanceId: string;
	seq: number;
}
```

A `null` `namespace` means a cross-namespace broadcast, which matches any
receiver exactly as it did when it was live. A `ResumePoint` resumes _after_ its
`seq`, and a foreign `instanceId` is ignored — replay is instance-local.

### Errors

All extend `SSEError`.

| Error                    | Thrown when                                     |
| ------------------------ | ----------------------------------------------- |
| `SSETerminatedError`     | Terminal status; carries `status` and `reason`  |
| `SSEConnectTimeoutError` | `connectTimeout` elapsed (retrying continues)   |
| `SSETimeoutError`        | `fetchTimeout` elapsed on an upstream call      |
| `SSERemoteError`         | The server refused; carries `code` and `status` |
| `SSEDisposedError`       | Client was disposed                             |

Branch on `instanceof`, not on message strings. `SSERemoteError.code` is the
machine-readable one — see `ERROR_CODE`.

### `SSEErrorInfo`

```typescript
{
	code: string;
	message: string;
}
```

The `error` field of a failed `/rpc` response, and what `SSERemoteError` is
built from. `code` is machine-readable — see `ERROR_CODE`. `message` is for
humans; never parse it.

## Wire protocol

### `GET {mount}/events`

Query: `clientId`, `namespace` (required), `rooms` (comma separated),
`auth`, `lastEventId`.

```
retry: 4137

event: hello
data: {"clientId":"c_a1b2","namespace":"org-1","protocol":1,
       "heartbeatInterval":15000,"resumed":false,"gap":false}

id: srv7f2:1042
event: message
data: {"room":"chat","namespace":"org-1","from":"c_x9f2",
       "payload":{"text":"hi"},"timestamp":1712345678901}

event: heartbeat
data: {"timestamp":1712345693901}
```

Only `message` events carry an `id:`, so `Last-Event-ID` always points at the
last real message. The server prefers the `Last-Event-ID` _header_ (sent
automatically by `EventSource` on its own reconnect) over the query parameter
(used when the client rebuilds the `EventSource` itself, since it cannot set
headers).

### `POST {mount}/rpc`

Header `x-sse-auth` carries the serialised `auth()` payload — always JSON, so a
bare token that looks like JSON round-trips as the string it was.

```jsonc
{ "op": "sub",       "clientId": "c_a1b2", "namespace": "org-1", "rooms": ["chat"] }
{ "op": "unsub",     "clientId": "c_a1b2", "namespace": "org-1", "rooms": ["chat"] }
{ "op": "pub",       "clientId": "c_a1b2", "namespace": "org-1", "room": "chat", "payload": {} }
{ "op": "broadcast", "clientId": "c_a1b2", "namespace": "org-1", "room": "chat", "payload": {} }
```

`200` → `{ "ok": true, "recipients": 3 }`.
`4xx` → `{ "ok": false, "error": { "code": "forbidden", "message": "…" } }`.

A `sub`/`unsub` against a client id with no open stream returns `200` rather
than an error: rooms ride the connect URL, so the intent is already recorded
client-side and the next stream applies it.

### `ClientOp` / `RpcResult`

Typed equivalents of the above, for anyone speaking the protocol directly.

```typescript
type ClientOp =
	| { op: "sub"; clientId: string; namespace: string; rooms: string[] }
	| { op: "unsub"; clientId: string; namespace: string; rooms: string[] }
	| { op: "pub"; clientId: string; namespace: string; room: string; payload: unknown }
	| {
		op: "broadcast";
		clientId: string;
		namespace: string;
		room: string;
		payload: unknown;
	};

interface RpcOk {
	ok: true;
	recipients?: number;
} // absent for sub/unsub
interface RpcErr {
	ok: false;
	error: SSEErrorInfo;
}

type RpcResult = RpcOk | RpcErr;
```

### `GET {mount}/ping`

`204` when `verify()` accepts, `401` when it does not. No stream, no side
effects — it is both the client's status probe and a serviceable health check.

## Constants

### `PROTOCOL_VERSION`

`1`. Announced by the server in `hello`.

### `SSE_EVENT`, `OP`, `ERROR_CODE`

```typescript
SSE_EVENT = { HELLO: "hello", MESSAGE: "message", HEARTBEAT: "heartbeat" };
OP = { SUB: "sub", UNSUB: "unsub", PUB: "pub", BROADCAST: "broadcast" };
ERROR_CODE = {
	UNAUTHORIZED: "unauthorized",
	FORBIDDEN: "forbidden",
	BAD_REQUEST: "bad_request",
	NOT_FOUND: "not_found",
	INTERNAL: "internal",
};
```

These are _protocol_ names and have nothing to do with whatever the application
puts inside `payload`.

### `DEFAULT_TERMINAL_STATUS_CODES`

`[401, 403]`. Everything else is retried, `500` and `503` included.

### Transport constants

| Constant              | Value           | Used for                                                              |
| --------------------- | --------------- | --------------------------------------------------------------------- |
| `AUTH_HEADER`         | `"x-sse-auth"`  | Serialised `auth()` payload on upstream RPC requests                  |
| `AUTH_PARAM`          | `"auth"`        | Same payload on the stream — `EventSource` cannot set headers         |
| `LAST_EVENT_ID_PARAM` | `"lastEventId"` | Resume point on a _manual_ reconnect; the browser's own uses a header |
| `BROADCAST_NAMESPACE` | `"*"`           | Stamped on a broadcast, which by definition has no single namespace   |
| `EVENT_ID_SEPARATOR`  | `":"`           | Between instance id and sequence number inside an event `id:`         |
