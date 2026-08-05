# @marianmeres/sse

[![NPM](https://img.shields.io/npm/v/@marianmeres/sse)](https://www.npmjs.com/package/@marianmeres/sse)
[![JSR](https://jsr.io/badges/@marianmeres/sse)](https://jsr.io/@marianmeres/sse)
[![License](https://img.shields.io/npm/l/@marianmeres/sse)](LICENSE)

A Server-Sent Events client with namespaces, rooms and **replay** — reconnects
pick up where they left off instead of silently losing messages — plus a
mountable reference server implementing the same protocol.

## Features

- **Resumes, doesn't just reconnect** — the server keeps a bounded window of
  recent messages, the browser resends `Last-Event-ID` for free, and a
  reconnect inside that window loses nothing
- **Built on native `EventSource`** — no hand-rolled stream parser, and the
  browser's own reconnect loop does the reconnecting
- **Detects silent streams** — the failure where a proxy drops the connection,
  no error ever fires, and `EventSource` sits at `OPEN` receiving nothing
- **Namespaces and rooms** — namespace isolates, rooms are channels within it
- **No send queue to reason about** — publishing is an ordinary HTTP request
  that works whether or not the stream is up
- **Acknowledged publishes** — `publish()` resolves with a recipient count
- **Runs everywhere** — `EventSource` is a global in browsers, Deno, Node 22+
  and Bun, so there is no polyfill and no transport dependency
- **Svelte-store compatible** reactive connection state

## Installation

```bash
npm install @marianmeres/sse
```

```bash
deno add jsr:@marianmeres/sse
```

> **npm ships the client only.** The reference server depends on
> `@marianmeres/demino`, which is JSR-only, so it exists solely as
> `jsr:@marianmeres/sse/server`. The client is fully runtime-agnostic, so npm
> consumers lose nothing they could have used.

## ws or sse?

`@marianmeres/ws` is the sibling package: same shape of API, different
transport, different strengths. In short —

- **Reach for `sse`** when traffic is mostly server → client (notifications,
  live dashboards, progress, activity feeds), when losing messages across a
  reconnect is not acceptable, or when you want something you can debug with
  `curl`.
- **Reach for `ws`** when traffic is genuinely bidirectional and chatty
  (collaborative editing, games, chat with typing indicators), when you need
  presence, or when you need binary frames.

They are not drop-in replacements for one another and are not meant to be. See
[COMPARISON.md](COMPARISON.md) for the full table.

## Usage

### Client

```typescript
import { createSSEClient } from "@marianmeres/sse";

const sse = createSSEClient({
	url: "/sse",
	namespace: "org-123", // required — there is deliberately no default
	auth: () => session.token, // called on every (re)connect, so refresh works
});

// Subscribe and handle in one call; the returned function detaches the handler
// and unsubscribes the room when it was the last one.
const unsub = await sse.subscribe("chat", (msg) => {
	console.log(msg.from, msg.payload, msg.timestamp);
});

const { recipients } = await sse.publish("chat", { text: "hello" });

unsub();
sse.dispose();
```

`connect()` is optional — the first `subscribe()` starts the stream. Call it
explicitly when you want a readiness gate:

```typescript
await sse.connect(); // resolves once connected; rejects only if retrying cannot help
```

Note `publish()` deliberately does **not** open a stream. It is an ordinary
HTTP request, so a publish-only client never pays for one.

### Reactive state (Svelte)

```svelte
<script>
    import { createSSEClient } from "@marianmeres/sse";
    const sse = createSSEClient({ url: "/sse", namespace: "org-123" });
    // Not named `state` — that would read as `$state`, which is a Svelte 5 rune.
    const connection = sse.state;
</script>

{#if $connection.connected}
    <Online />
{:else if $connection.attempt > 0}
    <p>Reconnecting… (attempt {$connection.attempt})</p>
{/if}
```

### Server

```typescript
import { createSSEApp } from "@marianmeres/sse/server";

const { app, service } = createSSEApp("/sse", [], {
	// `payload` is whatever the client's `auth()` returned — opaque, so narrow it.
	verify: async (payload) => {
		const { token } = (payload ?? {}) as { token?: string };
		const user = await authenticate(token);
		// Returning null rejects with 401, which the client treats as terminal.
		return user ? { clientId: user.id, namespace: user.orgId } : null;
	},
});

// Push to connected clients from anywhere in your app.
await service.publish("notifications", { text: "deploy finished" }, "org-123");

Deno.serve(app);
```

Mounted routes, relative to the mount path:

| Method | Path                          | Notes                                     |
| ------ | ----------------------------- | ----------------------------------------- |
| GET    | `/events`                     | The stream                                |
| GET    | `/ping`                       | `204`, or the auth status                 |
| POST   | `/rpc`                        | Client operations                         |
| GET    | `/stats`                      | Guarded by `httpAuth` when supplied       |
| POST   | `/publish/[namespace]/[room]` | Requires `httpAuth`, else **not mounted** |
| POST   | `/broadcast/[room]`           | Requires `httpAuth`, else **not mounted** |

## Concepts

**Namespace** — the isolation boundary. Clients in different namespaces can
subscribe to identically named rooms without ever seeing each other's messages.
A client may only publish into its own namespace. It is **required**: a shared
default would mean an app that forgets to set one has no isolation at all, and
that failure is silent.

**Room** — a channel within a namespace. Subscribe to receive its messages.

**Broadcast** — the one operation that crosses namespaces. It is a separate
method rather than a flag on `publish()` precisely because crossing an
isolation boundary deserves its own name and its own server-side check:
`allowBroadcast` **denies by default**.

## Behaviour worth knowing

**Replay is instance-local.** Event ids are prefixed with the server instance
that minted them. A client that reconnects to a _different_ instance is
correctly told to start live rather than replaying someone else's sequence
numbers — so a multi-instance deployment without a shared adapter quietly
degrades to at-most-once. `hello` reports `resumed` and `gap` so an application
can refetch when it matters.

**The reconnect loop is mostly the browser's.** `EventSource` retries on its
own, paced by the `retry:` value the server sends — jittered per connection so
a fleet does not come back in lockstep. This package only takes over when
`EventSource` gives up permanently, which per spec is on any non-200 response.

**A non-200 is ambiguous, so we ask.** `EventSource` hides the status, meaning
a `503` from a rolling deploy looks exactly like a `401`. On a permanent
failure the client probes `GET /ping` to find out which it was, then either
gives up (`401`, `403`) or resumes retrying with exponential backoff.

**Terminal failures are loud.** Giving up is the only non-retrying exit, so it
rejects any pending `connect()`, emits `terminated`, and logs at error level. A
silent one would be indistinguishable from a network that never recovered.

**Every request is authenticated, not just the stream.** There is no session to
trust — the stream and each upstream call are separate HTTP requests, and
`verify()` runs on all of them. Keep it cheap, and **make it deterministic**:
subscriptions are routed by client id, so a hook returning a different id per
call would misroute them.

**Heartbeats are real events, not `:` comments.** `EventSource` does not expose
comments to JavaScript, so a comment heartbeat cannot drive client-side
staleness detection. The client resets a deadline on every inbound byte and
forces a reconnect when the stream goes quiet.

**`disconnect()` is resumable; `dispose()` is terminal.** Handlers and rooms
survive a `disconnect()`, so a later `connect()` picks up where it left off.

## Example

A complete reference app — a live ops feed with a fake CI/CD pipeline
publishing into it:

```bash
deno task example        # builds the client bundle, then serves on :8000
```

Pause the stream, let the pipeline run on without you, then resume: replay
fills in exactly what you missed. Then overflow the window and watch it report
a gap instead. See [example/README.md](example/README.md).

## API

See [API.md](API.md) for complete API documentation, and
[docs/DESIGN.md](docs/DESIGN.md) for why the design is what it is.

## License

[MIT](LICENSE)
