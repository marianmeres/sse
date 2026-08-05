# @marianmeres/sse — Agent Guide

Server-Sent Events client with namespaces, rooms and replay, plus a
demino-mountable reference server.

## Quick Reference

```yaml
name: "@marianmeres/sse"
runtime: "deno (primary), browser/node/bun (client)"
entry: "./src/mod.ts"
exports: [".", "./server", "./protocol"]
test: "deno task test"
check: "deno task check" # lint + fmt + type check + deno doc --lint
```

| Task                     | File                       | Key symbol                     |
| ------------------------ | -------------------------- | ------------------------------ |
| Client                   | `src/client/sse-client.ts` | `createSSEClient`, `SSEClient` |
| Reconnect curve          | `src/client/backoff.ts`    | `backoffDelay`                 |
| Room refcounting         | `src/client/rooms.ts`      | `RoomRegistry`                 |
| Silent-stream detection  | `src/client/staleness.ts`  | `StalenessWatchdog`            |
| Server factory           | `src/server/mod.ts`        | `createSSEApp`                 |
| Streams, rooms, delivery | `src/server/service.ts`    | `SSEService`, `sseFrame`       |
| Replay window            | `src/server/replay.ts`     | `ReplayBuffer`, `parseEventId` |
| Wire definitions         | `src/protocol/*`           | `SSEMessage`, `ClientOp`, `OP` |

`RoomRegistry` and `StalenessWatchdog` are internal — exported from their
modules, but deliberately not re-exported from `src/mod.ts`.

## Project Structure

```
src/
├── mod.ts                 # client entry — browser safe, no demino
├── server.ts              # ./server entry  -> server/mod.ts
├── protocol.ts            # ./protocol entry -> protocol/mod.ts
├── protocol/              # dependency-free; imported by BOTH sides
│   ├── constants.ts       # PROTOCOL_VERSION, SSE_EVENT, OP, ERROR_CODE
│   ├── frames.ts          # SSEMessage, SSEHello, ClientOp, RpcResult
│   └── errors.ts          # typed errors
├── client/
└── server/
    └── adapters/          # cross-instance fan-out seam (local only today)
```

`src/server.ts` and `src/protocol.ts` exist because the npm build maps subpath
exports to `src/{name}.ts`. Do not inline them away — they also carry the
`@module` docs JSR renders as each subpath's landing page.

## Critical Conventions

**Protocol type is not application type.** The protocol owns a closed set of
event names; `payload` is opaque and must never be inspected or mutated. An
earlier implementation conflated them and had to `delete message.payload.type`
to compensate — do not reintroduce that.

**`protocol/` is the single source of truth.** Both sides import it. Changing a
shape means changing it there, and bumping `PROTOCOL_VERSION` if the change is
breaking.

**Native `EventSource`, not a hand-rolled parser.** This is the load-bearing
design choice. It means: no custom request headers (auth goes by cookie or
query string on the stream), the browser owns the retry loop, and `onerror`
carries no status. Do not "improve" this by switching to `fetch` streaming —
every consequence listed in this file follows from it.

**`readyState` is how failure is classified.** Per the HTML spec, `EventSource`
fails _permanently_ on a non-200 or wrong content type (`readyState` →
`CLOSED`) and _retries_ on a mid-stream drop (`CONNECTING`). Both behaviours are
verified in `tests/resilience.test.ts`. The client only intervenes on `CLOSED`.

**`GET /ping` exists for the probe.** `EventSource` hides the status, so a `503`
during a rolling deploy is indistinguishable from a `401`. The probe asks. Do
not delete the route thinking it is a stray health check.

**Snapshot `upTo` BEFORE registering the connection.** In
`SSEService.handleStream`, replay works only because the sequence ceiling is
taken before the connection joins the index, and nothing awaits between the
snapshot, the registration and the replay. Register first and you get
duplicates; skip the ceiling and you get them too. There is a test
(`a replayed message is not also delivered live`).

**Rooms ride the connect URL.** This is what removes the ordering hazard
`@marianmeres/ws` has to be careful about — there is no window in which a
message can land in a room the server has not registered yet. `#reconcileRooms`
only patches up subscriptions that changed _while_ the stream was connecting.

**Heartbeats must be real events, never `:` comments.** `EventSource` does not
expose comments to JavaScript, so a comment heartbeat cannot drive staleness
detection.

**Only `message` events carry an `id:`.** `hello` and `heartbeat` must not, or
`Last-Event-ID` would stop pointing at the last real message.

**`verify()` must be deterministic.** It runs on the stream request and on
every upstream call independently, and `sub`/`unsub` resolve the target stream
by client id. Non-determinism shows up as `"sub for unknown stream"` debug logs.

**Safe defaults are deny.** `allowBroadcast` denies; the HTTP injection routes
are not mounted without `httpAuth`. Do not "helpfully" relax either.

**Superseded streams are ignored via `#generation`.** Every `EventSource`
callback checks its captured generation. Without it a slow `onerror` from a
dead stream cancels the reconnect that replaced it.

**No outbox, deliberately.** Publishing is an independent HTTP request that
works while the stream is down, so there is no dead socket for a send to ride.
Adding a queue here would be cargo-culting `@marianmeres/ws`.

## Before Making Changes

1. `deno task test` — 135 tests against a real `Deno.serve` and Deno's global
   `EventSource`
2. `deno task check` — lint, fmt, type check, **and `deno doc --lint`**
3. Touching the wire? Update `src/protocol/` first, then both sides
4. Touching reconnect, replay or the probe? Read `tests/resilience.test.ts`
   first — those tests encode the failure modes the design exists to handle
5. Touching error branches or the adapter seam? `tests/edge.test.ts`
6. `deno task npm:build` if packaging changed (npm ships the **client only**;
   the server needs `demino`, which is JSR-only, and is excluded via
   `sourceFiles`)

## Publishing to JSR

`deno task check` is not optional — JSR rejects a package that fails either of
the two checks it bundles:

- **No slow types.** Every exported function, method and accessor needs an
  explicit return type. `deno publish --dry-run` is the authoritative check.
- **JSDoc on every public symbol**, interface fields included. A block of only
  `@param` tags does **not** count — `deno doc --lint` wants a leading
  description sentence, which is the one that trips constructors.

Also: every entry point (`src/mod.ts`, `src/server.ts`, `src/protocol.ts`)
carries a `@module` doc with a usage `@example`. That is what renders as the
package landing page on jsr.io, so treat it as the front door, not a comment.

`deno task rp` releases and publishes to both registries.

## Known Gaps

- **Replay is instance-local.** Event ids are prefixed with the instance that
  minted them, so a client reconnecting elsewhere starts live. Behind a load
  balancer without sticky sessions, replay silently does nothing
- `recipients` counts are instance-local; they stay that way under a
  distributed adapter
- Only `SSEPubSubLocal` exists — Redis/Deno-KV adapters are an unimplemented
  seam. Reference implementations exist in a private package and can be lifted
- No presence (`@marianmeres/ws` has it)
- No binary payloads — JSON text only
- No rate limiting. A per-`clientId` cap would be theatre when the client mints
  its own id; it belongs in a demino middleware keyed on something
  authenticated
- Every upstream message is a full HTTP request. Fine at a message a second,
  wrong at fifty — that workload wants `@marianmeres/ws`
- An SSE stream consumes one of the browser's ~6 per-origin HTTP/1.1
  connections. Non-issue over HTTP/2
- Node/Bun cannot run the server (`demino` is JSR-only)

## Documentation Index

| Document            | Purpose                                        |
| ------------------- | ---------------------------------------------- |
| `README.md`         | Human-facing overview and usage                |
| `API.md`            | Complete API reference and wire protocol       |
| `COMPARISON.md`     | How this differs from `@marianmeres/ws`        |
| `docs/DESIGN.md`    | Decision log — _why_ the conventions above are |
| `example/README.md` | Runnable reference app — `deno task example`   |

`docs/DESIGN.md` is the reasoning behind the rules in "Critical Conventions" —
read it before arguing with one of them. It is a decision log, not a reference;
it deliberately does not restate the API, and where it disagrees with the code,
the code wins.

## The Example

`example/` is a working ops feed exercising the whole surface — run it with
`deno task example`. It is excluded from both `deno publish` and the npm build,
and it imports `../src/` directly so it always builds against the working tree.

It is the fastest way to see replay behave: pause, wait, resume. Note that
`example/history.ts` is **not** how a reconnect recovers — that is
`Last-Event-ID` replay, which needs no application code at all. History exists
only for `hello.gap`. Conflating the two is the mistake the example is shaped
to prevent.
