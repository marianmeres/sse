# @marianmeres/sse — Agent Guide

Server-Sent Events client with namespaces, rooms and replay, plus a
demino-mountable reference server.

## Quick Reference

```yaml
name: "@marianmeres/sse"
runtime: "node (dev + reference); deno = JSR consumer runtime; client also browser"
registries: "npm (manifest of record: package.json, dist/ from tsc) + JSR (deno.json, src/)"
entry: "./src/mod.ts"
exports: [".", "./server", "./protocol", "./adapters/redis"]
install: "pnpm install"
test: "pnpm test" # node:test on Node; `pnpm run test:deno` runs the same suite on Deno
check: "pnpm run ci" # tsc --noEmit + Biome + tests + npm smoke
jsr: "pnpm run jsr:check" # deno doc --lint + deno publish --dry-run
```

| Task                     | File                           | Key symbol                     |
| ------------------------ | ------------------------------ | ------------------------------ |
| Client                   | `src/client/sse-client.ts`     | `createSSEClient`, `SSEClient` |
| Reconnect curve          | `src/client/backoff.ts`        | `backoffDelay`                 |
| Room refcounting         | `src/client/rooms.ts`          | `RoomRegistry`                 |
| Silent-stream detection  | `src/client/staleness.ts`      | `StalenessWatchdog`            |
| Server factory           | `src/server/mod.ts`            | `createSSEApp`                 |
| Streams, rooms, delivery | `src/server/service.ts`        | `SSEService`, `sseFrame`       |
| Replay window            | `src/server/replay.ts`         | `ReplayBuffer`, `parseEventId` |
| Redis fan-out            | `src/server/adapters/redis.ts` | `SSEPubSubRedis`, `RedisLike`  |
| Wire definitions         | `src/protocol/*`               | `SSEMessage`, `ClientOp`, `OP` |

`RoomRegistry` and `StalenessWatchdog` are internal — exported from their
modules, but deliberately not re-exported from `src/mod.ts`.

## Project Structure

```
src/
├── mod.ts                 # client entry — browser safe, no demino
├── server.ts              # ./server entry  -> server/mod.ts
├── protocol.ts            # ./protocol entry -> protocol/mod.ts
├── adapters/
│   └── redis.ts           # ./adapters/redis entry -> server/adapters/redis.ts
├── protocol/              # dependency-free; imported by BOTH sides
│   ├── constants.ts       # PROTOCOL_VERSION, SSE_EVENT, OP, ERROR_CODE
│   ├── frames.ts          # SSEMessage, SSEHello, ClientOp, RpcResult
│   └── errors.ts          # typed errors
├── client/
└── server/
    └── adapters/          # cross-instance fan-out seam (local + redis)
tests/                     # node:test + node:assert; runs on Node and on Deno
scripts/smoke-npm.mjs      # packs, installs and round-trips the npm tarball
package.json               # manifest of record: version, deps, exports -> dist/, scripts
deno.json                  # JSR manifest: exports -> src/, jsr: import map; no version, no tasks
```

`src/server.ts`, `src/protocol.ts` and `src/adapters/redis.ts` exist because
every subpath export maps to a flat `src/{name}.ts` (`dist/{name}.js` on npm).
Do not inline them away — they also carry the `@module` docs JSR renders as
each subpath's landing page. `./adapters/redis` is deliberately a separate
entry: importing the server must not drag Redis-flavoured code in.

**Two manifests, one package.** `package.json` is the only one with a
`version` and the only one with scripts; `deno.json` mirrors its dependencies
as `jsr:` imports with the same ranges. `tests/manifest-sync.test.ts` fails on
any drift — a new dependency goes into both. `@marianmeres/demino` is an
**optional peer** on npm (plus a devDependency), never a dependency: a
client-only install must not pull a server framework, and the host's demino
and ours must be one copy.

**Runtime policy.** Node is the dev runtime and the gate; Deno stays a tested
consumer runtime only while JSR consumers exist; nothing is built or tested
for Bun. No `Deno.*` / `Bun.*` in `src/`, no `jsr:`/`npm:`/`https:`
specifiers, erasable-only TS (`tsc` with `erasableSyntaxOnly` enforces it).
The tests pick demino's `/node` or `/deno` adapter by runtime
(`tests/_helpers.ts` `listen()`), so each runtime is tested the way its
consumers serve the app.

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

**An adapter must suppress its own echo.** The seam envelope has no origin
field and `SSEService` does no dedup — local delivery has already happened when
`publish()` is called, so an adapter that lets its own frame come back
double-delivers. `SSEPubSubRedis` stamps an `origin` id on the wire and skips
it on receipt; any new adapter must do the equivalent. The adapter must also be
listening _before_ the service is constructed (`onRemote` is wired
synchronously in the constructor) — hence `await adapter.init()` first.

## Before Making Changes

1. `pnpm test` — 159 tests on `node:test`, against a real server (demino's
   `/node` adapter) and Node's `EventSource` (the script passes
   `--experimental-eventsource`). Two of them run against a live Redis and are
   skipped unless `TEST_REDIS_URL` is set (e.g.
   `TEST_REDIS_URL=redis://localhost:6379/13 pnpm test`)
2. `pnpm run ci` — `tsc --noEmit` (the only type gate: Node strips types and
   never checks them), Biome lint + format check, the suite, and the npm smoke
   (`scripts/smoke-npm.mjs`: pack, install without demino, import the client;
   install demino, round-trip a message on Node)
3. `pnpm run test:deno` — the same suite on Deno (`deno test`, its own
   `EventSource`, demino's `/deno` adapter). Needed while JSR consumers exist
4. Touching the wire? Update `src/protocol/` first, then both sides
5. Touching reconnect, replay or the probe? Read `tests/resilience.test.ts`
   first — those tests encode the failure modes the design exists to handle
6. Touching error branches or the adapter seam? `tests/edge.test.ts`; the
   Redis adapter's contract lives in `tests/redis-adapter.test.ts` (in-memory
   fake) and `tests/redis-live.test.ts` (real wire)
7. Adding or bumping a dependency? Same range in `package.json` and
   `deno.json`; `tests/manifest-sync.test.ts` enforces it

## Publishing

`pnpm run rp` / `rpm` releases (`@marianmeres/release` bumps `package.json`,
rebuilds the example bundle before committing) and publishes to npm, then
JSR with the same version (`deno publish --set-version`). `pnpm run
publish:next` sends a prerelease to npm under the `next` tag. `prepublishOnly`
runs `pnpm run ci` and the `tsc` build, so npm never gets an untested `dist/`.

`pnpm run jsr:check` is not optional — JSR rejects a package that fails either
of the two checks it bundles (CI runs it in the Deno job):

- **No slow types.** Every exported function, method and accessor needs an
  explicit return type. `deno publish --dry-run` is the authoritative check.
- **JSDoc on every public symbol**, interface fields included. A block of only
  `@param` tags does **not** count — `deno doc --lint` wants a leading
  description sentence, which is the one that trips constructors.

`deno publish` type-checks with `tsconfig.json` (Deno reads it when a
`package.json` is present), which is why `lib` lists `esnext.disposable`:
tsc gets `Symbol.dispose` from `@types/node`, Deno does not.

Also: every entry point (`src/mod.ts`, `src/server.ts`, `src/protocol.ts`,
`src/adapters/redis.ts`) carries a `@module` doc with a usage `@example`. That
is what renders as the package landing page on jsr.io, so treat it as the
front door, not a comment.

## Known Gaps

- **Replay is instance-local.** Event ids are prefixed with the instance that
  minted them, so a client reconnecting elsewhere starts live. Behind a load
  balancer without sticky sessions, replay silently does nothing
- `recipients` counts are instance-local; they stay that way under a
  distributed adapter
- No zero-infra multi-instance option. Redis ships (`./adapters/redis`); an
  earlier implementation had a Deno-KV adapter for the same seam, deliberately
  not carried over — Deno KV is runtime lock-in. A SQLite or Postgres adapter
  would be the portable equivalent if one is ever wanted
- No presence (`@marianmeres/ws` has it)
- No binary payloads — JSON text only
- No rate limiting. A per-`clientId` cap would be theatre when the client mints
  its own id; it belongs in a demino middleware keyed on something
  authenticated
- Every upstream message is a full HTTP request. Fine at a message a second,
  wrong at fifty — that workload wants `@marianmeres/ws`
- An SSE stream consumes one of the browser's ~6 per-origin HTTP/1.1
  connections. Non-issue over HTTP/2
- On Node (22–26) the client needs `--experimental-eventsource`: it uses the
  global `EventSource`, which Node still keeps behind that flag. There is no
  option to inject a constructor
- Deno 2.9 prints a one-time `request.signal aborts on successful responses
  (legacy behavior)` notice during `test:deno`, from the service's use of
  `req.signal`. It predates the move to Node; unexamined

## Documentation Index

| Document            | Purpose                                        |
| ------------------- | ---------------------------------------------- |
| `README.md`         | Human-facing overview and usage                |
| `API.md`            | Complete API reference and wire protocol       |
| `COMPARISON.md`     | How this differs from `@marianmeres/ws`        |
| `docs/DESIGN.md`    | Decision log — _why_ the conventions above are |
| `example/README.md` | Runnable reference app — `pnpm run example`    |

`docs/DESIGN.md` is the reasoning behind the rules in "Critical Conventions" —
read it before arguing with one of them. It is a decision log, not a reference;
it deliberately does not restate the API, and where it disagrees with the code,
the code wins.

## The Example

`example/` is a working ops feed exercising the whole surface — run it with
`pnpm run example`. It is excluded from both `deno publish` and the npm package,
and it imports `../src/` directly so it always builds against the working tree.

It is the fastest way to see replay behave: pause, wait, resume. Note that
`example/history.ts` is **not** how a reconnect recovers — that is
`Last-Event-ID` replay, which needs no application code at all. History exists
only for `hello.gap`. Conflating the two is the mistake the example is shaped
to prevent.
