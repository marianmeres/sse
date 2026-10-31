# Reference implementation — a live ops feed

A complete, standalone app built on `@marianmeres/sse`: a
[demino](https://www.npmjs.com/package/@marianmeres/demino) server on Node and a
plain HTML client written with
[`@marianmeres/vanilla`](https://www.npmjs.com/package/@marianmeres/vanilla) and
bundled by [`@marianmeres/bundle`](https://www.npmjs.com/package/@marianmeres/bundle).
Styling is
[`@marianmeres/design-tokens`](https://www.npmjs.com/package/@marianmeres/design-tokens)
with the Bootstrap Reboot bridge — minimal, but every colour is a token.

```bash
pnpm run example        # builds the client bundle, then serves on :8000
```

Open <http://localhost:8000>. A fake CI/CD pipeline is already publishing, so
there is something to watch immediately.

> The sibling example in `@marianmeres/ws` is a **chat** — bidirectional,
> chatty, with presence and typing indicators. That is what WebSocket is for.
> This one is a **feed**: traffic is overwhelmingly server → client, and the
> thing it is built to show off is **replay**, which WebSocket has no answer to.

## What it exercises

| Feature                      | Where to look                                                       |
| ---------------------------- | ------------------------------------------------------------------- |
| **`Last-Event-ID` replay**   | the badge in the header; pause, wait, resume — nothing is lost      |
| **`hello.gap`**              | pause, hit "burst", resume — the window overflowed, so it refetches |
| `verify` on every request    | `server.ts` — operator → client id + namespace; a bad one is `401`  |
| Terminal vs retryable        | "simulate 503" retries forever; the name `admin` gives up at once   |
| The probe                    | that distinction is `GET /sse/ping` — see `#probe` in the client    |
| Namespaces                   | the "project" field; two projects never see each other              |
| Rooms                        | the sidebar; switching unsubscribes then subscribes                 |
| Acknowledged publishes       | the `✓ n` next to your comments — recipients from the response      |
| Publish with the stream down | comment while paused; it is plain HTTP, so it just works            |
| Broadcast + its gate         | the "all projects" checkbox; denied outside `#alerts`               |
| Reconnect                    | the status badge; kill the server and watch it recover              |
| Server-side injection        | `POST /sse/publish/…`; arrives with `from: null`                    |
| The adapter seam             | `history.ts` — an `SSEPubSubAdapter` teeing into a ring buffer      |

## Files

```
example/
├── server.ts          three demino apps: /sse (createSSEApp), /api, / (static)
├── pipeline.ts        a fake CI/CD run — calls service.publish() and nothing else
├── history.ts         SSEPubSubAdapter decorator → the backlog, for gaps only
├── shared.ts          the APPLICATION protocol — what goes inside `payload`
├── build-theme.ts     regenerates public/theme.css from a bundled theme
├── client/
│   ├── mod.ts         views (login + feed), built with @marianmeres/vanilla
│   └── store.ts       the SSEClient and all reactive state; no DOM
└── public/
    ├── index.html     templates + one <script type="module">
    ├── app.css        layout, entirely token-driven
    ├── reboot.css     Bootstrap Reboot (vendored, MIT)
    ├── theme.css      generated — design tokens + the --bs-* bridge
    └── dist/bundle.js generated — pnpm run example:build
```

## Tasks

| Task                     | What                                                   |
| ------------------------ | ------------------------------------------------------ |
| `pnpm run example`       | build the bundle, then serve                           |
| `pnpm run example:build` | bundle `client/mod.ts` → `public/dist/bundle.js`       |
| `pnpm run example:watch` | same, rebuilding on change (run the server separately) |
| `pnpm run example:theme` | regenerate `public/theme.css`                          |

`PORT` and `SSE_EXAMPLE_TOKEN` are read from the environment, or from a `.env`
file (see `.env.example`); defaults `8000` and `dev-secret`.

## Things worth trying

**Miss nothing.** Hit **⏸ Pause**. The stream closes, the pipeline keeps
publishing, and you fall behind. Hit **▶ Resume**: the badge says `resumed`,
a divider appears, and every message you missed arrives tagged `replayed` with
its original timestamp. No refetch, no application code — the browser resent
`Last-Event-ID` and the server replayed from its window. This is the whole
reason the package exists next to `@marianmeres/ws`, which in the same
situation would simply have lost them.

**Then overflow it.** The replay window here is deliberately tiny — 20
messages, against a production default of 500. Pause, hit **💥 Burst** (which
publishes 30), and resume. Now the badge says `gap`: the resume point aged out,
those messages are genuinely gone, and `hello.gap` told the client so. _That_ is
when the app falls back to its own history — `loadHistory()` in `store.ts`,
served from `history.ts`. Replay covers the 99% case; history covers the rest.

**Watch a 503 not be fatal.** Hit **🔧 Simulate 503**. Every `/sse` route now
answers `503`, and `EventSource` reacts the way the spec says it must: it gives
up _permanently_ on any non-200. A naive client would stop dead — which is
exactly wrong, because a rolling deploy looks like this. Instead the client
probes `GET /sse/ping`, sees a `503` rather than a `401`, and keeps retrying
with backoff. Turn it off and it comes back, with replay filling the outage.

**Then get genuinely rejected.** Log in as `admin`. `verify` returns `null`,
the response is `401`, the probe confirms it, and the client _stops_ — a
terminal status is the one exit that does not retry, so the app has to route
you back to the form.

**Comment with the stream down.** Pause, then send a comment. It goes through:
publishing is an ordinary `POST`, entirely independent of the stream, which is
precisely why this client has no outbox. Resume, and your own comment comes
back to you through replay.

**Watch it survive a restart.** Stop the server (Ctrl-C) with the page open.
The badge goes amber and counts attempts. Start it again and the client
reconnects, re-registers its rooms from the connect URL, and picks up where it
left off. Note the server's replay buffer is in memory, so a restart empties it
— the resume point is from a dead instance and is correctly ignored.

**Push from outside the browser.** The HTTP injection routes exist only because
`server.ts` supplies `httpAuth` — without it `createSSEApp` does not mount them
at all.

```bash
curl -X POST http://localhost:8000/sse/publish/acme/deploys \
  -H "authorization: Bearer dev-secret" \
  -H "content-type: application/json" \
  -d '{"kind":"event","id":"srv-1","level":"success","source":"deploy-bot","text":"Shipped from curl"}'
```

It arrives with `from: null`, which is how a client tells a server push apart
from peer traffic. Send a payload that is _not_ one of ours and the client falls
back to printing the raw JSON rather than dropping it.

**Cross the namespace boundary.** Log in twice with different projects and
comment in `#deploys` — neither side sees the other. Then tick "broadcast to all
projects" in `#alerts`, where the server's `allowBroadcast` says yes. Try the
same checkbox in `#deploys` and the refusal comes back as a `403`
(`SSERemoteError`), because the gate **denies by default**.

**Read the stream directly.** It is just HTTP, so `curl` is a first-class
client — which is half the argument for SSE over WebSocket. `verify` is
configured here, so the auth payload rides the query string (`EventSource`
cannot set headers, which is why the server accepts it there):

```bash
AUTH=$(printf '%s' '{"name":"curl","project":"acme","tab":"abcd"}' | jq -sRr @uri)

curl -N "http://localhost:8000/sse/events?namespace=acme&rooms=deploys&auth=$AUTH"
```

```
retry: 1138

event: hello
data: {"clientId":"curl#abcd","namespace":"acme","protocol":1,"heartbeatInterval":10000,"resumed":false,"gap":false}

id: t8nbpq:2
event: message
data: {"room":"deploys","namespace":"acme","from":null,"payload":{…},"timestamp":1785851159091}
```

Note the client id came back as `curl#abcd` — `verify` overrode it, and `hello`
is how you find out. Note also the `id:`, which is the resume point,
`{instanceId}:{seq}`. Kill the curl, wait for a few events to go by, then
reconnect passing the last id you saw:

```bash
curl -N -H "last-event-id: t8nbpq:12" \
  "http://localhost:8000/sse/events?namespace=acme&rooms=deploys&auth=$AUTH"
```

```
event: hello
data: {"clientId":"curl#abcd",…,"resumed":true,"gap":false}

id: t8nbpq:13        <- everything you missed, replayed before going live
event: message
data: {…}
```

```bash
# replay window state: "seq" is where the sequence is, "buffered" what is still resumable
curl -s http://localhost:8000/sse/stats -H "authorization: Bearer dev-secret"
# {"instanceId":"t8nbpq","connections":1,"rooms":2,"namespaces":{"acme":1},"seq":16,"buffered":16}
```

## Notes on the design

**The protocol does not know what a deploy is.** `shared.ts` defines
`{ kind: "event" | "comment", … }` and both ends import it. The library never
looks inside `payload` — a pipeline event and an operator comment are
indistinguishable to it, and telling them apart is entirely the app's job.

**Replay and history are different things.** Replay is bounded, automatic,
exact and free; it covers a blip, a laptop lid, a rolling deploy. History is the
application's own store, as large as it cares to make it; it covers the initial
page load and the one case replay cannot — `hello.gap`. Conflating them is the
easy mistake: you either refetch on every reconnect (wasteful, and racy against
live traffic) or you trust replay for absences it was never sized for.

**`history.ts` uses the adapter seam.** `SSEService` hands every publish and
broadcast to `adapter.publish(envelope)` after local delivery — the one place
all traffic converges. A real deployment puts Redis there; here a
decorator records messages and delegates the rest. Broadcasts (`namespace:
null`) belong to no single room and are deliberately not stored.

**`verify` has to be deterministic.** It runs on the stream request _and_ on
every `POST /sse/rpc`, independently, and `sub`/`unsub` resolve the target
stream by client id. That is why the per-tab suffix is proposed by the client
and merely validated by the server, rather than minted server-side: a `verify`
that returned a fresh id per call would silently misroute every subscription.

**Client ids are not operator names.** `verify` returns `name#suffix`. Ids are
unique by contract — a second stream authenticating with a live id replaces it —
so letting two tabs share one would make them kick each other forever. Keeping
the suffix stable across reconnects is what lets replay resume cleanly.

**The pipeline knows nothing about clients.** `pipeline.ts` calls
`service.publish()` and stops. No socket, no connection, no awareness of who is
listening — which is the shape most real SSE traffic has, and the reason a
half-duplex transport is not a compromise here.
