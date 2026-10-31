/**
 * Reference server for the ops-feed example.
 *
 * Three demino apps composed into one handler:
 *
 * | Mount  | What                                                          |
 * | ------ | ------------------------------------------------------------- |
 * | `/sse` | `createSSEApp()` — the stream, `/rpc`, `/ping`, `/stats`, injection |
 * | `/api` | this example's own routes (history + demo controls)           |
 * | `/`    | static files from `example/public`                            |
 *
 * Run:
 * ```
 * pnpm run example          # builds the client bundle, then serves
 * ```
 *
 * @module
 */

import {
	demino,
	deminoCompose,
	type DeminoContext,
	type DeminoHandler,
	logListenInfo,
} from "@marianmeres/demino";
import { serve } from "@marianmeres/demino/node";
import { dirname, join } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { createSSEApp } from "../src/server.ts";
import { HistoryAdapter } from "./history.ts";
import { Pipeline } from "./pipeline.ts";
import {
	BROADCAST_ROOM,
	DEFAULT_PROJECT,
	NAME_RE,
	PROJECT_RE,
	REPLAY_BUFFER_SIZE,
	RESERVED_NAMES,
	TAB_RE,
} from "./shared.ts";

const HERE = dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = join(HERE, "public");

const PORT = Number(process.env.PORT) || 8000;

/**
 * Shared secret for the HTTP injection routes. Hard-coded default because this
 * is a demo; the point is that the routes exist *only* because `httpAuth` does.
 */
const ADMIN_TOKEN = process.env.SSE_EXAMPLE_TOKEN || "dev-secret";

/**
 * Every message passes through here on its way out. See `history.ts` — the
 * adapter seam doing double duty as the app's own backlog, for the one case
 * `Last-Event-ID` replay cannot cover.
 */
const history = new HistoryAdapter();

/** Demo switch — see `maintenanceGate`. */
const maintenance = { on: false };

/**
 * Returns `503` for every `/sse` route while maintenance is on.
 *
 * This is the interesting failure to play with. `EventSource` gives up
 * permanently on *any* non-200, so a `503` from a rolling deploy is
 * indistinguishable from a `401` as far as the browser is concerned. The client
 * resolves the ambiguity by probing `GET /sse/ping` — which this gate also
 * covers — and only then decides whether to retry or give up.
 */
const maintenanceGate: DeminoHandler = () => {
	if (maintenance.on) {
		return new Response("Service Unavailable", {
			status: 503,
			headers: { "retry-after": "1" },
		});
	}
};

/**
 * Guards `POST /sse/publish/…` and `POST /sse/broadcast/…`.
 *
 * A middleware that *returns* short-circuits the chain, so returning a 401 here
 * is all it takes. Without this handler `createSSEApp` does not mount those
 * routes at all — an unauthenticated "push anything into any room" endpoint is
 * not something you get by forgetting to configure it.
 */
const requireAdminToken: DeminoHandler = (req: Request) => {
	if (req.headers.get("authorization") !== `Bearer ${ADMIN_TOKEN}`) {
		return new Response("Unauthorized", { status: 401 });
	}
};

const { app: sseApp, service } = createSSEApp("/sse", [maintenanceGate], {
	adapter: history,
	httpAuth: requireAdminToken,

	/**
	 * Deliberately tiny, so you can overflow it on purpose — hit "burst" while
	 * paused and watch `hello.gap` come back `true`. Production default is 500.
	 */
	replayBufferSize: REPLAY_BUFFER_SIZE,

	/** Short so the "connection is alive" beat is visible in devtools. */
	heartbeatInterval: 10_000,

	/**
	 * Base for the SSE `retry:` field, jittered ±50% per connection. Short here
	 * so a demo reconnect is quick; the jitter is what stops a whole fleet
	 * coming back in lockstep after a deploy.
	 */
	retryInterval: 2_000,

	/**
	 * Runs on the stream request **and on every upstream call** — they are
	 * separate HTTP requests and there is no session to trust. Keep it cheap.
	 *
	 * It must also be **deterministic**: `sub`/`unsub` resolve the target
	 * stream by client id, so returning a different id for the same payload
	 * would silently misroute subscriptions. That is why the tab suffix comes
	 * from the client rather than being minted here.
	 *
	 * Returning `null` produces a `401`, which the client treats as terminal —
	 * it stops retrying and emits `terminated`. Exactly right for a malformed
	 * name: retrying a typo forever helps nobody.
	 */
	verify: (payload: unknown) => {
		const { name, project, tab } = (payload ?? {}) as Record<string, unknown>;

		if (typeof name !== "string" || !NAME_RE.test(name)) return null;
		if (RESERVED_NAMES.includes(name.toLowerCase())) return null;

		const namespace = typeof project === "string" && PROJECT_RE.test(project)
			? project.toLowerCase()
			: DEFAULT_PROJECT;

		// The client proposes a per-tab suffix and we honour it when it looks
		// sane. Anything else gets a deterministic fallback — never trust the
		// payload, but never mint a random id here either.
		const suffix = typeof tab === "string" && TAB_RE.test(tab) ? tab : "notab";

		return {
			// Client ids are unique *by contract* — when a second stream opens
			// with an id that is already connected, the newcomer wins and the
			// older stream is closed. That is what makes reconnect-after-drop
			// recover instead of piling up ghosts, but it would also make two
			// tabs sharing a name kick each other in an endless loop.
			clientId: `${name}#${suffix}`,
			// The namespace is the isolation boundary: two projects can both
			// have a #deploys and never see each other.
			namespace,
			// Available to `allowBroadcast` (and any other hook) as `ctx.meta`.
			meta: { name },
		};
	},

	/**
	 * Broadcast crosses the namespace boundary, so it **denies by default** and
	 * has to be opened deliberately. Here: only in one room. Try the client's
	 * "all projects" checkbox in #deploys to see the refusal arrive as a `403`
	 * → `SSERemoteError`.
	 */
	allowBroadcast: (_ctx, room) => room === BROADCAST_ROOM,
});

const pipeline = new Pipeline(service);

/**
 * The example's own API: the backlog, plus two demo switches.
 *
 * In anything real these would be behind `requireAdminToken` like the injection
 * routes. They are open here so the buttons in the UI can reach them.
 */
const apiApp = demino("/api");

apiApp.get(
	"/history/[namespace]/[room]",
	(_req: Request, _info: unknown, ctx: DeminoContext) => ({
		messages: history.read(ctx.params.namespace, ctx.params.room),
	}),
);

apiApp.get("/maintenance", () => ({ on: maintenance.on }));

apiApp.post("/maintenance", async (req: Request) => {
	const body = await req.json().catch(() => ({})) as { on?: unknown };
	maintenance.on = !!body.on;
	console.log(`maintenance mode ${maintenance.on ? "ON (503)" : "off"}`);
	return { on: maintenance.on };
});

apiApp.post("/burst", async (req: Request) => {
	const body = await req.json().catch(() => ({})) as { count?: unknown };
	const count = Math.min(100, Math.max(1, Number(body.count) || 25));
	await pipeline.burst(count);
	return { ok: true, count, seq: service.stats().seq };
});

/** Static file server for the built client. */
const siteApp = demino();
siteApp.static("/", PUBLIC_DIR);

if (import.meta.main) {
	pipeline.start();

	await serve(deminoCompose([siteApp, apiApp, sseApp]), {
		port: PORT,
		// SSE is plain HTTP; on Node, upgrades would need the optional `ws` peer
		webSockets: false,
		onListen: (addr) => {
			logListenInfo(addr);
			const base = `http://localhost:${addr.port}`;
			console.log(`
  Open ${base}. A fake pipeline is already publishing.

  The thing to try: hit "pause", let the feed run on without you, then
  "resume" — replay fills in exactly what you missed. Then pause again,
  hit "burst", and resume: the window overflowed, so you get a gap.

  Server-side injection (arrives with from: null, rendered as a system line):

    curl -X POST ${base}/sse/publish/${DEFAULT_PROJECT}/deploys \\
      -H "authorization: Bearer ${ADMIN_TOKEN}" \\
      -H "content-type: application/json" \\
      -d '{"kind":"event","id":"srv-1","level":"success","source":"deploy-bot","text":"Shipped from curl"}'

  Replay state — "seq" is the sequence, "buffered" what is still resumable
  (guarded by the same token, since httpAuth is configured):

    curl -s ${base}/sse/stats -H "authorization: Bearer ${ADMIN_TOKEN}"
`);
		},
	});

	// Close every stream on Ctrl-C. The client sees the drop, EventSource
	// retries on its own, and restarting the server brings everyone back —
	// with replay filling the outage, as long as it fits the window.
	process.on("SIGINT", () => {
		console.log("\nshutting down…");
		pipeline.stop();
		service.close().finally(() => process.exit(0));
	});
}

export { apiApp, history, pipeline, service, siteApp, sseApp };
