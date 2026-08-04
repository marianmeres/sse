/**
 * Reference server — a demino app implementing the wire protocol.
 *
 * Deno-first because `demino` is JSR-only. Import it from
 * `@marianmeres/sse/server` so `demino` never reaches a browser bundle.
 *
 * @example
 * ```ts
 * import { createSSEApp } from "@marianmeres/sse/server";
 *
 * const { app, service } = createSSEApp("/sse", [], {
 *     verify: async (payload) => {
 *         const { token } = (payload ?? {}) as { token?: string };
 *         const user = await auth(token);
 *         return user ? { clientId: user.id, namespace: user.orgId } : null;
 *     },
 * });
 * Deno.serve(app);
 * ```
 *
 * @module
 */

import {
	type Demino,
	demino,
	type DeminoContext,
	type DeminoHandler,
	type DeminoOptions,
} from "@marianmeres/demino";
import { SSEService, type SSEServiceOptions } from "./service.ts";

export * from "./adapters/abstract.ts";
export * from "./adapters/local.ts";
export {
	formatEventId,
	parseEventId,
	ReplayBuffer,
	type ReplayEntry,
	type ResumePoint,
} from "./replay.ts";
export {
	type SSEConnectionContext,
	sseFrame,
	SSEService,
	type SSEServiceOptions,
	type SSEStats,
} from "./service.ts";

/** Configuration for {@link createSSEApp}. */
export interface SSEAppOptions extends SSEServiceOptions {
	/**
	 * Guards `POST /publish` and `POST /broadcast`.
	 *
	 * **Without it those routes are not mounted at all.** They are a separate
	 * trust boundary from the `verify` hook — mounting an unauthenticated
	 * "push anything into any room" endpoint by default would be a genuine
	 * vulnerability, so it has to be a deliberate act.
	 */
	httpAuth?: DeminoHandler;
	/**
	 * Passed straight through to `demino()`. HTTP-level concerns (access
	 * logging, error handling, CORS, rate limiting) are configured here,
	 * separately from the SSE `logger`.
	 */
	deminoOptions?: DeminoOptions;
}

/** What {@link createSSEApp} returns. */
export interface SSEApp {
	/** The demino app. Mount it, or serve it directly. */
	app: Demino;
	/** Inject messages from server-side code, read stats, shut down. */
	service: SSEService;
}

/**
 * Creates a mountable SSE server.
 *
 * Routes, relative to `mountPath`:
 *
 * | Method | Path                          | Notes                             |
 * | ------ | ----------------------------- | --------------------------------- |
 * | GET    | `/events`                     | The stream                        |
 * | GET    | `/ping`                       | `204`, or the auth status         |
 * | POST   | `/rpc`                        | Client operations                 |
 * | GET    | `/stats`                      | Guarded by `httpAuth` if set      |
 * | POST   | `/publish/[namespace]/[room]` | Requires `httpAuth`               |
 * | POST   | `/broadcast/[room]`           | Requires `httpAuth`               |
 */
export function createSSEApp(
	mountPath: string = "/sse",
	middlewares: DeminoHandler[] = [],
	options: SSEAppOptions = {},
): SSEApp {
	const service = new SSEService(options);
	const app = demino(mountPath, middlewares, options.deminoOptions);
	const { httpAuth } = options;

	app.get(
		"/events",
		(req: Request, _info: unknown, ctx: DeminoContext) =>
			// Forwarding ctx.headers is what lets a CORS middleware apply to the
			// stream — the handler returns a Response directly, so anything the
			// middleware set would otherwise be lost.
			service.handleStream(req, ctx.headers),
	);

	app.get("/ping", (req: Request) => service.handlePing(req));

	app.post("/rpc", (req: Request) => service.handleRpc(req));

	// Public when unguarded (handy in development), protected as soon as an
	// httpAuth middleware exists. Counts only — never client ids.
	if (httpAuth) app.get("/stats", httpAuth, () => service.stats());
	else app.get("/stats", () => service.stats());

	if (httpAuth) {
		app.post(
			"/publish/[namespace]/[room]",
			httpAuth,
			async (req: Request, _info: unknown, ctx: DeminoContext) => {
				const payload = await readJson(req);
				const recipients = await service.publish(
					ctx.params.room,
					payload,
					ctx.params.namespace,
				);
				return { ok: true, recipients };
			},
		);

		app.post(
			"/broadcast/[room]",
			httpAuth,
			async (req: Request, _info: unknown, ctx: DeminoContext) => {
				const payload = await readJson(req);
				const recipients = await service.broadcast(ctx.params.room, payload);
				return { ok: true, recipients };
			},
		);
	}

	return { app, service };
}

async function readJson(req: Request): Promise<unknown> {
	try {
		return await req.json();
	} catch {
		throw new Error("Invalid JSON body");
	}
}
