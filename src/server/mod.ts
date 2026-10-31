/**
 * Reference server — a demino app implementing the wire protocol.
 *
 * Import it from `@marianmeres/sse/server` so `demino` never reaches a browser
 * bundle. It uses no runtime API of its own: any demino 2.0 adapter serves it.
 *
 * @example
 * ```ts
 * import { serve } from "@marianmeres/demino/node";
 * import { createSSEApp } from "@marianmeres/sse/server";
 *
 * const { app, service } = createSSEApp("/sse", [], {
 *     verify: async (payload) => {
 *         const { token } = (payload ?? {}) as { token?: string };
 *         const user = await auth(token);
 *         return user ? { clientId: user.id, namespace: user.orgId } : null;
 *     },
 * });
 * await serve(app, { port: 8000 });
 * ```
 *
 * @module
 */

import {
	type Demino,
	type DeminoContext,
	type DeminoHandler,
	type DeminoOptions,
	demino,
} from "@marianmeres/demino";
import { SSEError } from "../protocol/errors.ts";
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
	SSEService,
	type SSEServiceOptions,
	type SSEStats,
	sseFrame,
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
	/**
	 * A pre-built service to mount the routes over, instead of constructing
	 * one here.
	 *
	 * For setups where the service must exist before — or independently of —
	 * the app: built at infrastructure time, handed to request middlewares,
	 * mounted later. Configure the service where you construct it — combining
	 * `service` with any {@link SSEServiceOptions} field **throws**, because
	 * silently ignoring, say, a `verify` or an `adapter` is exactly the kind
	 * of trap that ships unauthenticated. `httpAuth` and `deminoOptions` still
	 * apply. The caller keeps ownership, `close()` included.
	 */
	service?: SSEService;
}

/** The option keys that configure a service — invalid alongside `service`. */
const SERVICE_OPTION_KEYS: readonly (keyof SSEServiceOptions)[] = [
	"verify",
	"allowBroadcast",
	"heartbeatInterval",
	"retryInterval",
	"replayBufferSize",
	"maxRoomNameLength",
	"maxRoomsPerClient",
	"adapter",
	"logger",
];

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
	if (options.service) {
		const clashing = SERVICE_OPTION_KEYS.filter((k) => options[k] !== undefined);
		if (clashing.length) {
			throw new SSEError(
				`createSSEApp: 'service' cannot be combined with service options ` +
					`(${clashing.join(", ")}) — they would be silently ignored; ` +
					`configure them where the service is constructed`,
			);
		}
	}
	const service = options.service ?? new SSEService(options);
	const app = demino(mountPath, middlewares, options.deminoOptions);
	const { httpAuth } = options;

	app.get("/events", (req: Request, _info: unknown, ctx: DeminoContext) =>
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
