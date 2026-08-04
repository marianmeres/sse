/**
 * Connection registry, room index, replay and delivery.
 *
 * @module
 */

import { createClog, type Logger } from "@marianmeres/clog";
import { createTicker, type Ticker } from "@marianmeres/ticker";
import { base36 } from "@marianmeres/uid";

import {
	AUTH_HEADER,
	AUTH_PARAM,
	BROADCAST_NAMESPACE,
	ERROR_CODE,
	LAST_EVENT_ID_PARAM,
	OP,
	PROTOCOL_VERSION,
	SSE_EVENT,
} from "../protocol/constants.ts";
import type {
	AuthResult,
	ClientOp,
	SSEErrorInfo,
	SSEHello,
	SSEMessage,
} from "../protocol/frames.ts";
import type { SSEBroadcastEnvelope, SSEPubSubAdapter } from "./adapters/abstract.ts";
import { SSEPubSubLocal } from "./adapters/local.ts";
import { formatEventId, parseEventId, ReplayBuffer } from "./replay.ts";

/** What a hook knows about a request. */
export interface SSEConnectionContext {
	/** The id `verify()` settled on. */
	clientId: string;
	/** The namespace the client is acting in. */
	namespace: string;
	/** Whatever `verify()` returned as `meta`; empty when it returned none. */
	meta: Record<string, unknown>;
	/** The originating request, for headers, cookies and the like. */
	request: Request;
}

/** Server statistics. */
export interface SSEStats {
	/** This instance's id — the prefix on every event id. */
	instanceId: string;
	/** Open streams. */
	connections: number;
	/** Distinct room names in use. */
	rooms: number;
	/** Stream count per namespace. */
	namespaces: Record<string, number>;
	/** Most recently stamped sequence number. */
	seq: number;
	/** Messages currently retained for replay. */
	buffered: number;
}

/** Configuration for {@link SSEService}. */
export interface SSEServiceOptions {
	/**
	 * Authenticates a request. Return `null` (or throw) to reject with `401`.
	 * Omitted entirely means "no authentication", which is fine for
	 * development and not for anything else.
	 *
	 * Runs on the stream request **and every upstream call** — they are
	 * separate HTTP requests and there is no session to trust. Keep it cheap
	 * (verify a JWT, do not hit a database).
	 *
	 * **It must be deterministic for a given payload.** `sub`/`unsub` resolve
	 * the target stream by client id, so a hook that returns a different id
	 * per call silently misroutes subscriptions.
	 */
	verify?: (
		payload: unknown,
		request: Request,
	) => Promise<AuthResult | null> | AuthResult | null;
	/**
	 * Gate for cross-namespace broadcast. **Denies by default** — letting any
	 * client punch through every namespace boundary is not a safe default, and
	 * keeping `broadcast()` a distinct operation exists precisely so this check
	 * has somewhere to live.
	 */
	allowBroadcast?: (
		ctx: SSEConnectionContext,
		room: string,
	) => boolean | Promise<boolean>;
	/**
	 * Liveness beat cadence in ms. `0` disables. Default `15_000`.
	 *
	 * Sent as a real `heartbeat` event, not a `:` comment — `EventSource` does
	 * not expose comments to JavaScript, so a comment heartbeat cannot drive
	 * client-side staleness detection. It doubles as proxy keep-alive.
	 */
	heartbeatInterval?: number;
	/**
	 * Base value for the SSE `retry:` field in ms. Default `3_000`.
	 *
	 * Jittered ±50% per connection. `EventSource` retries on a fixed interval,
	 * so without this a fleet reconnects in lockstep after a deploy. Doing it
	 * server-side beats client-side jitter: the server can widen the window
	 * under load, and the client needs no code at all.
	 */
	retryInterval?: number;
	/** Messages retained for replay. `0` disables replay. Default `500`. */
	replayBufferSize?: number;
	/** Reject longer room names. Default `256`. */
	maxRoomNameLength?: number;
	/** Reject subscriptions beyond this many rooms per client. Default `100`. */
	maxRoomsPerClient?: number;
	/** Cross-instance fan-out. Default {@link SSEPubSubLocal} — no peers. */
	adapter?: SSEPubSubAdapter;
	/** `null` silences. */
	logger?: Logger | null;
}

interface Connection {
	id: string;
	namespace: string;
	rooms: Set<string>;
	meta: Record<string, unknown>;
	request: Request;
	controller: ReadableStreamDefaultController<Uint8Array>;
	closed: boolean;
}

const DEFAULTS = {
	heartbeatInterval: 15_000,
	retryInterval: 3_000,
	replayBufferSize: 500,
	maxRoomNameLength: 256,
	maxRoomsPerClient: 100,
} as const;

const encoder = new TextEncoder();

/** Serialises one SSE event. */
export function sseFrame(event: string, data: unknown, id?: string): string {
	let out = "";
	if (id) out += `id: ${id}\n`;
	out += `event: ${event}\n`;
	// JSON.stringify escapes newlines, so this is always a single line — the
	// split is defensive, and free.
	for (const line of JSON.stringify(data).split("\n")) out += `data: ${line}\n`;
	return `${out}\n`;
}

/** JSON response helper. */
function json(body: unknown, status = 200): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: { "content-type": "application/json; charset=utf-8" },
	});
}

function fail(code: string, message: string, status: number): Response {
	return json({ ok: false, error: { code, message } satisfies SSEErrorInfo }, status);
}

/**
 * Owns every stream, the room index, the replay buffer and delivery.
 *
 * Usable standalone (drive it from any `Deno.serve` handler) or through
 * `createSSEApp`, which mounts it as a demino app.
 */
export class SSEService {
	#verify: SSEServiceOptions["verify"];
	#allowBroadcast: SSEServiceOptions["allowBroadcast"];
	#heartbeatInterval: number;
	#retryInterval: number;
	#maxRoomNameLength: number;
	#maxRoomsPerClient: number;
	#adapter: SSEPubSubAdapter;

	/** Logger. Assignable — set to `null` to silence. */
	logger: Logger | null;

	/** Prefix on every event id, so a foreign resume point is recognisable. */
	readonly instanceId: string = base36(6);

	#connections = new Map<string, Connection>();
	/** room -> namespace -> client ids. */
	#index = new Map<string, Map<string, Set<string>>>();
	#replay: ReplayBuffer;

	#ticker: Ticker | null = null;
	#unsubscribeTicker: (() => void) | null = null;
	#unsubscribeRemote: () => void;
	#closed = false;

	/**
	 * Creates the service and starts its heartbeat. Pair it with `close()` —
	 * the heartbeat holds a timer for the process's lifetime otherwise.
	 *
	 * @param options - {@link SSEServiceOptions}; every field is optional
	 */
	constructor(options: SSEServiceOptions = {}) {
		this.logger = options.logger === undefined
			? createClog("sse:server")
			: options.logger;

		this.#verify = options.verify;
		this.#allowBroadcast = options.allowBroadcast;
		this.#heartbeatInterval = options.heartbeatInterval ??
			DEFAULTS.heartbeatInterval;
		this.#retryInterval = options.retryInterval ?? DEFAULTS.retryInterval;
		this.#maxRoomNameLength = options.maxRoomNameLength ??
			DEFAULTS.maxRoomNameLength;
		this.#maxRoomsPerClient = options.maxRoomsPerClient ??
			DEFAULTS.maxRoomsPerClient;
		this.#adapter = options.adapter ?? new SSEPubSubLocal();
		this.#replay = new ReplayBuffer(
			options.replayBufferSize ?? DEFAULTS.replayBufferSize,
		);

		this.#unsubscribeRemote = this.#adapter.onRemote((envelope) => {
			// A peer's message is stamped with *this* instance's sequence, which
			// is exactly why a resume point from another instance is ignored.
			const seq = this.#replay.add(envelope.namespace, envelope.message);
			this.#deliverLocal(envelope.namespace, envelope.message, seq);
		});

		this.#startHeartbeat();
	}

	// ------------------------------------------------------------- public API

	/**
	 * Opens an SSE stream and takes ownership of it.
	 *
	 * @param extraHeaders - merged into the response, so CORS middleware works
	 */
	async handleStream(
		request: Request,
		extraHeaders?: Headers,
	): Promise<Response> {
		const url = new URL(request.url);

		const auth = await this.#resolveAuth(
			request,
			url.searchParams.get(AUTH_PARAM),
		);
		if (!auth.ok) return auth.response;

		const clientId = auth.result.clientId || url.searchParams.get("clientId") ||
			`c_${base36(12)}`;
		const namespace = auth.result.namespace ||
			url.searchParams.get("namespace") || "";
		if (!namespace) {
			return fail(
				ERROR_CODE.BAD_REQUEST,
				"A namespace is required",
				400,
			);
		}

		const requested = (url.searchParams.get("rooms") ?? "")
			.split(",")
			.map((r) => r.trim())
			.filter(Boolean);
		const invalid = this.#validateRooms(requested);
		if (invalid) return fail(ERROR_CODE.BAD_REQUEST, invalid, 400);

		// The browser resends Last-Event-ID as a header on its own reconnect;
		// the query parameter is how a manually rebuilt EventSource carries the
		// resume point, since it cannot set headers. Header wins — it is the
		// fresher of the two.
		const resumeRaw = request.headers.get("last-event-id") ||
			url.searchParams.get(LAST_EVENT_ID_PARAM);
		const resume = parseEventId(resumeRaw);

		const meta = auth.result.meta ?? {};
		let conn: Connection;

		const body = new ReadableStream<Uint8Array>({
			start: (controller) => {
				// Snapshot BEFORE registering. Everything at or below this point
				// is replayed; everything after arrives live. Reversing the two
				// yields a duplicate, and skipping the snapshot yields a gap.
				// Nothing may await between here and the replay below.
				const upTo = this.#replay.currentSeq;

				const existing = this.#connections.get(clientId);
				if (existing) {
					this.logger?.debug?.(`replacing existing stream ${clientId}`);
					this.#drop(existing);
				}

				conn = {
					id: clientId,
					namespace,
					rooms: new Set(requested),
					meta,
					request,
					controller,
					closed: false,
				};
				this.#connections.set(clientId, conn);
				for (const room of requested) this.#indexAdd(room, namespace, clientId);

				// Jitter the retry hint so a fleet does not come back in lockstep.
				this.#writeRaw(conn, `retry: ${this.#jitteredRetry()}\n\n`);

				const usable = !!resume && resume.instanceId === this.instanceId;
				const oldest = this.#replay.oldestSeq;
				const gap = usable &&
					this.#replay.enabled &&
					oldest !== null &&
					resume.seq + 1 < oldest;

				const entries = usable
					? this.#replay.since(
						resume.seq,
						upTo,
						namespace,
						conn.rooms,
					)
					: [];

				const hello: SSEHello = {
					clientId,
					namespace,
					protocol: PROTOCOL_VERSION,
					heartbeatInterval: this.#heartbeatInterval,
					resumed: entries.length > 0,
					gap,
				};
				this.#write(conn, sseFrame(SSE_EVENT.HELLO, hello));

				for (const entry of entries) {
					this.#write(
						conn,
						sseFrame(
							SSE_EVENT.MESSAGE,
							{ ...entry.message, namespace },
							formatEventId(this.instanceId, entry.seq),
						),
					);
				}

				if (entries.length) {
					this.logger?.debug?.(
						`replayed ${entries.length} message(s) to ${clientId}`,
					);
				}

				// The everyday teardown: the peer went away and Deno told us.
				request.signal.addEventListener("abort", () => this.#drop(conn));
			},
			cancel: () => {
				if (conn) this.#drop(conn);
			},
		});

		const headers = new Headers(extraHeaders);
		headers.set("content-type", "text/event-stream; charset=utf-8");
		headers.set("cache-control", "no-cache, no-transform");
		headers.set("connection", "keep-alive");
		// nginx buffers proxied responses by default, which holds every event
		// until the buffer fills — indistinguishable from a broken server.
		headers.set("x-accel-buffering", "no");

		return new Response(body, { headers });
	}

	/** Handles a client operation posted to `/rpc`. */
	async handleRpc(request: Request): Promise<Response> {
		let op: ClientOp;
		try {
			op = await request.json();
		} catch {
			return fail(ERROR_CODE.BAD_REQUEST, "Invalid JSON body", 400);
		}
		if (!op || typeof op !== "object" || typeof op.op !== "string") {
			return fail(ERROR_CODE.BAD_REQUEST, "Missing operation", 400);
		}

		const auth = await this.#resolveAuth(
			request,
			request.headers.get(AUTH_HEADER),
		);
		if (!auth.ok) return auth.response;

		const clientId = auth.result.clientId || op.clientId;
		const namespace = auth.result.namespace || op.namespace;
		if (!clientId) return fail(ERROR_CODE.BAD_REQUEST, "Missing clientId", 400);
		if (!namespace) {
			return fail(ERROR_CODE.BAD_REQUEST, "Missing namespace", 400);
		}

		// A client may only act within its own namespace. Accepting the
		// requested one blindly would make the isolation boundary decorative.
		if (auth.result.namespace && op.namespace && op.namespace !== namespace) {
			return fail(
				ERROR_CODE.FORBIDDEN,
				`Not permitted in namespace "${op.namespace}"`,
				403,
			);
		}

		switch (op.op) {
			case OP.SUB:
			case OP.UNSUB:
				return this.#handleSubscription(op, clientId, namespace);

			case OP.PUB: {
				if (typeof op.room !== "string" || !op.room) {
					return fail(ERROR_CODE.BAD_REQUEST, "Missing room", 400);
				}
				const recipients = await this.publish(
					op.room,
					op.payload,
					namespace,
					clientId,
				);
				return json({ ok: true, recipients });
			}

			case OP.BROADCAST: {
				if (typeof op.room !== "string" || !op.room) {
					return fail(ERROR_CODE.BAD_REQUEST, "Missing room", 400);
				}
				let allowed = false;
				try {
					allowed = this.#allowBroadcast
						? await this.#allowBroadcast({
							clientId,
							namespace,
							meta: auth.result.meta ?? {},
							request,
						}, op.room)
						: false;
				} catch (e) {
					this.logger?.debug?.(`allowBroadcast threw: ${e}`);
					allowed = false;
				}
				if (!allowed) {
					return fail(ERROR_CODE.FORBIDDEN, "Broadcast not permitted", 403);
				}
				const recipients = await this.broadcast(op.room, op.payload, clientId);
				return json({ ok: true, recipients });
			}

			default:
				return fail(
					ERROR_CODE.BAD_REQUEST,
					`Unknown operation "${(op as ClientOp).op}"`,
					400,
				);
		}
	}

	/**
	 * Answers "would you accept me?" without opening a stream.
	 *
	 * `EventSource` closes permanently on any non-200 and hides the status, so
	 * the client cannot tell a `401` from a `503`. This is how it finds out —
	 * and it doubles as a health check.
	 */
	async handlePing(request: Request): Promise<Response> {
		const url = new URL(request.url);
		const auth = await this.#resolveAuth(
			request,
			request.headers.get(AUTH_HEADER) ?? url.searchParams.get(AUTH_PARAM),
		);
		if (!auth.ok) return auth.response;
		return new Response(null, { status: 204 });
	}

	/**
	 * Publishes into a namespace + room from server-side code.
	 *
	 * Delivered messages carry `from: null` by default, which is how clients
	 * distinguish server-injected messages from peer traffic.
	 */
	publish(
		room: string,
		payload: unknown,
		namespace: string,
		from: string | null = null,
	): Promise<number> {
		const message: SSEMessage = {
			room,
			namespace,
			from,
			payload,
			timestamp: Date.now(),
		};
		const seq = this.#replay.add(namespace, message);
		const recipients = this.#deliverLocal(namespace, message, seq);
		return this.#propagate({ namespace, message }, recipients);
	}

	/** Publishes into a room across every namespace. */
	broadcast(
		room: string,
		payload: unknown,
		from: string | null = null,
	): Promise<number> {
		const message: SSEMessage = {
			room,
			// A broadcast has no single namespace; receivers see their own.
			namespace: BROADCAST_NAMESPACE,
			from,
			payload,
			timestamp: Date.now(),
		};
		const seq = this.#replay.add(null, message);
		const recipients = this.#deliverLocal(null, message, seq);
		return this.#propagate({ namespace: null, message }, recipients);
	}

	/** Members of a room within a namespace. */
	members(room: string, namespace: string): string[] {
		return [...(this.#index.get(room)?.get(namespace) ?? [])];
	}

	/** Counts only — never client ids. Safe to expose. */
	stats(): SSEStats {
		const namespaces: Record<string, number> = {};
		for (const conn of this.#connections.values()) {
			namespaces[conn.namespace] = (namespaces[conn.namespace] ?? 0) + 1;
		}
		return {
			instanceId: this.instanceId,
			connections: this.#connections.size,
			rooms: this.#index.size,
			namespaces,
			seq: this.#replay.currentSeq,
			buffered: this.#replay.size,
		};
	}

	/** Closes every stream and releases all timers. */
	async close(): Promise<void> {
		if (this.#closed) return;
		this.#closed = true;

		this.#stopHeartbeat();
		this.#unsubscribeRemote();

		for (const conn of [...this.#connections.values()]) this.#drop(conn);
		this.#connections.clear();
		this.#index.clear();
		this.#replay.clear();

		await this.#adapter.close();
	}

	// -------------------------------------------------------------- internals

	#handleSubscription(
		op: Extract<ClientOp, { op: typeof OP.SUB | typeof OP.UNSUB }>,
		clientId: string,
		namespace: string,
	): Response {
		const rooms = Array.isArray(op.rooms) ? op.rooms : [];
		const invalid = this.#validateRooms(rooms);
		if (invalid) return fail(ERROR_CODE.BAD_REQUEST, invalid, 400);

		const conn = this.#connections.get(clientId);
		if (!conn) {
			// Not an error. Rooms ride the connect URL, so the intent is already
			// recorded client-side and will be applied by the next stream — this
			// is just an optimisation that had nothing to optimise. Failing here
			// would make the client discard a handler over a transient race.
			//
			// Persistent occurrences mean a non-deterministic `verify()`.
			this.logger?.debug?.(`${op.op} for unknown stream ${clientId}`);
			return json({ ok: true });
		}

		if (conn.namespace !== namespace) {
			return fail(
				ERROR_CODE.FORBIDDEN,
				"Namespace does not match the open stream",
				403,
			);
		}

		if (op.op === OP.SUB) {
			if (conn.rooms.size + rooms.length > this.#maxRoomsPerClient) {
				return fail(
					ERROR_CODE.BAD_REQUEST,
					`At most ${this.#maxRoomsPerClient} rooms per client`,
					400,
				);
			}
			for (const room of rooms) {
				conn.rooms.add(room);
				this.#indexAdd(room, namespace, clientId);
			}
		} else {
			for (const room of rooms) {
				if (!conn.rooms.delete(room)) continue;
				this.#indexRemove(room, namespace, clientId);
			}
		}

		return json({ ok: true });
	}

	/** @returns an error message, or `null` when every room is acceptable */
	#validateRooms(rooms: string[]): string | null {
		if (rooms.length > this.#maxRoomsPerClient) {
			return `At most ${this.#maxRoomsPerClient} rooms per client`;
		}
		for (const room of rooms) {
			if (typeof room !== "string" || !room) return "Room names must be non-empty";
			if (room.length > this.#maxRoomNameLength) {
				return `Room names must be at most ${this.#maxRoomNameLength} characters`;
			}
		}
		return null;
	}

	/**
	 * Runs `verify()` and normalises both rejection paths into a 401.
	 *
	 * `raw` is the serialised payload — always JSON, so a bare token that looks
	 * like JSON round-trips as the string it was.
	 */
	async #resolveAuth(
		request: Request,
		raw: string | null,
	): Promise<
		{ ok: true; result: AuthResult } | { ok: false; response: Response }
	> {
		if (!this.#verify) return { ok: true, result: {} };

		let payload: unknown = undefined;
		if (raw !== null) {
			try {
				payload = JSON.parse(raw);
			} catch {
				return {
					ok: false,
					response: fail(
						ERROR_CODE.BAD_REQUEST,
						"Malformed auth payload",
						400,
					),
				};
			}
		}

		let result: AuthResult | null;
		try {
			result = await this.#verify(payload, request);
		} catch (e) {
			this.logger?.debug?.(`verify threw: ${e}`);
			result = null;
		}

		if (result === null) {
			return {
				ok: false,
				response: fail(ERROR_CODE.UNAUTHORIZED, "Authentication failed", 401),
			};
		}
		return { ok: true, result };
	}

	async #propagate(
		envelope: SSEBroadcastEnvelope,
		recipients: number,
	): Promise<number> {
		try {
			await this.#adapter.publish(envelope);
		} catch (e) {
			this.logger?.error?.(`adapter propagation failed: ${e}`);
		}
		return recipients;
	}

	/**
	 * Delivers to local streams.
	 *
	 * @param namespace - target namespace, or `null` to cross all of them
	 * @returns how many streams received it, on this instance
	 */
	#deliverLocal(
		namespace: string | null,
		message: SSEMessage,
		seq: number,
	): number {
		const byNamespace = this.#index.get(message.room);
		if (!byNamespace) return 0;

		const id = formatEventId(this.instanceId, seq);
		let count = 0;

		const deliver = (ids: Set<string>, ns: string) => {
			// Receivers always see the namespace they actually live in, even for
			// a broadcast that originated outside it.
			const frame = sseFrame(SSE_EVENT.MESSAGE, { ...message, namespace: ns }, id);
			for (const clientId of ids) {
				const conn = this.#connections.get(clientId);
				if (!conn) continue;
				this.#write(conn, frame);
				count++;
			}
		};

		if (namespace === null) {
			for (const [ns, ids] of byNamespace) deliver(new Set(ids), ns);
		} else {
			const ids = byNamespace.get(namespace);
			if (ids) deliver(new Set(ids), namespace);
		}

		return count;
	}

	#indexAdd(room: string, namespace: string, clientId: string): void {
		let byNamespace = this.#index.get(room);
		if (!byNamespace) {
			byNamespace = new Map();
			this.#index.set(room, byNamespace);
		}
		let ids = byNamespace.get(namespace);
		if (!ids) {
			ids = new Set();
			byNamespace.set(namespace, ids);
		}
		ids.add(clientId);
	}

	#indexRemove(room: string, namespace: string, clientId: string): void {
		const byNamespace = this.#index.get(room);
		if (!byNamespace) return;
		const ids = byNamespace.get(namespace);
		if (!ids) return;
		ids.delete(clientId);
		if (ids.size === 0) byNamespace.delete(namespace);
		if (byNamespace.size === 0) this.#index.delete(room);
	}

	#drop(conn: Connection): void {
		if (conn.closed) return;
		conn.closed = true;

		// Only drop the registry entry if it still points at *this* stream — a
		// replaced connection must not evict its replacement.
		if (this.#connections.get(conn.id) === conn) {
			this.#connections.delete(conn.id);
		}
		for (const room of conn.rooms) {
			this.#indexRemove(room, conn.namespace, conn.id);
		}
		conn.rooms.clear();

		try {
			conn.controller.close();
		} catch {
			// Already closed by the runtime when the peer vanished.
		}
		this.logger?.debug?.(`stream closed (${conn.id})`);
	}

	#write(conn: Connection, frame: string): void {
		this.#writeRaw(conn, frame);
	}

	#writeRaw(conn: Connection, raw: string): void {
		if (conn.closed) return;
		try {
			conn.controller.enqueue(encoder.encode(raw));
		} catch (e) {
			// Writing to a vanished peer throws. That is the other half of
			// connection cleanup, alongside the abort signal — between them
			// there is nothing left for an idle sweeper to find, which is why
			// this server has none.
			this.logger?.debug?.(`write failed (${conn.id}): ${e}`);
			this.#drop(conn);
		}
	}

	#jitteredRetry(): number {
		const base = this.#retryInterval;
		return Math.round(base * (0.5 + Math.random()));
	}

	#startHeartbeat(): void {
		if (this.#heartbeatInterval <= 0) return;
		this.#ticker = createTicker(this.#heartbeatInterval);
		this.#unsubscribeTicker = this.#ticker.subscribe((timestamp) => {
			// The ticker emits 0 on stop; only real ticks should beat.
			if (!timestamp) return;
			const frame = sseFrame(SSE_EVENT.HEARTBEAT, { timestamp: Date.now() });
			for (const conn of [...this.#connections.values()]) {
				this.#write(conn, frame);
			}
		});
		this.#ticker.start();
	}

	#stopHeartbeat(): void {
		this.#unsubscribeTicker?.();
		this.#unsubscribeTicker = null;
		this.#ticker?.stop();
		this.#ticker = null;
	}
}
