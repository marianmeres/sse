/**
 * The SSE client.
 *
 * @module
 */

import { createClog, type Logger } from "@marianmeres/clog";
import { createPubSub, type Subscriber, type Unsubscriber } from "@marianmeres/pubsub";
import { base36 } from "@marianmeres/uid";

import {
	AUTH_HEADER,
	AUTH_PARAM,
	DEFAULT_TERMINAL_STATUS_CODES,
	LAST_EVENT_ID_PARAM,
	OP,
	SSE_EVENT,
} from "../protocol/constants.ts";
import type {
	ClientOp,
	RpcResult,
	SSEHello,
	SSEMessage,
	SSEPublishResult,
} from "../protocol/frames.ts";
import {
	SSEConnectTimeoutError,
	SSEDisposedError,
	SSEError,
	SSERemoteError,
	SSETerminatedError,
	SSETimeoutError,
} from "../protocol/errors.ts";
import { backoffDelay } from "./backoff.ts";
import { type MessageHandler, RoomRegistry } from "./rooms.ts";
import { StalenessWatchdog } from "./staleness.ts";

/**
 * Connection lifecycle states.
 *
 * A plain union rather than a state-machine dependency — the transition table
 * below is the whole of the machinery, and it is small enough to read.
 *
 * There is no `"authenticating"` state, unlike `@marianmeres/ws`: auth here is
 * part of the HTTP request rather than a post-connect handshake, so it has no
 * observable phase of its own.
 */
export type SSEConnectionState =
	| "idle"
	| "connecting"
	| "open"
	| "reconnecting"
	| "terminated"
	| "disposed";

/**
 * Legal transitions.
 *
 * This exists because the reconnect races are where ad-hoc boolean flags rot:
 * an error arriving mid-`connecting`, a `disconnect()` during backoff, a
 * terminal status while a probe is in flight. An illegal transition is a bug
 * in this file, so it logs loudly rather than failing quietly.
 *
 * Note `reconnecting -> open` is legal without passing through `connecting`:
 * when `EventSource` retries on its own we never see the attempt begin, only
 * the `hello` that ends it.
 */
const TRANSITIONS: Record<SSEConnectionState, readonly SSEConnectionState[]> = {
	idle: ["connecting", "disposed"],
	connecting: ["open", "reconnecting", "terminated", "idle", "disposed"],
	open: ["reconnecting", "terminated", "idle", "disposed"],
	reconnecting: ["connecting", "open", "terminated", "idle", "disposed"],
	// A terminal state is still re-entrant via an explicit connect() — the user
	// may have refreshed the credentials that got them rejected.
	terminated: ["connecting", "idle", "disposed"],
	disposed: [],
};

/** Events emitted by {@link SSEClient}. */
export interface SSEEvents {
	/** Stream connected; `hello` has not arrived yet. */
	open: void;
	/** `hello` received — identity confirmed and ready. */
	connected: SSEHello;
	/** Firehose — every message, regardless of room. */
	message: SSEMessage;
	/** Server liveness beat. Mostly useful for diagnostics. */
	heartbeat: { timestamp: number };
	/**
	 * The stream ended. `status` is `null` when the drop produced no HTTP
	 * response to read a status from — an ordinary network interruption.
	 */
	close: { status: number | null; reason: string; willReconnect: boolean };
	/** `delay` is `null` when `EventSource` owns the retry schedule. */
	reconnecting: { attempt: number; delay: number | null };
	/** Gave up — the only non-retrying exit. */
	terminated: { status: number; reason: string };
	/**
	 * Something went wrong but was survivable — a throwing handler, a malformed
	 * frame, a failed reconcile. Never a reason to stop retrying.
	 */
	error: Error;
}

/** Reactive connection state, delivered through the Svelte store contract. */
export interface SSEState {
	/** The raw lifecycle state. */
	state: SSEConnectionState;
	/** Shorthand for `state === "open"`. */
	connected: boolean;
	/** Shorthand for `state === "connecting" || state === "reconnecting"`. */
	connecting: boolean;
	/** Consecutive failed connection attempts; resets to 0 on success. */
	attempt: number;
	/** Most recent error, cleared on a successful connect. */
	lastError: Error | null;
}

/** Configuration for {@link SSEClient}. */
export interface SSEClientOptions<TAuth = unknown> {
	/**
	 * Base endpoint. An absolute `http(s)://` URL, or a path resolved against
	 * `location` in the browser. Default `/sse`.
	 */
	url?: string | URL;
	/**
	 * Isolation boundary. **Required** — there is deliberately no default.
	 *
	 * A shared `"default"` namespace means an app that forgets to set one has
	 * no isolation at all, which is a silent cross-tenant leak. Falling back to
	 * the client id instead makes a two-tab demo mysteriously not work. Being
	 * explicit costs one line and has neither failure mode.
	 */
	namespace: string;
	/**
	 * Preferred client id. Generated when absent.
	 *
	 * The client mints its own so upstream calls work before the stream is
	 * open — which is what removes the need for an outbox entirely. `verify()`
	 * may still override it; `hello` reports the final value.
	 */
	clientId?: string;
	/** Rooms joined on every (re)connect. */
	rooms?: string[];
	/**
	 * Auth payload provider, called before *every* (re)connect and before
	 * every upstream call, so token refresh works without reconfiguring.
	 *
	 * Travels as a query parameter on the stream (`EventSource` cannot set
	 * headers) and as the `x-sse-auth` header upstream. Return `undefined` to
	 * send nothing and rely on cookies instead.
	 */
	auth?: () => TAuth | Promise<TAuth>;
	/** First `subscribe()` starts the connection. Default `true`. */
	autoConnect?: boolean;
	/** Send cookies with the stream and upstream calls. Default `false`. */
	withCredentials?: boolean;
	/** `null` silences. */
	logger?: Logger | null;
	/** Initial backoff for reconnects *we* drive, ms. Default `500`. */
	reconnectDelay?: number;
	/** Backoff ceiling, ms. Default `30_000`. */
	reconnectDelayMax?: number;
	/**
	 * HTTP statuses after which retrying stops. Default `[401, 403]`.
	 *
	 * Everything else is retried, `500` and `503` included — a rolling deploy
	 * is exactly when clients must come back.
	 */
	terminalStatusCodes?: number[];
	/**
	 * Silence deadline in ms. `0` disables. Default `45_000`.
	 *
	 * Guards against the stream that dies without a FIN: `EventSource` stays
	 * at `OPEN` forever and delivers nothing. Should comfortably exceed the
	 * server's `heartbeatInterval`.
	 */
	staleTimeout?: number;
	/** Bounds the first `connect()` await. `0` waits indefinitely. Default `0`. */
	connectTimeout?: number;
	/** Per-request deadline for upstream calls, ms. Default `30_000`. */
	fetchTimeout?: number;
}

const DEFAULTS = {
	url: "/sse",
	reconnectDelay: 500,
	reconnectDelayMax: 30_000,
	staleTimeout: 45_000,
	connectTimeout: 0,
	fetchTimeout: 30_000,
} as const;

/** `EventSource.readyState` values, spelled out so they read at the call site. */
const READY_STATE = { CONNECTING: 0, OPEN: 1, CLOSED: 2 } as const;

/** Idempotent, `using`-compatible unsubscriber. */
function makeUnsubscriber(fn: () => void): Unsubscriber {
	let done = false;
	const u = (() => {
		if (done) return;
		done = true;
		fn();
	}) as Unsubscriber;
	(u as unknown as Record<symbol, unknown>)[Symbol.dispose] = u;
	return u;
}

/**
 * Resolves the configured endpoint to an absolute URL.
 *
 * A bare path needs a base, which only exists in a browser. Failing loudly
 * here beats a `fetch` that mysteriously 404s against the wrong origin.
 */
function resolveUrl(url: string | URL): URL {
	if (url instanceof URL) return new URL(url.href);
	const base = typeof location !== "undefined" ? location.href : undefined;
	try {
		return new URL(url, base);
	} catch {
		throw new SSEError(
			`Cannot resolve "${url}" — outside a browser the url option must be absolute`,
		);
	}
}

/** Joins a base URL path with a sub-path, tolerating a trailing slash. */
function endpoint(base: URL, path: string): URL {
	const u = new URL(base.href);
	u.pathname = `${u.pathname.replace(/\/+$/, "")}/${path}`;
	return u;
}

/**
 * A reconnecting SSE client with namespaces and rooms.
 *
 * @example
 * ```ts
 * const sse = createSSEClient({ url: "/sse", namespace: "org-123" });
 * const unsub = await sse.subscribe("chat", (msg) => console.log(msg.payload));
 * await sse.publish("chat", { text: "hi" });
 * ```
 */
export class SSEClient<TAuth = unknown> {
	#url: URL;
	#namespace: string;
	#clientId: string;
	#auth?: () => TAuth | Promise<TAuth>;
	#autoConnect: boolean;
	#withCredentials: boolean;
	#reconnectDelay: number;
	#reconnectDelayMax: number;
	#terminalStatusCodes: Set<number>;
	#staleTimeout: number;
	#connectTimeout: number;
	#fetchTimeout: number;

	/** Logger. Assignable — set to `null` to silence. */
	logger: Logger | null;

	#bus = createPubSub<Record<string, unknown>>();
	#stateBus = createPubSub<Record<string, unknown>>();
	#rooms = new RoomRegistry();
	#watchdog: StalenessWatchdog;

	#es: EventSource | null = null;
	#state: SSEConnectionState = "idle";
	#attempt = 0;
	#lastError: Error | null = null;
	#lastEventId: string | null = null;
	#hello: SSEHello | null = null;

	/**
	 * Rooms baked into the current connect URL.
	 *
	 * Kept so `hello` can reconcile: a `subscribe()` issued while the stream
	 * was mid-connect missed the URL, and a `unsubscribe()` in the same window
	 * left a room in it that is no longer wanted.
	 */
	#urlRooms: string[] = [];

	/**
	 * Bumped on every stream we open. Every callback captures its value and
	 * bails when it no longer matches — without this a slow `onerror` from a
	 * superseded stream cancels the reconnect that replaced it.
	 */
	#generation = 0;

	#reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	#connectTimer: ReturnType<typeof setTimeout> | undefined;
	#connectDeferred:
		| { promise: Promise<void>; resolve: () => void; reject: (e: Error) => void }
		| null = null;
	/** Set by `disconnect()`/`dispose()` so a trailing error is not retried. */
	#intentional = false;
	#wakeListeners: Array<() => void> = [];

	/**
	 * Creates a client. Nothing connects yet — the first `subscribe()` starts
	 * the stream when `autoConnect` is on, or call `connect()` yourself.
	 *
	 * @param options - {@link SSEClientOptions}; `namespace` is required and
	 * throws {@link SSEError} when absent
	 */
	constructor(options: SSEClientOptions<TAuth>) {
		if (!options?.namespace || typeof options.namespace !== "string") {
			throw new SSEError("The `namespace` option is required");
		}

		this.logger = options.logger === undefined ? createClog("sse") : options.logger;

		this.#url = resolveUrl(options.url ?? DEFAULTS.url);
		this.#namespace = options.namespace;
		this.#clientId = options.clientId || `c_${base36(12)}`;
		this.#auth = options.auth;
		this.#autoConnect = options.autoConnect ?? true;
		this.#withCredentials = !!options.withCredentials;
		this.#reconnectDelay = options.reconnectDelay ?? DEFAULTS.reconnectDelay;
		this.#reconnectDelayMax = options.reconnectDelayMax ??
			DEFAULTS.reconnectDelayMax;
		this.#terminalStatusCodes = new Set(
			options.terminalStatusCodes ?? DEFAULT_TERMINAL_STATUS_CODES,
		);
		this.#staleTimeout = options.staleTimeout ?? DEFAULTS.staleTimeout;
		this.#connectTimeout = options.connectTimeout ?? DEFAULTS.connectTimeout;
		this.#fetchTimeout = options.fetchTimeout ?? DEFAULTS.fetchTimeout;

		this.#watchdog = new StalenessWatchdog({
			timeout: this.#staleTimeout,
			onStale: () => this.#onStale(),
		});

		// Rooms configured up front get a noop handler purely so they hold a
		// refcount: they are meant to stay joined for the client's lifetime,
		// and `unsubscribe()` still drops them explicitly.
		for (const room of options.rooms ?? []) this.#rooms.add(room, () => {});

		this.#addWakeListeners();
	}

	// -------------------------------------------------------------- accessors

	/** Whether the stream is open *and* `hello` has arrived. */
	get connected(): boolean {
		return this.#state === "open";
	}

	/** The current lifecycle state. */
	get connectionState(): SSEConnectionState {
		return this.#state;
	}

	/** The current client id. Reflects any `verify()` override once connected. */
	get clientId(): string {
		return this.#clientId;
	}

	/** The current namespace. Reflects any `verify()` override once connected. */
	get namespace(): string {
		return this.#namespace;
	}

	/** Rooms currently held, in subscription order. */
	get rooms(): string[] {
		return this.#rooms.rooms;
	}

	/** The underlying `EventSource`, or `null` when not streaming. */
	get eventSource(): EventSource | null {
		return this.#es;
	}

	/** The resolved base endpoint. A copy — mutating it changes nothing. */
	get url(): URL {
		return new URL(this.#url.href);
	}

	/** Resume point carried into the next reconnect. */
	get lastEventId(): string | null {
		return this.#lastEventId;
	}

	/**
	 * Reactive state, Svelte-store compatible: the callback fires immediately
	 * with the current value and again on every change.
	 */
	get state(): { subscribe: (cb: Subscriber<SSEState>) => Unsubscriber } {
		return {
			subscribe: (cb: Subscriber<SSEState>) => {
				cb(this.#snapshot());
				return this.#stateBus.subscribe("state", cb as Subscriber);
			},
		};
	}

	/** Debug snapshot. */
	dump(): Record<string, unknown> {
		return {
			url: this.#url.href,
			state: this.#state,
			clientId: this.#clientId,
			namespace: this.#namespace,
			rooms: this.#rooms.rooms,
			attempt: this.#attempt,
			lastEventId: this.#lastEventId,
			resumed: this.#hello?.resumed ?? null,
			gap: this.#hello?.gap ?? null,
		};
	}

	// ----------------------------------------------------------------- events

	/**
	 * Listens for a lifecycle event. See {@link SSEEvents}.
	 *
	 * @returns an idempotent unsubscriber
	 */
	on<K extends keyof SSEEvents>(
		event: K,
		cb: (data: SSEEvents[K]) => void,
	): Unsubscriber {
		return this.#bus.subscribe(event as string, cb as Subscriber);
	}

	/** As {@link SSEClient.on}, but detaches after the first delivery. */
	once<K extends keyof SSEEvents>(
		event: K,
		cb: (data: SSEEvents[K]) => void,
	): Unsubscriber {
		return this.#bus.subscribeOnce(event as string, cb as Subscriber);
	}

	// -------------------------------------------------------------- lifecycle

	/**
	 * Opens the stream.
	 *
	 * Idempotent — concurrent calls share one promise, and it resolves
	 * immediately when already connected. It rejects **only** where retrying
	 * cannot help: {@link SSETerminatedError} (a terminal status) or
	 * {@link SSEConnectTimeoutError} (`connectTimeout` elapsed; retrying
	 * continues in the background). Ordinary network failure never rejects.
	 */
	connect(): Promise<void> {
		this.#assertUsable();
		if (this.connected) return Promise.resolve();
		if (this.#connectDeferred) return this.#connectDeferred.promise;

		let resolve!: () => void;
		let reject!: (e: Error) => void;
		const promise = new Promise<void>((res, rej) => {
			resolve = res;
			reject = rej;
		});
		this.#connectDeferred = { promise, resolve, reject };

		if (this.#connectTimeout > 0) {
			this.#connectTimer = setTimeout(() => {
				this.#settleConnect(new SSEConnectTimeoutError(this.#connectTimeout));
			}, this.#connectTimeout);
		}

		this.#intentional = false;
		this.#attempt = 0;
		this.#openStream();
		return promise;
	}

	/**
	 * Closes the stream but keeps handlers, rooms and identity, so a later
	 * `connect()` picks up where it left off.
	 */
	disconnect(): void {
		if (this.#state === "disposed") return;
		this.logger?.debug?.("disconnect()");
		this.#intentional = true;
		this.#clearTimers();
		this.#closeStream();
		this.#settleConnect(new SSETerminatedError(0, "disconnect() called"));
		this.#setState("idle");
	}

	/**
	 * Terminal teardown: disconnects, then drops every handler, room, timer and
	 * pending promise. The instance is unusable afterwards.
	 */
	dispose(): void {
		if (this.#state === "disposed") return;
		this.logger?.debug?.("dispose()");
		this.disconnect();
		this.#rooms.clear();
		this.#bus.unsubscribeAll();
		this.#stateBus.unsubscribeAll();
		this.#removeWakeListeners();
		this.#setState("disposed");
	}

	// ---------------------------------------------------------- subscriptions

	/**
	 * Subscribes to a room and attaches a handler.
	 *
	 * The handler is attached **synchronously**, before anything goes out, so
	 * nothing arriving between the request and its acknowledgement is lost.
	 *
	 * Rooms are refcounted: N handlers produce one wire subscription, and the
	 * returned unsubscriber detaches this handler — unsubscribing only when it
	 * was the last one.
	 *
	 * When connected this awaits the server's answer, so a refused
	 * subscription rejects here. When not connected it resolves once
	 * registered: rooms ride the connect URL, so the subscription is
	 * established by the stream request itself.
	 */
	async subscribe<T = unknown>(
		room: string,
		handler: MessageHandler<T>,
	): Promise<Unsubscriber> {
		this.#assertUsable();

		const messageHandler = handler as MessageHandler;
		const created = this.#rooms.add(room, messageHandler);

		const unsubscriber = makeUnsubscriber(() => {
			const emptied = this.#rooms.remove(room, messageHandler);
			if (emptied && this.connected) {
				void this.#rpc({
					op: OP.UNSUB,
					clientId: this.#clientId,
					namespace: this.#namespace,
					rooms: [room],
				}).catch((e) => this.#fail(e, "unsubscribe"));
			}
		});

		if (this.#autoConnect) this.#ensureStarted();

		if (created && this.connected) {
			try {
				await this.#rpc({
					op: OP.SUB,
					clientId: this.#clientId,
					namespace: this.#namespace,
					rooms: [room],
				});
			} catch (e) {
				// Keep local state honest: if the server refused, we are not
				// subscribed, and pretending otherwise would silently swallow
				// every message for this room.
				this.#rooms.remove(room, messageHandler);
				throw e;
			}
		}

		return unsubscriber;
	}

	/** Drops a room and every handler attached to it. */
	async unsubscribe(room: string): Promise<void> {
		this.#assertUsable();
		if (!this.#rooms.removeRoom(room)) return;
		if (!this.connected) return;
		await this.#rpc({
			op: OP.UNSUB,
			clientId: this.#clientId,
			namespace: this.#namespace,
			rooms: [room],
		});
	}

	/** Whether at least one handler is attached to the room. */
	isSubscribed(room: string): boolean {
		return this.#rooms.has(room);
	}

	// --------------------------------------------------------------- outbound

	/**
	 * Publishes into a room within this client's namespace.
	 *
	 * Note this does **not** open the stream, even with `autoConnect`. Upstream
	 * calls are ordinary HTTP requests with no dependence on the stream, so a
	 * publish-only client never pays for one.
	 */
	async publish<T = unknown>(
		room: string,
		payload: T,
	): Promise<SSEPublishResult> {
		this.#assertUsable();
		const result = await this.#rpc({
			op: OP.PUB,
			clientId: this.#clientId,
			namespace: this.#namespace,
			room,
			payload,
		});
		return { recipients: result.recipients ?? 0 };
	}

	/**
	 * Publishes into a room across every namespace.
	 *
	 * A separate method rather than a flag on `publish()` precisely because
	 * crossing an isolation boundary deserves its own name and its own
	 * server-side check — `allowBroadcast` **denies by default**.
	 */
	async broadcast<T = unknown>(
		room: string,
		payload: T,
	): Promise<SSEPublishResult> {
		this.#assertUsable();
		const result = await this.#rpc({
			op: OP.BROADCAST,
			clientId: this.#clientId,
			namespace: this.#namespace,
			room,
			payload,
		});
		return { recipients: result.recipients ?? 0 };
	}

	// ------------------------------------------------------------ the stream

	#ensureStarted(): void {
		if (
			this.#state === "idle" || this.#state === "terminated"
		) {
			void this.connect().catch(() => {
				// connect()'s rejection is already surfaced as an `error` event
				// and to whoever awaited it; an unhandled rejection here would
				// just be noise.
			});
		}
	}

	#openStream(): void {
		if (!this.#setState("connecting")) return;

		const generation = ++this.#generation;
		void this.#buildStreamUrl().then((url) => {
			if (generation !== this.#generation || this.#state === "disposed") return;

			this.logger?.debug?.(`opening stream (attempt ${this.#attempt})`);
			let es: EventSource;
			try {
				es = new EventSource(url.href, {
					withCredentials: this.#withCredentials,
				});
			} catch (e) {
				this.#fail(e, "EventSource construction");
				this.#scheduleReconnect();
				return;
			}
			this.#es = es;

			es.onopen = () => {
				if (generation !== this.#generation) return;
				this.logger?.debug?.("stream open");
				this.#emit("open", undefined);
				this.#watchdog.start();
			};

			es.onerror = () => {
				if (generation !== this.#generation) return;
				this.#onStreamError(es);
			};

			es.addEventListener(SSE_EVENT.HELLO, (ev) => {
				if (generation !== this.#generation) return;
				this.#watchdog.alive();
				this.#onHello(ev as MessageEvent);
			});

			es.addEventListener(SSE_EVENT.MESSAGE, (ev) => {
				if (generation !== this.#generation) return;
				this.#watchdog.alive();
				this.#onMessage(ev as MessageEvent);
			});

			es.addEventListener(SSE_EVENT.HEARTBEAT, (ev) => {
				if (generation !== this.#generation) return;
				this.#watchdog.alive();
				try {
					this.#emit("heartbeat", JSON.parse((ev as MessageEvent).data));
				} catch {
					// A malformed heartbeat still proves liveness, which is the
					// only thing it is for.
				}
			});
		}).catch((e) => {
			if (generation !== this.#generation) return;
			// Almost always a throwing auth() — retrying is right, the token
			// provider may simply be briefly unavailable.
			this.#fail(e, "stream setup");
			this.#scheduleReconnect();
		});
	}

	async #buildStreamUrl(): Promise<URL> {
		const url = endpoint(this.#url, "events");
		url.searchParams.set("clientId", this.#clientId);
		url.searchParams.set("namespace", this.#namespace);

		this.#urlRooms = this.#rooms.rooms;
		if (this.#urlRooms.length) {
			url.searchParams.set("rooms", this.#urlRooms.join(","));
		}

		const auth = await this.#authValue();
		if (auth !== null) url.searchParams.set(AUTH_PARAM, auth);

		// EventSource's own reconnect resends Last-Event-ID as a header, which
		// the server prefers. This parameter is for the reconnects we drive,
		// where a brand new EventSource has no memory of the resume point.
		if (this.#lastEventId) {
			url.searchParams.set(LAST_EVENT_ID_PARAM, this.#lastEventId);
		}

		return url;
	}

	#onHello(ev: MessageEvent): void {
		let hello: SSEHello;
		try {
			hello = JSON.parse(ev.data);
		} catch (e) {
			this.#fail(e, "malformed hello");
			return;
		}

		this.#hello = hello;
		this.#clientId = hello.clientId;
		this.#namespace = hello.namespace;
		this.#attempt = 0;
		this.#lastError = null;

		if (hello.gap) {
			this.logger?.warn?.(
				"resume point expired — messages were missed, refetch if that matters",
			);
		}

		if (!this.#setState("open")) return;
		this.#clearTimers();
		this.#settleConnect(null);
		this.logger?.debug?.(
			`connected as ${hello.clientId} in "${hello.namespace}"` +
				(hello.resumed ? " (resumed)" : ""),
		);
		this.#emit("connected", hello);

		this.#reconcileRooms();
	}

	/**
	 * Aligns server-side subscriptions with local ones after a connect.
	 *
	 * Rooms normally ride the connect URL, but a `subscribe()` or
	 * `unsubscribe()` landing while the stream was mid-connect missed that
	 * window. Both sets are usually identical and this does nothing.
	 */
	#reconcileRooms(): void {
		const local = new Set(this.#rooms.rooms);
		const sent = new Set(this.#urlRooms);

		const missing = [...local].filter((r) => !sent.has(r));
		const extra = [...sent].filter((r) => !local.has(r));

		if (missing.length) {
			void this.#rpc({
				op: OP.SUB,
				clientId: this.#clientId,
				namespace: this.#namespace,
				rooms: missing,
			}).catch((e) => this.#fail(e, "reconcile subscribe"));
		}
		if (extra.length) {
			void this.#rpc({
				op: OP.UNSUB,
				clientId: this.#clientId,
				namespace: this.#namespace,
				rooms: extra,
			}).catch((e) => this.#fail(e, "reconcile unsubscribe"));
		}
		this.#urlRooms = [...local];
	}

	#onMessage(ev: MessageEvent): void {
		if (ev.lastEventId) this.#lastEventId = ev.lastEventId;

		let message: SSEMessage;
		try {
			message = JSON.parse(ev.data);
		} catch (e) {
			this.#fail(e, "malformed message");
			return;
		}

		this.#emit("message", message);
		this.#rooms.deliver(
			message.room,
			message,
			(e) => this.#fail(e, `handler for room "${message.room}"`),
		);
	}

	/**
	 * The one place `EventSource`'s built-in behaviour is overridden.
	 *
	 * `onerror` carries no status, but `readyState` distinguishes the two cases
	 * the spec defines: `CONNECTING` means it is retrying on its own (an
	 * ordinary drop), `CLOSED` means it gave up permanently — which happens on
	 * a non-200 response or a wrong content type. Only the second needs us.
	 */
	#onStreamError(es: EventSource): void {
		if (this.#intentional || this.#state === "disposed") return;

		const wasOpen = this.#state === "open";
		this.#watchdog.stop();

		if (es.readyState === READY_STATE.CONNECTING) {
			this.#attempt++;
			if (wasOpen) {
				this.#emit("close", {
					status: null,
					reason: "stream interrupted",
					willReconnect: true,
				});
			}
			if (this.#setState("reconnecting")) {
				this.logger?.debug?.(
					`stream dropped, EventSource retrying (attempt ${this.#attempt})`,
				);
				this.#emit("reconnecting", { attempt: this.#attempt, delay: null });
			}
			return;
		}

		// CLOSED — EventSource will not retry. Find out whether we should.
		this.#closeStream();
		this.#attempt++;
		void this.#probe().then((status) => {
			if (this.#intentional || this.#state === "disposed") return;

			const terminal = status !== null && this.#terminalStatusCodes.has(status);
			const reason = terminal
				? `rejected with ${status}`
				: `stream failed${status === null ? "" : ` with ${status}`}`;

			this.#emit("close", { status, reason, willReconnect: !terminal });

			if (terminal) return this.#terminate(status, reason);
			this.#scheduleReconnect();
		});
	}

	/**
	 * Asks the server *why* the stream was refused.
	 *
	 * `EventSource` closes permanently on any non-200, so a 503 from a rolling
	 * deploy looks identical to a 401. `/ping` runs the same `verify()` and
	 * costs nothing, which is the whole reason it exists.
	 *
	 * @returns the status, or `null` when even the probe could not get through
	 */
	async #probe(): Promise<number | null> {
		try {
			const url = endpoint(this.#url, "ping");
			const auth = await this.#authValue();
			if (auth !== null) url.searchParams.set(AUTH_PARAM, auth);

			const res = await this.#fetch(url, { method: "GET" });
			return res.status;
		} catch (e) {
			this.logger?.debug?.(`probe failed: ${e}`);
			// Could not reach the server at all — that is a network problem, and
			// network problems are always worth retrying.
			return null;
		}
	}

	#onStale(): void {
		if (this.#state !== "open" && this.#state !== "connecting") return;
		this.logger?.warn?.(
			`no traffic for ${this.#staleTimeout}ms — assuming a dead stream`,
		);
		this.#closeStream();
		this.#attempt++;
		this.#emit("close", {
			status: null,
			reason: "stream went silent",
			willReconnect: true,
		});
		this.#scheduleReconnect();
	}

	#scheduleReconnect(): void {
		if (this.#intentional || this.#state === "disposed") return;
		if (!this.#setState("reconnecting")) return;

		const delay = backoffDelay(
			this.#attempt,
			this.#reconnectDelay,
			this.#reconnectDelayMax,
		);
		this.logger?.debug?.(
			`reconnecting in ${delay}ms (attempt ${this.#attempt})`,
		);
		this.#emit("reconnecting", { attempt: this.#attempt, delay });

		clearTimeout(this.#reconnectTimer);
		this.#reconnectTimer = setTimeout(() => this.#openStream(), delay);
	}

	#terminate(status: number, reason: string): void {
		this.logger?.error?.(`giving up: ${reason}`);
		this.#clearTimers();
		this.#closeStream();
		this.#setState("terminated");
		const error = new SSETerminatedError(status, reason);
		this.#lastError = error;
		this.#settleConnect(error);
		this.#emit("terminated", { status, reason });
	}

	#closeStream(): void {
		this.#watchdog.stop();
		// Bumping the generation orphans every callback still attached to the
		// old stream, including ones already queued on the microtask queue.
		this.#generation++;
		const es = this.#es;
		this.#es = null;
		try {
			es?.close();
		} catch {
			// Already gone.
		}
	}

	// -------------------------------------------------------------- upstream

	async #rpc(op: ClientOp): Promise<{ recipients?: number }> {
		const url = endpoint(this.#url, "rpc");
		const auth = await this.#authValue();

		const headers: Record<string, string> = {
			"content-type": "application/json",
		};
		if (auth !== null) headers[AUTH_HEADER] = auth;

		const res = await this.#fetch(url, {
			method: "POST",
			headers,
			body: JSON.stringify(op),
		});

		let body: RpcResult | null = null;
		try {
			body = await res.json();
		} catch {
			// Handled below — a non-JSON body from a 5xx is normal enough.
		}

		if (!res.ok || !body || body.ok !== true) {
			const info = (body && body.ok === false && body.error) || {
				code: "internal",
				message: `Request failed with ${res.status}`,
			};
			throw new SSERemoteError(info, res.status);
		}

		return body;
	}

	async #fetch(url: URL, init: RequestInit): Promise<Response> {
		const controller = new AbortController();
		const timer = setTimeout(() => controller.abort(), this.#fetchTimeout);
		try {
			return await fetch(url.href, {
				...init,
				signal: controller.signal,
				credentials: this.#withCredentials ? "include" : "same-origin",
			});
		} catch (e) {
			if (controller.signal.aborted) {
				throw new SSETimeoutError(this.#fetchTimeout);
			}
			throw e;
		} finally {
			clearTimeout(timer);
		}
	}

	/**
	 * Serialises the `auth()` payload.
	 *
	 * Always JSON, including for plain strings, so the server can always
	 * `JSON.parse` — a bare token that happens to look like JSON (`"123"`)
	 * would otherwise arrive as a number.
	 */
	async #authValue(): Promise<string | null> {
		if (!this.#auth) return null;
		const value = await this.#auth();
		if (value === undefined) return null;
		return JSON.stringify(value);
	}

	// -------------------------------------------------------------- internals

	/**
	 * Retries immediately when the environment says the network is back.
	 *
	 * A tab that slept through the whole backoff should not then sit out the
	 * remainder of a 30s timer with a perfectly good connection available.
	 */
	#addWakeListeners(): void {
		if (typeof globalThis.addEventListener !== "function") return;

		// Reached through globalThis so this file needs no DOM lib — it has to
		// type-check for Deno and Node just as much as for a browser.
		const doc = (globalThis as { document?: { hidden?: boolean } }).document;

		const wake = () => {
			if (this.#state !== "reconnecting") return;
			if (doc?.hidden) return;
			this.logger?.debug?.("woke up — retrying now");
			clearTimeout(this.#reconnectTimer);
			this.#openStream();
		};

		for (const event of ["online", "visibilitychange", "focus"]) {
			globalThis.addEventListener(event, wake);
			this.#wakeListeners.push(() => globalThis.removeEventListener(event, wake));
		}
	}

	#removeWakeListeners(): void {
		for (const remove of this.#wakeListeners) remove();
		this.#wakeListeners = [];
	}

	#clearTimers(): void {
		clearTimeout(this.#reconnectTimer);
		this.#reconnectTimer = undefined;
		clearTimeout(this.#connectTimer);
		this.#connectTimer = undefined;
	}

	#settleConnect(error: Error | null): void {
		const deferred = this.#connectDeferred;
		if (!deferred) return;
		this.#connectDeferred = null;
		clearTimeout(this.#connectTimer);
		this.#connectTimer = undefined;
		if (error) deferred.reject(error);
		else deferred.resolve();
	}

	#assertUsable(): void {
		if (this.#state === "disposed") throw new SSEDisposedError();
	}

	#setState(next: SSEConnectionState): boolean {
		if (this.#state === next) return true;
		if (!TRANSITIONS[this.#state].includes(next)) {
			this.logger?.warn?.(
				`ignoring illegal state transition ${this.#state} -> ${next}`,
			);
			return false;
		}
		this.#state = next;
		this.#publishState();
		return true;
	}

	#snapshot(): SSEState {
		return {
			state: this.#state,
			connected: this.#state === "open",
			connecting: this.#state === "connecting" ||
				this.#state === "reconnecting",
			attempt: this.#attempt,
			lastError: this.#lastError,
		};
	}

	#publishState(): void {
		this.#stateBus.publish("state", this.#snapshot());
	}

	#emit<K extends keyof SSEEvents>(event: K, data: SSEEvents[K]): void {
		this.#bus.publish(event as string, data);
	}

	#fail(error: unknown, context: string): void {
		const err = error instanceof Error ? error : new SSEError(String(error));
		this.#lastError = err;
		this.logger?.error?.(`${context}: ${err.message}`);
		this.#emit("error", err);
	}
}

/**
 * Creates an {@link SSEClient}.
 *
 * Both this and the class are exported, following the `PubSub` /
 * `createPubSub` precedent in `@marianmeres/pubsub`.
 */
export function createSSEClient<TAuth = unknown>(
	options: SSEClientOptions<TAuth>,
): SSEClient<TAuth> {
	return new SSEClient<TAuth>(options);
}
