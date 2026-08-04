/**
 * Wire protocol constants shared by client and server.
 *
 * This module is intentionally dependency free — it is the single source of
 * truth both sides import, which is what keeps them from drifting apart.
 *
 * @module
 */

/**
 * Protocol version, announced by the server in its `hello` event.
 *
 * It costs nothing today and it is the only thing that makes a breaking
 * protocol change survivable later.
 */
export const PROTOCOL_VERSION = 1;

/**
 * SSE event names sent by the server.
 *
 * Named events rather than a discriminator inside `data`, because
 * `EventSource` dispatches by name natively — the client is three
 * `addEventListener` calls instead of a parse-and-switch.
 *
 * Note these are *protocol* names and have nothing to do with whatever the
 * application puts inside `payload` — the payload is opaque and is never
 * inspected nor mutated by this library.
 */
export const SSE_EVENT = {
	/** Always first. Confirms the client's final identity. Carries no `id:`. */
	HELLO: "hello",
	/** An application message. The only event carrying an `id:`. */
	MESSAGE: "message",
	/**
	 * Liveness beat.
	 *
	 * A real event, not a `:` comment — `EventSource` does not expose comments
	 * to JavaScript, so a comment heartbeat is invisible to the client and
	 * cannot drive staleness detection.
	 */
	HEARTBEAT: "heartbeat",
} as const;

/** Client operations, carried in the body of `POST {mount}/rpc`. */
export const OP = {
	SUB: "sub",
	UNSUB: "unsub",
	PUB: "pub",
	BROADCAST: "broadcast",
} as const;

/** Machine-readable error codes returned by `/rpc`. */
export const ERROR_CODE = {
	UNAUTHORIZED: "unauthorized",
	FORBIDDEN: "forbidden",
	BAD_REQUEST: "bad_request",
	NOT_FOUND: "not_found",
	INTERNAL: "internal",
} as const;

/**
 * HTTP statuses after which reconnecting is pointless.
 *
 * Everything *not* in this list is retried — including `500` and `503`,
 * because a rolling deploy is exactly when clients must come back.
 *
 * SSE speaks HTTP, so failures are expressed in HTTP statuses rather than the
 * synthetic `4xxx` close-code table `@marianmeres/ws` needs. One vocabulary,
 * visible in devtools and access logs.
 */
export const DEFAULT_TERMINAL_STATUS_CODES: readonly number[] = [401, 403];

/** Header carrying the serialised `auth()` payload on upstream RPC requests. */
export const AUTH_HEADER = "x-sse-auth";

/**
 * Query parameter carrying the serialised `auth()` payload on the stream
 * request, because `EventSource` cannot set headers.
 */
export const AUTH_PARAM = "auth";

/**
 * Query parameter carrying the resume point on a *manual* reconnect.
 *
 * `EventSource`'s own reconnect sends the `Last-Event-ID` header for us; only
 * the reconnects we drive ourselves need this.
 */
export const LAST_EVENT_ID_PARAM = "lastEventId";

/** Namespace stamped on a broadcast, which by definition has no single one. */
export const BROADCAST_NAMESPACE = "*";

/** Separator between instance id and sequence number inside an event `id:`. */
export const EVENT_ID_SEPARATOR = ":";
