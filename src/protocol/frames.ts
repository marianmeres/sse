/**
 * Wire definitions.
 *
 * The central rule of this protocol: **the protocol event name and the
 * application message type are separate things.** The protocol owns a small
 * closed set of event names; `payload` is opaque and belongs entirely to the
 * application. Your payload may contain its own `type` field and nothing will
 * collide.
 *
 * @module
 */

import type { OP } from "./constants.ts";

/** Structured error detail returned by `/rpc`. */
export interface SSEErrorInfo {
	/** Machine-readable code — see `ERROR_CODE`. */
	code: string;
	/** Human-readable explanation. Never parse this. */
	message: string;
}

/**
 * A message as delivered to application code.
 *
 * The `data` of a `message` event *is* an `SSEMessage` — there is no
 * translation layer and no divergence between wire names and API names.
 */
export interface SSEMessage<T = unknown> {
	/** Room the message was published to. */
	room: string;
	/**
	 * Namespace the message belongs to.
	 *
	 * Receivers always see the namespace they actually live in, even for a
	 * broadcast that originated outside it.
	 */
	namespace: string;
	/**
	 * Originating client id, or `null` when the message was injected
	 * server-side (via the service API or the HTTP routes).
	 */
	from: string | null;
	/** Opaque application payload. */
	payload: T;
	/** Server-assigned epoch milliseconds. */
	timestamp: number;
}

/**
 * The first event on every stream.
 *
 * Carries the client's *final* identity: `verify()` may have overridden what
 * the client proposed, and this is how the client finds out.
 */
export interface SSEHello {
	/** The id the server settled on — the client's proposal, or a `verify()` override. */
	clientId: string;
	/** The namespace the stream actually lives in. */
	namespace: string;
	/** See `PROTOCOL_VERSION`. */
	protocol: number;
	/**
	 * Server's heartbeat cadence in ms, so the client can size its staleness
	 * deadline against the actual server rather than a guess.
	 */
	heartbeatInterval: number;
	/** Whether anything was replayed from `Last-Event-ID`. */
	resumed: boolean;
	/**
	 * `true` when the resume point had already been evicted from the replay
	 * buffer — messages are genuinely gone and the application should refetch
	 * rather than assume it is up to date.
	 */
	gap: boolean;
}

/** Liveness beat. Carries no `id:`, so it never moves the resume point. */
export interface SSEHeartbeat {
	/** Server-side epoch milliseconds at the moment of the beat. */
	timestamp: number;
}

/** Client operations, posted to `{mount}/rpc`. */
export type ClientOp =
	| { op: typeof OP.SUB; clientId: string; namespace: string; rooms: string[] }
	| { op: typeof OP.UNSUB; clientId: string; namespace: string; rooms: string[] }
	| {
		op: typeof OP.PUB;
		clientId: string;
		namespace: string;
		room: string;
		payload: unknown;
	}
	| {
		op: typeof OP.BROADCAST;
		clientId: string;
		namespace: string;
		room: string;
		payload: unknown;
	};

/** A successful `/rpc` response. */
export interface RpcOk {
	/** Discriminator — always `true` here. */
	ok: true;
	/**
	 * Streams the message was handed to **on this server instance**.
	 *
	 * Best-effort telemetry, never a delivery guarantee — and it stays
	 * instance-local once a distributed adapter is in play. Absent for
	 * `sub`/`unsub`.
	 */
	recipients?: number;
}

/** A failed `/rpc` response. Always paired with a 4xx/5xx status. */
export interface RpcErr {
	/** Discriminator — always `false` here. */
	ok: false;
	/** What went wrong. */
	error: SSEErrorInfo;
}

/** Any `/rpc` response. */
export type RpcResult = RpcOk | RpcErr;

/** Result of a successful `publish()` / `broadcast()`. */
export interface SSEPublishResult {
	/** See {@link RpcOk.recipients} — instance-local telemetry. */
	recipients: number;
}

/**
 * Outcome of the server's `verify()` hook.
 *
 * **`verify()` must be deterministic for a given auth payload.** The stream
 * request and every RPC request run it independently, and `sub`/`unsub`
 * resolve the connection by client id — a hook that returns a different id per
 * call breaks subscription routing.
 */
export interface AuthResult {
	/** Override the client-proposed id. */
	clientId?: string;
	/** Force the connection's namespace, overriding the client's request. */
	namespace?: string;
	/** Arbitrary data to associate with the connection (available to hooks). */
	meta?: Record<string, unknown>;
}
