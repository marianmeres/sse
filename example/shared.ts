/**
 * The **application** protocol — what this feed puts inside `payload`.
 *
 * This file is the whole point of the "protocol type is not application type"
 * rule: `@marianmeres/sse` owns a closed set of event names (`hello`,
 * `message`, `heartbeat`) and treats `payload` as opaque. Everything below is
 * ours, it lives on top, and the library never looks at it.
 *
 * Imported by **both** sides — the server uses it to decide what is worth
 * keeping in history, the client uses it to render. Sharing one module is what
 * keeps the two ends from drifting.
 *
 * @module
 */

/** Rooms the example offers. Purely an app-level list; rooms are created on demand. */
export const ROOMS = ["deploys", "alerts", "chatter"] as const;

/** Room the server's `allowBroadcast` gate permits. See `example/server.ts`. */
export const BROADCAST_ROOM = "alerts";

/** Room joined first. */
export const DEFAULT_ROOM = ROOMS[0];

/** Namespace used when the login form is left empty. */
export const DEFAULT_PROJECT = "acme";

/** How many entries per (namespace, room) the history API keeps. */
export const HISTORY_LIMIT = 200;

/**
 * How many messages the server retains for **replay**.
 *
 * Deliberately tiny. The whole point of the demo is that you can overflow it
 * on purpose and watch `hello.gap` go true — a production default is 500.
 */
export const REPLAY_BUFFER_SIZE = 20;

/** Severity, which is the app's concept and not the library's. */
export type Level = "info" | "success" | "warn" | "error";

/** Something the pipeline (or an injection route) emitted. */
export interface EventPayload {
	kind: "event";
	/** App-level id. Distinct from the SSE event id, which is the resume point. */
	id: string;
	level: Level;
	/** What produced it — a stage name, a service, `deploy-bot`, … */
	source: string;
	text: string;
}

/** A line typed by an operator. The only thing clients ever publish. */
export interface CommentPayload {
	kind: "comment";
	id: string;
	/** Display name. Distinct from the client id, which carries a suffix. */
	who: string;
	text: string;
}

/** Everything this app sends through `publish()` / `broadcast()`. */
export type AppPayload = EventPayload | CommentPayload;

function isRecord(v: unknown): v is Record<string, unknown> {
	return typeof v === "object" && v !== null;
}

/** Narrows an opaque payload to a pipeline event. */
export function isEventPayload(payload: unknown): payload is EventPayload {
	return (
		isRecord(payload) &&
		payload.kind === "event" &&
		typeof payload.text === "string" &&
		typeof payload.source === "string"
	);
}

/** Narrows an opaque payload to an operator comment. */
export function isCommentPayload(payload: unknown): payload is CommentPayload {
	return (
		isRecord(payload) &&
		payload.kind === "comment" &&
		typeof payload.text === "string" &&
		typeof payload.who === "string"
	);
}

/** Either of ours. Anything else is someone's hand-rolled curl payload. */
export function isAppPayload(payload: unknown): payload is AppPayload {
	return isEventPayload(payload) || isCommentPayload(payload);
}

/**
 * Operator name rules.
 *
 * The server enforces these in `verify` (a rejected name is a `401`, which the
 * client treats as terminal — it must not retry). The client applies the same
 * regex before connecting purely so the common typo gets an inline message
 * instead of a round trip.
 */
export const NAME_RE = /^[a-z0-9][a-z0-9_-]{1,15}$/i;

/** Same idea for the project, which becomes the connection's namespace. */
export const PROJECT_RE = /^[a-z0-9][a-z0-9_-]{1,23}$/i;

/**
 * Shape of the per-tab suffix the client asks for, appended to the name to
 * form the client id. Validated server-side like everything else in the auth
 * payload — it is client-supplied data, so it does not get to be trusted.
 */
export const TAB_RE = /^[a-z0-9]{4,8}$/;

/** Names the server refuses, so `from: null` server pushes stay unambiguous. */
export const RESERVED_NAMES: readonly string[] = ["pipeline", "system", "admin"];
