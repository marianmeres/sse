/**
 * Bounded replay buffer.
 *
 * This is the capability SSE has and WebSocket does not: the browser resends
 * `Last-Event-ID` on every reconnect for free, so a short server-side memory
 * turns an ordinary reconnect from "at-most-once, hope you did not need that
 * message" into "nothing was missed".
 *
 * Deliberately in-memory and instance-local. A shared buffer needs a globally
 * ordered log (Redis Streams or similar), which is a pubsub-adapter concern,
 * not this one. Ids are prefixed with the instance id so a resume landing on a
 * different instance is recognised and safely ignored.
 *
 * @module
 */

import { EVENT_ID_SEPARATOR } from "../protocol/constants.ts";
import type { SSEMessage } from "../protocol/frames.ts";

/** One buffered message plus the sequence number stamped on it. */
export interface ReplayEntry {
	/** Monotonic sequence number, unique within this server instance. */
	seq: number;
	/** Target namespace, or `null` for a cross-namespace broadcast. */
	namespace: string | null;
	/** The message as it was delivered live. */
	message: SSEMessage;
}

/** A parsed `Last-Event-ID`. */
export interface ResumePoint {
	/** The instance that minted the id. A foreign one is ignored. */
	instanceId: string;
	/** Resume *after* this sequence number. */
	seq: number;
}

/**
 * Parses an event id of the form `{instanceId}:{seq}`.
 *
 * @returns `null` for anything malformed — a bad resume point is not an error,
 * it just means starting live.
 */
export function parseEventId(raw: string | null | undefined): ResumePoint | null {
	if (!raw) return null;
	const at = raw.lastIndexOf(EVENT_ID_SEPARATOR);
	if (at <= 0) return null;
	const instanceId = raw.slice(0, at);
	const rawSeq = raw.slice(at + 1);
	// `Number("")` is 0, which would turn a truncated id into "resume from the
	// beginning" and replay the whole buffer.
	if (!/^\d+$/.test(rawSeq)) return null;
	const seq = Number(rawSeq);
	if (!Number.isSafeInteger(seq)) return null;
	return { instanceId, seq };
}

/** Formats an event id. */
export function formatEventId(instanceId: string, seq: number): string {
	return `${instanceId}${EVENT_ID_SEPARATOR}${seq}`;
}

/**
 * A fixed-capacity window over the most recent messages.
 *
 * Backed by a plain array rather than a true circular buffer: at the default
 * capacity of 500 the shift cost is irrelevant, and the straightforward
 * version is the one that stays correct.
 */
export class ReplayBuffer {
	#entries: ReplayEntry[] = [];
	#capacity: number;
	#seq = 0;

	/**
	 * Creates the buffer.
	 *
	 * @param capacity - messages retained; `0` disables replay entirely
	 */
	constructor(capacity: number) {
		this.#capacity = Math.max(0, Math.floor(capacity));
	}

	/** Whether replay is switched on at all. */
	get enabled(): boolean {
		return this.#capacity > 0;
	}

	/** The sequence number of the most recently stamped message. */
	get currentSeq(): number {
		return this.#seq;
	}

	/** How many entries are currently retained. */
	get size(): number {
		return this.#entries.length;
	}

	/**
	 * Sequence number of the oldest retained entry, or `null` when empty.
	 *
	 * A resume point older than this means messages are genuinely gone — the
	 * difference between "resumed" and "resumed with a gap".
	 */
	get oldestSeq(): number | null {
		return this.#entries.length ? this.#entries[0].seq : null;
	}

	/**
	 * Stamps a message with the next sequence number and retains it.
	 *
	 * Always increments the sequence, even when capacity is `0`, so event ids
	 * stay monotonic and meaningful whether or not replay is enabled.
	 */
	add(namespace: string | null, message: SSEMessage): number {
		const seq = ++this.#seq;
		if (!this.enabled) return seq;

		this.#entries.push({ seq, namespace, message });
		const excess = this.#entries.length - this.#capacity;
		if (excess > 0) this.#entries.splice(0, excess);
		return seq;
	}

	/**
	 * Entries in `(after, upTo]` that the given rooms would have received.
	 *
	 * `upTo` is a snapshot taken *before* the connection was registered, so
	 * anything published afterwards is delivered live instead. Without that
	 * bound a message could be both replayed and delivered live.
	 *
	 * A broadcast (`namespace === null`) matches any namespace, exactly as it
	 * did when it was live.
	 */
	since(
		after: number,
		upTo: number,
		namespace: string,
		rooms: ReadonlySet<string>,
	): ReplayEntry[] {
		if (!this.enabled || !rooms.size) return [];
		return this.#entries.filter(
			(entry) =>
				entry.seq > after &&
				entry.seq <= upTo &&
				rooms.has(entry.message.room) &&
				(entry.namespace === null || entry.namespace === namespace),
		);
	}

	/** Drops every retained entry. The sequence counter keeps going. */
	clear(): void {
		this.#entries = [];
	}
}
