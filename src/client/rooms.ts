/**
 * Refcounted room registry.
 *
 * N handlers on one room produce exactly one wire subscription; the `unsub`
 * goes out when the last handler detaches. This is what lets `subscribe()`
 * hand back a plain unsubscriber that is safe to call from component teardown
 * without any coordination between call sites.
 *
 * @module
 */

import type { SSEMessage } from "../protocol/frames.ts";

/** Receives messages published to a room. */
export type MessageHandler<T = unknown> = (msg: SSEMessage<T>) => void;

/** Tracks rooms and their handlers. */
export class RoomRegistry {
	#rooms = new Map<string, Set<MessageHandler>>();

	/** Rooms currently held, in insertion order. */
	get rooms(): string[] {
		return [...this.#rooms.keys()];
	}

	has(room: string): boolean {
		return this.#rooms.has(room);
	}

	/**
	 * Attaches a handler, creating the room entry if needed.
	 *
	 * @returns `true` when the room is new and a `sub` is required.
	 */
	add(room: string, handler: MessageHandler): boolean {
		let handlers = this.#rooms.get(room);
		const created = !handlers;
		if (!handlers) {
			handlers = new Set();
			this.#rooms.set(room, handlers);
		}
		handlers.add(handler);
		return created;
	}

	/**
	 * Detaches a handler.
	 *
	 * @returns `true` when the room became empty and should be unsubscribed.
	 */
	remove(room: string, handler: MessageHandler): boolean {
		const handlers = this.#rooms.get(room);
		if (!handlers) return false;

		handlers.delete(handler);
		if (handlers.size === 0) {
			this.#rooms.delete(room);
			return true;
		}
		return false;
	}

	/** Force-removes a room and every handler attached to it. */
	removeRoom(room: string): boolean {
		return this.#rooms.delete(room);
	}

	/**
	 * Delivers to every handler for the room.
	 *
	 * Handlers are snapshotted first, so a handler that unsubscribes (or
	 * subscribes) during delivery cannot corrupt the in-flight iteration.
	 */
	deliver(room: string, msg: SSEMessage, onError: (e: unknown) => void): void {
		const handlers = this.#rooms.get(room);
		if (!handlers) return;
		for (const handler of [...handlers]) {
			try {
				handler(msg);
			} catch (e) {
				onError(e);
			}
		}
	}

	clear(): void {
		this.#rooms.clear();
	}
}
