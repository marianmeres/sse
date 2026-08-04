/**
 * Unbounded-ish feed history, implemented as a **decorating `SSEPubSubAdapter`**.
 *
 * Note carefully what this is *not*: it is not how the example recovers from a
 * reconnect. That is `Last-Event-ID` replay, which the library does for you and
 * which needs no application code at all.
 *
 * This exists for the one case replay cannot cover — `hello.gap === true`, when
 * the client was away longer than the replay window and the messages it missed
 * have been evicted. Then, and only then, the client refetches from here. The
 * two mechanisms are complementary and the split is the honest one:
 *
 *   · **replay** — bounded, automatic, exact, and free. Covers the 99% case:
 *     a blip, a laptop lid, a rolling deploy.
 *   · **history** — the application's own store, as large as it cares to make
 *     it. Covers the long absence, and the initial page load.
 *
 * Why the adapter seam rather than a hook? Because it is the one place every
 * message passes through: `SSEService` calls `adapter.publish(envelope)` after
 * local delivery for *every* publish and broadcast, whoever originated it —
 * a connected client, `service.publish()`, or the HTTP injection route. A real
 * deployment would put Redis or Deno KV here; we tee into a ring buffer and
 * delegate the actual (non-)propagation to the wrapped adapter.
 *
 * Note this is application code inspecting an application payload. The library
 * itself still never looks inside `payload` — see `example/shared.ts`.
 *
 * @module
 */

import {
	type SSEBroadcastEnvelope,
	type SSEPubSubAdapter,
	SSEPubSubLocal,
} from "../src/server.ts";
import type { SSEMessage } from "../src/protocol.ts";
import { HISTORY_LIMIT, isAppPayload } from "./shared.ts";

/** Composite key for the buffer map. `|` cannot appear in a validated namespace. */
const key = (namespace: string, room: string) => `${namespace}|${room}`;

/**
 * Records messages per `(namespace, room)` and forwards everything to the
 * wrapped adapter unchanged.
 */
export class HistoryAdapter implements SSEPubSubAdapter {
	readonly #inner: SSEPubSubAdapter;
	readonly #limit: number;
	readonly #buffers = new Map<string, SSEMessage[]>();

	constructor(inner: SSEPubSubAdapter = new SSEPubSubLocal(), limit = HISTORY_LIMIT) {
		this.#inner = inner;
		this.#limit = limit;
	}

	publish(envelope: SSEBroadcastEnvelope): Promise<void> {
		this.#record(envelope);
		// Delegate rather than resolve: whatever real adapter is wrapped still
		// has to see every envelope, or peer instances go deaf.
		return this.#inner.publish(envelope);
	}

	onRemote(cb: (envelope: SSEBroadcastEnvelope) => void): () => void {
		// Messages arriving *from* peers are history too — a client that
		// reconnects onto this instance should still see them.
		return this.#inner.onRemote((envelope) => {
			this.#record(envelope);
			cb(envelope);
		});
	}

	close(): Promise<void> {
		this.#buffers.clear();
		return this.#inner.close();
	}

	/** Oldest first. Returns a copy, so callers cannot corrupt the buffer. */
	read(namespace: string, room: string): SSEMessage[] {
		return [...(this.#buffers.get(key(namespace, room)) ?? [])];
	}

	#record({ namespace, message }: SSEBroadcastEnvelope): void {
		// Only shapes we understand are worth keeping — an unknown payload is
		// not ours to interpret.
		if (!isAppPayload(message.payload)) return;

		// `namespace: null` means a cross-namespace broadcast, which belongs to
		// no single room history. Broadcasts are live-only, by design.
		if (namespace === null) return;

		const k = key(namespace, message.room);
		const buffer = this.#buffers.get(k) ?? [];
		buffer.push(message);
		if (buffer.length > this.#limit) buffer.splice(0, buffer.length - this.#limit);
		this.#buffers.set(k, buffer);
	}
}
