/**
 * Redis-backed cross-instance fan-out.
 *
 * Every instance publishes to — and subscribes on — one shared pub/sub
 * channel. There is deliberately no channel-per-room scheme: the adapter seam
 * has no topic semantics (room and namespace filtering happen in the service
 * after receipt), so finer-grained channels would only add moving parts.
 *
 * Frames on the wire are `{ origin, envelope }`. The `origin` id is what makes
 * an instance's own echo recognisable: Redis pub/sub delivers to *every*
 * subscriber, including the publisher's, and the seam contract makes self-echo
 * suppression the adapter's job — {@link SSEService} does no dedup, and local
 * delivery has already happened by the time `publish()` is called.
 *
 * @module
 */

import { createClog, type Logger } from "@marianmeres/clog";
import { base36 } from "@marianmeres/uid";

import { SSEError } from "../../protocol/errors.ts";
import type { SSEBroadcastEnvelope, SSEPubSubAdapter } from "./abstract.ts";

/**
 * The slice of a Redis client this adapter needs.
 *
 * Deliberately structural, so the package takes no dependency on any concrete
 * Redis library — node-redis v6 (`redis` on npm) satisfies it as-is, and anything
 * else that walks like it will do.
 */
export interface RedisLike {
	/** Whether the underlying connection is open. */
	readonly isOpen: boolean;
	/** Opens the connection. */
	connect(): Promise<unknown>;
	/** Creates a second, unconnected client sharing this one's configuration. */
	duplicate(): RedisLike;
	/** Publishes `message` on `channel`. */
	publish(channel: string, message: string): Promise<unknown>;
	/** Subscribes `listener` to `channel`, putting the client in subscriber mode. */
	subscribe(
		channel: string,
		listener: (message: string, channel: string) => void,
	): Promise<unknown>;
	/** Tears the connection down immediately. */
	destroy(): unknown;
	/**
	 * Event hook, if the client is an event emitter. When present, the adapter
	 * attaches an `"error"` listener to both connections — a node-redis client
	 * emitting a socket error with no listener crashes the whole process, which
	 * would otherwise be the fate of the internally `duplicate()`d subscriber
	 * that no caller can ever reach.
	 */
	on?(event: "error", listener: (err: unknown) => void): unknown;
}

/** Configuration for {@link SSEPubSubRedis}. */
export interface SSEPubSubRedisOptions {
	/**
	 * The Redis client to publish through. **The adapter takes ownership**: it
	 * `duplicate()`s it for the subscriber connection (Redis clients in
	 * subscriber mode cannot issue other commands) and `close()` destroys both.
	 * Hand it a dedicated client, not one shared with the rest of the app.
	 */
	client: RedisLike;
	/**
	 * The pub/sub channel all peer instances share. Default `"sse"`.
	 *
	 * Instances only see each other when this matches, so give each app (and
	 * each test run) its own.
	 */
	channel?: string;
	/** `null` silences. */
	logger?: Logger | null;
}

/** What one instance sends another, as serialised onto the Redis channel. */
interface WireFrame {
	/** The publishing instance's {@link SSEPubSubRedis.origin}. */
	origin: string;
	/** The seam envelope, verbatim. */
	envelope: SSEBroadcastEnvelope;
}

/**
 * Propagates messages between server instances over Redis pub/sub.
 *
 * Lifecycle: construct, `await init()`, then hand it to the service — the
 * service wires `onRemote` synchronously in its constructor, so the adapter
 * must already be listening. `SSEService.close()` calls `close()` for you.
 *
 * ```ts
 * const adapter = new SSEPubSubRedis({ client, channel: "myapp" });
 * await adapter.init();
 * const service = new SSEService({ adapter });
 * ```
 *
 * Delivery is fire-and-forget, at-most-once across instances — the same
 * guarantee the seam documents. A `publish()` rejection is logged and swallowed
 * by the service; reconnect behaviour is whatever the injected client does
 * (node-redis reconnects and resubscribes on its own).
 */
export class SSEPubSubRedis implements SSEPubSubAdapter {
	/** Logger. Assignable — set to `null` to silence. */
	logger: Logger | null;

	/**
	 * This instance's identity on the wire — the `origin` stamped on every frame
	 * it publishes, and the value it skips on receipt.
	 */
	readonly origin: string = base36(12);

	#client: RedisLike;
	#channel: string;
	#subscriber: RedisLike | null = null;
	#sinks = new Set<(envelope: SSEBroadcastEnvelope) => void>();
	#initPromise: Promise<void> | null = null;
	#closed = false;

	/**
	 * Creates the adapter. Nothing connects until `init()`.
	 *
	 * @param options - {@link SSEPubSubRedisOptions}; only `client` is required
	 */
	constructor(options: SSEPubSubRedisOptions) {
		this.#client = options.client;
		this.#channel = options.channel ?? "sse";
		this.logger = options.logger === undefined
			? createClog("sse:redis")
			: options.logger;
	}

	/**
	 * Connects and starts listening. **Await it before constructing the
	 * service** — the seam has no async hook, so readiness is this method's job.
	 *
	 * Idempotent and concurrency-safe: every call shares one underlying attempt.
	 * A failed attempt stays failed (subsequent calls reject the same way) and
	 * has already destroyed whatever it connected — construct a fresh adapter
	 * with a fresh client rather than retrying a broken one.
	 */
	init(): Promise<void> {
		if (this.#closed) {
			return Promise.reject(new SSEError("SSEPubSubRedis is closed"));
		}
		this.#initPromise ??= this.#init();
		return this.#initPromise;
	}

	async #init(): Promise<void> {
		try {
			// A socket error with no listener would crash the process; route both
			// connections' errors into the log instead. node-redis recovers on
			// its own (reconnect + resubscribe) once the error is consumed.
			this.#client.on?.("error", (err) => {
				this.logger?.warn?.(`redis client error: ${err}`);
			});
			if (!this.#client.isOpen) await this.#client.connect();
			if (this.#closed) return; // close() raced us; it cleans up (below)
			// A dedicated connection: Redis clients in subscriber mode cannot
			// issue other commands, so the publisher must stay separate.
			const subscriber = this.#client.duplicate();
			this.#subscriber = subscriber;
			subscriber.on?.("error", (err) => {
				this.logger?.warn?.(`redis subscriber error: ${err}`);
			});
			if (!subscriber.isOpen) await subscriber.connect();
			if (this.#closed) return;
			await subscriber.subscribe(
				this.#channel,
				(message: string) => this.#onWire(message),
			);
			this.logger?.debug?.(
				`redis fan-out up (channel=${this.#channel}, origin=${this.origin})`,
			);
		} catch (e) {
			// Leave nothing half-connected behind: the adapter owns both
			// connections, so a failed init releases them before rethrowing.
			this.#destroyClients();
			throw e;
		}
	}

	/** Hands the envelope to peer instances. Local delivery has already happened. */
	async publish(envelope: SSEBroadcastEnvelope): Promise<void> {
		if (this.#closed) throw new SSEError("SSEPubSubRedis is closed");
		if (!this.#initPromise) {
			throw new SSEError("SSEPubSubRedis is not initialized — await init() first");
		}
		await this.#initPromise;
		const frame: WireFrame = { origin: this.origin, envelope };
		await this.#client.publish(this.#channel, JSON.stringify(frame));
	}

	/** Registers a sink for envelopes arriving from peers. Returns its remover. */
	onRemote(cb: (envelope: SSEBroadcastEnvelope) => void): () => void {
		this.#sinks.add(cb);
		return () => {
			this.#sinks.delete(cb);
		};
	}

	/**
	 * Destroys both connections (the subscriber it created and the client it
	 * was given). Idempotent; called by `SSEService.close()`. Safe to call
	 * while `init()` is still in flight — it waits for the attempt to settle
	 * so nothing can finish materializing after the teardown.
	 */
	async close(): Promise<void> {
		if (this.#closed) return;
		this.#closed = true;
		this.#sinks.clear();
		// Two passes. The first also forces a client stuck in a reconnect loop
		// to give up, so the in-flight init() settles instead of pending
		// forever; the second sweeps up anything that finished materializing
		// between the passes — without it such a connection would leak for the
		// process lifetime, unreachable behind the #closed latch.
		this.#destroyClients();
		await this.#initPromise?.catch(() => {});
		this.#destroyClients();
		this.#subscriber = null;
	}

	#destroyClients(): void {
		for (const client of [this.#subscriber, this.#client]) {
			try {
				if (client?.isOpen) client.destroy();
			} catch (e) {
				this.logger?.warn?.(`error destroying redis client: ${e}`);
			}
		}
	}

	#onWire(raw: string): void {
		let parsed: unknown;
		try {
			parsed = JSON.parse(raw);
		} catch {
			this.logger?.warn?.(`dropping non-JSON frame on "${this.#channel}"`);
			return;
		}

		const frame = parsed as Partial<WireFrame> | null;
		const envelope = frame?.envelope;
		if (
			typeof frame?.origin !== "string" ||
			envelope === null || typeof envelope !== "object" ||
			(envelope.namespace !== null && typeof envelope.namespace !== "string") ||
			envelope.message === null || typeof envelope.message !== "object" ||
			// Without a room the message is undeliverable, yet it would still
			// occupy (and evict) replay-buffer slots on every peer.
			typeof envelope.message.room !== "string"
		) {
			this.logger?.warn?.(`dropping foreign frame on "${this.#channel}"`);
			return;
		}

		// Redis delivered our own publish back to us; local delivery already
		// happened before it ever left, so passing it on would double-deliver.
		if (frame.origin === this.origin) return;

		for (const sink of this.#sinks) {
			try {
				sink(envelope);
			} catch (e) {
				// One misbehaving sink must not starve the others, and nothing
				// may propagate into the redis client's message dispatch.
				this.logger?.error?.(`remote sink threw: ${e}`);
			}
		}
	}
}
