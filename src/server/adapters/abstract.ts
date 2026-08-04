/**
 * Cross-instance fan-out seam.
 *
 * The v1 server is in-memory and single-instance. This interface exists so a
 * Redis or Deno-KV adapter can drop in later without a breaking change, and it
 * is deliberately shaped like the adapters in `@marianmeres/stack-sse` so
 * extracting those is mechanical rather than a rewrite.
 *
 * Note the division of labour: **local delivery is the service's job**, always.
 * An adapter only propagates to *peer* instances and receives what peers send.
 * That is why `recipients` counts are instance-local and documented as
 * best-effort telemetry rather than a delivery guarantee.
 *
 * The same boundary is why replay is instance-local: a peer's message enters
 * this instance's replay buffer with this instance's sequence numbers, so a
 * client that resumes elsewhere is correctly told to start live instead.
 *
 * @module
 */

import type { SSEMessage } from "../../protocol/frames.ts";

/** A message crossing the instance boundary. */
export interface SSEBroadcastEnvelope {
	/** Target namespace, or `null` for a cross-namespace broadcast. */
	namespace: string | null;
	/** The message itself, exactly as local subscribers received it. */
	message: SSEMessage;
}

/** Propagates messages between server instances. */
export interface SSEPubSubAdapter {
	/** Hand off to peer instances. Local delivery has already happened. */
	publish(envelope: SSEBroadcastEnvelope): Promise<void>;
	/** Register the sink for messages arriving from peers. */
	onRemote(cb: (envelope: SSEBroadcastEnvelope) => void): () => void;
	/** Release whatever the adapter holds. Called by `SSEService.close()`. */
	close(): Promise<void>;
}
