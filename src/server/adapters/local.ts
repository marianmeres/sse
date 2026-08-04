/**
 * Single-instance adapter.
 *
 * @module
 */

import type { SSEBroadcastEnvelope, SSEPubSubAdapter } from "./abstract.ts";

/**
 * The default adapter: there are no peer instances, so propagation is a no-op
 * and nothing ever arrives from elsewhere.
 *
 * Everything still works — the service delivers locally regardless of adapter.
 * This one simply declines to gossip.
 */
export class SSEPubSubLocal implements SSEPubSubAdapter {
	/** No-op — there are no peers to tell. */
	publish(_envelope: SSEBroadcastEnvelope): Promise<void> {
		// No peers to tell.
		return Promise.resolve();
	}

	/** No-op — nothing ever arrives from elsewhere. Returns a no-op unsubscriber. */
	onRemote(_cb: (envelope: SSEBroadcastEnvelope) => void): () => void {
		return () => {};
	}

	/** No-op — nothing is held. */
	close(): Promise<void> {
		// Nothing held.
		return Promise.resolve();
	}
}
