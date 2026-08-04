/**
 * Silent-stream watchdog.
 *
 * A stream that dies without a FIN is *the* defining SSE failure: a proxy drops
 * the connection, no `error` event ever fires, and `EventSource` sits at
 * `OPEN` delivering nothing. Unlike WebSocket there is no ping frame to send —
 * the channel is one-directional — so detection is purely receive-side: the
 * server beats, and silence past a deadline means the stream is dead.
 *
 * This is deliberately simpler than the ws heartbeat, which has to reason about
 * the *oldest unanswered* ping because it owns both directions. Here every
 * inbound byte is proof of life and resets the clock, full stop.
 *
 * @module
 */

import { createTicker, type Ticker } from "@marianmeres/ticker";

/** Configuration for {@link StalenessWatchdog}. */
export interface StalenessWatchdogOptions {
	/** Silence deadline in ms. `0` disables the watchdog entirely. */
	timeout: number;
	/** Nothing arrived in time — the stream is dead but pretending. */
	onStale: () => void;
	/** Clock source, injectable for tests. */
	now?: () => number;
}

/**
 * Watches for a stream that has gone silent.
 *
 * Polls at a quarter of the deadline rather than arming a `setTimeout` per
 * inbound event: a busy stream would otherwise churn thousands of timers a
 * second, and the coarser granularity costs at most 25% extra detection
 * latency.
 */
export class StalenessWatchdog {
	#options: StalenessWatchdogOptions;
	#now: () => number;
	#ticker: Ticker | null = null;
	#unsubscribe: (() => void) | null = null;
	#lastSeen = 0;

	constructor(options: StalenessWatchdogOptions) {
		this.#options = options;
		this.#now = options.now ?? Date.now;
	}

	/** Whether the watchdog is currently running. */
	get running(): boolean {
		return !!this.#ticker;
	}

	start(): void {
		if (this.#options.timeout <= 0 || this.#ticker) return;

		this.#lastSeen = this.#now();
		const step = Math.max(50, Math.floor(this.#options.timeout / 4));

		this.#ticker = createTicker(step);
		this.#unsubscribe = this.#ticker.subscribe((timestamp) => {
			// The ticker emits 0 on stop; only real ticks should check.
			if (!timestamp) return;
			if (this.#now() - this.#lastSeen >= this.#options.timeout) {
				// Stop first: the callback tears the stream down and restarts us,
				// and a watchdog that fires twice would race its own replacement.
				this.stop();
				this.#options.onStale();
			}
		});
		this.#ticker.start();
	}

	stop(): void {
		this.#unsubscribe?.();
		this.#unsubscribe = null;
		this.#ticker?.stop();
		this.#ticker = null;
	}

	/** Any inbound event proves the stream is alive — heartbeats included. */
	alive(): void {
		this.#lastSeen = this.#now();
	}
}
