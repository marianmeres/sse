/**
 * A fake CI/CD pipeline, so the feed has something to say.
 *
 * This is the shape SSE is actually for: traffic that is overwhelmingly
 * server → client, produced by something that has no idea a browser exists.
 * It calls `service.publish()` and nothing else — no socket, no client, no
 * awareness of who is listening.
 *
 * Everything it emits arrives with `from: null`, which is how a client tells a
 * server push apart from peer traffic.
 *
 * @module
 */

import { createTicker, type Ticker } from "@marianmeres/ticker";
import { base36 } from "@marianmeres/uid";
import type { SSEService } from "../src/server.ts";
import type { EventPayload, Level } from "./shared.ts";

/** One scripted step of a deploy run. */
interface Step {
	room: "deploys" | "alerts";
	level: Level;
	source: string;
	/** `#` is replaced with the build number. */
	text: string;
}

/**
 * A deliberately boring, deliberately repetitive script.
 *
 * Repetition is the point: you need to be able to glance at the feed after a
 * reconnect and tell instantly whether you missed anything.
 */
const SCRIPT: readonly Step[] = [
	{ room: "deploys", level: "info", source: "queue", text: "build #— queued" },
	{ room: "deploys", level: "info", source: "build", text: "installing dependencies" },
	{ room: "deploys", level: "info", source: "test", text: "running tests" },
	{ room: "deploys", level: "success", source: "test", text: "135 passed, 0 failed" },
	{ room: "deploys", level: "info", source: "image", text: "pushing image" },
	{ room: "alerts", level: "warn", source: "canary", text: "p99 latency 412ms" },
	{ room: "deploys", level: "info", source: "rollout", text: "rolling out 3 replicas" },
	{ room: "deploys", level: "success", source: "rollout", text: "build #— is live" },
	{ room: "alerts", level: "error", source: "health", text: "replica-2 failed probe" },
	{ room: "alerts", level: "success", source: "health", text: "replica-2 recovered" },
];

/** Drives the script and publishes into every namespace that has listeners. */
export class Pipeline {
	readonly #service: SSEService;
	readonly #interval: number;
	#ticker: Ticker | null = null;
	#unsubscribe: (() => void) | null = null;
	#at = 0;
	#build = 100;

	constructor(service: SSEService, interval = 2_500) {
		this.#service = service;
		this.#interval = interval;
	}

	start(): void {
		if (this.#ticker) return;
		this.#ticker = createTicker(this.#interval);
		this.#unsubscribe = this.#ticker.subscribe((timestamp) => {
			// The ticker emits 0 on stop; only real ticks should publish.
			if (timestamp) void this.tick();
		});
		this.#ticker.start();
	}

	stop(): void {
		this.#unsubscribe?.();
		this.#unsubscribe = null;
		this.#ticker?.stop();
		this.#ticker = null;
	}

	/** Emits the next scripted step. Exposed so tests and `/api/burst` can drive it. */
	async tick(): Promise<void> {
		const step = SCRIPT[this.#at % SCRIPT.length];
		if (this.#at % SCRIPT.length === 0) this.#build++;
		this.#at++;

		const payload: EventPayload = {
			kind: "event",
			id: `evt-${base36(10)}`,
			level: step.level,
			source: step.source,
			text: step.text.replace("#—", `#${this.#build}`),
		};

		// Publish into every namespace someone is actually watching. A real
		// pipeline knows which project it is building; this one has to ask.
		for (const namespace of this.#namespaces()) {
			await this.#service.publish(step.room, payload, namespace);
		}
	}

	/**
	 * Emits `count` steps back to back.
	 *
	 * Exists so you can deliberately overflow the replay buffer while
	 * disconnected and watch `hello.gap` come back `true`.
	 */
	async burst(count: number): Promise<void> {
		for (let i = 0; i < count; i++) await this.tick();
	}

	/**
	 * Namespaces with at least one open stream, plus whatever was seen before.
	 *
	 * The remembered set matters for the demo: while you are disconnected there
	 * are no live namespaces, and a pipeline that went quiet exactly then would
	 * have nothing to replay when you came back.
	 */
	#seen = new Set<string>();

	#namespaces(): string[] {
		for (const ns of Object.keys(this.#service.stats().namespaces)) {
			this.#seen.add(ns);
		}
		return [...this.#seen];
	}
}
