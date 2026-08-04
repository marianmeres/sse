/**
 * `@marianmeres/sse` — a Server-Sent Events client with namespaces, rooms,
 * replay and auto-reconnect.
 *
 * The client is dependency-light and runs in browsers, Deno, Node 22+ and Bun —
 * `EventSource` is a global in all of them, so there is no polyfill and no
 * transport dependency.
 *
 * The reference server lives behind a separate entry point,
 * `@marianmeres/sse/server`, so `demino` never lands in a browser bundle.
 *
 * See `COMPARISON.md` for how this differs from `@marianmeres/ws`, and which
 * of the two a given problem wants.
 *
 * @example
 * ```ts
 * import { createSSEClient } from "@marianmeres/sse";
 *
 * const sse = createSSEClient({ url: "/sse", namespace: "org-123" });
 *
 * const unsub = await sse.subscribe("chat", (msg) => {
 *     console.log(msg.from, msg.payload);
 * });
 *
 * await sse.publish("chat", { text: "hello" });
 * ```
 *
 * @module
 */

export {
	createSSEClient,
	SSEClient,
	type SSEClientOptions,
	type SSEConnectionState,
	type SSEEvents,
	type SSEState,
} from "./client/sse-client.ts";

export type { MessageHandler } from "./client/rooms.ts";

export { backoffDelay } from "./client/backoff.ts";

export * from "./protocol/mod.ts";
