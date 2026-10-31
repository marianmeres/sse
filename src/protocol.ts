/**
 * `@marianmeres/sse/protocol` — the dependency-free wire definitions.
 *
 * Both the client and the reference server import this module, which is what
 * keeps the two sides from drifting apart. It is exported as its own subpath
 * for anyone implementing this protocol against a different server or client:
 * it pulls in nothing, so it costs nothing.
 *
 * Everything here is also re-exported from the package root, so there is no
 * need to import both.
 *
 * The central rule: **the protocol event name and the application message type
 * are separate things.** The protocol owns a small closed set of event names;
 * `payload` is opaque and belongs entirely to the application.
 *
 * @example
 * ```ts
 * import { OP, SSE_EVENT, type SSEMessage } from "@marianmeres/sse/protocol";
 *
 * // Speak the protocol directly — subscribe without the bundled client.
 * await fetch("/sse/rpc", {
 *     method: "POST",
 *     headers: { "content-type": "application/json" },
 *     body: JSON.stringify({
 *         op: OP.SUB,
 *         clientId: "c_a1b2",
 *         namespace: "org-123",
 *         rooms: ["chat"],
 *     }),
 * });
 *
 * const es = new EventSource("/sse/events?clientId=c_a1b2&namespace=org-123");
 * es.addEventListener(SSE_EVENT.MESSAGE, (ev) => {
 *     const msg: SSEMessage = JSON.parse(ev.data);
 *     console.log(msg.room, msg.payload);
 * });
 * ```
 *
 * @module
 */

// Kept as a top-level file because every subpath export maps to a flat
// `src/{name}.ts` (`dist/{name}.js` on npm); the implementation lives in
// `./protocol/`.
export * from "./protocol/mod.ts";
