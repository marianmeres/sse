/**
 * `@marianmeres/sse/server` — the reference SSE server, a demino app
 * implementing the wire protocol.
 *
 * A demino app, which is why it sits behind its own entry point rather than in
 * the root: `demino` never reaches a browser bundle, and the npm build ships the
 * client alone (this entry is JSR-only). It uses no Deno API — any demino 2.0
 * adapter serves it (`@marianmeres/demino/deno`, or `/node`).
 *
 * `createSSEApp` is the mountable app; `SSEService` underneath it owns every
 * stream, the room index and the replay buffer, and is usable standalone from
 * any `(req) => Response` handler.
 *
 * @example
 * ```ts
 * import { createSSEApp } from "@marianmeres/sse/server";
 *
 * // Your own authentication — whatever turns a token into a user.
 * declare function auth(token?: string): Promise<{ id: string; orgId: string } | null>;
 *
 * const { app, service } = createSSEApp("/sse", [], {
 *     verify: async (payload) => {
 *         const { token } = (payload ?? {}) as { token?: string };
 *         const user = await auth(token);
 *         // null rejects with 401, which the client treats as terminal.
 *         return user ? { clientId: user.id, namespace: user.orgId } : null;
 *     },
 * });
 *
 * // Push to connected clients from anywhere in your app.
 * await service.publish("notifications", { text: "deploy finished" }, "org-123");
 *
 * Deno.serve(app);
 * ```
 *
 * @module
 */

// Kept as a top-level file because the npm build maps subpath exports to
// `src/{name}.ts`; the implementation lives in `./server/`.
export * from "./server/mod.ts";
