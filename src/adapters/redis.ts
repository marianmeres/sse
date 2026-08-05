/**
 * Redis-backed cross-instance fan-out — `@marianmeres/sse/adapters/redis`.
 *
 * A separate entry point so `@marianmeres/sse/server` stays free of anything
 * Redis-flavoured: import it only when you actually run more than one
 * instance. The adapter is structural over {@link RedisLike}, so this package
 * still depends on no concrete Redis library — bring your own client
 * (`npm:redis` v6 fits as-is). Like `/server`, this entry is JSR-only.
 *
 * @example
 * ```ts
 * import { createClient } from "npm:redis";
 * import { SSEPubSubRedis } from "@marianmeres/sse/adapters/redis";
 * import { createSSEApp } from "@marianmeres/sse/server";
 *
 * const adapter = new SSEPubSubRedis({
 *     // a dedicated client — the adapter takes ownership of it
 *     client: createClient({ url: "redis://localhost:6379" }),
 *     channel: "myapp",
 * });
 * await adapter.init();
 *
 * const { app, service } = createSSEApp("/sse", [], { adapter });
 * Deno.serve(app);
 * ```
 *
 * @module
 */

export {
	type RedisLike,
	SSEPubSubRedis,
	type SSEPubSubRedisOptions,
} from "../server/adapters/redis.ts";
