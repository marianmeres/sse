/**
 * The Redis adapter against a real Redis — the compile-time proof that
 * node-redis (`redis`) satisfies `RedisLike` structurally, and the runtime proof that
 * fan-out, self-echo suppression and teardown behave on an actual wire.
 *
 * Gated: set `TEST_REDIS_URL` (e.g. `redis://localhost:6379/13`) or the whole
 * file is skipped. Each run uses a uid-suffixed channel, so no flushing and no
 * interference with anything else on the instance.
 */

import assert from "node:assert/strict";
import { env } from "node:process";
import { test } from "node:test";
import { base36 } from "@marianmeres/uid";
import { createClient } from "redis";
import { SSEPubSubRedis } from "../src/adapters/redis.ts";
import { createSSEClient, type SSEClient } from "../src/client/sse-client.ts";
import type { SSEMessage } from "../src/protocol/frames.ts";
import type { SSEBroadcastEnvelope } from "../src/server/mod.ts";
import { sleep, startServer, until } from "./_helpers.ts";

const TEST_REDIS_URL = env.TEST_REDIS_URL ?? "";
const skip = !TEST_REDIS_URL;

function liveAdapter(channel: string): SSEPubSubRedis {
	return new SSEPubSubRedis({
		client: createClient({ url: TEST_REDIS_URL }),
		channel,
		logger: null,
	});
}

function client(
	url: string,
	options: Partial<Parameters<typeof createSSEClient>[0]> = {},
): SSEClient {
	return createSSEClient({
		url,
		namespace: "ns",
		logger: null,
		staleTimeout: 0,
		reconnectDelay: 50,
		reconnectDelayMax: 200,
		...options,
	});
}

test("live redis: a frame crosses adapters and never echoes back", { skip }, async () => {
	const channel = `sse_test_${base36(8)}`;
	const a = liveAdapter(channel);
	const b = liveAdapter(channel);
	await a.init();
	await b.init();
	try {
		const atA: SSEBroadcastEnvelope[] = [];
		const atB: SSEBroadcastEnvelope[] = [];
		a.onRemote((e) => atA.push(e));
		b.onRemote((e) => atB.push(e));

		const sent: SSEBroadcastEnvelope = {
			namespace: "ns",
			message: {
				room: "chat",
				namespace: "ns",
				from: null,
				payload: { v: 1 },
				timestamp: 1,
			},
		};
		await a.publish(sent);

		await until(() => atB.length === 1, "peer received over live redis");
		assert.deepEqual(atB[0], sent, "envelope crosses verbatim");
		await sleep(100);
		assert.deepEqual(atA.length, 0, "own echo is suppressed");
	} finally {
		await a.close();
		await b.close();
	}
});

test("live redis: two services fan out, no double delivery at the origin", {
	skip,
}, async () => {
	const channel = `sse_test_${base36(8)}`;
	const adapterA = liveAdapter(channel);
	const adapterB = liveAdapter(channel);
	await adapterA.init();
	await adapterB.init();

	const a = await startServer({ adapter: adapterA });
	const b = await startServer({ adapter: adapterB });
	const sseA = client(a.url, { clientId: "c-a" });
	const sseB = client(b.url, { clientId: "c-b" });
	try {
		const gotA: SSEMessage[] = [];
		const gotB: SSEMessage[] = [];
		await sseA.connect();
		await sseB.connect();
		await sseA.subscribe("chat", (m) => gotA.push(m));
		await sseB.subscribe("chat", (m) => gotB.push(m));

		const recipients = await a.service.publish("chat", { v: 1 }, "ns");
		assert.deepEqual(recipients, 1, "recipients counts stay instance-local");

		await until(() => gotB.length === 1, "peer instance delivered it");
		assert.deepEqual(gotB[0].payload, { v: 1 });

		await sleep(100);
		assert.deepEqual(gotA.length, 1, "origin delivered exactly once");
	} finally {
		sseA.dispose();
		sseB.dispose();
		// stop() closes each service, which closes its adapter.
		await a.stop();
		await b.stop();
	}
});
