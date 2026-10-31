import assert from "node:assert/strict";
import { test } from "node:test";

import { backoffDelay } from "../src/client/backoff.ts";
import { RoomRegistry } from "../src/client/rooms.ts";
import { StalenessWatchdog } from "../src/client/staleness.ts";
import { createSSEClient, SSEClient } from "../src/client/sse-client.ts";
import { SSEError } from "../src/protocol/errors.ts";
import type { SSEMessage } from "../src/protocol/frames.ts";
import { formatEventId, parseEventId, ReplayBuffer } from "../src/server/replay.ts";
import { sseFrame } from "../src/server/service.ts";
import { sleep } from "./_helpers.ts";

function msg(room: string, namespace = "ns", payload: unknown = {}): SSEMessage {
	return { room, namespace, from: null, payload, timestamp: 0 };
}

// ---------------------------------------------------------------- backoff

test("backoffDelay stays within [d/2, d]", () => {
	for (let attempt = 1; attempt <= 12; attempt++) {
		const full = Math.min(30_000, 500 * Math.pow(2, attempt - 1));
		for (let i = 0; i < 50; i++) {
			const delay = backoffDelay(attempt, 500, 30_000);
			assert.ok(delay >= Math.floor(full / 2));
			assert.ok(delay <= full);
		}
	}
});

test("backoffDelay honours the ceiling", () => {
	assert.deepEqual(backoffDelay(50, 500, 30_000, () => 1), 30_000);
	assert.deepEqual(backoffDelay(50, 500, 30_000, () => 0), 15_000);
});

test("backoffDelay treats attempt < 1 as the first attempt", () => {
	assert.deepEqual(backoffDelay(0, 1_000, 30_000, () => 0), 500);
	assert.deepEqual(backoffDelay(-5, 1_000, 30_000, () => 0), 500);
});

test("backoffDelay grows exponentially", () => {
	const rnd = () => 1;
	assert.deepEqual(backoffDelay(1, 100, 999_999, rnd), 100);
	assert.deepEqual(backoffDelay(2, 100, 999_999, rnd), 200);
	assert.deepEqual(backoffDelay(3, 100, 999_999, rnd), 400);
});

// ------------------------------------------------------------ event ids

test("event ids round trip", () => {
	assert.deepEqual(formatEventId("srv7", 42), "srv7:42");
	assert.deepEqual(parseEventId("srv7:42"), { instanceId: "srv7", seq: 42 });
});

test("malformed event ids parse to null rather than throwing", () => {
	for (
		const bad of [null, undefined, "", "42", ":42", "srv7:", "srv7:abc", "srv7:-1"]
	) {
		assert.deepEqual(parseEventId(bad), null, `expected null for ${JSON.stringify(bad)}`);
	}
});

// --------------------------------------------------------- replay buffer

test("ReplayBuffer stamps a monotonic sequence", () => {
	const buffer = new ReplayBuffer(10);
	assert.deepEqual(buffer.add("ns", msg("a")), 1);
	assert.deepEqual(buffer.add("ns", msg("a")), 2);
	assert.deepEqual(buffer.currentSeq, 2);
});

test("ReplayBuffer keeps stamping when replay is disabled", () => {
	// Ids must stay meaningful even with the buffer off, so replay can be
	// switched on later without the sequence restarting.
	const buffer = new ReplayBuffer(0);
	assert.deepEqual(buffer.enabled, false);
	assert.deepEqual(buffer.add("ns", msg("a")), 1);
	assert.deepEqual(buffer.add("ns", msg("a")), 2);
	assert.deepEqual(buffer.size, 0);
	assert.deepEqual(buffer.since(0, 2, "ns", new Set(["a"])), []);
});

test("ReplayBuffer evicts oldest beyond capacity", () => {
	const buffer = new ReplayBuffer(3);
	for (let i = 0; i < 5; i++) buffer.add("ns", msg("a"));
	assert.deepEqual(buffer.size, 3);
	assert.deepEqual(buffer.oldestSeq, 3);
	assert.deepEqual(buffer.currentSeq, 5);
});

test("ReplayBuffer.oldestSeq is null when empty", () => {
	assert.deepEqual(new ReplayBuffer(3).oldestSeq, null);
});

test("ReplayBuffer.since filters by room", () => {
	const buffer = new ReplayBuffer(10);
	buffer.add("ns", msg("a"));
	buffer.add("ns", msg("b"));
	buffer.add("ns", msg("a"));

	const got = buffer.since(0, 3, "ns", new Set(["a"]));
	assert.deepEqual(got.map((e) => e.seq), [1, 3]);
});

test("ReplayBuffer.since filters by namespace", () => {
	const buffer = new ReplayBuffer(10);
	buffer.add("ns1", msg("a", "ns1"));
	buffer.add("ns2", msg("a", "ns2"));

	assert.deepEqual(buffer.since(0, 2, "ns1", new Set(["a"])).map((e) => e.seq), [1]);
	assert.deepEqual(buffer.since(0, 2, "ns2", new Set(["a"])).map((e) => e.seq), [2]);
});

test("ReplayBuffer.since replays broadcasts into any namespace", () => {
	const buffer = new ReplayBuffer(10);
	buffer.add(null, msg("a", "*"));
	assert.deepEqual(buffer.since(0, 1, "anything", new Set(["a"])).length, 1);
});

test("ReplayBuffer.since respects the upTo snapshot", () => {
	// The bound is what prevents a message being both replayed and delivered
	// live: anything past the snapshot is the live stream's job.
	const buffer = new ReplayBuffer(10);
	buffer.add("ns", msg("a"));
	buffer.add("ns", msg("a"));
	buffer.add("ns", msg("a"));

	assert.deepEqual(buffer.since(0, 2, "ns", new Set(["a"])).map((e) => e.seq), [1, 2]);
});

test("ReplayBuffer.since excludes the resume point itself", () => {
	const buffer = new ReplayBuffer(10);
	buffer.add("ns", msg("a"));
	buffer.add("ns", msg("a"));
	assert.deepEqual(buffer.since(1, 2, "ns", new Set(["a"])).map((e) => e.seq), [2]);
});

test("ReplayBuffer.since returns nothing for a client with no rooms", () => {
	const buffer = new ReplayBuffer(10);
	buffer.add("ns", msg("a"));
	assert.deepEqual(buffer.since(0, 1, "ns", new Set()), []);
});

// ------------------------------------------------------- frame encoding

test("sseFrame emits event and data", () => {
	assert.deepEqual(sseFrame("hello", { a: 1 }), 'event: hello\ndata: {"a":1}\n\n');
});

test("sseFrame emits id only when given", () => {
	assert.deepEqual(
		sseFrame("message", { a: 1 }, "srv:7"),
		'id: srv:7\nevent: message\ndata: {"a":1}\n\n',
	);
});

test("sseFrame keeps newline-bearing payloads on one data line", () => {
	// JSON.stringify escapes the newline, so the frame stays single-line — the
	// property that matters is that a payload can never inject a frame break.
	const frame = sseFrame("message", { text: "a\nb" });
	assert.deepEqual(frame.split("\n").filter((l) => l.startsWith("data:")).length, 1);
	assert.ok(frame.endsWith("\n\n"));
});

test("sseFrame cannot be broken out of by payload content", () => {
	const frame = sseFrame("message", { text: "\n\nevent: fake\ndata: gotcha" });
	const blocks = frame.split("\n\n").filter(Boolean);
	assert.deepEqual(blocks.length, 1);
});

// -------------------------------------------------------- room registry

test("RoomRegistry reports the first handler as creating the room", () => {
	const rooms = new RoomRegistry();
	assert.deepEqual(rooms.add("a", () => {}), true);
	assert.deepEqual(rooms.add("a", () => {}), false);
	assert.deepEqual(rooms.rooms, ["a"]);
});

test("RoomRegistry only reports empty on the last handler", () => {
	const rooms = new RoomRegistry();
	const h1 = () => {};
	const h2 = () => {};
	rooms.add("a", h1);
	rooms.add("a", h2);

	assert.deepEqual(rooms.remove("a", h1), false);
	assert.deepEqual(rooms.has("a"), true);
	assert.deepEqual(rooms.remove("a", h2), true);
	assert.deepEqual(rooms.has("a"), false);
});

test("RoomRegistry.remove on an unknown room is a no-op", () => {
	assert.deepEqual(new RoomRegistry().remove("nope", () => {}), false);
});

test("RoomRegistry delivers to every handler", () => {
	const rooms = new RoomRegistry();
	const seen: number[] = [];
	rooms.add("a", () => seen.push(1));
	rooms.add("a", () => seen.push(2));
	rooms.deliver("a", msg("a"), () => {});
	assert.deepEqual(seen, [1, 2]);
});

test("RoomRegistry survives a handler that unsubscribes mid-delivery", () => {
	const rooms = new RoomRegistry();
	const seen: number[] = [];
	const h2 = () => seen.push(2);
	const h1 = () => {
		seen.push(1);
		rooms.remove("a", h2);
	};
	rooms.add("a", h1);
	rooms.add("a", h2);

	rooms.deliver("a", msg("a"), () => {});
	// h2 was snapshotted before delivery started, so it still runs this round.
	assert.deepEqual(seen, [1, 2]);
});

test("RoomRegistry routes a throwing handler to onError and continues", () => {
	const rooms = new RoomRegistry();
	const errors: unknown[] = [];
	const seen: number[] = [];
	rooms.add("a", () => {
		throw new Error("boom");
	});
	rooms.add("a", () => seen.push(2));

	rooms.deliver("a", msg("a"), (e) => errors.push(e));
	assert.deepEqual(errors.length, 1);
	assert.deepEqual(seen, [2]);
});

test("RoomRegistry.removeRoom drops every handler", () => {
	const rooms = new RoomRegistry();
	rooms.add("a", () => {});
	rooms.add("a", () => {});
	assert.deepEqual(rooms.removeRoom("a"), true);
	assert.deepEqual(rooms.has("a"), false);
	assert.deepEqual(rooms.removeRoom("a"), false);
});

// ---------------------------------------------------- staleness watchdog

test("StalenessWatchdog fires after silence", async () => {
	let fired = 0;
	const watchdog = new StalenessWatchdog({
		timeout: 150,
		onStale: () => fired++,
	});
	watchdog.start();
	await sleep(450);
	watchdog.stop();

	assert.deepEqual(fired, 1, "should fire exactly once, then stop itself");
});

test("StalenessWatchdog is kept alive by traffic", async () => {
	let fired = 0;
	const watchdog = new StalenessWatchdog({
		timeout: 200,
		onStale: () => fired++,
	});
	watchdog.start();

	const deadline = Date.now() + 500;
	while (Date.now() < deadline) {
		watchdog.alive();
		await sleep(40);
	}
	watchdog.stop();

	assert.deepEqual(fired, 0);
});

test("StalenessWatchdog with timeout 0 never runs", () => {
	const watchdog = new StalenessWatchdog({ timeout: 0, onStale: () => {} });
	watchdog.start();
	assert.deepEqual(watchdog.running, false);
});

test("StalenessWatchdog.stop is idempotent", () => {
	const watchdog = new StalenessWatchdog({ timeout: 100, onStale: () => {} });
	watchdog.start();
	assert.deepEqual(watchdog.running, true);
	watchdog.stop();
	watchdog.stop();
	assert.deepEqual(watchdog.running, false);
});

// ---------------------------------------------------- client construction

test("client requires a namespace", () => {
	assert.throws(
		() => createSSEClient({ url: "http://x/sse" } as never),
		{ name: "SSEError", message: /namespace/ },
	);
	assert.throws(
		() => createSSEClient({ url: "http://x/sse", namespace: "" }),
		SSEError,
	);
});

test("client rejects a relative url outside a browser", () => {
	assert.throws(
		() => createSSEClient({ url: "/sse", namespace: "ns" }),
		{ name: "SSEError", message: /absolute/ },
	);
});

test("client mints a client id when none is given", () => {
	const a = createSSEClient({ url: "http://x/sse", namespace: "ns", logger: null });
	const b = createSSEClient({ url: "http://x/sse", namespace: "ns", logger: null });
	try {
		assert.ok(a.clientId.length > 2);
		assert.ok(a.clientId !== b.clientId);
	} finally {
		a.dispose();
		b.dispose();
	}
});

test("client honours a supplied client id", () => {
	const sse = createSSEClient({
		url: "http://x/sse",
		namespace: "ns",
		clientId: "mine",
		logger: null,
	});
	try {
		assert.deepEqual(sse.clientId, "mine");
	} finally {
		sse.dispose();
	}
});

test("client starts idle and reports its configuration", () => {
	const sse = createSSEClient({
		url: "http://x/sse",
		namespace: "ns",
		rooms: ["a", "b"],
		logger: null,
	});
	try {
		assert.deepEqual(sse.connectionState, "idle");
		assert.deepEqual(sse.connected, false);
		assert.deepEqual(sse.namespace, "ns");
		assert.deepEqual(sse.rooms, ["a", "b"]);
		assert.deepEqual(sse.eventSource, null);
		assert.deepEqual(sse.lastEventId, null);
		assert.deepEqual(sse.url.href, "http://x/sse");

		const dump = sse.dump();
		assert.deepEqual(dump.state, "idle");
		assert.deepEqual(dump.namespace, "ns");
		assert.deepEqual(dump.rooms, ["a", "b"]);
	} finally {
		sse.dispose();
	}
});

test("client state store fires immediately with a snapshot", () => {
	const sse = createSSEClient({ url: "http://x/sse", namespace: "ns", logger: null });
	try {
		let snapshot: unknown = null;
		const unsub = sse.state.subscribe((s) => snapshot = s);
		unsub();
		assert.deepEqual(snapshot, {
			state: "idle",
			connected: false,
			connecting: false,
			attempt: 0,
			lastError: null,
		});
	} finally {
		sse.dispose();
	}
});

test("a disposed client refuses further use", async () => {
	const sse = createSSEClient({ url: "http://x/sse", namespace: "ns", logger: null });
	sse.dispose();

	assert.deepEqual(sse.connectionState, "disposed");
	assert.throws(() => sse.connect());
	await sse.subscribe("a", () => {}).then(
		() => {
			throw new Error("should have thrown");
		},
		() => {},
	);
	// dispose() twice is a no-op, not an error.
	sse.dispose();
});

test("SSEClient and createSSEClient are the same thing", () => {
	const sse = new SSEClient({ url: "http://x/sse", namespace: "ns", logger: null });
	try {
		assert.ok(sse instanceof SSEClient);
	} finally {
		sse.dispose();
	}
});
