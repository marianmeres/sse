import {
	assert,
	assertEquals,
	assertGreaterOrEqual,
	assertLessOrEqual,
	assertThrows,
} from "@std/assert";

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

Deno.test("backoffDelay stays within [d/2, d]", () => {
	for (let attempt = 1; attempt <= 12; attempt++) {
		const full = Math.min(30_000, 500 * Math.pow(2, attempt - 1));
		for (let i = 0; i < 50; i++) {
			const delay = backoffDelay(attempt, 500, 30_000);
			assertGreaterOrEqual(delay, Math.floor(full / 2));
			assertLessOrEqual(delay, full);
		}
	}
});

Deno.test("backoffDelay honours the ceiling", () => {
	assertEquals(backoffDelay(50, 500, 30_000, () => 1), 30_000);
	assertEquals(backoffDelay(50, 500, 30_000, () => 0), 15_000);
});

Deno.test("backoffDelay treats attempt < 1 as the first attempt", () => {
	assertEquals(backoffDelay(0, 1_000, 30_000, () => 0), 500);
	assertEquals(backoffDelay(-5, 1_000, 30_000, () => 0), 500);
});

Deno.test("backoffDelay grows exponentially", () => {
	const rnd = () => 1;
	assertEquals(backoffDelay(1, 100, 999_999, rnd), 100);
	assertEquals(backoffDelay(2, 100, 999_999, rnd), 200);
	assertEquals(backoffDelay(3, 100, 999_999, rnd), 400);
});

// ------------------------------------------------------------ event ids

Deno.test("event ids round trip", () => {
	assertEquals(formatEventId("srv7", 42), "srv7:42");
	assertEquals(parseEventId("srv7:42"), { instanceId: "srv7", seq: 42 });
});

Deno.test("malformed event ids parse to null rather than throwing", () => {
	for (
		const bad of [null, undefined, "", "42", ":42", "srv7:", "srv7:abc", "srv7:-1"]
	) {
		assertEquals(parseEventId(bad), null, `expected null for ${JSON.stringify(bad)}`);
	}
});

// --------------------------------------------------------- replay buffer

Deno.test("ReplayBuffer stamps a monotonic sequence", () => {
	const buffer = new ReplayBuffer(10);
	assertEquals(buffer.add("ns", msg("a")), 1);
	assertEquals(buffer.add("ns", msg("a")), 2);
	assertEquals(buffer.currentSeq, 2);
});

Deno.test("ReplayBuffer keeps stamping when replay is disabled", () => {
	// Ids must stay meaningful even with the buffer off, so replay can be
	// switched on later without the sequence restarting.
	const buffer = new ReplayBuffer(0);
	assertEquals(buffer.enabled, false);
	assertEquals(buffer.add("ns", msg("a")), 1);
	assertEquals(buffer.add("ns", msg("a")), 2);
	assertEquals(buffer.size, 0);
	assertEquals(buffer.since(0, 2, "ns", new Set(["a"])), []);
});

Deno.test("ReplayBuffer evicts oldest beyond capacity", () => {
	const buffer = new ReplayBuffer(3);
	for (let i = 0; i < 5; i++) buffer.add("ns", msg("a"));
	assertEquals(buffer.size, 3);
	assertEquals(buffer.oldestSeq, 3);
	assertEquals(buffer.currentSeq, 5);
});

Deno.test("ReplayBuffer.oldestSeq is null when empty", () => {
	assertEquals(new ReplayBuffer(3).oldestSeq, null);
});

Deno.test("ReplayBuffer.since filters by room", () => {
	const buffer = new ReplayBuffer(10);
	buffer.add("ns", msg("a"));
	buffer.add("ns", msg("b"));
	buffer.add("ns", msg("a"));

	const got = buffer.since(0, 3, "ns", new Set(["a"]));
	assertEquals(got.map((e) => e.seq), [1, 3]);
});

Deno.test("ReplayBuffer.since filters by namespace", () => {
	const buffer = new ReplayBuffer(10);
	buffer.add("ns1", msg("a", "ns1"));
	buffer.add("ns2", msg("a", "ns2"));

	assertEquals(buffer.since(0, 2, "ns1", new Set(["a"])).map((e) => e.seq), [1]);
	assertEquals(buffer.since(0, 2, "ns2", new Set(["a"])).map((e) => e.seq), [2]);
});

Deno.test("ReplayBuffer.since replays broadcasts into any namespace", () => {
	const buffer = new ReplayBuffer(10);
	buffer.add(null, msg("a", "*"));
	assertEquals(buffer.since(0, 1, "anything", new Set(["a"])).length, 1);
});

Deno.test("ReplayBuffer.since respects the upTo snapshot", () => {
	// The bound is what prevents a message being both replayed and delivered
	// live: anything past the snapshot is the live stream's job.
	const buffer = new ReplayBuffer(10);
	buffer.add("ns", msg("a"));
	buffer.add("ns", msg("a"));
	buffer.add("ns", msg("a"));

	assertEquals(buffer.since(0, 2, "ns", new Set(["a"])).map((e) => e.seq), [1, 2]);
});

Deno.test("ReplayBuffer.since excludes the resume point itself", () => {
	const buffer = new ReplayBuffer(10);
	buffer.add("ns", msg("a"));
	buffer.add("ns", msg("a"));
	assertEquals(buffer.since(1, 2, "ns", new Set(["a"])).map((e) => e.seq), [2]);
});

Deno.test("ReplayBuffer.since returns nothing for a client with no rooms", () => {
	const buffer = new ReplayBuffer(10);
	buffer.add("ns", msg("a"));
	assertEquals(buffer.since(0, 1, "ns", new Set()), []);
});

// ------------------------------------------------------- frame encoding

Deno.test("sseFrame emits event and data", () => {
	assertEquals(sseFrame("hello", { a: 1 }), 'event: hello\ndata: {"a":1}\n\n');
});

Deno.test("sseFrame emits id only when given", () => {
	assertEquals(
		sseFrame("message", { a: 1 }, "srv:7"),
		'id: srv:7\nevent: message\ndata: {"a":1}\n\n',
	);
});

Deno.test("sseFrame keeps newline-bearing payloads on one data line", () => {
	// JSON.stringify escapes the newline, so the frame stays single-line — the
	// property that matters is that a payload can never inject a frame break.
	const frame = sseFrame("message", { text: "a\nb" });
	assertEquals(frame.split("\n").filter((l) => l.startsWith("data:")).length, 1);
	assert(frame.endsWith("\n\n"));
});

Deno.test("sseFrame cannot be broken out of by payload content", () => {
	const frame = sseFrame("message", { text: "\n\nevent: fake\ndata: gotcha" });
	const blocks = frame.split("\n\n").filter(Boolean);
	assertEquals(blocks.length, 1);
});

// -------------------------------------------------------- room registry

Deno.test("RoomRegistry reports the first handler as creating the room", () => {
	const rooms = new RoomRegistry();
	assertEquals(rooms.add("a", () => {}), true);
	assertEquals(rooms.add("a", () => {}), false);
	assertEquals(rooms.rooms, ["a"]);
});

Deno.test("RoomRegistry only reports empty on the last handler", () => {
	const rooms = new RoomRegistry();
	const h1 = () => {};
	const h2 = () => {};
	rooms.add("a", h1);
	rooms.add("a", h2);

	assertEquals(rooms.remove("a", h1), false);
	assertEquals(rooms.has("a"), true);
	assertEquals(rooms.remove("a", h2), true);
	assertEquals(rooms.has("a"), false);
});

Deno.test("RoomRegistry.remove on an unknown room is a no-op", () => {
	assertEquals(new RoomRegistry().remove("nope", () => {}), false);
});

Deno.test("RoomRegistry delivers to every handler", () => {
	const rooms = new RoomRegistry();
	const seen: number[] = [];
	rooms.add("a", () => seen.push(1));
	rooms.add("a", () => seen.push(2));
	rooms.deliver("a", msg("a"), () => {});
	assertEquals(seen, [1, 2]);
});

Deno.test("RoomRegistry survives a handler that unsubscribes mid-delivery", () => {
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
	assertEquals(seen, [1, 2]);
});

Deno.test("RoomRegistry routes a throwing handler to onError and continues", () => {
	const rooms = new RoomRegistry();
	const errors: unknown[] = [];
	const seen: number[] = [];
	rooms.add("a", () => {
		throw new Error("boom");
	});
	rooms.add("a", () => seen.push(2));

	rooms.deliver("a", msg("a"), (e) => errors.push(e));
	assertEquals(errors.length, 1);
	assertEquals(seen, [2]);
});

Deno.test("RoomRegistry.removeRoom drops every handler", () => {
	const rooms = new RoomRegistry();
	rooms.add("a", () => {});
	rooms.add("a", () => {});
	assertEquals(rooms.removeRoom("a"), true);
	assertEquals(rooms.has("a"), false);
	assertEquals(rooms.removeRoom("a"), false);
});

// ---------------------------------------------------- staleness watchdog

Deno.test("StalenessWatchdog fires after silence", async () => {
	let fired = 0;
	const watchdog = new StalenessWatchdog({
		timeout: 150,
		onStale: () => fired++,
	});
	watchdog.start();
	await sleep(450);
	watchdog.stop();

	assertEquals(fired, 1, "should fire exactly once, then stop itself");
});

Deno.test("StalenessWatchdog is kept alive by traffic", async () => {
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

	assertEquals(fired, 0);
});

Deno.test("StalenessWatchdog with timeout 0 never runs", () => {
	const watchdog = new StalenessWatchdog({ timeout: 0, onStale: () => {} });
	watchdog.start();
	assertEquals(watchdog.running, false);
});

Deno.test("StalenessWatchdog.stop is idempotent", () => {
	const watchdog = new StalenessWatchdog({ timeout: 100, onStale: () => {} });
	watchdog.start();
	assertEquals(watchdog.running, true);
	watchdog.stop();
	watchdog.stop();
	assertEquals(watchdog.running, false);
});

// ---------------------------------------------------- client construction

Deno.test("client requires a namespace", () => {
	assertThrows(
		() => createSSEClient({ url: "http://x/sse" } as never),
		SSEError,
		"namespace",
	);
	assertThrows(
		() => createSSEClient({ url: "http://x/sse", namespace: "" }),
		SSEError,
	);
});

Deno.test("client rejects a relative url outside a browser", () => {
	assertThrows(
		() => createSSEClient({ url: "/sse", namespace: "ns" }),
		SSEError,
		"absolute",
	);
});

Deno.test("client mints a client id when none is given", () => {
	const a = createSSEClient({ url: "http://x/sse", namespace: "ns", logger: null });
	const b = createSSEClient({ url: "http://x/sse", namespace: "ns", logger: null });
	try {
		assert(a.clientId.length > 2);
		assert(a.clientId !== b.clientId);
	} finally {
		a.dispose();
		b.dispose();
	}
});

Deno.test("client honours a supplied client id", () => {
	const sse = createSSEClient({
		url: "http://x/sse",
		namespace: "ns",
		clientId: "mine",
		logger: null,
	});
	try {
		assertEquals(sse.clientId, "mine");
	} finally {
		sse.dispose();
	}
});

Deno.test("client starts idle and reports its configuration", () => {
	const sse = createSSEClient({
		url: "http://x/sse",
		namespace: "ns",
		rooms: ["a", "b"],
		logger: null,
	});
	try {
		assertEquals(sse.connectionState, "idle");
		assertEquals(sse.connected, false);
		assertEquals(sse.namespace, "ns");
		assertEquals(sse.rooms, ["a", "b"]);
		assertEquals(sse.eventSource, null);
		assertEquals(sse.lastEventId, null);
		assertEquals(sse.url.href, "http://x/sse");

		const dump = sse.dump();
		assertEquals(dump.state, "idle");
		assertEquals(dump.namespace, "ns");
		assertEquals(dump.rooms, ["a", "b"]);
	} finally {
		sse.dispose();
	}
});

Deno.test("client state store fires immediately with a snapshot", () => {
	const sse = createSSEClient({ url: "http://x/sse", namespace: "ns", logger: null });
	try {
		let snapshot: unknown = null;
		const unsub = sse.state.subscribe((s) => snapshot = s);
		unsub();
		assertEquals(snapshot, {
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

Deno.test("a disposed client refuses further use", async () => {
	const sse = createSSEClient({ url: "http://x/sse", namespace: "ns", logger: null });
	sse.dispose();

	assertEquals(sse.connectionState, "disposed");
	assertThrows(() => sse.connect());
	await sse.subscribe("a", () => {}).then(
		() => {
			throw new Error("should have thrown");
		},
		() => {},
	);
	// dispose() twice is a no-op, not an error.
	sse.dispose();
});

Deno.test("SSEClient and createSSEClient are the same thing", () => {
	const sse = new SSEClient({ url: "http://x/sse", namespace: "ns", logger: null });
	try {
		assert(sse instanceof SSEClient);
	} finally {
		sse.dispose();
	}
});
