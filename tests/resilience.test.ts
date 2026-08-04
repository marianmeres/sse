/**
 * The failure modes the design exists to handle.
 *
 * If you are changing reconnect, replay or the probe, read this file first —
 * these tests encode *why* the code is shaped the way it is.
 */

import { assert, assertEquals, assertInstanceOf, assertRejects } from "@std/assert";

import { createSSEClient, type SSEClient } from "../src/client/sse-client.ts";
import { SSEConnectTimeoutError, SSETerminatedError } from "../src/protocol/errors.ts";
import type { SSEHello, SSEMessage } from "../src/protocol/frames.ts";
import {
	freePort,
	readStream,
	sleep,
	startFlakyServer,
	startServer,
	until,
} from "./_helpers.ts";

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

// --------------------------------------------------- terminal vs retryable

Deno.test("a 401 terminates instead of retrying forever", async () => {
	const server = startServer({ verify: () => null });
	const sse = client(server.url);
	try {
		const terminated: Array<{ status: number; reason: string }> = [];
		sse.on("terminated", (e) => terminated.push(e));

		const error = await assertRejects(() => sse.connect(), SSETerminatedError);
		assertEquals(error.status, 401);
		assertEquals(sse.connectionState, "terminated");
		assertEquals(
			terminated.length,
			1,
			"terminated event must fire — giving up has to be loud",
		);
		assertEquals(terminated[0].status, 401);

		// And it stays given up.
		await sleep(300);
		assertEquals(sse.connectionState, "terminated");
		assertEquals(server.service.stats().connections, 0);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

Deno.test("a 403 terminates too", async () => {
	const flaky = startFlakyServer();
	flaky.status = 403;
	const sse = client(flaky.url);
	try {
		const error = await assertRejects(() => sse.connect(), SSETerminatedError);
		assertEquals(error.status, 403);
	} finally {
		sse.dispose();
		await flaky.stop();
	}
});

Deno.test("a 503 is retried, not treated as terminal", async () => {
	// EventSource closes permanently on *any* non-200, so without the probe a
	// rolling deploy would look exactly like an auth rejection.
	const flaky = startFlakyServer();
	flaky.status = 503;
	const sse = client(flaky.url);
	try {
		let reconnects = 0;
		sse.on("reconnecting", () => reconnects++);

		const connecting = sse.connect();
		await until(() => reconnects >= 1, "client retried a 503");
		assertEquals(sse.connectionState, "reconnecting");

		flaky.status = 200;
		await connecting;
		assertEquals(sse.connected, true);
	} finally {
		sse.dispose();
		await flaky.stop();
	}
});

Deno.test("terminalStatusCodes is configurable", async () => {
	const flaky = startFlakyServer();
	flaky.status = 402;
	const sse = client(flaky.url, { terminalStatusCodes: [402] });
	try {
		const error = await assertRejects(() => sse.connect(), SSETerminatedError);
		assertEquals(error.status, 402);
	} finally {
		sse.dispose();
		await flaky.stop();
	}
});

Deno.test("a terminated client can be revived by an explicit connect", async () => {
	// The credentials that got you rejected may since have been refreshed.
	const flaky = startFlakyServer();
	flaky.status = 401;
	const sse = client(flaky.url);
	try {
		await assertRejects(() => sse.connect(), SSETerminatedError);
		assertEquals(sse.connectionState, "terminated");

		flaky.status = 200;
		await sse.connect();
		assertEquals(sse.connected, true);
	} finally {
		sse.dispose();
		await flaky.stop();
	}
});

// ----------------------------------------------------------- reconnecting

Deno.test("EventSource's own retry recovers a dropped stream", async () => {
	const flaky = startFlakyServer();
	const sse = client(flaky.url);
	try {
		await sse.connect();
		assertEquals(flaky.hits, 1);

		let reconnecting = 0;
		sse.on("reconnecting", () => reconnecting++);

		flaky.dropStreams();

		await until(() => flaky.hits >= 2, "EventSource reconnected on its own", 4_000);
		await until(() => sse.connected, "client back to open", 4_000);
		assert(reconnecting >= 1, "a self-driven retry still reports as reconnecting");
	} finally {
		sse.dispose();
		await flaky.stop();
	}
});

Deno.test("a client survives a full server restart and re-subscribes", async () => {
	const port = freePort();
	let server = startServer({}, port);
	const sse = client(server.url, { clientId: "c1" });
	try {
		const got: SSEMessage[] = [];
		await sse.connect();
		await sse.subscribe("chat", (m) => got.push(m));
		assertEquals(server.service.members("chat", "ns"), ["c1"]);

		await server.stop();
		await until(() => !sse.connected, "client noticed the server went away");

		server = startServer({}, port);
		await until(() => sse.connected, "client came back", 10_000);

		// Rooms ride the connect url, so they are restored by the stream request
		// itself — there is no re-subscribe step that could be mis-ordered.
		await until(
			() => server.service.members("chat", "ns").length === 1,
			"room restored after restart",
		);

		await server.service.publish("chat", { v: "after restart" }, "ns");
		await until(() => got.length === 1, "delivery works again");
	} finally {
		sse.dispose();
		await server.stop();
	}
});

Deno.test("connectTimeout bounds the await while retrying continues", async () => {
	const flaky = startFlakyServer();
	flaky.status = 503;
	const sse = client(flaky.url, { connectTimeout: 150 });
	try {
		await assertRejects(() => sse.connect(), SSEConnectTimeoutError);

		// The rejection bounds *your await*, not the connection attempt.
		const before = flaky.hits;
		flaky.status = 200;
		await until(() => flaky.hits > before, "still retrying in the background");
		await until(() => sse.connected, "and it eventually connects");
	} finally {
		sse.dispose();
		await flaky.stop();
	}
});

Deno.test("a silent stream is detected and reconnected", async () => {
	// The defining SSE failure: a proxy drops the connection, no error fires,
	// and EventSource sits at OPEN delivering nothing forever.
	const flaky = startFlakyServer();
	const sse = client(flaky.url, { staleTimeout: 200 });
	try {
		await sse.connect();
		assertEquals(flaky.hits, 1);

		let closes = 0;
		sse.on("close", (e) => {
			if (e.reason.includes("silent")) closes++;
		});

		await until(() => flaky.hits >= 2, "watchdog forced a reconnect", 4_000);
		assert(closes >= 1, "the forced teardown is reported as a close");
	} finally {
		sse.dispose();
		await flaky.stop();
	}
});

// ----------------------------------------------------------------- replay

Deno.test("a reconnect replays what was missed", async () => {
	const server = startServer({ replayBufferSize: 50 });
	const sse = client(server.url, { clientId: "c1" });
	try {
		const got: SSEMessage[] = [];
		const hellos: SSEHello[] = [];
		sse.on("connected", (e) => hellos.push(e));

		await sse.connect();
		await sse.subscribe("chat", (m) => got.push(m));

		await server.service.publish("chat", { n: 1 }, "ns");
		await until(() => got.length === 1, "first message live");
		assert(sse.lastEventId, "the client tracked a resume point");

		sse.disconnect();
		await until(
			() => server.service.stats().connections === 0,
			"server saw the disconnect",
		);

		// Missed entirely — nobody is subscribed while these are published.
		await server.service.publish("chat", { n: 2 }, "ns");
		await server.service.publish("chat", { n: 3 }, "ns");

		await sse.connect();
		await until(() => got.length === 3, "the gap was replayed");

		assertEquals(got.map((m) => (m.payload as { n: number }).n), [1, 2, 3]);
		assertEquals(hellos.at(-1)!.resumed, true);
		assertEquals(hellos.at(-1)!.gap, false);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

Deno.test("a replayed message is not also delivered live", async () => {
	// The upTo snapshot is what guarantees this: everything at or below it is
	// replayed, everything after arrives live, and nothing is both.
	const server = startServer({ replayBufferSize: 50 });
	const sse = client(server.url, { clientId: "c1" });
	try {
		const got: SSEMessage[] = [];
		await sse.connect();
		await sse.subscribe("chat", (m) => got.push(m));

		await server.service.publish("chat", { n: 1 }, "ns");
		await until(() => got.length === 1, "live");

		sse.disconnect();
		await until(() => server.service.stats().connections === 0, "disconnected");
		await server.service.publish("chat", { n: 2 }, "ns");

		await sse.connect();
		await until(() => got.length === 2, "replayed");

		await server.service.publish("chat", { n: 3 }, "ns");
		await until(() => got.length === 3, "live again");

		await sleep(150);
		assertEquals(
			got.map((m) => (m.payload as { n: number }).n),
			[1, 2, 3],
			"each message exactly once, in order",
		);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

Deno.test("an evicted resume point is reported as a gap", async () => {
	const server = startServer({ replayBufferSize: 2 });
	try {
		for (let n = 1; n <= 5; n++) {
			await server.service.publish("chat", { n }, "ns");
		}

		const resume = `${server.service.instanceId}:1`;
		const events = await readStream(
			`${server.url}/events?clientId=c1&namespace=ns&rooms=chat&lastEventId=${resume}`,
			{ count: 3 },
		);

		const hello = events[0].json as SSEHello;
		assertEquals(hello.resumed, true);
		assertEquals(hello.gap, true, "messages 2 and 3 are genuinely gone");

		// Only what the buffer still holds comes back.
		assertEquals(events.slice(1).map((e) => (e.json as SSEMessage).payload), [
			{ n: 4 },
			{ n: 5 },
		]);
	} finally {
		await server.stop();
	}
});

Deno.test("a resume point from another instance is ignored", async () => {
	// Multi-instance without a shared adapter degrades to at-most-once, safely
	// and silently, rather than replaying someone else's sequence numbers.
	const server = startServer({ replayBufferSize: 50 });
	try {
		await server.service.publish("chat", { n: 1 }, "ns");

		const events = await readStream(
			`${server.url}/events?clientId=c1&namespace=ns&rooms=chat&lastEventId=someone-else:0`,
			{ count: 1 },
		);
		const hello = events[0].json as SSEHello;
		assertEquals(hello.resumed, false);
		assertEquals(hello.gap, false);
	} finally {
		await server.stop();
	}
});

Deno.test("a malformed resume point starts live rather than failing", async () => {
	const server = startServer({ replayBufferSize: 50 });
	try {
		await server.service.publish("chat", { n: 1 }, "ns");
		const events = await readStream(
			`${server.url}/events?clientId=c1&namespace=ns&rooms=chat&lastEventId=garbage`,
			{ count: 1 },
		);
		assertEquals((events[0].json as SSEHello).resumed, false);
	} finally {
		await server.stop();
	}
});

Deno.test("replay only returns rooms the client actually holds", async () => {
	const server = startServer({ replayBufferSize: 50 });
	try {
		await server.service.publish("chat", { n: 1 }, "ns");
		await server.service.publish("other", { n: 2 }, "ns");
		await server.service.publish("chat", { n: 3 }, "ns");

		const resume = `${server.service.instanceId}:0`;
		const events = await readStream(
			`${server.url}/events?clientId=c1&namespace=ns&rooms=chat&lastEventId=${resume}`,
			{ count: 3 },
		);
		assertEquals(events.slice(1).map((e) => (e.json as SSEMessage).payload), [
			{ n: 1 },
			{ n: 3 },
		]);
	} finally {
		await server.stop();
	}
});

Deno.test("replay is off when the buffer size is zero", async () => {
	const server = startServer({ replayBufferSize: 0 });
	try {
		await server.service.publish("chat", { n: 1 }, "ns");
		const resume = `${server.service.instanceId}:0`;
		const events = await readStream(
			`${server.url}/events?clientId=c1&namespace=ns&rooms=chat&lastEventId=${resume}`,
			{ count: 1 },
		);
		assertEquals((events[0].json as SSEHello).resumed, false);

		// Ids stay monotonic regardless, so replay can be switched on later.
		assertEquals(server.service.stats().seq, 1);
		assertEquals(server.service.stats().buffered, 0);
	} finally {
		await server.stop();
	}
});

// ------------------------------------------------------------- lifecycle

Deno.test("a duplicate client id replaces the older stream", async () => {
	// This is what makes a reconnect after a half-open drop actually recover
	// instead of accumulating ghosts.
	const server = startServer({});
	const first = client(server.url, { clientId: "same" });
	const second = client(server.url, { clientId: "same" });
	try {
		await first.connect();
		assertEquals(server.service.stats().connections, 1);

		await second.connect();
		await sleep(100);
		assertEquals(
			server.service.stats().connections,
			1,
			"the newcomer wins, the stale stream goes",
		);
	} finally {
		first.dispose();
		second.dispose();
		await server.stop();
	}
});

Deno.test("disconnect is resumable", async () => {
	const server = startServer({});
	const sse = client(server.url, { clientId: "c1" });
	try {
		const got: SSEMessage[] = [];
		await sse.connect();
		await sse.subscribe("chat", (m) => got.push(m));

		sse.disconnect();
		assertEquals(sse.connectionState, "idle");
		assertEquals(sse.isSubscribed("chat"), true, "rooms survive a disconnect");

		await sse.connect();
		await until(
			() => server.service.members("chat", "ns").length === 1,
			"room restored",
		);
		await server.service.publish("chat", { v: 1 }, "ns");
		await until(() => got.length === 1, "handlers survive a disconnect");
	} finally {
		sse.dispose();
		await server.stop();
	}
});

Deno.test("disconnect rejects a pending connect", async () => {
	const flaky = startFlakyServer();
	flaky.status = 503;
	const sse = client(flaky.url);
	try {
		const pending = sse.connect();
		await sleep(30);
		sse.disconnect();
		const error = await assertRejects(() => pending);
		assertInstanceOf(error, SSETerminatedError);
	} finally {
		sse.dispose();
		await flaky.stop();
	}
});

Deno.test("dispose is terminal", async () => {
	const server = startServer({});
	const sse = client(server.url);
	try {
		await sse.connect();
		await sse.subscribe("chat", () => {});
		sse.dispose();

		assertEquals(sse.connectionState, "disposed");
		assertEquals(sse.rooms, []);
		await until(
			() => server.service.stats().connections === 0,
			"the server saw the stream go",
		);
	} finally {
		await server.stop();
	}
});

Deno.test("publishing works while the stream is down", async () => {
	// Upstream calls are plain HTTP, which is exactly why this client needs no
	// outbox: there is no dead socket for a send to ride.
	const server = startServer({});
	const receiver = client(server.url, { clientId: "rx" });
	const sender = client(server.url, { clientId: "tx" });
	try {
		const got: SSEMessage[] = [];
		await receiver.connect();
		await receiver.subscribe("chat", (m) => got.push(m));

		await sender.connect();
		sender.disconnect();
		assertEquals(sender.connected, false);

		const { recipients } = await sender.publish("chat", { v: 1 });
		assertEquals(recipients, 1);
		await until(() => got.length === 1, "published while disconnected");
	} finally {
		receiver.dispose();
		sender.dispose();
		await server.stop();
	}
});

Deno.test("the reactive state store tracks the whole lifecycle", async () => {
	const server = startServer({});
	const sse = client(server.url);
	const seen: string[] = [];
	try {
		const unsub = sse.state.subscribe((s) => seen.push(s.state));
		await sse.connect();
		sse.disconnect();
		unsub();

		assertEquals(seen[0], "idle");
		assert(seen.includes("connecting"));
		assert(seen.includes("open"));
		assertEquals(seen.at(-1), "idle");
	} finally {
		sse.dispose();
		await server.stop();
	}
});

Deno.test("auth is re-evaluated on every reconnect", async () => {
	// A token that expires mid-session must be refreshable without
	// reconfiguring the client.
	let token = "first";
	const seen: unknown[] = [];
	const server = startServer({
		verify: (payload) => {
			seen.push(payload);
			return payload === "expired" ? null : {};
		},
	});
	const sse = client(server.url, { auth: () => token });
	try {
		await sse.connect();
		assertEquals(seen.at(-1), "first");

		token = "second";
		sse.disconnect();
		await sse.connect();
		assertEquals(seen.at(-1), "second", "auth() was called again");
	} finally {
		sse.dispose();
		await server.stop();
	}
});
