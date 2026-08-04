import { assert, assertEquals, assertRejects } from "@std/assert";

import { createSSEClient, type SSEClient } from "../src/client/sse-client.ts";
import { SSERemoteError } from "../src/protocol/errors.ts";
import type { SSEMessage } from "../src/protocol/frames.ts";
import { readStream, rpc, startServer, type TestServer, until } from "./_helpers.ts";

/** Creates a client wired to the test server, with logging off. */
function client(
	server: TestServer,
	options: Partial<Parameters<typeof createSSEClient>[0]> = {},
): SSEClient {
	return createSSEClient({
		url: server.url,
		namespace: "ns",
		logger: null,
		staleTimeout: 0,
		...options,
	});
}

/** Runs `fn` with a server and guarantees teardown. */
async function withServer(
	options: Parameters<typeof startServer>[0],
	fn: (server: TestServer) => Promise<void>,
): Promise<void> {
	const server = startServer(options);
	try {
		await fn(server);
	} finally {
		await server.stop();
	}
}

// ------------------------------------------------------------- connecting

Deno.test("connect resolves once hello arrives", async () => {
	await withServer({}, async (server) => {
		const sse = client(server, { clientId: "c1" });
		try {
			await sse.connect();
			assertEquals(sse.connected, true);
			assertEquals(sse.connectionState, "open");
			assertEquals(sse.clientId, "c1");
			assertEquals(sse.namespace, "ns");
			assertEquals(server.service.stats().connections, 1);
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("connect emits open then connected", async () => {
	await withServer({}, async (server) => {
		const sse = client(server);
		const seen: string[] = [];
		sse.on("open", () => seen.push("open"));
		sse.on("connected", () => seen.push("connected"));
		try {
			await sse.connect();
			assertEquals(seen, ["open", "connected"]);
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("concurrent connect calls share one attempt", async () => {
	await withServer({}, async (server) => {
		const sse = client(server);
		try {
			await Promise.all([sse.connect(), sse.connect(), sse.connect()]);
			assertEquals(server.service.stats().connections, 1);
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("verify overrides the client-proposed id and namespace", async () => {
	await withServer({
		verify: () => ({ clientId: "server-chosen", namespace: "forced" }),
	}, async (server) => {
		const sse = client(server, { clientId: "i-wanted-this", namespace: "mine" });
		try {
			await sse.connect();
			assertEquals(sse.clientId, "server-chosen");
			assertEquals(sse.namespace, "forced");
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("verify receives the auth payload verbatim", async () => {
	const seen: unknown[] = [];
	await withServer({
		verify: (payload) => {
			seen.push(payload);
			return {};
		},
	}, async (server) => {
		const sse = client(server, { auth: () => ({ token: "abc", n: 1 }) });
		try {
			await sse.connect();
			assertEquals(seen[0], { token: "abc", n: 1 });
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("a bare string auth payload survives the round trip", async () => {
	// Always-JSON serialisation exists so a token that looks like a number
	// ("123") does not arrive as one.
	const seen: unknown[] = [];
	await withServer({
		verify: (payload) => {
			seen.push(payload);
			return {};
		},
	}, async (server) => {
		const sse = client(server, { auth: () => "123" });
		try {
			await sse.connect();
			assertEquals(seen[0], "123");
		} finally {
			sse.dispose();
		}
	});
});

// ------------------------------------------------------------ subscribing

Deno.test("subscribe then publish delivers to the handler", async () => {
	await withServer({}, async (server) => {
		const sse = client(server);
		try {
			const got: SSEMessage[] = [];
			await sse.connect();
			await sse.subscribe("chat", (m) => got.push(m));

			const { recipients } = await sse.publish("chat", { text: "hi" });
			assertEquals(recipients, 1);

			await until(() => got.length === 1, "message delivered");
			assertEquals(got[0].room, "chat");
			assertEquals(got[0].namespace, "ns");
			assertEquals(got[0].payload, { text: "hi" });
			assertEquals(got[0].from, sse.clientId);
			assert(got[0].timestamp > 0);
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("rooms given up front ride the connect url", async () => {
	await withServer({}, async (server) => {
		const sse = client(server, { rooms: ["preset"], clientId: "c1" });
		try {
			await sse.connect();
			// No rpc round trip was needed for this to be registered.
			assertEquals(server.service.members("preset", "ns"), ["c1"]);
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("subscribe before connect is applied by the connect url", async () => {
	await withServer({}, async (server) => {
		const sse = client(server, { autoConnect: false, clientId: "c1" });
		try {
			const got: SSEMessage[] = [];
			await sse.subscribe("chat", (m) => got.push(m));
			assertEquals(sse.isSubscribed("chat"), true);

			await sse.connect();
			assertEquals(server.service.members("chat", "ns"), ["c1"]);

			await server.service.publish("chat", { v: 1 }, "ns");
			await until(() => got.length === 1, "message after late connect");
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("multiple handlers on one room produce one subscription", async () => {
	await withServer({}, async (server) => {
		const sse = client(server, { clientId: "c1" });
		try {
			const a: SSEMessage[] = [];
			const b: SSEMessage[] = [];
			await sse.connect();
			const unsubA = await sse.subscribe("chat", (m) => a.push(m));
			await sse.subscribe("chat", (m) => b.push(m));

			assertEquals(server.service.members("chat", "ns"), ["c1"]);

			await server.service.publish("chat", { v: 1 }, "ns");
			await until(() => a.length === 1 && b.length === 1, "both handlers");

			// Detaching one leaves the room subscribed for the other.
			unsubA();
			await until(
				() => server.service.members("chat", "ns").length === 1,
				"still subscribed",
			);

			await server.service.publish("chat", { v: 2 }, "ns");
			await until(() => b.length === 2, "second handler still receiving");
			assertEquals(a.length, 1);
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("the last unsubscriber drops the room", async () => {
	await withServer({}, async (server) => {
		const sse = client(server, { clientId: "c1" });
		try {
			await sse.connect();
			const unsub = await sse.subscribe("chat", () => {});
			unsub();
			await until(
				() => server.service.members("chat", "ns").length === 0,
				"room dropped",
			);
			assertEquals(sse.isSubscribed("chat"), false);
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("unsubscribe drops the room and every handler", async () => {
	await withServer({}, async (server) => {
		const sse = client(server, { clientId: "c1" });
		try {
			const got: SSEMessage[] = [];
			await sse.connect();
			await sse.subscribe("chat", (m) => got.push(m));
			await sse.subscribe("chat", (m) => got.push(m));

			await sse.unsubscribe("chat");
			assertEquals(sse.isSubscribed("chat"), false);
			await until(
				() => server.service.members("chat", "ns").length === 0,
				"unsubscribed",
			);

			await server.service.publish("chat", { v: 1 }, "ns");
			await new Promise((r) => setTimeout(r, 100));
			assertEquals(got.length, 0);
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("unsubscribe on an unheld room is a no-op", async () => {
	await withServer({}, async (server) => {
		const sse = client(server);
		try {
			await sse.connect();
			await sse.unsubscribe("never-held");
		} finally {
			sse.dispose();
		}
	});
});

// -------------------------------------------------------------- isolation

Deno.test("namespaces isolate identically named rooms", async () => {
	await withServer({}, async (server) => {
		const a = client(server, { namespace: "org-a", clientId: "a" });
		const b = client(server, { namespace: "org-b", clientId: "b" });
		try {
			const gotA: SSEMessage[] = [];
			const gotB: SSEMessage[] = [];
			await a.connect();
			await b.connect();
			await a.subscribe("chat", (m) => gotA.push(m));
			await b.subscribe("chat", (m) => gotB.push(m));

			const { recipients } = await a.publish("chat", { from: "a" });
			assertEquals(recipients, 1, "only a's namespace");

			await until(() => gotA.length === 1, "a received");
			await new Promise((r) => setTimeout(r, 100));
			assertEquals(gotB.length, 0, "b must not see a's message");
		} finally {
			a.dispose();
			b.dispose();
		}
	});
});

Deno.test("broadcast is denied by default", async () => {
	await withServer({}, async (server) => {
		const sse = client(server);
		try {
			await sse.connect();
			const error = await assertRejects(
				() => sse.broadcast("chat", { v: 1 }),
				SSERemoteError,
			);
			assertEquals(error.status, 403);
			assertEquals(error.code, "forbidden");
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("allowBroadcast opens the namespace boundary", async () => {
	await withServer({ allowBroadcast: () => true }, async (server) => {
		const a = client(server, { namespace: "org-a", clientId: "a" });
		const b = client(server, { namespace: "org-b", clientId: "b" });
		try {
			const gotB: SSEMessage[] = [];
			await a.connect();
			await b.connect();
			await a.subscribe("alerts", () => {});
			await b.subscribe("alerts", (m) => gotB.push(m));

			const { recipients } = await a.broadcast("alerts", { v: 1 });
			assertEquals(recipients, 2);

			await until(() => gotB.length === 1, "b received the broadcast");
			// Receivers always see the namespace they live in.
			assertEquals(gotB[0].namespace, "org-b");
			assertEquals(gotB[0].from, "a");
		} finally {
			a.dispose();
			b.dispose();
		}
	});
});

Deno.test("allowBroadcast sees the requesting context", async () => {
	const seen: Array<{ clientId: string; namespace: string; room: string }> = [];
	await withServer({
		verify: () => ({ meta: { role: "admin" } }),
		allowBroadcast: (ctx, room) => {
			seen.push({ clientId: ctx.clientId, namespace: ctx.namespace, room });
			return ctx.meta.role === "admin";
		},
	}, async (server) => {
		const sse = client(server, { clientId: "c1", auth: () => "t" });
		try {
			await sse.connect();
			await sse.broadcast("alerts", { v: 1 });
			assertEquals(seen, [{ clientId: "c1", namespace: "ns", room: "alerts" }]);
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("a client cannot publish into a namespace verify did not grant", async () => {
	await withServer({ verify: () => ({ namespace: "forced" }) }, async (server) => {
		const { status, json } = await rpc(server.url, {
			op: "pub",
			clientId: "c1",
			namespace: "somewhere-else",
			room: "chat",
			payload: {},
		});
		assertEquals(status, 403);
		assertEquals(json.error.code, "forbidden");
	});
});

// ------------------------------------------------------- server injection

Deno.test("service.publish arrives with from null", async () => {
	await withServer({}, async (server) => {
		const sse = client(server);
		try {
			const got: SSEMessage[] = [];
			await sse.connect();
			await sse.subscribe("news", (m) => got.push(m));

			const recipients = await server.service.publish("news", { v: 1 }, "ns");
			assertEquals(recipients, 1);

			await until(() => got.length === 1, "server push delivered");
			assertEquals(got[0].from, null, "null from marks a server injection");
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("injection routes are not mounted without httpAuth", async () => {
	await withServer({}, async (server) => {
		const publish = await fetch(`${server.url}/publish/ns/chat`, {
			method: "POST",
			body: "{}",
		});
		await publish.body?.cancel();
		assertEquals(publish.status, 404);

		const broadcast = await fetch(`${server.url}/broadcast/chat`, {
			method: "POST",
			body: "{}",
		});
		await broadcast.body?.cancel();
		assertEquals(broadcast.status, 404);
	});
});

Deno.test("injection routes work behind httpAuth", async () => {
	await withServer({
		httpAuth: (req: Request) => {
			if (req.headers.get("x-key") !== "secret") {
				return new Response("nope", { status: 401 });
			}
		},
	}, async (server) => {
		const sse = client(server);
		try {
			const got: SSEMessage[] = [];
			await sse.connect();
			await sse.subscribe("chat", (m) => got.push(m));

			const denied = await fetch(`${server.url}/publish/ns/chat`, {
				method: "POST",
				body: JSON.stringify({ v: 0 }),
			});
			await denied.body?.cancel();
			assertEquals(denied.status, 401);

			const res = await fetch(`${server.url}/publish/ns/chat`, {
				method: "POST",
				headers: { "x-key": "secret", "content-type": "application/json" },
				body: JSON.stringify({ v: 1 }),
			});
			assertEquals((await res.json()).recipients, 1);

			await until(() => got.length === 1, "injected message delivered");
			assertEquals(got[0].payload, { v: 1 });
			assertEquals(got[0].from, null);
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("the broadcast injection route crosses namespaces", async () => {
	await withServer({ httpAuth: () => {} }, async (server) => {
		const a = client(server, { namespace: "org-a" });
		const b = client(server, { namespace: "org-b" });
		try {
			const got: SSEMessage[] = [];
			await a.connect();
			await b.connect();
			await a.subscribe("alerts", (m) => got.push(m));
			await b.subscribe("alerts", (m) => got.push(m));

			const res = await fetch(`${server.url}/broadcast/alerts`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ v: 1 }),
			});
			assertEquals((await res.json()).recipients, 2);
			await until(() => got.length === 2, "both namespaces received");
		} finally {
			a.dispose();
			b.dispose();
		}
	});
});

// -------------------------------------------------------------- endpoints

Deno.test("stats reports connections, rooms and namespaces", async () => {
	await withServer({}, async (server) => {
		const a = client(server, { namespace: "org-a", clientId: "a" });
		const b = client(server, { namespace: "org-b", clientId: "b" });
		try {
			await a.connect();
			await b.connect();
			await a.subscribe("chat", () => {});
			await b.subscribe("chat", () => {});
			await until(
				() => server.service.members("chat", "org-b").length === 1,
				"both subscribed",
			);

			const res = await fetch(`${server.url}/stats`);
			const stats = await res.json();
			assertEquals(stats.connections, 2);
			assertEquals(stats.rooms, 1);
			assertEquals(stats.namespaces, { "org-a": 1, "org-b": 1 });
			assertEquals(stats.instanceId, server.service.instanceId);
		} finally {
			a.dispose();
			b.dispose();
		}
	});
});

Deno.test("stats is guarded when httpAuth is supplied", async () => {
	await withServer({
		httpAuth: (req: Request) => {
			if (!req.headers.get("x-key")) return new Response("nope", { status: 401 });
		},
	}, async (server) => {
		const denied = await fetch(`${server.url}/stats`);
		await denied.body?.cancel();
		assertEquals(denied.status, 401);

		const ok = await fetch(`${server.url}/stats`, { headers: { "x-key": "y" } });
		assertEquals((await ok.json()).connections, 0);
	});
});

Deno.test("ping answers 204 when unauthenticated servers accept everyone", async () => {
	await withServer({}, async (server) => {
		const res = await fetch(`${server.url}/ping`);
		await res.body?.cancel();
		assertEquals(res.status, 204);
	});
});

Deno.test("ping reflects the auth verdict", async () => {
	await withServer({
		verify: (payload) => (payload === "good" ? {} : null),
	}, async (server) => {
		const bad = await fetch(`${server.url}/ping`);
		await bad.body?.cancel();
		assertEquals(bad.status, 401);

		const good = await fetch(
			`${server.url}/ping?auth=${encodeURIComponent(JSON.stringify("good"))}`,
		);
		await good.body?.cancel();
		assertEquals(good.status, 204);
	});
});

// ------------------------------------------------------------- validation

Deno.test("the stream requires a namespace", async () => {
	await withServer({}, async (server) => {
		const res = await fetch(`${server.url}/events?clientId=c1`);
		const body = await res.json();
		assertEquals(res.status, 400);
		assertEquals(body.error.code, "bad_request");
	});
});

Deno.test("rpc rejects a malformed body", async () => {
	await withServer({}, async (server) => {
		const res = await fetch(`${server.url}/rpc`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: "not json",
		});
		assertEquals(res.status, 400);
		assertEquals((await res.json()).error.code, "bad_request");
	});
});

Deno.test("rpc rejects an unknown operation", async () => {
	await withServer({}, async (server) => {
		const { status, json } = await rpc(server.url, {
			op: "nonsense",
			clientId: "c1",
			namespace: "ns",
		});
		assertEquals(status, 400);
		assert(String(json.error.message).includes("nonsense"));
	});
});

Deno.test("rpc rejects a publish with no room", async () => {
	await withServer({}, async (server) => {
		const { status } = await rpc(server.url, {
			op: "pub",
			clientId: "c1",
			namespace: "ns",
			payload: {},
		});
		assertEquals(status, 400);
	});
});

Deno.test("over-long room names are rejected", async () => {
	await withServer({ maxRoomNameLength: 10 }, async (server) => {
		const { status, json } = await rpc(server.url, {
			op: "sub",
			clientId: "c1",
			namespace: "ns",
			rooms: ["x".repeat(11)],
		});
		assertEquals(status, 400);
		assert(String(json.error.message).includes("10 characters"));

		const res = await fetch(
			`${server.url}/events?namespace=ns&rooms=${"x".repeat(11)}`,
		);
		await res.body?.cancel();
		assertEquals(res.status, 400);
	});
});

Deno.test("the per-client room cap is enforced", async () => {
	await withServer({ maxRoomsPerClient: 2 }, async (server) => {
		const sse = client(server, { clientId: "c1" });
		try {
			await sse.connect();
			await sse.subscribe("a", () => {});
			await sse.subscribe("b", () => {});
			await assertRejects(
				() => sse.subscribe("c", () => {}),
				SSERemoteError,
			);
			// The refused room must not linger locally.
			assertEquals(sse.isSubscribed("c"), false);
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("empty room names are rejected", async () => {
	await withServer({}, async (server) => {
		const { status } = await rpc(server.url, {
			op: "sub",
			clientId: "c1",
			namespace: "ns",
			rooms: [""],
		});
		assertEquals(status, 400);
	});
});

Deno.test("a malformed auth payload is a 400, not a 401", async () => {
	await withServer({ verify: () => ({}) }, async (server) => {
		const res = await fetch(`${server.url}/events?namespace=ns&auth=%7Bnope`);
		assertEquals(res.status, 400);
		assertEquals((await res.json()).error.code, "bad_request");
	});
});

// ------------------------------------------------------------- wire format

Deno.test("the stream opens with retry then hello", async () => {
	await withServer({ retryInterval: 4_000 }, async (server) => {
		const events = await readStream(
			`${server.url}/events?clientId=c1&namespace=ns`,
			{ count: 1 },
		);
		assertEquals(events.length, 1);
		assertEquals(events[0].event, "hello");
		assertEquals(events[0].id, null, "hello must not move the resume point");

		const hello = events[0].json as Record<string, unknown>;
		assertEquals(hello.clientId, "c1");
		assertEquals(hello.namespace, "ns");
		assertEquals(hello.protocol, 1);
		assertEquals(hello.resumed, false);
		assertEquals(hello.gap, false);
	});
});

Deno.test("messages carry an instance-prefixed id", async () => {
	await withServer({}, async (server) => {
		const reading = readStream(
			`${server.url}/events?clientId=c1&namespace=ns&rooms=chat`,
			{ count: 2 },
		);
		await until(
			() => server.service.members("chat", "ns").length === 1,
			"subscribed",
		);
		await server.service.publish("chat", { v: 1 }, "ns");

		const events = await reading;
		assertEquals(events[1].event, "message");
		assertEquals(events[1].id, `${server.service.instanceId}:1`);
		assertEquals((events[1].json as SSEMessage).payload, { v: 1 });
	});
});

Deno.test("heartbeats are real events and carry no id", async () => {
	await withServer({ heartbeatInterval: 60 }, async (server) => {
		const events = await readStream(
			`${server.url}/events?clientId=c1&namespace=ns`,
			{ count: 3 },
		);
		assertEquals(events[0].event, "hello");
		assertEquals(events[1].event, "heartbeat");
		assertEquals(events[2].event, "heartbeat");
		assertEquals(events[1].id, null, "a heartbeat must not move the resume point");
		assert((events[1].json as { timestamp: number }).timestamp > 0);
	});
});

Deno.test("the client observes heartbeats", async () => {
	await withServer({ heartbeatInterval: 60 }, async (server) => {
		const sse = client(server);
		try {
			let beats = 0;
			sse.on("heartbeat", () => beats++);
			await sse.connect();
			await until(() => beats >= 2, "heartbeats observed");
		} finally {
			sse.dispose();
		}
	});
});

Deno.test("the stream sets the headers proxies need", async () => {
	await withServer({}, async (server) => {
		const controller = new AbortController();
		try {
			const res = await fetch(
				`${server.url}/events?clientId=c1&namespace=ns`,
				{ signal: controller.signal },
			);
			assertEquals(
				res.headers.get("content-type"),
				"text/event-stream; charset=utf-8",
			);
			assertEquals(res.headers.get("cache-control"), "no-cache, no-transform");
			assertEquals(res.headers.get("x-accel-buffering"), "no");
			await res.body?.cancel();
		} finally {
			controller.abort();
		}
	});
});

// ----------------------------------------------------------- upstream only

Deno.test("publish works without ever opening a stream", async () => {
	// The point of the client minting its own id: upstream calls are ordinary
	// HTTP and need no stream at all, so a publish-only client never opens one.
	await withServer({}, async (server) => {
		const receiver = client(server, { clientId: "rx" });
		const sender = client(server, { clientId: "tx", autoConnect: false });
		try {
			const got: SSEMessage[] = [];
			await receiver.connect();
			await receiver.subscribe("chat", (m) => got.push(m));

			const { recipients } = await sender.publish("chat", { v: 1 });
			assertEquals(recipients, 1);
			assertEquals(sender.connectionState, "idle", "sender never connected");
			assertEquals(server.service.stats().connections, 1);

			await until(() => got.length === 1, "delivered from an unconnected sender");
			assertEquals(got[0].from, "tx");
		} finally {
			receiver.dispose();
			sender.dispose();
		}
	});
});

Deno.test("subscribing against an unknown stream is not an error", async () => {
	// Rooms ride the connect url, so a sub for a stream that is not open is a
	// no-op the next connect will cover — failing it would make the client
	// discard a handler over a transient race.
	await withServer({}, async (server) => {
		const { status, json } = await rpc(server.url, {
			op: "sub",
			clientId: "never-connected",
			namespace: "ns",
			rooms: ["chat"],
		});
		assertEquals(status, 200);
		assertEquals(json.ok, true);
	});
});

Deno.test("a sub against a stream in another namespace is refused", async () => {
	await withServer({}, async (server) => {
		const sse = client(server, { clientId: "c1", namespace: "ns" });
		try {
			await sse.connect();
			const { status, json } = await rpc(server.url, {
				op: "sub",
				clientId: "c1",
				namespace: "other",
				rooms: ["chat"],
			});
			assertEquals(status, 403);
			assertEquals(json.error.code, "forbidden");
		} finally {
			sse.dispose();
		}
	});
});
