/**
 * Edge paths: the error branches, the adapter seam, and the client behaviours
 * that only show up when something misbehaves.
 */

import assert from "node:assert/strict";
import { test } from "node:test";

import { createSSEClient, type SSEClient } from "../src/client/sse-client.ts";
import { SSEError, SSERemoteError } from "../src/protocol/errors.ts";
import type { SSEMessage } from "../src/protocol/frames.ts";
import type {
	SSEBroadcastEnvelope,
	SSEPubSubAdapter,
} from "../src/server/adapters/abstract.ts";
import { listen, rejection, rpc, sleep, startServer, until } from "./_helpers.ts";

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

// ------------------------------------------------------------ adapter seam

/** An in-memory stand-in for a Redis fan-out, wiring two services together. */
function createTestBus(): { adapter(): SSEPubSubAdapter } {
	const sinks = new Set<(e: SSEBroadcastEnvelope) => void>();

	return {
		adapter(): SSEPubSubAdapter {
			let mine: ((e: SSEBroadcastEnvelope) => void) | null = null;
			return {
				publish(envelope) {
					// Local delivery is the service's job; an adapter only gossips.
					for (const sink of sinks) if (sink !== mine) sink(envelope);
					return Promise.resolve();
				},
				onRemote(cb) {
					mine = cb;
					sinks.add(cb);
					return () => sinks.delete(cb);
				},
				close() {
					if (mine) sinks.delete(mine);
					return Promise.resolve();
				},
			};
		},
	};
}

test("an adapter propagates a publish to a peer instance", async () => {
	const bus = createTestBus();
	const a = await startServer({ adapter: bus.adapter() });
	const b = await startServer({ adapter: bus.adapter() });
	const sse = client(b.url, { clientId: "c1" });
	try {
		const got: SSEMessage[] = [];
		await sse.connect();
		await sse.subscribe("chat", (m) => got.push(m));

		// Published on the instance the client is *not* connected to.
		const recipients = await a.service.publish("chat", { v: 1 }, "ns");
		assert.deepEqual(recipients, 0, "recipients counts are instance-local");

		await until(() => got.length === 1, "peer instance delivered it");
		assert.deepEqual(got[0].payload, { v: 1 });
	} finally {
		sse.dispose();
		await a.stop();
		await b.stop();
	}
});

test("an adapter propagates a broadcast to a peer instance", async () => {
	const bus = createTestBus();
	const a = await startServer({ adapter: bus.adapter() });
	const b = await startServer({ adapter: bus.adapter() });
	const sse = client(b.url, { namespace: "org-b" });
	try {
		const got: SSEMessage[] = [];
		await sse.connect();
		await sse.subscribe("alerts", (m) => got.push(m));

		await a.service.broadcast("alerts", { v: 1 });
		await until(() => got.length === 1, "broadcast crossed the instance");
		assert.deepEqual(got[0].namespace, "org-b");
	} finally {
		sse.dispose();
		await a.stop();
		await b.stop();
	}
});

test("a failing adapter does not fail the publish", async () => {
	// Local delivery already happened; a peer that cannot be reached is a
	// logged problem, not a caller-visible one.
	const broken: SSEPubSubAdapter = {
		publish: () => Promise.reject(new Error("peer unreachable")),
		onRemote: () => () => {},
		close: () => Promise.resolve(),
	};
	const server = await startServer({ adapter: broken });
	const sse = client(server.url);
	try {
		const got: SSEMessage[] = [];
		await sse.connect();
		await sse.subscribe("chat", (m) => got.push(m));

		const { recipients } = await sse.publish("chat", { v: 1 });
		assert.deepEqual(recipients, 1);
		await until(() => got.length === 1, "local delivery is unaffected");
	} finally {
		sse.dispose();
		await server.stop();
	}
});

// --------------------------------------------------------- server branches

test("rpc rejects a body with no operation", async () => {
	const server = await startServer({});
	try {
		const { status } = await rpc(server.url, { clientId: "c1", namespace: "ns" });
		assert.deepEqual(status, 400);

		const bare = await rpc(server.url, null);
		assert.deepEqual(bare.status, 400);
	} finally {
		await server.stop();
	}
});

test("rpc rejects a missing clientId or namespace", async () => {
	const server = await startServer({});
	try {
		const noClient = await rpc(server.url, {
			op: "pub",
			namespace: "ns",
			room: "chat",
			payload: {},
		});
		assert.deepEqual(noClient.status, 400);
		assert.ok(String(noClient.json.error.message).includes("clientId"));

		const noNamespace = await rpc(server.url, {
			op: "pub",
			clientId: "c1",
			room: "chat",
			payload: {},
		});
		assert.deepEqual(noNamespace.status, 400);
		assert.ok(String(noNamespace.json.error.message).includes("namespace"));
	} finally {
		await server.stop();
	}
});

test("rpc enforces auth on upstream calls, not just the stream", async () => {
	// Every request is a separate HTTP call with no session to trust, so the
	// write path has to be verified in its own right.
	const server = await startServer({ verify: (p) => (p === "good" ? {} : null) });
	try {
		const denied = await rpc(server.url, {
			op: "pub",
			clientId: "c1",
			namespace: "ns",
			room: "chat",
			payload: {},
		});
		assert.deepEqual(denied.status, 401);

		const allowed = await rpc(server.url, {
			op: "pub",
			clientId: "c1",
			namespace: "ns",
			room: "chat",
			payload: {},
		}, { "x-sse-auth": JSON.stringify("good") });
		assert.deepEqual(allowed.status, 200);
	} finally {
		await server.stop();
	}
});

test("broadcast without a room is rejected", async () => {
	const server = await startServer({ allowBroadcast: () => true });
	try {
		const { status } = await rpc(server.url, {
			op: "broadcast",
			clientId: "c1",
			namespace: "ns",
			payload: {},
		});
		assert.deepEqual(status, 400);
	} finally {
		await server.stop();
	}
});

test("a throwing allowBroadcast denies rather than crashing", async () => {
	const server = await startServer({
		allowBroadcast: () => {
			throw new Error("policy lookup failed");
		},
	});
	const sse = client(server.url);
	try {
		await sse.connect();
		const error = await rejection(
			() => sse.broadcast("alerts", {}),
			SSERemoteError,
		);
		assert.deepEqual(error.status, 403);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("a throwing verify rejects rather than crashing", async () => {
	const server = await startServer({
		verify: () => {
			throw new Error("token service down");
		},
	});
	try {
		const res = await fetch(`${server.url}/ping`);
		await res.body?.cancel();
		assert.deepEqual(res.status, 401);
	} finally {
		await server.stop();
	}
});

test("unsubscribing from a room that is not held is a no-op", async () => {
	const server = await startServer({});
	const sse = client(server.url, { clientId: "c1" });
	try {
		await sse.connect();
		const { status, json } = await rpc(server.url, {
			op: "unsub",
			clientId: "c1",
			namespace: "ns",
			rooms: ["never-joined"],
		});
		assert.deepEqual(status, 200);
		assert.deepEqual(json.ok, true);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("too many rooms in the connect url is rejected", async () => {
	const server = await startServer({ maxRoomsPerClient: 3 });
	try {
		const rooms = ["a", "b", "c", "d"].join(",");
		const res = await fetch(`${server.url}/events?namespace=ns&rooms=${rooms}`);
		const body = await res.json();
		assert.deepEqual(res.status, 400);
		assert.ok(String(body.error.message).includes("3 rooms"));
	} finally {
		await server.stop();
	}
});

test("the injection route rejects an invalid JSON body", async () => {
	const server = await startServer({ httpAuth: () => {} });
	try {
		const res = await fetch(`${server.url}/publish/ns/chat`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: "{{{",
		});
		await res.body?.cancel();
		assert.deepEqual(res.status, 500);
	} finally {
		await server.stop();
	}
});

test("closing the service twice is a no-op", async () => {
	const server = await startServer({});
	await server.service.close();
	await server.service.close();
	await server.stop();
});

test("a cancelled stream drops the connection", async () => {
	const server = await startServer({});
	try {
		const controller = new AbortController();
		const res = await fetch(
			`${server.url}/events?clientId=c1&namespace=ns&rooms=chat`,
			{ signal: controller.signal },
		);
		const reader = res.body!.getReader();
		await reader.read();
		await until(() => server.service.stats().connections === 1, "registered");

		await reader.cancel();
		controller.abort();
		await until(
			() => server.service.stats().connections === 0,
			"cancel released the connection",
		);
	} finally {
		await server.stop();
	}
});

// --------------------------------------------------------- client branches

test("client accepts a URL instance", () => {
	const sse = createSSEClient({
		url: new URL("http://example.test/sse"),
		namespace: "ns",
		logger: null,
	});
	try {
		assert.deepEqual(sse.url.href, "http://example.test/sse");
	} finally {
		sse.dispose();
	}
});

test("connect on an already connected client resolves immediately", async () => {
	const server = await startServer({});
	const sse = client(server.url);
	try {
		await sse.connect();
		await sse.connect();
		assert.deepEqual(server.service.stats().connections, 1);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("once fires exactly one time", async () => {
	const server = await startServer({});
	const sse = client(server.url);
	try {
		let calls = 0;
		sse.once("connected", () => calls++);
		await sse.connect();
		sse.disconnect();
		await sse.connect();
		await sleep(50);
		assert.deepEqual(calls, 1);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("subscribe starts the connection when autoConnect is on", async () => {
	const server = await startServer({});
	const sse = client(server.url, { clientId: "c1" });
	try {
		assert.deepEqual(sse.connectionState, "idle");
		await sse.subscribe("chat", () => {});
		await until(() => sse.connected, "subscribe brought the stream up");
		await until(
			() => server.service.members("chat", "ns").length === 1,
			"and registered the room",
		);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("subscribe does not connect when autoConnect is off", async () => {
	const server = await startServer({});
	const sse = client(server.url, { autoConnect: false });
	try {
		await sse.subscribe("chat", () => {});
		await sleep(100);
		assert.deepEqual(sse.connectionState, "idle");
		assert.deepEqual(sse.isSubscribed("chat"), true);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("unsubscribe while disconnected only touches local state", async () => {
	const server = await startServer({});
	const sse = client(server.url, { autoConnect: false });
	try {
		await sse.subscribe("chat", () => {});
		await sse.unsubscribe("chat");
		assert.deepEqual(sse.isSubscribed("chat"), false);
		assert.deepEqual(sse.connectionState, "idle");
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("rooms subscribed mid-connect are reconciled after hello", async () => {
	// The connect url was already built, so this room missed it — hello has to
	// notice and catch it up.
	const server = await startServer({});
	const sse = client(server.url, { clientId: "c1", autoConnect: false });
	try {
		const connecting = sse.connect();
		await sse.subscribe("late", () => {});
		await connecting;

		await until(
			() => server.service.members("late", "ns").length === 1,
			"late room reconciled",
		);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("rooms unsubscribed mid-connect are reconciled after hello", async () => {
	const server = await startServer({});
	const sse = client(server.url, {
		clientId: "c1",
		rooms: ["preset"],
		autoConnect: false,
	});
	try {
		const connecting = sse.connect();
		await sse.unsubscribe("preset");
		await connecting;

		await until(
			() => server.service.members("preset", "ns").length === 0,
			"stale room reconciled away",
		);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("a throwing message handler surfaces as an error event", async () => {
	const server = await startServer({});
	const sse = client(server.url);
	try {
		const errors: Error[] = [];
		sse.on("error", (e) => errors.push(e));

		await sse.connect();
		await sse.subscribe("chat", () => {
			throw new Error("handler blew up");
		});
		await server.service.publish("chat", { v: 1 }, "ns");

		await until(() => errors.length === 1, "handler error reported");
		assert.deepEqual(errors[0].message, "handler blew up");
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("a throwing auth is retried, not fatal", async () => {
	const server = await startServer({});
	let broken = true;
	const sse = client(server.url, {
		auth: () => {
			if (broken) throw new Error("token service down");
			return "ok";
		},
	});
	try {
		const errors: Error[] = [];
		sse.on("error", (e) => errors.push(e));

		const connecting = sse.connect();
		await until(() => errors.length >= 1, "auth failure reported");
		assert.deepEqual(sse.connectionState, "reconnecting");

		broken = false;
		await connecting;
		assert.deepEqual(sse.connected, true);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("the client reports a replay gap", async () => {
	const server = await startServer({ replayBufferSize: 1 });
	const sse = client(server.url, { clientId: "c1" });
	try {
		await sse.connect();
		await sse.subscribe("chat", () => {});
		await server.service.publish("chat", { n: 1 }, "ns");
		await until(() => !!sse.lastEventId, "resume point captured");

		sse.disconnect();
		await until(() => server.service.stats().connections === 0, "disconnected");
		for (const n of [2, 3, 4]) {
			await server.service.publish("chat", { n }, "ns");
		}

		await sse.connect();
		await until(() => sse.dump().gap === true, "gap reported to the app");
	} finally {
		sse.dispose();
		await server.stop();
	}
});

// ------------------------------------------------------------- probe paths

test("an unreachable probe is treated as retryable", async () => {
	// The probe answers "why did the stream fail?". When it cannot answer at
	// all that is a network problem, and network problems are always retried.
	let failEvents = true;
	let hangPing = true;

	const server = await listen(
		async (req) => {
			const url = new URL(req.url);
			if (url.pathname === "/sse/ping") {
				// Hang until the client's fetchTimeout aborts us. Honouring the
				// signal matters: otherwise the handler outlives the test and
				// blocks shutdown.
				if (hangPing) {
					await new Promise((resolve) => {
						const timer = setTimeout(resolve, 30_000);
						req.signal.addEventListener("abort", () => {
							clearTimeout(timer);
							resolve(undefined);
						});
					});
				}
				return new Response(null, { status: 204 });
			}
			if (failEvents) return new Response("nope", { status: 500 });

			const body = new ReadableStream<Uint8Array>({
				start(ctrl) {
					ctrl.enqueue(
						new TextEncoder().encode(
							`event: hello\ndata: ${
								JSON.stringify({
									clientId: "c1",
									namespace: "ns",
									protocol: 1,
									heartbeatInterval: 0,
									resumed: false,
									gap: false,
								})
							}\n\n`,
						),
					);
				},
			});
			return new Response(body, {
				headers: { "content-type": "text/event-stream" },
			});
		},
	);
	const port = server.port;
	const sse = client(`http://127.0.0.1:${port}/sse`, { fetchTimeout: 100 });

	try {
		let reconnects = 0;
		sse.on("reconnecting", () => reconnects++);

		const connecting = sse.connect();
		await until(() => reconnects >= 1, "retried despite an unanswerable probe");

		failEvents = false;
		hangPing = false;
		await connecting;
		assert.deepEqual(sse.connected, true);
	} finally {
		sse.dispose();
		await server.shutdown();
	}
});

// ------------------------------------------------------- malformed input

/** Serves a stream whose `hello` or `message` payload is not JSON. */
async function startGarbageServer(what: "hello" | "message") {
	const server = await listen(
		(req) => {
			const url = new URL(req.url);
			if (url.pathname === "/sse/ping") return new Response(null, { status: 204 });

			const body = new ReadableStream<Uint8Array>({
				start(ctrl) {
					const enc = new TextEncoder();
					if (what === "hello") {
						ctrl.enqueue(enc.encode("event: hello\ndata: not-json\n\n"));
					} else {
						ctrl.enqueue(enc.encode(
							`event: hello\ndata: ${
								JSON.stringify({
									clientId: "c1",
									namespace: "ns",
									protocol: 1,
									heartbeatInterval: 0,
									resumed: false,
									gap: false,
								})
							}\n\n`,
						));
						ctrl.enqueue(enc.encode(
							"id: x:1\nevent: message\ndata: not-json\n\n",
						));
					}
				},
			});
			return new Response(body, {
				headers: { "content-type": "text/event-stream" },
			});
		},
	);
	const port = server.port;
	return {
		url: `http://127.0.0.1:${port}/sse`,
		stop: () => server.shutdown(),
	};
}

test("a malformed hello is reported rather than crashing", async () => {
	const server = await startGarbageServer("hello");
	const sse = client(server.url);
	try {
		const errors: Error[] = [];
		sse.on("error", (e) => errors.push(e));
		sse.connect().catch(() => {});

		await until(() => errors.length >= 1, "malformed hello reported");
		assert.deepEqual(sse.connected, false);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("a malformed message is reported rather than crashing", async () => {
	const server = await startGarbageServer("message");
	const sse = client(server.url);
	try {
		const errors: Error[] = [];
		sse.on("error", (e) => errors.push(e));

		await sse.connect();
		await until(() => errors.length >= 1, "malformed message reported");
		assert.deepEqual(sse.connected, true, "one bad frame does not kill the stream");
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("a malformed heartbeat still counts as proof of life", async () => {
	const server = await listen(
		(req) => {
			const url = new URL(req.url);
			if (url.pathname === "/sse/ping") return new Response(null, { status: 204 });
			const body = new ReadableStream<Uint8Array>({
				start(ctrl) {
					const enc = new TextEncoder();
					ctrl.enqueue(enc.encode(
						`event: hello\ndata: ${
							JSON.stringify({
								clientId: "c1",
								namespace: "ns",
								protocol: 1,
								heartbeatInterval: 0,
								resumed: false,
								gap: false,
							})
						}\n\n`,
					));
					const beat = () =>
						ctrl.enqueue(enc.encode("event: heartbeat\ndata: nope\n\n"));
					beat();
					const timer = setInterval(beat, 50);
					req.signal.addEventListener("abort", () => clearInterval(timer));
				},
			});
			return new Response(body, {
				headers: { "content-type": "text/event-stream" },
			});
		},
	);
	const port = server.port;
	const sse = client(`http://127.0.0.1:${port}/sse`, { staleTimeout: 250 });
	try {
		await sse.connect();
		// Unparseable, but bytes arrived — the watchdog must not fire.
		await sleep(600);
		assert.deepEqual(sse.connected, true);
	} finally {
		sse.dispose();
		await server.shutdown();
	}
});

// ----------------------------------------------------------------- misc

test("an unsubscriber is idempotent", async () => {
	const server = await startServer({});
	const sse = client(server.url, { clientId: "c1" });
	try {
		await sse.connect();
		const unsub = await sse.subscribe("chat", () => {});
		unsub();
		unsub();
		unsub();
		await until(
			() => server.service.members("chat", "ns").length === 0,
			"unsubscribed once",
		);
	} finally {
		sse.dispose();
		await server.stop();
	}
});

test("a disposed client rejects publish and broadcast", async () => {
	const sse = client("http://example.test/sse");
	sse.dispose();
	await assert.rejects(() => sse.publish("chat", {}), SSEError);
	await assert.rejects(() => sse.broadcast("chat", {}), SSEError);
	await assert.rejects(() => sse.unsubscribe("chat"), SSEError);
	assert.throws(() => sse.connect(), SSEError);
});
