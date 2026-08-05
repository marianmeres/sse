/**
 * The Redis adapter, exercised against an in-memory fake that mimics the one
 * property of Redis pub/sub the adapter exists to handle: every subscriber
 * gets every message, **including the publisher's own connection** — which is
 * exactly why self-echo suppression is the adapter's job.
 */

import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";

import { createSSEClient, type SSEClient } from "../src/client/sse-client.ts";
import { SSEError } from "../src/protocol/errors.ts";
import type { SSEMessage } from "../src/protocol/frames.ts";
import type { RedisLike } from "../src/adapters/redis.ts";
import { SSEPubSubRedis } from "../src/adapters/redis.ts";
import type { SSEBroadcastEnvelope } from "../src/server/mod.ts";
import { createSSEApp, SSEService } from "../src/server/mod.ts";
import { sleep, startServer, until } from "./_helpers.ts";

// ------------------------------------------------------------- fake redis

type WireListener = (message: string, channel: string) => void;

/** The shared "server": one process-wide pub/sub bus. */
class FakeRedisHub {
	listeners = new Map<string, Set<WireListener>>();
	/** Every connection ever created, duplicates included. */
	clients: FakeRedisClient[] = [];
	subscribeCalls = 0;

	publish(channel: string, message: string): void {
		// Snapshot, then deliver async — like a real network, not a call stack.
		for (const listener of [...(this.listeners.get(channel) ?? [])]) {
			queueMicrotask(() => listener(message, channel));
		}
	}

	liveSubscriptions(): number {
		return [...this.listeners.values()].reduce((n, s) => n + s.size, 0);
	}
}

/** One connection to the hub, implementing the adapter's structural contract. */
class FakeRedisClient implements RedisLike {
	#hub: FakeRedisHub;
	#open = false;
	#subs = new Map<string, Set<WireListener>>();

	/** Attached `"error"` listeners — the adapter must add one per connection. */
	errorListeners: ((err: unknown) => void)[] = [];
	/** Make `connect()` reject, mimicking an unreachable or refusing server. */
	failConnect = false;
	/** Make `subscribe()` reject, mimicking e.g. an ACL denying SUBSCRIBE. */
	failSubscribe = false;

	constructor(hub: FakeRedisHub) {
		this.#hub = hub;
		hub.clients.push(this);
	}

	get isOpen(): boolean {
		return this.#open;
	}

	connect(): Promise<unknown> {
		if (this.failConnect) return Promise.reject(new Error("connect refused"));
		this.#open = true;
		return Promise.resolve(this);
	}

	duplicate(): RedisLike {
		const twin = new FakeRedisClient(this.#hub);
		twin.failConnect = this.failConnect;
		twin.failSubscribe = this.failSubscribe;
		return twin;
	}

	publish(channel: string, message: string): Promise<unknown> {
		if (!this.#open) return Promise.reject(new Error("client not open"));
		// RESP2 semantics: a subscribed connection can issue no other commands.
		// This restriction is the entire reason the adapter duplicate()s.
		if (this.#subs.size) {
			return Promise.reject(new Error("PUBLISH not allowed in subscriber mode"));
		}
		this.#hub.publish(channel, message);
		return Promise.resolve(1);
	}

	subscribe(channel: string, listener: WireListener): Promise<unknown> {
		if (!this.#open) return Promise.reject(new Error("client not open"));
		if (this.failSubscribe) return Promise.reject(new Error("NOPERM subscribe"));
		this.#hub.subscribeCalls++;
		let set = this.#hub.listeners.get(channel);
		if (!set) this.#hub.listeners.set(channel, set = new Set());
		set.add(listener);
		let mine = this.#subs.get(channel);
		if (!mine) this.#subs.set(channel, mine = new Set());
		mine.add(listener);
		return Promise.resolve();
	}

	destroy(): unknown {
		this.#open = false;
		for (const [channel, mine] of this.#subs) {
			const set = this.#hub.listeners.get(channel);
			for (const listener of mine) set?.delete(listener);
		}
		this.#subs.clear();
		return undefined;
	}

	on(_event: "error", listener: (err: unknown) => void): unknown {
		this.errorListeners.push(listener);
		return this;
	}
}

function adapter(
	hub: FakeRedisHub,
	channel = "test",
): { adapter: SSEPubSubRedis; client: FakeRedisClient } {
	const client = new FakeRedisClient(hub);
	return { adapter: new SSEPubSubRedis({ client, channel, logger: null }), client };
}

function envelope(
	payload: unknown,
	namespace: string | null = "ns",
): SSEBroadcastEnvelope {
	return {
		namespace,
		message: {
			room: "chat",
			namespace: namespace ?? "*",
			from: null,
			payload,
			timestamp: 1,
		},
	};
}

// ------------------------------------------------------------ conformance

Deno.test("redis adapter: publish before init() rejects", async () => {
	const hub = new FakeRedisHub();
	const { adapter: a } = adapter(hub);
	await assertRejects(() => a.publish(envelope({ v: 1 })), SSEError, "not initialized");
	await a.close();
});

Deno.test("redis adapter: init() is idempotent and concurrency-safe", async () => {
	const hub = new FakeRedisHub();
	const { adapter: a } = adapter(hub);
	await Promise.all([a.init(), a.init()]);
	await a.init();
	assertEquals(hub.subscribeCalls, 1, "one shared subscription");
	await a.close();
});

Deno.test("redis adapter: a frame crosses adapters and never echoes back", async () => {
	const hub = new FakeRedisHub();
	const { adapter: a } = adapter(hub);
	const { adapter: b } = adapter(hub);
	await a.init();
	await b.init();

	const atA: SSEBroadcastEnvelope[] = [];
	const atB: SSEBroadcastEnvelope[] = [];
	a.onRemote((e) => atA.push(e));
	b.onRemote((e) => atB.push(e));

	const sent = envelope({ v: 1 });
	await a.publish(sent);

	await until(() => atB.length === 1, "peer received the envelope");
	assertEquals(atB[0], sent, "envelope crosses verbatim");
	await sleep(20);
	assertEquals(atA.length, 0, "own echo is suppressed");

	await a.close();
	await b.close();
});

Deno.test("redis adapter: a broadcast envelope (namespace null) crosses intact", async () => {
	const hub = new FakeRedisHub();
	const { adapter: a } = adapter(hub);
	const { adapter: b } = adapter(hub);
	await a.init();
	await b.init();

	const atB: SSEBroadcastEnvelope[] = [];
	b.onRemote((e) => atB.push(e));

	await a.publish(envelope({ v: 2 }, null));
	await until(() => atB.length === 1, "broadcast crossed");
	assertEquals(atB[0].namespace, null);

	await a.close();
	await b.close();
});

Deno.test("redis adapter: adapters on different channels do not see each other", async () => {
	const hub = new FakeRedisHub();
	const { adapter: a } = adapter(hub, "app-a");
	const { adapter: b } = adapter(hub, "app-b");
	await a.init();
	await b.init();

	const atB: SSEBroadcastEnvelope[] = [];
	b.onRemote((e) => atB.push(e));

	await a.publish(envelope({ v: 1 }));
	await sleep(20);
	assertEquals(atB.length, 0);

	await a.close();
	await b.close();
});

Deno.test("redis adapter: malformed and foreign frames are dropped, subscription survives", async () => {
	const hub = new FakeRedisHub();
	const { adapter: a } = adapter(hub);
	const { adapter: b } = adapter(hub);
	await a.init();
	await b.init();

	const atB: SSEBroadcastEnvelope[] = [];
	b.onRemote((e) => atB.push(e));

	// Raw garbage straight onto the channel, bypassing any adapter.
	const raw = new FakeRedisClient(hub);
	await raw.connect();
	await raw.publish("test", "not json at all");
	await raw.publish("test", JSON.stringify({ foo: "bar" }));
	await raw.publish("test", JSON.stringify({ origin: 42, envelope: {} }));
	await raw.publish("test", JSON.stringify({ origin: "x", envelope: null }));
	await raw.publish(
		"test",
		JSON.stringify({ origin: "x", envelope: { namespace: 1, message: {} } }),
	);
	// Passes every type check except the room — undeliverable, so it must be
	// dropped before it can occupy peers' replay buffers.
	await raw.publish(
		"test",
		JSON.stringify({ origin: "x", envelope: { namespace: "ns", message: {} } }),
	);
	await sleep(20);
	assertEquals(atB.length, 0, "nothing malformed got through");

	// And a legitimate frame afterwards proves the handler survived it all.
	await a.publish(envelope({ v: 3 }));
	await until(() => atB.length === 1, "valid frame still delivered");
	raw.destroy();

	await a.close();
	await b.close();
});

Deno.test("redis adapter: multiple sinks each receive; unsubscribing one keeps the rest", async () => {
	const hub = new FakeRedisHub();
	const { adapter: a } = adapter(hub);
	const { adapter: b } = adapter(hub);
	await a.init();
	await b.init();

	const first: SSEBroadcastEnvelope[] = [];
	const second: SSEBroadcastEnvelope[] = [];
	const offFirst = b.onRemote((e) => first.push(e));
	b.onRemote((e) => second.push(e));

	await a.publish(envelope({ v: 1 }));
	await until(() => first.length === 1 && second.length === 1, "both sinks fired");

	offFirst();
	await a.publish(envelope({ v: 2 }));
	await until(() => second.length === 2, "remaining sink still fires");
	await sleep(20);
	assertEquals(first.length, 1, "removed sink stayed silent");

	await a.close();
	await b.close();
});

Deno.test("redis adapter: a throwing sink does not starve the others", async () => {
	const hub = new FakeRedisHub();
	const { adapter: a } = adapter(hub);
	const { adapter: b } = adapter(hub);
	await a.init();
	await b.init();

	const got: SSEBroadcastEnvelope[] = [];
	b.onRemote(() => {
		throw new Error("misbehaving sink");
	});
	b.onRemote((e) => got.push(e));

	await a.publish(envelope({ v: 1 }));
	await until(() => got.length === 1, "second sink still delivered");

	await a.close();
	await b.close();
});

Deno.test("redis adapter: close() destroys both connections and is idempotent", async () => {
	const hub = new FakeRedisHub();
	const { adapter: a, client } = adapter(hub);
	await a.init();
	assert(client.isOpen);

	await a.close();
	assertEquals(client.isOpen, false, "provided client destroyed (adapter owns it)");
	assertEquals(
		[...hub.listeners.values()].reduce((n, s) => n + s.size, 0),
		0,
		"subscriber connection deregistered",
	);

	await a.close(); // second close is a no-op
	await assertRejects(() => a.publish(envelope({ v: 1 })), SSEError, "closed");
	await assertRejects(() => a.init(), SSEError, "closed");
});

Deno.test("redis adapter: close() before init() resolves quietly", async () => {
	const hub = new FakeRedisHub();
	const { adapter: a, client } = adapter(hub);
	await a.close();
	assertEquals(client.isOpen, false, "never-connected client stays closed");
});

Deno.test("redis adapter: close() racing an in-flight init() leaves nothing behind", async () => {
	const hub = new FakeRedisHub();
	const { adapter: a, client } = adapter(hub);
	const init = a.init(); // deliberately not awaited
	await a.close();
	await init.catch(() => {});
	assertEquals(client.isOpen, false, "provided client destroyed");
	assertEquals(hub.liveSubscriptions(), 0, "no subscription finished materializing");
	for (const c of hub.clients) {
		assertEquals(c.isOpen, false, "no connection leaked past close()");
	}
});

Deno.test("redis adapter: attaches an error listener to both connections", async () => {
	// A node-redis client emitting a socket error with no listener kills the
	// process — and nobody but the adapter can ever reach the duplicate.
	const hub = new FakeRedisHub();
	const { adapter: a, client } = adapter(hub);
	await a.init();
	assertEquals(hub.clients.length, 2, "publisher + duplicated subscriber");
	assertEquals(client.errorListeners.length, 1);
	assertEquals(hub.clients[1].errorListeners.length, 1);
	await a.close();
});

Deno.test("redis adapter: a failed init() stays failed and releases what it connected", async () => {
	const hub = new FakeRedisHub();
	const client = new FakeRedisClient(hub);
	client.failSubscribe = true; // publisher connects fine; subscriber cannot SUBSCRIBE
	const a = new SSEPubSubRedis({ client, channel: "test", logger: null });

	const first = await assertRejects(() => a.init(), Error, "NOPERM");
	assertEquals(client.isOpen, false, "failed init released the connected publisher");
	for (const c of hub.clients) assertEquals(c.isOpen, false);

	const second = await assertRejects(() => a.init(), Error, "NOPERM");
	assert(first === second, "the attempt is memoized — same rejection, no retry");

	await assertRejects(() => a.publish(envelope({ v: 1 })), Error, "NOPERM");
	await a.close(); // resolves quietly after a failed init
});

Deno.test("redis adapter: init() propagates a connect failure", async () => {
	const hub = new FakeRedisHub();
	const client = new FakeRedisClient(hub);
	client.failConnect = true;
	const a = new SSEPubSubRedis({ client, channel: "test", logger: null });
	await assertRejects(() => a.init(), Error, "connect refused");
	await a.close();
});

// ------------------------------------------------- end-to-end with services

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

Deno.test("redis adapter: two services fan out, no double delivery at the origin", async () => {
	const hub = new FakeRedisHub();
	const { adapter: adapterA, client: clientA } = adapter(hub);
	const { adapter: adapterB, client: clientB } = adapter(hub);
	await adapterA.init();
	await adapterB.init();

	const a = startServer({ adapter: adapterA });
	const b = startServer({ adapter: adapterB });
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
		assertEquals(recipients, 1, "recipients counts stay instance-local");

		await until(() => gotB.length === 1, "peer instance delivered it");
		assertEquals(gotB[0].payload, { v: 1 });

		await sleep(30);
		assertEquals(gotA.length, 1, "origin delivered exactly once — no redis echo");
	} finally {
		sseA.dispose();
		sseB.dispose();
		await a.stop();
		await b.stop();
	}

	// The seam contract: SSEService.close() (inside stop()) closes its adapter.
	assertEquals(clientA.isOpen, false, "service.close() closed adapter A");
	assertEquals(clientB.isOpen, false, "service.close() closed adapter B");
	assertEquals(hub.liveSubscriptions(), 0, "no redis subscription survived");
});

// --------------------------------------------------- service injection seam

Deno.test("createSSEApp refuses service combined with service options", async () => {
	const service = new SSEService({ logger: null, heartbeatInterval: 0 });
	try {
		// The trap this closes: injecting a service while passing `verify`,
		// which would otherwise be silently ignored — shipping unauthenticated.
		assertThrows(
			() => createSSEApp("/sse", [], { service, verify: () => null }),
			SSEError,
			"cannot be combined",
		);
	} finally {
		await service.close();
	}
});

Deno.test("createSSEApp mounts over an injected service", async () => {
	const service = new SSEService({ logger: null, heartbeatInterval: 0 });
	const { app, service: returned } = createSSEApp("/sse", [], {
		service,
		deminoOptions: { logger: null },
	});
	assert(returned === service, "the injected service is the app's service");

	const server = Deno.serve(
		{ port: 0, onListen: () => {}, hostname: "127.0.0.1" },
		app,
	);
	const url = `http://127.0.0.1:${(server.addr as Deno.NetAddr).port}/sse`;
	const sse = client(url);
	try {
		const got: SSEMessage[] = [];
		await sse.connect();
		await sse.subscribe("chat", (m) => got.push(m));

		await service.publish("chat", { v: 1 }, "ns");
		await until(() => got.length === 1, "publish on the injected service arrived");
	} finally {
		sse.dispose();
		await service.close();
		await server.shutdown();
	}
});
