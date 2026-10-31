import assert from "node:assert/strict";
import { createServer } from "node:net";
import { versions } from "node:process";
import type { DeminoServeHandler } from "@marianmeres/demino";
import type { ServeFn, ServeHandle } from "@marianmeres/demino/node";
import { createSSEApp, type SSEAppOptions, type SSEService } from "../src/server/mod.ts";

export interface TestServer {
	/** Base url the client is pointed at. */
	url: string;
	port: number;
	service: SSEService;
	stop(): Promise<void>;
}

let servePromise: Promise<ServeFn> | undefined;

/**
 * The runtime's own demino adapter, so each runtime is tested the way its
 * consumers serve the app: `/node` on Node (npm), `/deno` on Deno (JSR).
 */
function pickServe(): Promise<ServeFn> {
	servePromise ??= versions.deno
		? import("@marianmeres/demino/deno").then((m) => m.serve)
		: import("@marianmeres/demino/node").then((m) => m.serve);
	return servePromise;
}

/**
 * Serves `handler` silently on `127.0.0.1:port` (or an ephemeral port). No
 * WebSocket upgrades: SSE never needs them, and on Node they would mean `ws`.
 */
export async function listen(
	handler: DeminoServeHandler,
	port = 0,
): Promise<ServeHandle> {
	const serve = await pickServe();
	return serve(handler, {
		port,
		hostname: "127.0.0.1",
		onListen: () => {},
		webSockets: false,
	});
}

/** Grabs a port, releases it, and hands back the number. */
export function freePort(): Promise<number> {
	return new Promise((resolve, reject) => {
		const server = createServer();
		server.once("error", reject);
		server.listen(0, "127.0.0.1", () => {
			const { port } = server.address() as { port: number };
			server.close(() => resolve(port));
		});
	});
}

/**
 * Asserts that `fn` rejects with an instance of `ErrorClass` and returns the
 * error, which `assert.rejects` does not.
 */
export async function rejection<E extends Error>(
	fn: () => Promise<unknown>,
	ErrorClass: new (...args: never[]) => E,
): Promise<E> {
	try {
		await fn();
	} catch (e) {
		assert.ok(e instanceof ErrorClass, `expected ${ErrorClass.name}, got ${e}`);
		return e;
	}
	assert.fail(`expected a rejection with ${ErrorClass.name}`);
}

/**
 * Boots the reference server on `port` (or an ephemeral one).
 *
 * Defaults are tuned for tests: a short `retryInterval` so `EventSource`'s own
 * reconnect happens within a test's lifetime, and heartbeats off unless a test
 * asks for them.
 */
export async function startServer(
	options: SSEAppOptions = {},
	port = 0,
): Promise<TestServer> {
	const { app, service } = createSSEApp("/sse", [], {
		logger: null,
		deminoOptions: { logger: null },
		heartbeatInterval: 0,
		retryInterval: 100,
		...options,
	});
	const server = await listen(app, port);

	return {
		url: `http://127.0.0.1:${server.port}/sse`,
		port: server.port,
		service,
		async stop() {
			await service.close();
			await server.shutdown();
		},
	};
}

/** Controls what the next `/events` and `/ping` requests get back. */
export interface FlakyServer {
	url: string;
	port: number;
	/** HTTP status returned by `/events` and `/ping`. `200` streams normally. */
	status: number;
	/** Requests seen at `/events`. */
	hits: number;
	/** Close every open stream without a proper shutdown. */
	dropStreams(): void;
	stop(): Promise<void>;
}

/**
 * A server whose failure mode is settable.
 *
 * Needed because the reference server cannot be made to return a `503` on
 * demand, and telling a retryable failure from a terminal one is the whole
 * point of the probe.
 */
export async function startFlakyServer(port = 0): Promise<FlakyServer> {
	const open = new Set<ReadableStreamDefaultController<Uint8Array>>();
	const enc = new TextEncoder();

	const state: { status: number; hits: number } = { status: 200, hits: 0 };

	const server = await listen(
		(req) => {
			const url = new URL(req.url);

			if (url.pathname === "/sse/ping") {
				return state.status === 200
					? new Response(null, { status: 204 })
					: new Response("nope", { status: state.status });
			}

			if (url.pathname !== "/sse/events") {
				return new Response("not found", { status: 404 });
			}

			state.hits++;
			if (state.status !== 200) {
				return new Response("nope", { status: state.status });
			}

			const body = new ReadableStream<Uint8Array>({
				start(ctrl) {
					open.add(ctrl);
					ctrl.enqueue(enc.encode("retry: 100\n\n"));
					ctrl.enqueue(enc.encode(
						`event: hello\ndata: ${
							JSON.stringify({
								clientId: url.searchParams.get("clientId") ?? "x",
								namespace: url.searchParams.get("namespace") ?? "ns",
								protocol: 1,
								heartbeatInterval: 0,
								resumed: false,
								gap: false,
							})
						}\n\n`,
					));
					req.signal.addEventListener("abort", () => open.delete(ctrl));
				},
				cancel(this: void) {
					// nothing held beyond the set entry, removed on abort
				},
			});
			return new Response(body, {
				headers: { "content-type": "text/event-stream" },
			});
		},
		port,
	);

	const flaky: FlakyServer = {
		url: `http://127.0.0.1:${server.port}/sse`,
		port: server.port,
		get status() {
			return state.status;
		},
		set status(v: number) {
			state.status = v;
		},
		get hits() {
			return state.hits;
		},
		set hits(v: number) {
			state.hits = v;
		},
		dropStreams() {
			for (const ctrl of open) {
				try {
					ctrl.close();
				} catch { /* already gone */ }
			}
			open.clear();
		},
		async stop() {
			flaky.dropStreams();
			await server.shutdown();
		},
	};
	return flaky;
}

/** One parsed SSE event. */
export interface RawEvent {
	id: string | null;
	event: string;
	data: string;
	/** Parsed `data`, or `null` when it was not JSON. */
	json: unknown;
}

/**
 * Reads an SSE stream with a hand-rolled parser.
 *
 * Deliberately independent of the client: these tests assert the *wire*, so
 * going through `EventSource` would only prove the two agree with each other.
 */
export async function readStream(
	url: string,
	options: {
		headers?: Record<string, string>;
		/** Stop once this many events have arrived. */
		count: number;
		timeout?: number;
	},
): Promise<RawEvent[]> {
	const controller = new AbortController();
	const timer = setTimeout(
		() => controller.abort(),
		options.timeout ?? 5_000,
	);

	const events: RawEvent[] = [];
	try {
		const res = await fetch(url, {
			headers: options.headers,
			signal: controller.signal,
		});
		if (!res.ok) {
			await res.body?.cancel();
			throw new Error(`stream request failed with ${res.status}`);
		}

		const reader = res.body!.getReader();
		const decoder = new TextDecoder();
		let buffer = "";

		try {
			while (events.length < options.count) {
				const { done, value } = await reader.read();
				if (done) break;
				buffer += decoder.decode(value, { stream: true });

				let at: number;
				while ((at = buffer.indexOf("\n\n")) !== -1) {
					const block = buffer.slice(0, at);
					buffer = buffer.slice(at + 2);
					const parsed = parseBlock(block);
					if (parsed) events.push(parsed);
				}
				if (events.length >= options.count) break;
			}
		} finally {
			await reader.cancel().catch(() => {});
		}
	} finally {
		clearTimeout(timer);
		controller.abort();
	}

	return events;
}

function parseBlock(block: string): RawEvent | null {
	let id: string | null = null;
	let event = "message";
	const data: string[] = [];
	let sawData = false;

	for (const line of block.split("\n")) {
		if (!line || line.startsWith(":")) continue;
		const colon = line.indexOf(":");
		const field = colon === -1 ? line : line.slice(0, colon);
		const value = colon === -1 ? "" : line.slice(colon + 1).replace(/^ /, "");

		if (field === "id") id = value;
		else if (field === "event") event = value;
		else if (field === "data") {
			data.push(value);
			sawData = true;
		}
	}

	// A lone `retry:` block carries no event.
	if (!sawData) return null;

	const raw = data.join("\n");
	let json: unknown = null;
	try {
		json = JSON.parse(raw);
	} catch { /* not JSON, leave null */ }

	return { id, event, data: raw, json };
}

/** Polls until `predicate` holds, or throws. */
export async function until(
	predicate: () => boolean,
	message = "condition not met",
	timeout = 5_000,
	step = 10,
): Promise<void> {
	const deadline = Date.now() + timeout;
	while (Date.now() < deadline) {
		if (predicate()) return;
		await new Promise((r) => setTimeout(r, step));
	}
	throw new Error(`Timed out waiting: ${message}`);
}

export function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}

/** POSTs an rpc operation without going through the client. */
export async function rpc(
	base: string,
	body: unknown,
	headers: Record<string, string> = {},
	// biome-ignore lint/suspicious/noExplicitAny: tests poke at arbitrary response shapes
): Promise<{ status: number; json: any }> {
	const res = await fetch(`${base}/rpc`, {
		method: "POST",
		headers: { "content-type": "application/json", ...headers },
		body: JSON.stringify(body),
	});
	const json = await res.json().catch(() => null);
	return { status: res.status, json };
}
