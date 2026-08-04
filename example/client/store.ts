/**
 * The feed's state and everything that talks to the wire.
 *
 * Deliberately view-free: it owns an `SSEClient` plus a handful of
 * `@marianmeres/vanilla` observables, and the views in `mod.ts` only read them.
 * Keeping the split means the interesting part — how replay, a room switch, an
 * acknowledged publish and a reconnect actually behave — is readable without
 * any DOM in the way.
 *
 * @module
 */

import { type Observable, observable } from "@marianmeres/vanilla";
// A consumer would write `from "@marianmeres/sse"`. The example points at the
// local source so it always builds against the working tree.
import {
	createSSEClient,
	type SSEClient,
	type SSEHello,
	type SSEMessage,
	type SSEState,
} from "../../src/mod.ts";
import {
	type CommentPayload,
	DEFAULT_ROOM,
	isCommentPayload,
	isEventPayload,
	type Level,
} from "../shared.ts";

/** Cap on rendered lines. The feed is infinite; the DOM should not be. */
const MAX_ENTRIES = 300;

/** Who we are, as entered in the login form. */
export interface Session {
	name: string;
	project: string;
}

/** One rendered line. Not a wire type — the view's shape, nothing more. */
export interface Entry {
	/** De-duplication key; the app-level id when there is one. */
	key: string;
	kind: "event" | "comment" | "system" | "notice" | "error" | "divider";
	level: Level;
	/** Display author or source. */
	who: string;
	text: string;
	ts: number;
	own: boolean;
	/**
	 * `true` when this line arrived through `Last-Event-ID` replay rather than
	 * live. See `markReplayed` below for how that is determined.
	 */
	replayed: boolean;
	/**
	 * Recipient count from the publish acknowledgement, once it lands. `null`
	 * until then (and forever, for messages we did not send).
	 */
	recipients: number | null;
}

/** What the last `hello` told us about resuming. */
export interface ResumeInfo {
	resumed: boolean;
	gap: boolean;
	/** Resume point we presented, for display. */
	from: string | null;
}

/** A client id is `name#suffix`; the suffix only exists to keep ids unique. */
export const nameOf = (clientId: string | null): string =>
	clientId ? clientId.split("#")[0] : "pipeline";

let entrySeq = 0;
const localKey = () => `local-${++entrySeq}`;

/** Stable de-dup key for a wire message: ours if it has one, else from+time. */
function messageKey(msg: SSEMessage): string {
	const { payload } = msg;
	if (isEventPayload(payload) || isCommentPayload(payload)) {
		if (payload.id) return payload.id;
	}
	return `${msg.from ?? "server"}:${msg.timestamp}`;
}

/** Everything a view needs, plus the actions it can invoke. */
export interface FeedStore {
	readonly sse: SSEClient;
	readonly session: Session;
	/** Live connection state, mirrored from the client's Svelte-store `state`. */
	readonly connection: Observable<SSEState>;
	readonly room: Observable<string>;
	readonly entries: Observable<Entry[]>;
	/** What the last `hello` said about resuming. */
	readonly resume: Observable<ResumeInfo>;
	/** `true` while the stream is deliberately closed. */
	readonly paused: Observable<boolean>;
	/** `true` while the server is returning 503 for /sse. */
	readonly maintenance: Observable<boolean>;
	/** `true` while a room switch is in flight. */
	readonly joining: Observable<boolean>;

	join(room: string): Promise<void>;
	comment(text: string, broadcast: boolean): Promise<void>;
	pause(): void;
	unpause(): void;
	setMaintenance(on: boolean): Promise<void>;
	burst(count: number): Promise<void>;
	leave(): void;
}

/**
 * Builds the store and starts connecting.
 *
 * @param session       operator name + project from the login form
 * @param onTerminated  called when the server rejected us for good (`401` /
 *                      `403`) — the one exit that does not retry, so the view
 *                      has to take the user back to the login form.
 */
export function createFeedStore(
	session: Session,
	onTerminated: (reason: string) => void,
): FeedStore {
	// Suffix appended to the name to form the client id. Held in memory, so it
	// is stable across reconnects and unique per tab — two tabs on the same
	// name would otherwise keep replacing each other's stream. The server
	// validates it and has the final say.
	const tab = Math.random().toString(36).slice(2, 8);

	const sse = createSSEClient({
		url: "/sse",
		// The requested namespace. The server's `verify` has the final say — it
		// returns the validated project, and that is what we actually get.
		namespace: session.project,
		// Called before *every* (re)connect **and every upstream call**. A real
		// app would return a fresh token here; refresh needs no other
		// machinery. It must be deterministic for a given session — see the
		// server's `verify`.
		auth: () => ({ name: session.name, project: session.project, tab }),
		// Bounds the `connect()` await in `join()` below. Reconnecting carries
		// on in the background regardless — this only stops the UI from waiting
		// on a server that is not there.
		connectTimeout: 8_000,
		// Short so a wedged stream is noticed quickly in a demo. The default is
		// 45s, comfortably above the server's 15s heartbeat.
		staleTimeout: 30_000,
	});

	const connection = observable<SSEState>({
		state: "idle",
		connected: false,
		connecting: false,
		attempt: 0,
		lastError: null,
	});
	const room = observable<string>(DEFAULT_ROOM);
	const entries = observable<Entry[]>([]);
	const resume = observable<ResumeInfo>({ resumed: false, gap: false, from: null });
	const paused = observable(false);
	const maintenance = observable(false);
	const joining = observable(false);

	/** Keys already rendered — history, replay and live traffic all overlap. */
	let seen = new Set<string>();
	/**
	 * Timestamp of the most recent `hello`.
	 *
	 * Replay only returns messages published *before* the stream opened, so a
	 * message older than this arrived through replay and anything newer is
	 * live. (Same-millisecond publishes are ambiguous; for a label on a demo
	 * line that is fine.)
	 */
	let connectedAt = 0;
	let markReplayed = false;

	// The client's `state` follows the Svelte store contract (fires immediately,
	// then on every change), so bridging it into an observable is one line.
	sse.state.subscribe((s) => connection.set(s));

	sse.on("terminated", ({ status, reason }) => {
		onTerminated(`${reason || "connection refused"} (HTTP ${status})`);
	});

	sse.on("connected", (hello: SSEHello) => {
		connectedAt = Date.now();
		markReplayed = hello.resumed;
		resume.set({
			resumed: hello.resumed,
			gap: hello.gap,
			from: sse.lastEventId,
		});

		if (hello.resumed) {
			divider(
				hello.gap
					? "resumed with a gap — the window overflowed"
					: "resumed — nothing was missed",
			);
		}

		// A gap means messages are genuinely gone from the replay window, and
		// this is the *only* case where the application's own history is
		// needed. Replay handles everything shorter.
		if (hello.gap) void loadHistory(room.get(), { append: false });
	});

	function push(entry: Entry): void {
		if (seen.has(entry.key)) return;
		seen.add(entry.key);
		// Immutable update — the change guard is reference equality, so mutating
		// the array in place would fire nothing.
		entries.update((list) => {
			const next = [...list, entry];
			return next.length > MAX_ENTRIES ? next.slice(-MAX_ENTRIES) : next;
		});
	}

	/** Appends a purely local line — never leaves the browser. */
	function notice(text: string, kind: Entry["kind"] = "notice"): void {
		push({
			key: localKey(),
			kind,
			level: kind === "error" ? "error" : "info",
			who: "",
			text,
			ts: Date.now(),
			own: false,
			replayed: false,
			recipients: null,
		});
	}

	function divider(text: string): void {
		push({
			key: localKey(),
			kind: "divider",
			level: "info",
			who: "",
			text,
			ts: Date.now(),
			own: false,
			replayed: false,
			recipients: null,
		});
	}

	/** Turns a wire message into an entry. */
	function toEntry(msg: SSEMessage): Entry {
		const key = messageKey(msg);
		const fromServer = msg.from === null;
		const replayed = markReplayed && msg.timestamp < connectedAt;

		if (isCommentPayload(msg.payload)) {
			return {
				key,
				kind: "comment",
				level: "info",
				// A server injection may name itself in the payload; peer
				// messages are labelled from the client id, which the server
				// controls and a client cannot spoof.
				who: fromServer ? msg.payload.who : nameOf(msg.from),
				text: msg.payload.text,
				ts: msg.timestamp,
				own: !fromServer && msg.from === sse.clientId,
				replayed,
				recipients: null,
			};
		}

		if (isEventPayload(msg.payload)) {
			return {
				key,
				kind: fromServer ? "event" : "system",
				level: msg.payload.level,
				who: msg.payload.source,
				text: msg.payload.text,
				ts: msg.timestamp,
				own: false,
				replayed,
				recipients: null,
			};
		}

		// Something else entirely — most likely a hand-rolled curl payload.
		// Rendering it raw beats silently dropping it.
		return {
			key,
			kind: "system",
			level: "info",
			who: fromServer ? "server" : nameOf(msg.from),
			text: JSON.stringify(msg.payload),
			ts: msg.timestamp,
			own: false,
			replayed,
			recipients: null,
		};
	}

	const onMessage = (msg: SSEMessage) => push(toEntry(msg));

	/**
	 * Fetches the room backlog over plain HTTP.
	 *
	 * Only used on first join and after a `gap` — replay covers everything in
	 * between, which is the whole reason this package exists next to
	 * `@marianmeres/ws`. See `example/history.ts`.
	 */
	async function loadHistory(
		target: string,
		{ append }: { append: boolean },
	): Promise<void> {
		const ns = sse.namespace;
		try {
			const res = await fetch(
				`/api/history/${encodeURIComponent(ns)}/${encodeURIComponent(target)}`,
			);
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			const { messages } = await res.json() as { messages: SSEMessage[] };
			if (room.get() !== target) return; // user switched away mid-fetch

			const backlog: Entry[] = [];
			for (const msg of messages) {
				const key = messageKey(msg);
				if (seen.has(key)) continue; // already arrived live or by replay
				seen.add(key);
				backlog.push({ ...toEntry(msg), replayed: false });
			}
			if (!backlog.length) return;

			if (append) {
				entries.update((list) => [...list, ...backlog]);
			} else {
				// Backfilling a gap: these are older than what is on screen.
				entries.update((list) => {
					const next = [...backlog, ...list];
					return next.length > MAX_ENTRIES ? next.slice(-MAX_ENTRIES) : next;
				});
			}
		} catch (err) {
			notice(`could not load history: ${(err as Error).message}`, "error");
		}
	}

	async function join(target: string): Promise<void> {
		const previous = room.get();
		joining.set(true);
		try {
			// Gate on a live stream before touching rooms. `connect()` is
			// optional — the first `subscribe()` starts it anyway — but waiting
			// keeps the history fetch below from straddling the handshake.
			try {
				await sse.connect();
			} catch (err) {
				// Either the bounded wait elapsed — retrying carries on in the
				// background — or the server refused us for good, which the
				// `terminated` handler is already dealing with. Go on either
				// way: the room registers locally regardless, and it rides the
				// connect url of the next stream.
				notice(`not connected yet — ${(err as Error).message}`);
			}

			if (target !== previous && sse.isSubscribed(previous)) {
				await sse.unsubscribe(previous);
			}

			room.set(target);
			entries.set([]);
			seen = new Set();

			// Subscribe *before* fetching history. `subscribe()` attaches the
			// handler synchronously, before anything goes out, so a message
			// published while the backlog is in flight is queued into `entries`
			// rather than lost — and the key-based de-dup sorts out the overlap.
			await sse.subscribe(target, onMessage);
			await loadHistory(target, { append: false });
		} catch (err) {
			notice(`could not join #${target}: ${(err as Error).message}`, "error");
		} finally {
			joining.set(false);
		}
	}

	async function comment(text: string, broadcast: boolean): Promise<void> {
		const trimmed = text.trim();
		if (!trimmed) return;

		const target = room.get();
		const payload: CommentPayload = {
			kind: "comment",
			id: crypto.randomUUID(),
			who: session.name,
			text: trimmed,
		};

		// Note there is no "queued" branch here, unlike the ws example. This is
		// an ordinary HTTP request: it works whether or not the stream is up,
		// which is exactly why this client needs no outbox. Comment while
		// paused and it will come back to you through replay on resume.
		try {
			// `publish` stays inside our namespace; `broadcast` crosses every
			// namespace and is refused unless the server's `allowBroadcast`
			// gate says otherwise.
			const { recipients } = broadcast
				? await sse.broadcast(target, payload)
				: await sse.publish(target, payload);

			entries.update((list) =>
				list.map((e) => (e.key === payload.id ? { ...e, recipients } : e))
			);
			if (!sse.connected) {
				notice(`sent to ${recipients} recipient(s) with the stream down`);
			}
		} catch (err) {
			notice(`✗ ${(err as Error).message}`, "error");
		}
	}

	function pause(): void {
		if (paused.get()) return;
		paused.set(true);
		// Resumable, unlike `dispose()`: handlers, rooms and identity all
		// survive, so `connect()` later picks up exactly where this left off —
		// including the resume point.
		sse.disconnect();
		divider(`paused at ${sse.lastEventId ?? "the start"}`);
	}

	function unpause(): void {
		if (!paused.get()) return;
		paused.set(false);
		sse.connect().catch(() => {
			// Surfaced through `terminated` / the state store already.
		});
	}

	async function setMaintenance(on: boolean): Promise<void> {
		try {
			const res = await fetch("/api/maintenance", {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ on }),
			});
			const body = await res.json() as { on: boolean };
			maintenance.set(body.on);
			notice(
				body.on
					? "server is now answering /sse with 503 — watch it retry, not give up"
					: "maintenance over",
			);
		} catch (err) {
			notice(`could not toggle maintenance: ${(err as Error).message}`, "error");
		}
	}

	async function burst(count: number): Promise<void> {
		try {
			await fetch("/api/burst", {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ count }),
			});
		} catch (err) {
			notice(`could not burst: ${(err as Error).message}`, "error");
		}
	}

	function leave(): void {
		// Terminal, unlike `disconnect()` — handlers, rooms and pending
		// promises are all released and the client cannot be reused.
		sse.dispose();
	}

	// Reflect the server's current maintenance state on load, so a reload
	// during maintenance does not show a stale "off".
	fetch("/api/maintenance")
		.then((r) => r.json())
		.then((b: { on: boolean }) => maintenance.set(!!b.on))
		.catch(() => {});

	return {
		sse,
		session,
		connection,
		room,
		entries,
		resume,
		paused,
		maintenance,
		joining,
		join,
		comment,
		pause,
		unpause,
		setMaintenance,
		burst,
		leave,
	};
}
