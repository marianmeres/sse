/// <reference lib="dom" />
/// <reference lib="dom.iterable" />

/**
 * Client entry point — the views, and nothing else.
 *
 * Bundled to `public/dist/bundle.js` by `@marianmeres/deno-build`:
 * ```
 * deno task example:build     # or example:watch
 * ```
 *
 * Two views swap in `#app`: a login form, then the feed. All state lives in
 * `store.ts`; here we only clone templates and wire them up with
 * `@marianmeres/vanilla`.
 *
 * @module
 */

import {
	applyBindings,
	createView,
	delegate,
	fromTemplate,
	observable,
	reactTo,
	refs,
	type ViewInstance,
} from "@marianmeres/vanilla";
import {
	BROADCAST_ROOM,
	DEFAULT_PROJECT,
	DEFAULT_ROOM,
	NAME_RE,
	PROJECT_RE,
	REPLAY_BUFFER_SIZE,
	ROOMS,
} from "../shared.ts";
import { createFeedStore, type Entry, type FeedStore, type Session } from "./store.ts";

const SESSION_KEY = "sse-feed-session";
const root = document.getElementById("app")!;

const time = new Intl.DateTimeFormat(undefined, {
	hour: "2-digit",
	minute: "2-digit",
	second: "2-digit",
});

/* ------------------------------------------------------------------ login */

interface LoginProps {
	initial: Session;
	error: string;
	onSubmit: (session: Session) => void;
}

function createLogin({ initial, error, onSubmit }: LoginProps): ViewInstance {
	return createView((track) => {
		const el = fromTemplate("tpl-login");
		const r = refs(el);

		(r.name as HTMLInputElement).value = initial.name;
		(r.project as HTMLInputElement).value = initial.project;
		r.error.textContent = error;
		r.error.hidden = !error;

		track(
			delegate(el, {
				submit: (e: Event) => {
					e.preventDefault();
					const name = (r.name as HTMLInputElement).value.trim();
					const project =
						(r.project as HTMLInputElement).value.trim() || DEFAULT_PROJECT;

					// The same rules the server enforces in `verify`. Checking
					// here too is purely for the error message — the server is
					// still the authority, and a name it dislikes comes back as
					// a terminal 401.
					if (!NAME_RE.test(name)) {
						return fail("2–16 chars: letters, digits, _ or -");
					}
					if (!PROJECT_RE.test(project)) {
						return fail("project: 2–24 chars, same alphabet");
					}
					onSubmit({ name, project });
				},
			}),
		);

		function fail(message: string) {
			r.error.textContent = message;
			r.error.hidden = false;
		}

		queueMicrotask(() => (r.name as HTMLInputElement).focus());
		return { el };
	});
}

/* ------------------------------------------------------------------- feed */

interface FeedProps {
	store: FeedStore;
	onLeave: () => void;
}

function createFeed({ store, onLeave }: FeedProps): ViewInstance {
	return createView((track) => {
		const el = fromTemplate("tpl-feed");
		const r = refs(el);
		const broadcast = observable(false);

		r.you.textContent = store.session.name;
		r.project.textContent = store.session.project;
		r.replaySize.textContent = String(REPLAY_BUFFER_SIZE);

		// Room buttons, straight off the shared list.
		for (const name of ROOMS) {
			const button = document.createElement("button");
			button.type = "button";
			button.dataset.on = "click:pickRoom";
			button.dataset.room = name;
			button.textContent = `#${name}`;
			r.rooms.appendChild(button);
		}

		track(
			delegate(el, {
				pickRoom: (_e, target) => {
					const next = target.dataset.room!;
					if (next !== store.room.get()) void store.join(next);
				},
				send: (e: Event) => {
					e.preventDefault();
					const input = r.input as HTMLInputElement;
					const text = input.value;
					input.value = "";
					input.focus();
					void store.comment(text, broadcast.get());
				},
				toggleBroadcast: (_e, target) => {
					broadcast.set((target as HTMLInputElement).checked);
				},
				togglePause: () => {
					if (store.paused.get()) store.unpause();
					else store.pause();
				},
				toggleMaintenance: () => {
					void store.setMaintenance(!store.maintenance.get());
				},
				burst: () => void store.burst(REPLAY_BUFFER_SIZE + 10),
				leave: onLeave,
			}),
		);

		/* --- connection badge ------------------------------------------- */
		// `store.connection` mirrors the client's reactive state, so a dropped
		// stream, the retry attempts and the recovery all show up here with no
		// polling.
		track(
			store.connection.subscribe(({ state, connected, attempt, lastError }) => {
				r.status.dataset.state = connected ? "open" : state;
				r.status.textContent = connected
					? "connected"
					: attempt > 0
						? `reconnecting… (attempt ${attempt})`
						: state;
				r.status.title = lastError?.message ?? "";
			}),
		);

		/* --- resume badge ------------------------------------------------ */
		// The point of the whole example: what `hello` said about replay.
		track(
			store.resume.subscribe(({ resumed, gap, from }) => {
				const badge = r.resume as HTMLElement;
				badge.dataset.state = gap ? "gap" : resumed ? "resumed" : "live";
				badge.textContent = gap
					? "gap — refetched"
					: resumed
						? "resumed"
						: "live";
				badge.title = from ? `last event id: ${from}` : "no resume point yet";
			}),
		);

		/* --- pause / maintenance buttons --------------------------------- */
		track(
			reactTo([store.paused, store.maintenance], () => {
				const paused = store.paused.get();
				(r.pause as HTMLButtonElement).textContent = paused
					? "▶ Resume"
					: "⏸ Pause";
				(r.pause as HTMLButtonElement).setAttribute(
					"aria-pressed",
					String(paused),
				);

				const down = store.maintenance.get();
				(r.maintenance as HTMLButtonElement).textContent = down
					? "🔧 503 is ON"
					: "🔧 Simulate 503";
				(r.maintenance as HTMLButtonElement).setAttribute(
					"aria-pressed",
					String(down),
				);
			}),
		);

		/* --- room highlight + labels ------------------------------------ */
		track(
			reactTo([store.room, store.joining], () => {
				const current = store.room.get();
				const busy = store.joining.get();
				r.roomName.textContent = `#${current}`;
				el.querySelectorAll<HTMLButtonElement>("[data-room]").forEach((b) => {
					b.setAttribute("aria-pressed", String(b.dataset.room === current));
					b.disabled = busy;
				});
				// The broadcast gate is server-side; this is only a hint.
				r.broadcastHint.textContent =
					current === BROADCAST_ROOM
						? "allowed here"
						: `denied outside #${BROADCAST_ROOM}`;
			}),
		);

		/* --- the feed ---------------------------------------------------- */
		// Rendering the whole list on every change is the honest thing at this
		// size — no diffing, no keys, no surprises.
		track(
			store.entries.subscribe((list) => {
				const atBottom =
					r.log.scrollHeight - r.log.scrollTop - r.log.clientHeight < 40;
				r.log.replaceChildren(...list.map(renderEntry));
				r.empty.hidden = list.length > 0;
				if (atBottom) r.log.scrollTop = r.log.scrollHeight;
			}),
		);

		function renderEntry(entry: Entry): HTMLElement {
			const li = fromTemplate<HTMLLIElement>("tpl-entry");
			li.dataset.kind = entry.kind;
			li.dataset.level = entry.level;
			li.classList.toggle("own", entry.own);
			li.classList.toggle("replayed", entry.replayed);
			applyBindings(li, {
				who: entry.who,
				text: entry.text,
				when: time.format(entry.ts),
				noWho: !entry.who,
				// The publish acknowledgement carries how many streams the
				// server handed the message to.
				ack: entry.recipients === null ? "" : `✓ ${entry.recipients}`,
				noAck: entry.recipients === null,
				noTag: !entry.replayed,
			});
			return li;
		}

		// Join the default room once the view is live.
		void store.join(DEFAULT_ROOM);
		queueMicrotask(() => (r.input as HTMLInputElement).focus());

		return { el };
	});
}

/* ------------------------------------------------------------------- app */

/**
 * Swaps between the two views. Page-level and intentionally not tracked — it
 * lives as long as the tab does.
 */
const current = observable<ViewInstance | null>(null);
let store: FeedStore | null = null;

function show(view: ViewInstance): void {
	current.get()?.destroy();
	current.set(view);
	root.replaceChildren(view.el!);
}

function loadSession(): Session {
	try {
		const saved = JSON.parse(localStorage.getItem(SESSION_KEY) || "null");
		if (saved && typeof saved.name === "string") {
			return { name: saved.name, project: saved.project || DEFAULT_PROJECT };
		}
	} catch {
		/* ignore */
	}
	return { name: "", project: DEFAULT_PROJECT };
}

function showLogin(error = ""): void {
	store = null;
	show(createLogin({ initial: loadSession(), error, onSubmit: startFeed }));
}

function startFeed(session: Session): void {
	try {
		localStorage.setItem(SESSION_KEY, JSON.stringify(session));
	} catch {
		/* ignore */
	}

	store = createFeedStore(session, (reason) => {
		// Terminal — the client gave up on purpose and will not retry, so the
		// only sensible move is back to the form.
		store?.leave();
		showLogin(`Server refused the connection: ${reason}`);
	});

	show(
		createFeed({
			store,
			onLeave: () => {
				store?.leave();
				showLogin();
			},
		}),
	);
}

showLogin();

// Poke at it from the console: `feed.sse.dump()`, `feed.sse.lastEventId`, …
Object.defineProperty(globalThis, "feed", { get: () => store });
