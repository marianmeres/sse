/**
 * Smoke-tests the built npm package (`pnpm run smoke`, or
 * `node scripts/smoke-npm.mjs` after `pnpm run build`).
 *
 * `pnpm pack`s the repo root - the tarball `pnpm publish` would upload - and in
 * a throw-away project under the OS temp dir installs it with npm, as a
 * consumer gets it, then on Node:
 *
 *  1. checks the installed package.json (the four `exports` keys and their
 *     files, the runtime dependencies, demino as an optional peer) and that the
 *     tarball carries `dist/` and none of the repo-only directories;
 *  2. **without demino installed**, imports `.` and `./protocol` - the
 *     client-only install the optional peer exists for;
 *  3. installs demino and runs one round trip on it: `createSSEApp` served by
 *     `@marianmeres/demino/node`, the client subscribes over `EventSource`, a
 *     server-side publish arrives; `./adapters/redis` imports.
 *
 * The consumer runs with `--experimental-eventsource`: Node 22-26 keep
 * `EventSource` behind that flag, and the client uses the global one.
 *
 * Repo tool, Node only (nothing here is shipped).
 */

import { execFileSync } from "node:child_process";
import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const PKG = "@marianmeres/sse";
const DEMINO = "@marianmeres/demino";

const EXPECTED_EXPORTS = [".", "./adapters/redis", "./protocol", "./server"];
/** Repo directories the published tarball must not carry. */
const REPO_ONLY = ["src", "tests", "scripts", "tmp", "example", "node_modules"];
const EXPECTED_DEPENDENCIES = [
	"@marianmeres/clog",
	"@marianmeres/pubsub",
	"@marianmeres/ticker",
	"@marianmeres/uid",
];

/** Step 2: runs before demino is installed. */
const CLIENT_ONLY = `
import assert from "node:assert/strict";
import { createSSEClient, SSE_EVENT } from "${PKG}";
import { OP } from "${PKG}/protocol";

assert.equal(typeof createSSEClient, "function", "createSSEClient");
assert.equal(typeof SSE_EVENT, "object", "SSE_EVENT");
assert.equal(typeof OP, "object", "OP");
console.log("smoke: . and ./protocol import without demino");
`;

/** Step 3: runs after demino is installed. */
const ROUND_TRIP = `
import assert from "node:assert/strict";
import { serve } from "${DEMINO}/node";
import { createSSEClient } from "${PKG}";
import { SSEPubSubRedis } from "${PKG}/adapters/redis";
import { createSSEApp } from "${PKG}/server";

assert.equal(typeof SSEPubSubRedis, "function", "SSEPubSubRedis");

const { app, service } = createSSEApp("/sse", [], {
	logger: null,
	deminoOptions: { logger: null },
	heartbeatInterval: 0,
});
const handle = await serve(app, {
	port: 0,
	hostname: "127.0.0.1",
	onListen: () => {},
	webSockets: false,
});
const sse = createSSEClient({
	url: \`http://127.0.0.1:\${handle.port}/sse\`,
	namespace: "smoke",
	logger: null,
});
try {
	const { promise: received, resolve } = Promise.withResolvers();
	await sse.connect();
	await sse.subscribe("room", resolve);
	assert.equal(await service.publish("room", { v: 1 }, "smoke"), 1, "recipients");
	const message = await received;
	assert.deepEqual(message.payload, { v: 1 }, "payload");
	console.log("smoke: createSSEApp on demino/node -> EventSource client round trip");
} finally {
	sse.dispose();
	await service.close();
	await handle.shutdown();
}
`;

function run(cmd, args, cwd) {
	console.log(`smoke: $ ${[cmd, ...args].join(" ")}`);
	execFileSync(cmd, args, { cwd, stdio: "inherit" });
}

function check(condition, what) {
	if (!condition) throw new Error(`smoke: ${what}`);
}

function checkInstalledManifest(dir) {
	const pkg = JSON.parse(readFileSync(join(dir, "package.json"), "utf8"));
	check(pkg.name === PKG, `installed package is "${pkg.name}", expected "${PKG}"`);
	const exportKeys = Object.keys(pkg.exports ?? {}).sort();
	check(
		JSON.stringify(exportKeys) === JSON.stringify(EXPECTED_EXPORTS),
		`exports keys are ${JSON.stringify(exportKeys)}`,
	);
	for (const [key, entry] of Object.entries(pkg.exports)) {
		for (const kind of ["types", "default"]) {
			check(
				typeof entry[kind] === "string" && existsSync(join(dir, entry[kind])),
				`exports["${key}"].${kind} does not exist: ${entry?.[kind]}`,
			);
		}
	}
	for (const name of EXPECTED_DEPENDENCIES) {
		check(name in (pkg.dependencies ?? {}), `runtime dependency "${name}" missing`);
	}
	check(!(DEMINO in (pkg.dependencies ?? {})), `${DEMINO} must not be a dependency`);
	check(typeof pkg.peerDependencies?.[DEMINO] === "string", `${DEMINO} is not a peer`);
	check(pkg.peerDependenciesMeta?.[DEMINO]?.optional === true, `${DEMINO} peer is not optional`);
	for (const name of REPO_ONLY) {
		check(!existsSync(join(dir, name)), `the tarball carries "${name}/"`);
	}
	console.log(
		`smoke: ${pkg.name}@${pkg.version} - exports ${exportKeys.length}, ` +
			`peer ${DEMINO} ${pkg.peerDependencies[DEMINO]} (optional)`,
	);
	return pkg.peerDependencies[DEMINO];
}

check(
	existsSync(join(ROOT, "dist", "mod.js")),
	"dist/mod.js is missing - run `pnpm run build` first",
);

const node = [process.execPath, "--experimental-eventsource", "--disable-warning=UNDICI-ES"];
const npmInstall = ["install", "--no-audit", "--no-fund", "--loglevel=error"];

const project = mkdtempSync(join(tmpdir(), "sse-npm-smoke-"));
try {
	writeFileSync(
		join(project, "package.json"),
		JSON.stringify({ name: "sse-npm-smoke", private: true, type: "module" }, null, "\t"),
	);
	const packDir = join(project, "pack");
	mkdirSync(packDir);
	run("pnpm", ["pack", "--pack-destination", packDir], ROOT);
	const tarballs = readdirSync(packDir).filter((f) => f.endsWith(".tgz"));
	check(tarballs.length === 1, `expected one tarball, got ${JSON.stringify(tarballs)}`);

	run("npm", [...npmInstall, join(packDir, tarballs[0])], project);
	const deminoRange = checkInstalledManifest(join(project, "node_modules", PKG));
	check(!existsSync(join(project, "node_modules", DEMINO)), `${DEMINO} got installed`);
	writeFileSync(join(project, "client-only.mjs"), CLIENT_ONLY);
	run(node[0], [...node.slice(1), "client-only.mjs"], project);

	run("npm", [...npmInstall, `${DEMINO}@${deminoRange}`], project);
	writeFileSync(join(project, "round-trip.mjs"), ROUND_TRIP);
	run(node[0], [...node.slice(1), "round-trip.mjs"], project);
	console.log("smoke: OK");
} finally {
	rmSync(project, { recursive: true, force: true });
}
