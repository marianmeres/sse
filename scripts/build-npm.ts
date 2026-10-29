import { npmBuild, versionizeDeps } from "@marianmeres/npmbuild";

const denoJson = JSON.parse(Deno.readTextFileSync("deno.json"));

/**
 * The npm package ships the **client only**.
 *
 * The reference server and the Redis adapter are left out. They need
 * `@marianmeres/demino` (on npm since 2.0), and shipping them means declaring
 * it an optional peer and smoke-testing the server on Node, as
 * `@marianmeres/ws` does — not done yet. `jsr:@marianmeres/sse/server` remains
 * the way to get it.
 *
 * The client itself is runtime-agnostic (`EventSource` is a global in
 * browsers, Deno, Node 22+ and Bun), so npm consumers lose nothing they could
 * have used.
 */
await npmBuild({
	name: denoJson.name,
	version: denoJson.version,
	repository: denoJson.name.replace(/^@/, ""),
	sourceFiles: [
		"mod.ts",
		"protocol.ts",
		"protocol/mod.ts",
		"protocol/constants.ts",
		"protocol/frames.ts",
		"protocol/errors.ts",
		"client/sse-client.ts",
		"client/backoff.ts",
		"client/rooms.ts",
		"client/staleness.ts",
	],
	dependencies: versionizeDeps([
		"@marianmeres/clog",
		"@marianmeres/pubsub",
		"@marianmeres/ticker",
		"@marianmeres/uid",
	], denoJson),
	entryPoints: ["mod", "protocol"],
});
