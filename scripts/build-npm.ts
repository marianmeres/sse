import { npmBuild, versionizeDeps } from "@marianmeres/npmbuild";

const denoJson = JSON.parse(Deno.readTextFileSync("deno.json"));

/**
 * The npm package ships the **client only**.
 *
 * The reference server depends on `@marianmeres/demino`, which is published to
 * JSR and not to npm, so there is nothing coherent to publish for Node.
 * Shipping a subpath that cannot resolve would be worse than not shipping it:
 * `jsr:@marianmeres/sse/server` remains the one way to get it.
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
