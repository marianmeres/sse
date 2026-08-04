/**
 * Typed errors.
 *
 * Every rejection this library produces is one of these, so callers can branch
 * on `instanceof` rather than string-matching messages.
 *
 * @module
 */

import type { SSEErrorInfo } from "./frames.ts";

/** Base class for everything thrown by this library. */
export class SSEError extends Error {
	/**
	 * Creates the error, naming it after the concrete subclass so
	 * `err.name` is useful even after the class is minified.
	 *
	 * @param message - human-readable explanation
	 */
	constructor(message: string) {
		super(message);
		this.name = new.target.name;
	}
}

/**
 * The connection reached a terminal state and will not retry.
 *
 * The only path by which an infinitely-retrying client gives up, which is why
 * it is logged at error level and rejects any pending `connect()`. A silent
 * one would be indistinguishable from a network that never recovered.
 */
export class SSETerminatedError extends SSEError {
	/**
	 * Creates the error.
	 *
	 * @param status - the HTTP status that ended it, or `0` for a local
	 * `disconnect()`
	 * @param reason - human-readable explanation
	 */
	constructor(readonly status: number, readonly reason: string) {
		super(`Connection terminated (${status})${reason ? `: ${reason}` : ""}`);
	}
}

/**
 * `connectTimeout` elapsed before the first successful connect.
 *
 * Note the client keeps retrying in the background — this rejection bounds
 * *your await*, not the connection attempt.
 */
export class SSEConnectTimeoutError extends SSEError {
	/**
	 * Creates the error.
	 *
	 * @param ms - the `connectTimeout` that elapsed
	 */
	constructor(ms: number) {
		super(`Not connected within ${ms}ms (still retrying in background)`);
	}
}

/** An upstream RPC call did not complete within `fetchTimeout`. */
export class SSETimeoutError extends SSEError {
	/**
	 * Creates the error.
	 *
	 * @param ms - the `fetchTimeout` that elapsed
	 */
	constructor(ms: number) {
		super(`Request did not complete within ${ms}ms`);
	}
}

/** The server rejected an upstream operation. */
export class SSERemoteError extends SSEError {
	/** Machine-readable code — see `ERROR_CODE`. Branch on this, not the message. */
	readonly code: string;

	/**
	 * Creates the error, flattening the server's `SSEErrorInfo` into `code`
	 * and the standard `message`.
	 *
	 * @param info - the structured error the server returned
	 * @param status - the HTTP status it came with
	 */
	constructor(info: SSEErrorInfo, readonly status: number) {
		super(info.message);
		this.code = info.code;
	}
}

/** The client was disposed while the operation was pending. */
export class SSEDisposedError extends SSEError {
	constructor() {
		super("Client disposed");
	}
}
