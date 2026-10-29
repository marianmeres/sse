const Scheduler = (()=>{
    const queues = {
        microtask: {
            map: new Map(),
            scheduled: false,
            arm: (cb)=>queueMicrotask(cb)
        },
        raf: {
            map: new Map(),
            scheduled: false,
            arm: (cb)=>requestAnimationFrame(cb)
        }
    };
    let flushing = false;
    let chainDepth = 0;
    function flush(kind) {
        const q = queues[kind];
        const entries = q.map;
        q.map = new Map();
        q.scheduled = false;
        flushing = true;
        try {
            entries.forEach((read, fn)=>fn(read()));
        } finally{
            flushing = false;
        }
    }
    function enqueue(fn, kind, read) {
        const q = queues[kind] ?? queues.microtask;
        const arming = !q.scheduled;
        if (arming) {
            if (flushing) {
                if (++chainDepth > 1000) {
                    chainDepth = 0;
                    throw new Error(`vanilla: maximum update depth exceeded (${1000}) — ` + `likely a feedback loop where an effect's set() retriggers ` + `itself (a writes b, b writes a, …). Check your reactTo/computed ` + `wiring, or make the loop converge so the equality guard can ` + `stop it.`);
                }
            } else {
                chainDepth = 0;
            }
        }
        q.map.set(fn, read);
        if (arming) {
            q.scheduled = true;
            q.arm(()=>flush(kind));
        }
    }
    return {
        enqueue
    };
})();
let computing = 0;
function observable(value) {
    const subs = new Map();
    function notify() {
        subs.forEach((kind, fn)=>Scheduler.enqueue(fn, kind, ()=>value));
    }
    const self = {
        get: ()=>value,
        set (v) {
            if (computing > 0) {
                throw new Error("vanilla: cannot set() an observable from inside a computed's " + "calc — calc must be a pure derivation of its sources (no writes " + "/ side effects). Move the write into a reactTo(...) effect.");
            }
            if (v === value) return;
            value = v;
            notify();
        },
        update (fn) {
            self.set(fn(value));
        },
        subscribe (fn, { immediate = true, scheduler = "microtask" } = {}) {
            subs.set(fn, scheduler);
            if (immediate) fn(value);
            return ()=>{
                subs.delete(fn);
            };
        }
    };
    return self;
}
function reactTo(sources, fn, { immediate = true, scheduler = "microtask" } = {}) {
    const unsubs = sources.map((o)=>o.subscribe(fn, {
            immediate: false,
            scheduler
        }));
    if (immediate) fn();
    return ()=>unsubs.forEach((u)=>u());
}
function fromTemplate(id) {
    const tpl = document.getElementById(id);
    if (!tpl) throw new Error(`Template not found: #${id}`);
    const first = tpl.content.firstElementChild;
    if (!first) throw new Error(`Template #${id} has no element content`);
    return first.cloneNode(true);
}
function inScope(root, el) {
    const scope = el.closest("[data-scope]");
    return !scope || scope === root || !root.contains(scope);
}
function refs(root) {
    const map = {};
    const self = root;
    if (self.matches?.("[data-ref]")) map[self.dataset.ref] = self;
    root.querySelectorAll("[data-ref]").forEach((el)=>{
        if (inScope(root, el)) map[el.dataset.ref] = el;
    });
    return map;
}
const BIND_ALIAS = {
    text: "textContent",
    html: "innerHTML",
    class: "className"
};
function applyBindings(root, data) {
    const targets = [
        ...root.matches?.("[data-bind]") ? [
            root
        ] : [],
        ...[
            ...root.querySelectorAll("[data-bind]")
        ].filter((el)=>inScope(root, el))
    ];
    targets.forEach((el)=>{
        el.dataset.bind.split(";").forEach((rule)=>{
            const [kind, field] = rule.split(":").map((s)=>s.trim());
            if (!kind) return;
            const prop = BIND_ALIAS[kind] ?? kind;
            el[prop] = data[field];
        });
    });
}
function tracker() {
    const cleanups = [];
    return {
        track: (unsub)=>{
            cleanups.push(unsub);
            return unsub;
        },
        dispose: ()=>{
            cleanups.forEach((fn)=>fn());
            cleanups.length = 0;
        }
    };
}
function createView(mountFn) {
    const { track, dispose } = tracker();
    const api = mountFn(track) ?? {};
    return {
        ...api,
        destroy () {
            dispose();
            api.el?.remove();
        }
    };
}
function delegate(root, handlers) {
    const seen = new Set();
    const collect = (scope)=>scope.querySelectorAll("[data-on]").forEach((el)=>el.dataset.on.split(";").forEach((r)=>seen.add(r.split(":")[0].trim())));
    if (root.matches?.("[data-on]")) {
        root.dataset.on.split(";").forEach((r)=>seen.add(r.split(":")[0].trim()));
    }
    collect(root);
    document.querySelectorAll("template").forEach((t)=>collect(t.content));
    const unsubs = [];
    seen.forEach((evt)=>{
        const listener = (e)=>{
            const el = e.target?.closest("[data-on]");
            if (!el || !root.contains(el) || !inScope(root, el)) return;
            el.dataset.on.split(";").forEach((rule)=>{
                const [type, action] = rule.split(":").map((s)=>s.trim());
                if (type === evt && handlers[action]) handlers[action](e, el);
            });
        };
        root.addEventListener(evt, listener);
        unsubs.push(()=>root.removeEventListener(evt, listener));
    });
    return ()=>unsubs.forEach((u)=>u());
}
new Map();
new Map();
const ROOMS = [
    "deploys",
    "alerts",
    "chatter"
];
const BROADCAST_ROOM = "alerts";
const DEFAULT_ROOM = ROOMS[0];
const DEFAULT_PROJECT = "acme";
const REPLAY_BUFFER_SIZE = 20;
function isRecord(v) {
    return typeof v === "object" && v !== null;
}
function isEventPayload(payload) {
    return isRecord(payload) && payload.kind === "event" && typeof payload.text === "string" && typeof payload.source === "string";
}
function isCommentPayload(payload) {
    return isRecord(payload) && payload.kind === "comment" && typeof payload.text === "string" && typeof payload.who === "string";
}
const NAME_RE = /^[a-z0-9][a-z0-9_-]{1,15}$/i;
const PROJECT_RE = /^[a-z0-9][a-z0-9_-]{1,23}$/i;
const CLOG_STYLED = Symbol.for("@marianmeres/clog-styled");
const SAFE_COLORS = {
    gray: "#969696",
    grey: "#969696",
    red: "#d26565",
    orange: "#cba14d",
    yellow: "#cba14d",
    green: "#3dc73d",
    teal: "#4dcba1",
    cyan: "#4dcba1",
    blue: "#67afd3",
    purple: "#8e8ed4",
    magenta: "#b080c8",
    pink: "#be5b9d"
};
const AUTO_PALETTE = [
    SAFE_COLORS.gray,
    SAFE_COLORS.red,
    SAFE_COLORS.orange,
    "#8eba36",
    SAFE_COLORS.green,
    SAFE_COLORS.teal,
    SAFE_COLORS.blue,
    SAFE_COLORS.purple,
    SAFE_COLORS.magenta,
    SAFE_COLORS.pink
];
const _autoColorCache = new Map();
function autoColor(namespace) {
    const cached = _autoColorCache.get(namespace);
    if (cached !== undefined) return cached;
    const color = AUTO_PALETTE[strHash(namespace) % AUTO_PALETTE.length];
    _autoColorCache.set(namespace, color);
    return color;
}
function strHash(str) {
    let hash = 0;
    for(let i = 0; i < str.length; i++){
        hash = (hash << 5) - hash + str.charCodeAt(i) | 0;
    }
    return hash >>> 0;
}
const LEVEL_MAP = {
    debug: "DEBUG",
    log: "INFO",
    warn: "WARNING",
    error: "ERROR"
};
const CLOG_SKIP = Symbol.for("@marianmeres/clog-skip");
const DEFAULT_JSON_FIELD_NAMES = {
    timestamp: "timestamp",
    level: "level",
    logger: "logger",
    message: "message",
    meta: "meta",
    arg: "arg",
    stack: "stack"
};
const CLOG_INSTANCE = Symbol.for("@marianmeres/clog-instance");
const GLOBAL_KEY = Symbol.for("@marianmeres/clog");
const GLOBAL = globalThis[GLOBAL_KEY] ??= {
    hook: undefined,
    writer: undefined,
    jsonOutput: false,
    debug: undefined
};
let _cachedRuntime = null;
function detectRuntime() {
    if (_cachedRuntime !== null) return _cachedRuntime;
    if (typeof window !== "undefined" && window?.document) {
        return _cachedRuntime = "browser";
    }
    if (globalThis.Deno?.version?.deno) return _cachedRuntime = "deno";
    if (globalThis.process?.versions?.node) {
        return _cachedRuntime = "node";
    }
    return _cachedRuntime = "unknown";
}
const CLOG_FRAME_MARKERS = [
    "clog.ts",
    "colors.ts"
];
function isClogFrame(line) {
    return CLOG_FRAME_MARKERS.some((m)=>line.includes(m));
}
function captureStackLines(limit) {
    const stack = new Error().stack || "";
    const lines = stack.split("\n");
    const relevant = [];
    for (const raw of lines){
        const line = raw.trimEnd();
        if (!line) continue;
        if (/^Error(:|$)/.test(line.trim())) continue;
        if (isClogFrame(line)) continue;
        relevant.push(line);
    }
    if (typeof limit === "number" && limit > 0) {
        return relevant.slice(0, limit);
    }
    return relevant;
}
function formatStack(lines) {
    return "\n---\nStack:\n" + lines.map((v)=>"  " + v.trim()).join("\n");
}
function renderNs(ns) {
    if (!ns) return "";
    return ns.split(":").map((s)=>`[${s}]`).join(" ");
}
function _stringifyArgs(args, config) {
    if (!(config?.stringify ?? GLOBAL.stringify)) return args;
    return args.map((arg)=>{
        if (arg === null || arg === undefined) return arg;
        if (typeof arg !== "object") return arg;
        if (arg?.[CLOG_STYLED]) return arg.text;
        try {
            return JSON.stringify(arg);
        } catch  {
            return String(arg);
        }
    });
}
function stringifyValue(arg) {
    if (arg === null) return "null";
    if (arg === undefined) return "undefined";
    if (typeof arg !== "object") return String(arg);
    if (arg?.[CLOG_STYLED]) return arg.text;
    try {
        return JSON.stringify(arg);
    } catch  {
        return String(arg);
    }
}
function _hasStyledArgs(args) {
    return args.some((arg)=>arg?.[CLOG_STYLED]);
}
function _cleanStyledArgs(args) {
    return args.map((arg)=>arg?.[CLOG_STYLED] ? arg.text : arg);
}
function _processStyledArgs(args) {
    let format = "";
    const values = [];
    for (const arg of args){
        if (arg?.[CLOG_STYLED]) {
            format += `%c${arg.text}%c `;
            values.push(arg.style, "");
        } else if (typeof arg === "string") {
            format += `${arg} `;
        } else {
            format += "%o ";
            values.push(arg);
        }
    }
    return [
        format.trim(),
        values
    ];
}
function firstArgAsString(args, config) {
    if (args.length === 0) return "";
    const concat = config?.concat ?? GLOBAL.concat;
    const stringify = config?.stringify ?? GLOBAL.stringify;
    if (concat || stringify) return stringifyValue(args[0]);
    return String(args[0] ?? "");
}
function _resolveMetaSource(src) {
    if (!src) return undefined;
    try {
        return typeof src === "function" ? src() : src;
    } catch  {
        return undefined;
    }
}
function _mergeMetaSources(sources) {
    let out;
    for (const src of sources){
        const resolved = _resolveMetaSource(src);
        if (resolved != null) out = Object.assign(out ?? {}, resolved);
    }
    return out;
}
const CORE_JSON_FIELDS = [
    "timestamp",
    "level",
    "logger",
    "message",
    "meta",
    "stack"
];
function _isReservedJsonKey(key, names) {
    if (CORE_JSON_FIELDS.some((k)=>names[k] === key)) return true;
    const prefix = `${names.arg}_`;
    return key.startsWith(prefix) && /^\d+$/.test(key.slice(prefix.length));
}
function toJsonRecord(data) {
    const { level, namespace, timestamp, config, stack } = data;
    const args = _cleanStyledArgs(_stringifyArgs(data.args, config));
    const fieldNames = {
        ...DEFAULT_JSON_FIELD_NAMES,
        ...GLOBAL.jsonFieldNames,
        ...config?.jsonFieldNames
    };
    const meta = data.meta;
    let promoted;
    let rest;
    const topLevelKeys = config?.jsonTopLevelMeta ?? GLOBAL.jsonTopLevelMeta;
    if (meta && topLevelKeys?.length) {
        for (const key of topLevelKeys){
            if (!Object.hasOwn(meta, key) || meta[key] === undefined) continue;
            if (_isReservedJsonKey(key, fieldNames)) continue;
            rest ??= {
                ...meta
            };
            (promoted ??= {})[key] = meta[key];
            delete rest[key];
        }
    }
    const writtenMeta = rest ? Object.keys(rest).length ? rest : undefined : meta;
    const record = {
        [fieldNames.timestamp]: timestamp,
        [fieldNames.level]: level,
        ...namespace ? {
            [fieldNames.logger]: namespace
        } : {},
        [fieldNames.message]: args[0],
        ...promoted,
        ...writtenMeta && {
            [fieldNames.meta]: writtenMeta
        }
    };
    args.slice(1).forEach((arg, i)=>{
        record[`${fieldNames.arg}_${i}`] = arg?.stack ?? arg;
    });
    if (stack && stack.length) record[fieldNames.stack] = formatStack(stack);
    return record;
}
const CONSOLE_METHOD = {
    DEBUG: "debug",
    INFO: "log",
    WARNING: "warn",
    ERROR: "error"
};
const defaultWriter = (data)=>{
    const { level, namespace, args, timestamp, config, stack } = data;
    const runtime = detectRuntime();
    const consoleMethod = CONSOLE_METHOD[level];
    const nsText = renderNs(namespace);
    const stackStr = stack && stack.length ? formatStack(stack) : null;
    const shouldConcat = config?.concat ?? GLOBAL.concat;
    if (shouldConcat) {
        const stringified = args.map(stringifyValue).join(" ");
        const output = runtime === "browser" ? nsText ? `${nsText} ${stringified}` : stringified : `[${timestamp}] [${level}]${nsText ? ` ${nsText}` : ""} ${stringified}`;
        console[consoleMethod](output, ...stackStr ? [
            stackStr
        ] : []);
        return;
    }
    if (runtime !== "browser" && (config?.jsonOutput ?? GLOBAL.jsonOutput)) {
        console[consoleMethod](JSON.stringify(toJsonRecord(data)));
        return;
    }
    const processedArgs = _stringifyArgs(args, config);
    const hasStyled = _hasStyledArgs(processedArgs);
    if ((runtime === "browser" || runtime === "deno") && hasStyled) {
        const [content, contentValues] = _processStyledArgs(processedArgs);
        if (runtime === "browser") {
            console[consoleMethod](nsText ? `${nsText} ${content}` : content, ...contentValues, ...stackStr ? [
                stackStr
            ] : []);
        } else {
            const prefix = `[${timestamp}] [${level}]${nsText ? ` ${nsText}` : ""}`;
            console[consoleMethod](`${prefix} ${content}`, ...contentValues, ...stackStr ? [
                stackStr
            ] : []);
        }
        return;
    }
    const cleanedArgs = _cleanStyledArgs(processedArgs);
    if (runtime === "browser") {
        if (nsText) {
            console[consoleMethod](nsText, ...cleanedArgs, ...stackStr ? [
                stackStr
            ] : []);
        } else {
            console[consoleMethod](...cleanedArgs, ...stackStr ? [
                stackStr
            ] : []);
        }
        return;
    }
    const prefix = `[${timestamp}] [${level}]${nsText ? ` ${nsText}` : ""}`.trim();
    console[consoleMethod](prefix, ...cleanedArgs, ...stackStr ? [
        stackStr
    ] : []);
};
const colorWriter = (configuredColor)=>(data)=>{
        const { level, namespace, args, timestamp, config, stack } = data;
        const runtime = detectRuntime();
        if (runtime !== "browser" && runtime !== "deno" || !namespace || (config?.concat ?? GLOBAL.concat) || (config?.jsonOutput ?? GLOBAL.jsonOutput)) {
            return defaultWriter(data);
        }
        const color = configuredColor === "auto" ? autoColor(namespace) : configuredColor;
        const processedArgs = _stringifyArgs(args, config);
        const consoleMethod = CONSOLE_METHOD[level];
        const stackStr = stack && stack.length ? formatStack(stack) : null;
        const nsText = renderNs(namespace);
        if (_hasStyledArgs(processedArgs)) {
            const [content, contentValues] = _processStyledArgs(processedArgs);
            if (runtime === "browser") {
                console[consoleMethod](`%c${nsText}%c ${content}`, `color:${color}`, "", ...contentValues, ...stackStr ? [
                    stackStr
                ] : []);
            } else {
                const prefix = `[${timestamp}] [${level}] %c${nsText}%c`;
                console[consoleMethod](`${prefix} ${content}`, `color:${color}`, "", ...contentValues, ...stackStr ? [
                    stackStr
                ] : []);
            }
            return;
        }
        if (runtime === "browser") {
            console[consoleMethod](`%c${nsText}`, `color:${color}`, ...processedArgs, ...stackStr ? [
                stackStr
            ] : []);
        } else {
            const prefix = `[${timestamp}] [${level}] %c${nsText}`;
            console[consoleMethod](prefix, `color:${color}`, ...processedArgs, ...stackStr ? [
                stackStr
            ] : []);
        }
    };
function createClog(namespace, config) {
    const ns = namespace ?? false;
    const _apply = (level, args)=>{
        const clonedArgs = args.slice();
        const getMetaFn = config?.getMeta ?? GLOBAL.getMeta;
        const extraMeta = config?.meta;
        const stacktraceConfig = config?.stacktrace ?? GLOBAL.stacktrace;
        const stack = stacktraceConfig ? captureStackLines(typeof stacktraceConfig === "number" ? stacktraceConfig : undefined) : undefined;
        const data = {
            level: LEVEL_MAP[level],
            namespace: ns,
            args: clonedArgs,
            timestamp: new Date().toISOString(),
            config,
            stack
        };
        if (getMetaFn || extraMeta) {
            let _meta;
            let _metaComputed = false;
            Object.defineProperty(data, "meta", {
                get () {
                    if (!_metaComputed) {
                        _metaComputed = true;
                        _meta = extraMeta ? _mergeMetaSources([
                            getMetaFn,
                            extraMeta
                        ]) : _resolveMetaSource(getMetaFn);
                    }
                    return _meta;
                },
                enumerable: true,
                configurable: true
            });
        }
        const hookResult = GLOBAL.hook?.(data);
        if (hookResult !== CLOG_SKIP) {
            let writer = GLOBAL.writer ?? config?.writer;
            if (!writer && config?.color) writer = colorWriter(config.color);
            writer = writer ?? defaultWriter;
            writer(data);
        }
        return firstArgAsString(clonedArgs, config);
    };
    const logger = (...args)=>_apply("log", args);
    logger.debug = (...args)=>{
        if ((config?.debug ?? GLOBAL.debug) === false) {
            return firstArgAsString(args, config);
        }
        return _apply("debug", args);
    };
    logger.log = (...args)=>_apply("log", args);
    logger.warn = (...args)=>_apply("warn", args);
    logger.error = (...args)=>_apply("error", args);
    Object.defineProperty(logger, "ns", {
        value: ns,
        writable: false
    });
    Object.defineProperty(logger, CLOG_INSTANCE, {
        value: {
            ns,
            config
        },
        enumerable: false,
        writable: false
    });
    return logger;
}
createClog.global = GLOBAL;
createClog.reset = ()=>{
    createClog.global.hook = undefined;
    createClog.global.writer = undefined;
    createClog.global.jsonOutput = false;
    createClog.global.jsonFieldNames = undefined;
    createClog.global.jsonTopLevelMeta = undefined;
    createClog.global.debug = undefined;
    createClog.global.stringify = undefined;
    createClog.global.concat = undefined;
    createClog.global.stacktrace = undefined;
    createClog.global.getMeta = undefined;
};
if (typeof Symbol.dispose === "undefined") {
    Symbol.dispose = Symbol.for("Symbol.dispose");
}
const WILDCARD = "*";
class PubSub {
    #subs = new Map();
    #onError;
    constructor(options){
        this.#onError = options?.onError ?? this.#defaultErrorHandler;
        this.publish = this.publish.bind(this);
        this.subscribe = this.subscribe.bind(this);
        this.subscribeOnce = this.subscribeOnce.bind(this);
        this.subscribeMany = this.subscribeMany.bind(this);
        this.unsubscribe = this.unsubscribe.bind(this);
        this.unsubscribeAll = this.unsubscribeAll.bind(this);
        this.isSubscribed = this.isSubscribed.bind(this);
        this.subscriberCount = this.subscriberCount.bind(this);
        this.hasSubscribers = this.hasSubscribers.bind(this);
        this.topics = this.topics.bind(this);
    }
    #defaultErrorHandler(error, topic, isWildcard) {
        const prefix = isWildcard ? "wildcard subscriber" : "subscriber";
        console.error(`Error in ${prefix} for topic "${topic}":`, error);
    }
    #invoke(cb, data, topic, isWildcard) {
        try {
            const result = cb(data);
            if (result && typeof result.then === "function") {
                result.catch((reason)=>{
                    const err = reason instanceof Error ? reason : new Error(String(reason));
                    this.#onError(err, topic, isWildcard);
                });
            }
        } catch (error) {
            this.#onError(error, topic, isWildcard);
        }
    }
    #makeUnsubscriber(fn) {
        const u = ()=>fn();
        u[Symbol.dispose] = fn;
        return u;
    }
    publish(topic, data) {
        if (topic === WILDCARD) {
            throw new Error(`Cannot publish to wildcard topic "*". "*" is reserved for subscribers; publish to a real topic name instead.`);
        }
        const direct = this.#subs.get(topic);
        const hadDirect = !!direct && direct.size > 0;
        if (direct) {
            for (const cb of [
                ...direct
            ]){
                this.#invoke(cb, data, topic, false);
            }
        }
        const wildcards = this.#subs.get(WILDCARD);
        if (wildcards && wildcards.size > 0) {
            const envelope = {
                event: topic,
                data
            };
            for (const cb of [
                ...wildcards
            ]){
                this.#invoke(cb, envelope, topic, true);
            }
        }
        return hadDirect;
    }
    subscribe(topic, cb) {
        let bucket = this.#subs.get(topic);
        if (!bucket) {
            bucket = new Set();
            this.#subs.set(topic, bucket);
        }
        bucket.add(cb);
        return this.#makeUnsubscriber(()=>{
            this.unsubscribe(topic, cb);
        });
    }
    subscribeOnce(topic, cb) {
        let fired = false;
        const onceWrapper = (data)=>{
            if (fired) return;
            fired = true;
            this.unsubscribe(topic, onceWrapper);
            return cb(data);
        };
        return this.subscribe(topic, onceWrapper);
    }
    subscribeMany(topics, cb) {
        const unsubs = topics.map((t)=>this.subscribe(t, cb));
        return this.#makeUnsubscriber(()=>{
            for (const u of unsubs)u();
        });
    }
    unsubscribe(topic, cb) {
        const bucket = this.#subs.get(topic);
        if (!bucket) return false;
        if (typeof cb === "function") {
            const removed = bucket.delete(cb);
            if (bucket.size === 0) this.#subs.delete(topic);
            return removed;
        }
        return this.#subs.delete(topic);
    }
    unsubscribeAll(topic) {
        if (topic !== undefined) return this.#subs.delete(topic);
        if (this.#subs.size === 0) return false;
        this.#subs.clear();
        return true;
    }
    isSubscribed(topic, cb, considerWildcard = true) {
        if (this.#subs.get(topic)?.has(cb)) return true;
        if (considerWildcard && this.#subs.get(WILDCARD)?.has(cb)) return true;
        return false;
    }
    subscriberCount(topic) {
        if (topic !== undefined) return this.#subs.get(topic)?.size ?? 0;
        let total = 0;
        for (const set of this.#subs.values())total += set.size;
        return total;
    }
    hasSubscribers(topic) {
        return (this.#subs.get(topic)?.size ?? 0) > 0;
    }
    topics() {
        return [
            ...this.#subs.keys()
        ];
    }
    __dump() {
        const out = {};
        for (const [topic, set] of this.#subs.entries()){
            out[topic] = new Set(set);
        }
        return out;
    }
}
function createPubSub(options) {
    return new PubSub(options);
}
const ALPHABETS = {
    hex: "0123456789abcdef",
    base32: "0123456789ABCDEFGHJKMNPQRSTVWXYZ",
    base36: "0123456789abcdefghijklmnopqrstuvwxyz",
    base56: "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz",
    base58: "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
    base62: "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    numeric: "0123456789",
    alphanumeric: "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
    nanoid: "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"
};
function assertPositiveInt(n, label) {
    if (typeof n !== "number" || !Number.isInteger(n) || n < 1) {
        throw new TypeError(`"${label}" must be a positive integer, got ${String(n)}`);
    }
}
function assertNonNegativeInt(n, label) {
    if (typeof n !== "number" || !Number.isInteger(n) || n < 0) {
        throw new TypeError(`"${label}" must be a non-negative integer, got ${String(n)}`);
    }
}
function randomBytes(size) {
    assertPositiveInt(size, "size");
    const bytes = new Uint8Array(size);
    const MAX = 65536;
    for(let offset = 0; offset < size; offset += MAX){
        crypto.getRandomValues(bytes.subarray(offset, Math.min(offset + 65536, size)));
    }
    return bytes;
}
function randomString(length, alphabet = ALPHABETS.nanoid) {
    assertPositiveInt(length, "length");
    if (typeof alphabet !== "string") {
        throw new TypeError(`"alphabet" must be a string, got ${typeof alphabet}`);
    }
    const alen = alphabet.length;
    if (alen < 1 || alen > 256) {
        throw new RangeError(`"alphabet" length must be between 1 and 256, got ${alen}`);
    }
    if (alen === 1) return alphabet.repeat(length);
    const mask = (2 << Math.floor(Math.log2(alen - 1))) - 1;
    const step = Math.ceil(1.6 * mask * length / alen);
    let id = "";
    while(true){
        const bytes = randomBytes(step);
        for(let i = 0; i < step; i++){
            const idx = bytes[i] & mask;
            if (idx < alen) {
                id += alphabet[idx];
                if (id.length === length) return id;
            }
        }
    }
}
function bytesToUuid(bytes) {
    let h = "";
    for(let i = 0; i < 16; i++)h += bytes[i].toString(16).padStart(2, "0");
    return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}
function uuid() {
    return crypto.randomUUID();
}
function uuidv7(timestamp = Date.now()) {
    const ts = Math.floor(timestamp);
    if (!Number.isFinite(ts) || ts < 0 || ts > 0xffffffffffff) {
        throw new RangeError(`uuidv7: timestamp must be in [0, 2^48), got ${String(timestamp)}`);
    }
    const bytes = randomBytes(16);
    bytes[0] = Math.floor(ts / 2 ** 40) & 0xff;
    bytes[1] = Math.floor(ts / 2 ** 32) & 0xff;
    bytes[2] = Math.floor(ts / 2 ** 24) & 0xff;
    bytes[3] = Math.floor(ts / 2 ** 16) & 0xff;
    bytes[4] = Math.floor(ts / 2 ** 8) & 0xff;
    bytes[5] = ts & 0xff;
    bytes[6] = bytes[6] & 0x0f | 0x70;
    bytes[8] = bytes[8] & 0x3f | 0x80;
    return bytesToUuid(bytes);
}
function ulid(timestamp = Date.now()) {
    const ts = Math.floor(timestamp);
    if (!Number.isFinite(ts) || ts < 0 || ts > 0xffffffffffff) {
        throw new RangeError(`ulid: timestamp must be in [0, 2^48), got ${String(timestamp)}`);
    }
    const enc = ALPHABETS.base32;
    let time = ts;
    let timeChars = "";
    for(let i = 0; i < 10; i++){
        timeChars = enc[time % 32] + timeChars;
        time = Math.floor(time / 32);
    }
    return timeChars + randomString(16, enc);
}
const BASE56 = ALPHABETS.base56;
function base56(length = 21) {
    return randomString(length, BASE56);
}
function uuidToBase56(uuid) {
    const hex = uuid.replace(/-/g, "").toLowerCase();
    if (!/^[0-9a-f]{32}$/.test(hex)) {
        throw new TypeError(`uuidToBase56: invalid uuid "${uuid}"`);
    }
    let num = BigInt("0x" + hex);
    let result = "";
    while(num > 0n){
        result = BASE56[Number(num % 56n)] + result;
        num = num / 56n;
    }
    return result.padStart(23, BASE56[0]);
}
function nanoid(length = 21, alphabet = ALPHABETS.nanoid) {
    return randomString(length, alphabet);
}
function hex(length = 21) {
    return randomString(length, ALPHABETS.hex);
}
function base32(length = 21) {
    return randomString(length, ALPHABETS.base32);
}
function base36(length = 21) {
    return randomString(length, ALPHABETS.base36);
}
function base58(length = 21) {
    return randomString(length, ALPHABETS.base58);
}
function base62(length = 21) {
    return randomString(length, ALPHABETS.base62);
}
function numeric(length = 21) {
    return randomString(length, ALPHABETS.numeric);
}
function validate(o) {
    const { prefix = "", start = 0, step = 1, pad = 0 } = o;
    if (typeof prefix !== "string") {
        throw new TypeError(`counter: "prefix" must be a string`);
    }
    if (!Number.isInteger(start)) {
        throw new TypeError(`counter: "start" must be an integer`);
    }
    if (!Number.isInteger(step) || step < 1) {
        throw new TypeError(`counter: "step" must be a positive integer`);
    }
    assertNonNegativeInt(pad, "pad");
    return {
        prefix,
        start,
        step,
        pad
    };
}
function format(prefix, n, pad) {
    const s = pad > 0 ? String(n).padStart(pad, "0") : String(n);
    return prefix + s;
}
const _counters = new Map();
function counter(options = {}) {
    const { prefix, start, step, pad } = validate(options);
    const existing = _counters.get(prefix);
    const entry = existing ? {
        ...existing,
        value: existing.value + existing.step
    } : {
        value: start,
        step,
        pad
    };
    _counters.set(prefix, entry);
    return format(prefix, entry.value, entry.pad);
}
function consistentShuffle(alphabet, salt) {
    if (!salt.length) return alphabet;
    const a = alphabet.split("");
    for(let i = a.length - 1, v = 0, p = 0; i > 0; i--, v++){
        v %= salt.length;
        const __int = salt.charCodeAt(v);
        p += __int;
        const j = (__int + v + p) % i;
        const tmp = a[i];
        a[i] = a[j];
        a[j] = tmp;
    }
    return a.join("");
}
function assertReversibleAlphabet(alphabet, fn) {
    if (alphabet.length < 2) {
        throw new RangeError(`${fn}: "alphabet" needs at least 2 characters`);
    }
    if (new Set(alphabet).size !== alphabet.length) {
        throw new RangeError(`${fn}: "alphabet" must have unique characters`);
    }
}
function encodeInt(value, options = {}) {
    const { alphabet = ALPHABETS.base62, salt = "", minLength = 0 } = options;
    if (!Number.isInteger(value) || value < 0 || value > Number.MAX_SAFE_INTEGER) {
        throw new RangeError(`encodeInt: value must be a safe non-negative integer, got ${String(value)}`);
    }
    assertReversibleAlphabet(alphabet, "encodeInt");
    const a = consistentShuffle(alphabet, salt);
    const base = a.length;
    let n = value;
    let out = "";
    do {
        out = a[n % base] + out;
        n = Math.floor(n / base);
    }while (n > 0)
    while(out.length < minLength)out = a[0] + out;
    return out;
}
const _registry = new Map();
function registerStrategy(name, fn) {
    if (typeof name !== "string" || !name) {
        throw new TypeError(`registerStrategy: "name" must be a non-empty string`);
    }
    if (typeof fn !== "function") {
        throw new TypeError(`registerStrategy: "fn" must be a function`);
    }
    _registry.set(name, fn);
}
registerStrategy("uuid", ()=>uuid());
registerStrategy("uuidv4", ()=>uuid());
registerStrategy("uuidv7", (o = {})=>uuidv7(o.timestamp));
registerStrategy("ulid", (o = {})=>ulid(o.timestamp));
registerStrategy("base56", (o = {})=>{
    const { uuid: u, length } = o;
    return u != null ? uuidToBase56(u) : base56(length);
});
registerStrategy("nanoid", (o = {})=>{
    const { length, alphabet } = o;
    return nanoid(length, alphabet);
});
registerStrategy("hex", (o = {})=>hex(o.length));
registerStrategy("base32", (o = {})=>base32(o.length));
registerStrategy("base36", (o = {})=>base36(o.length));
registerStrategy("base58", (o = {})=>base58(o.length));
registerStrategy("base62", (o = {})=>base62(o.length));
registerStrategy("numeric", (o = {})=>numeric(o.length));
registerStrategy("custom", (o = {})=>{
    const { alphabet, length } = o;
    if (typeof alphabet !== "string") {
        throw new TypeError(`uid("custom"): "alphabet" option is required`);
    }
    return randomString(length ?? 21, alphabet);
});
registerStrategy("counter", (o = {})=>counter(o));
registerStrategy("reversible", (o = {})=>{
    const { value, ...rest } = o;
    if (typeof value !== "number") {
        throw new TypeError(`uid("reversible"): "value" option is required`);
    }
    return encodeInt(value, rest);
});
const SSE_EVENT = {
    HELLO: "hello",
    MESSAGE: "message",
    HEARTBEAT: "heartbeat"
};
const OP = {
    SUB: "sub",
    UNSUB: "unsub",
    PUB: "pub",
    BROADCAST: "broadcast"
};
const DEFAULT_TERMINAL_STATUS_CODES = [
    401,
    403
];
const AUTH_HEADER = "x-sse-auth";
const AUTH_PARAM = "auth";
const LAST_EVENT_ID_PARAM = "lastEventId";
class SSEError extends Error {
    constructor(message){
        super(message);
        this.name = new.target.name;
    }
}
class SSETerminatedError extends SSEError {
    status;
    reason;
    constructor(status, reason){
        super(`Connection terminated (${status})${reason ? `: ${reason}` : ""}`);
        this.status = status;
        this.reason = reason;
    }
}
class SSEConnectTimeoutError extends SSEError {
    constructor(ms){
        super(`Not connected within ${ms}ms (still retrying in background)`);
    }
}
class SSETimeoutError extends SSEError {
    constructor(ms){
        super(`Request did not complete within ${ms}ms`);
    }
}
class SSERemoteError extends SSEError {
    status;
    code;
    constructor(info, status){
        super(info.message);
        this.status = status;
        this.code = info.code;
    }
}
class SSEDisposedError extends SSEError {
    constructor(){
        super("Client disposed");
    }
}
function backoffDelay(attempt, base, max, rnd = Math.random) {
    const n = Math.max(1, Math.floor(attempt));
    const full = Math.min(max, base * Math.pow(2, n - 1));
    const half = full / 2;
    return Math.round(half + rnd() * half);
}
class RoomRegistry {
    #rooms = new Map();
    get rooms() {
        return [
            ...this.#rooms.keys()
        ];
    }
    has(room) {
        return this.#rooms.has(room);
    }
    add(room, handler) {
        let handlers = this.#rooms.get(room);
        const created = !handlers;
        if (!handlers) {
            handlers = new Set();
            this.#rooms.set(room, handlers);
        }
        handlers.add(handler);
        return created;
    }
    remove(room, handler) {
        const handlers = this.#rooms.get(room);
        if (!handlers) return false;
        handlers.delete(handler);
        if (handlers.size === 0) {
            this.#rooms.delete(room);
            return true;
        }
        return false;
    }
    removeRoom(room) {
        return this.#rooms.delete(room);
    }
    deliver(room, msg, onError) {
        const handlers = this.#rooms.get(room);
        if (!handlers) return;
        for (const handler of [
            ...handlers
        ]){
            try {
                handler(msg);
            } catch (e) {
                onError(e);
            }
        }
    }
    clear() {
        this.#rooms.clear();
    }
}
const isFn = (v)=>typeof v === "function";
const assertFn = (v, prefix = "")=>{
    if (!isFn(v)) throw new TypeError(`${prefix} Expecting function arg`.trim());
};
const strictEqual = (a, b)=>a === b;
function createStore(initial, options = null) {
    const _equal = options?.equal ?? strictEqual;
    const _maybePersist = (v)=>{
        if (options?.persist) {
            try {
                options.persist(v);
            } catch (e) {
                if (options.onPersistError) {
                    options.onPersistError(e);
                } else {
                    console.warn("Store persistence failed:", e);
                }
            }
        }
    };
    const _handleInitialSubscriberError = (e)=>{
        const err = e instanceof Error ? e : new Error(String(e));
        if (options?.onError) {
            options.onError(err, "change", false);
        } else {
            console.error(`Error in subscriber for topic "change":`, err);
        }
    };
    const _pubsub = createPubSub(options?.onError ? {
        onError: (e, topic, isWildcard)=>options.onError(e, topic, isWildcard)
    } : undefined);
    let _value = initial;
    if (options?.eagerPersist !== false) {
        _maybePersist(_value);
    }
    const get = ()=>_value;
    let _notifying = false;
    let _hasPending = false;
    let _pendingValue;
    const _applyChange = (value)=>{
        _value = value;
        _maybePersist(_value);
        _pubsub.publish("change", _value);
    };
    const set = (value)=>{
        if (_equal(_value, value)) return;
        if (_notifying) {
            _hasPending = true;
            _pendingValue = value;
            return;
        }
        _notifying = true;
        try {
            _applyChange(value);
            while(_hasPending){
                const next = _pendingValue;
                _hasPending = false;
                if (!_equal(_value, next)) _applyChange(next);
            }
        } finally{
            _notifying = false;
            _hasPending = false;
        }
    };
    const update = (cb)=>{
        assertFn(cb, "[update]");
        set(cb(get()));
    };
    const subscribe = (cb)=>{
        assertFn(cb, "[subscribe]");
        try {
            cb(_value);
        } catch (e) {
            _handleInitialSubscriberError(e);
        }
        return _pubsub.subscribe("change", cb);
    };
    return {
        set,
        get,
        update,
        subscribe
    };
}
new Map();
function isBrowser() {
    return typeof window !== "undefined" && typeof document !== "undefined" && globalThis === window;
}
const getRaf = (()=>{
    let instance = null;
    return ()=>{
        if (instance) return instance;
        if (isBrowser() && globalThis.requestAnimationFrame && globalThis.cancelAnimationFrame) {
            instance = {
                requestAnimationFrame: globalThis.requestAnimationFrame.bind(globalThis),
                cancelAnimationFrame: globalThis.cancelAnimationFrame.bind(globalThis)
            };
            return instance;
        }
        const pendingFrames = new Map();
        let nextCallbackId = 0;
        instance = {
            requestAnimationFrame (callback) {
                const callbackId = ++nextCallbackId;
                if (nextCallbackId >= Number.MAX_SAFE_INTEGER) {
                    nextCallbackId = 0;
                }
                const timerId = setTimeout(()=>{
                    const frame = pendingFrames.get(callbackId);
                    if (frame) {
                        pendingFrames.delete(callbackId);
                        callback(Date.now());
                    }
                }, 1000 / 60);
                pendingFrames.set(callbackId, {
                    callback,
                    timerId
                });
                return callbackId;
            },
            cancelAnimationFrame (callbackId) {
                const frame = pendingFrames.get(callbackId);
                if (frame) {
                    clearTimeout(frame.timerId);
                    pendingFrames.delete(callbackId);
                }
            }
        };
        return instance;
    };
})();
function setTimeoutRAF(cb, delay) {
    const { requestAnimationFrame: requestAnimationFrame1, cancelAnimationFrame } = getRaf();
    let start = null;
    function on_frame(timestamp) {
        if (start === null) {
            start = timestamp;
        }
        const elapsed = timestamp - start;
        if (elapsed >= delay) {
            cb();
        } else {
            requestId = requestAnimationFrame1(on_frame);
        }
    }
    let requestId = requestAnimationFrame1(on_frame);
    return ()=>cancelAnimationFrame(requestId);
}
const _assertValidInterval = (ms)=>{
    const n = typeof ms === "number" ? ms : Number(ms);
    if (!Number.isFinite(n) || n <= 0) {
        throw new TypeError(`Invalid interval. Expecting positive finite number of milliseconds.`);
    }
    return Math.floor(n);
};
function _createTicker(interval = 1000, start = false, logger = null, useRaf = false, onError = null) {
    const _log = (...v)=>typeof logger === "function" ? logger.apply(null, v) : null;
    if (useRaf && typeof interval === "number" && interval < 1000 / 60) {
        console.warn([
            "Smaller interval than 60Hz may not be accurate with RAF ticker.",
            "Consider using `createTicker` instead of `createTickerRAF`."
        ].join(" "));
    }
    const MIN_TIMEOUT = useRaf ? 1000 / 60 : 0;
    const _setTimeout = useRaf ? setTimeoutRAF : setTimeout;
    const _store = createStore(0, onError ? {
        onError
    } : undefined);
    let _timerId = null;
    const _getInterval = (previous)=>_assertValidInterval(typeof interval === "function" ? interval(previous, _store.get()) : interval);
    let _baseInterval = _getInterval(0);
    let _scheduledDelay = _baseInterval;
    let _isStarted = start;
    let _last = 0;
    const _tick = ()=>{
        _store.set(Date.now());
        if (!_isStarted) return;
        const _now = Date.now();
        const _offset = _last === 0 ? 0 : _now - _last - _scheduledDelay;
        _baseInterval = _getInterval(_baseInterval);
        _scheduledDelay = Math.max(MIN_TIMEOUT, _baseInterval - _offset);
        _timerId = _setTimeout(_tick, _scheduledDelay);
        _last = _now;
        _log({
            _now,
            _offset,
            _baseInterval,
            _scheduledDelay
        });
    };
    const _clearTimer = ()=>{
        if (_timerId !== null) {
            if (typeof _timerId === "function") _timerId();
            else clearTimeout(_timerId);
            _timerId = null;
        }
    };
    const ticker = {
        subscribe: _store.subscribe,
        start: ()=>{
            _isStarted = true;
            _last = 0;
            _baseInterval = _getInterval(0);
            _scheduledDelay = _baseInterval;
            if (_timerId === null) _tick();
            return ticker;
        },
        stop: ()=>{
            _isStarted = false;
            _store.set(0);
            _clearTimer();
            _last = 0;
            return ticker;
        },
        toggle: ()=>{
            _isStarted ? ticker.stop() : ticker.start();
            return ticker;
        },
        isStarted: ()=>_isStarted,
        setInterval: (msOrFn)=>{
            interval = msOrFn;
            _baseInterval = _getInterval(_baseInterval);
            return ticker;
        },
        getInterval: ()=>_baseInterval
    };
    if (start) ticker.start();
    return ticker;
}
function createTicker(interval = 1000, startOrOptions = false, logger = null) {
    if (typeof startOrOptions === "object" && startOrOptions !== null) {
        const opts = startOrOptions;
        return _createTicker(interval, opts.start ?? false, opts.logger ?? null, false, opts.onError ?? null);
    }
    return _createTicker(interval, startOrOptions, logger, false, null);
}
class StalenessWatchdog {
    #options;
    #now;
    #ticker = null;
    #unsubscribe = null;
    #lastSeen = 0;
    constructor(options){
        this.#options = options;
        this.#now = options.now ?? Date.now;
    }
    get running() {
        return !!this.#ticker;
    }
    start() {
        if (this.#options.timeout <= 0 || this.#ticker) return;
        this.#lastSeen = this.#now();
        const step = Math.max(50, Math.floor(this.#options.timeout / 4));
        this.#ticker = createTicker(step);
        this.#unsubscribe = this.#ticker.subscribe((timestamp)=>{
            if (!timestamp) return;
            if (this.#now() - this.#lastSeen >= this.#options.timeout) {
                this.stop();
                this.#options.onStale();
            }
        });
        this.#ticker.start();
    }
    stop() {
        this.#unsubscribe?.();
        this.#unsubscribe = null;
        this.#ticker?.stop();
        this.#ticker = null;
    }
    alive() {
        this.#lastSeen = this.#now();
    }
}
const TRANSITIONS = {
    idle: [
        "connecting",
        "disposed"
    ],
    connecting: [
        "open",
        "reconnecting",
        "terminated",
        "idle",
        "disposed"
    ],
    open: [
        "reconnecting",
        "terminated",
        "idle",
        "disposed"
    ],
    reconnecting: [
        "connecting",
        "open",
        "terminated",
        "idle",
        "disposed"
    ],
    terminated: [
        "connecting",
        "idle",
        "disposed"
    ],
    disposed: []
};
const DEFAULTS = {
    url: "/sse",
    reconnectDelay: 500,
    reconnectDelayMax: 30_000,
    staleTimeout: 45_000,
    connectTimeout: 0,
    fetchTimeout: 30_000
};
const READY_STATE = {
    CONNECTING: 0,
    OPEN: 1,
    CLOSED: 2
};
function makeUnsubscriber(fn) {
    let done = false;
    const u = ()=>{
        if (done) return;
        done = true;
        fn();
    };
    u[Symbol.dispose] = u;
    return u;
}
function resolveUrl(url) {
    if (url instanceof URL) return new URL(url.href);
    const base = typeof location !== "undefined" ? location.href : undefined;
    try {
        return new URL(url, base);
    } catch  {
        throw new SSEError(`Cannot resolve "${url}" — outside a browser the url option must be absolute`);
    }
}
function endpoint(base, path) {
    const u = new URL(base.href);
    u.pathname = `${u.pathname.replace(/\/+$/, "")}/${path}`;
    return u;
}
class SSEClient {
    #url;
    #namespace;
    #clientId;
    #auth;
    #autoConnect;
    #withCredentials;
    #reconnectDelay;
    #reconnectDelayMax;
    #terminalStatusCodes;
    #staleTimeout;
    #connectTimeout;
    #fetchTimeout;
    logger;
    #bus = createPubSub();
    #stateBus = createPubSub();
    #rooms = new RoomRegistry();
    #watchdog;
    #es = null;
    #state = "idle";
    #attempt = 0;
    #lastError = null;
    #lastEventId = null;
    #hello = null;
    #urlRooms = [];
    #generation = 0;
    #reconnectTimer;
    #connectTimer;
    #connectDeferred = null;
    #intentional = false;
    #wakeListeners = [];
    constructor(options){
        if (!options?.namespace || typeof options.namespace !== "string") {
            throw new SSEError("The `namespace` option is required");
        }
        this.logger = options.logger === undefined ? createClog("sse") : options.logger;
        this.#url = resolveUrl(options.url ?? DEFAULTS.url);
        this.#namespace = options.namespace;
        this.#clientId = options.clientId || `c_${base36(12)}`;
        this.#auth = options.auth;
        this.#autoConnect = options.autoConnect ?? true;
        this.#withCredentials = !!options.withCredentials;
        this.#reconnectDelay = options.reconnectDelay ?? DEFAULTS.reconnectDelay;
        this.#reconnectDelayMax = options.reconnectDelayMax ?? DEFAULTS.reconnectDelayMax;
        this.#terminalStatusCodes = new Set(options.terminalStatusCodes ?? DEFAULT_TERMINAL_STATUS_CODES);
        this.#staleTimeout = options.staleTimeout ?? DEFAULTS.staleTimeout;
        this.#connectTimeout = options.connectTimeout ?? DEFAULTS.connectTimeout;
        this.#fetchTimeout = options.fetchTimeout ?? DEFAULTS.fetchTimeout;
        this.#watchdog = new StalenessWatchdog({
            timeout: this.#staleTimeout,
            onStale: ()=>this.#onStale()
        });
        for (const room of options.rooms ?? [])this.#rooms.add(room, ()=>{});
        this.#addWakeListeners();
    }
    get connected() {
        return this.#state === "open";
    }
    get connectionState() {
        return this.#state;
    }
    get clientId() {
        return this.#clientId;
    }
    get namespace() {
        return this.#namespace;
    }
    get rooms() {
        return this.#rooms.rooms;
    }
    get eventSource() {
        return this.#es;
    }
    get url() {
        return new URL(this.#url.href);
    }
    get lastEventId() {
        return this.#lastEventId;
    }
    get state() {
        return {
            subscribe: (cb)=>{
                cb(this.#snapshot());
                return this.#stateBus.subscribe("state", cb);
            }
        };
    }
    dump() {
        return {
            url: this.#url.href,
            state: this.#state,
            clientId: this.#clientId,
            namespace: this.#namespace,
            rooms: this.#rooms.rooms,
            attempt: this.#attempt,
            lastEventId: this.#lastEventId,
            resumed: this.#hello?.resumed ?? null,
            gap: this.#hello?.gap ?? null
        };
    }
    on(event, cb) {
        return this.#bus.subscribe(event, cb);
    }
    once(event, cb) {
        return this.#bus.subscribeOnce(event, cb);
    }
    connect() {
        this.#assertUsable();
        if (this.connected) return Promise.resolve();
        if (this.#connectDeferred) return this.#connectDeferred.promise;
        let resolve;
        let reject;
        const promise = new Promise((res, rej)=>{
            resolve = res;
            reject = rej;
        });
        this.#connectDeferred = {
            promise,
            resolve,
            reject
        };
        if (this.#connectTimeout > 0) {
            this.#connectTimer = setTimeout(()=>{
                this.#settleConnect(new SSEConnectTimeoutError(this.#connectTimeout));
            }, this.#connectTimeout);
        }
        this.#intentional = false;
        this.#attempt = 0;
        this.#openStream();
        return promise;
    }
    disconnect() {
        if (this.#state === "disposed") return;
        this.logger?.debug?.("disconnect()");
        this.#intentional = true;
        this.#clearTimers();
        this.#closeStream();
        this.#settleConnect(new SSETerminatedError(0, "disconnect() called"));
        this.#setState("idle");
    }
    dispose() {
        if (this.#state === "disposed") return;
        this.logger?.debug?.("dispose()");
        this.disconnect();
        this.#rooms.clear();
        this.#bus.unsubscribeAll();
        this.#stateBus.unsubscribeAll();
        this.#removeWakeListeners();
        this.#setState("disposed");
    }
    async subscribe(room, handler) {
        this.#assertUsable();
        const messageHandler = handler;
        const created = this.#rooms.add(room, messageHandler);
        const unsubscriber = makeUnsubscriber(()=>{
            const emptied = this.#rooms.remove(room, messageHandler);
            if (emptied && this.connected) {
                void this.#rpc({
                    op: OP.UNSUB,
                    clientId: this.#clientId,
                    namespace: this.#namespace,
                    rooms: [
                        room
                    ]
                }).catch((e)=>this.#fail(e, "unsubscribe"));
            }
        });
        if (this.#autoConnect) this.#ensureStarted();
        if (created && this.connected) {
            try {
                await this.#rpc({
                    op: OP.SUB,
                    clientId: this.#clientId,
                    namespace: this.#namespace,
                    rooms: [
                        room
                    ]
                });
            } catch (e) {
                this.#rooms.remove(room, messageHandler);
                throw e;
            }
        }
        return unsubscriber;
    }
    async unsubscribe(room) {
        this.#assertUsable();
        if (!this.#rooms.removeRoom(room)) return;
        if (!this.connected) return;
        await this.#rpc({
            op: OP.UNSUB,
            clientId: this.#clientId,
            namespace: this.#namespace,
            rooms: [
                room
            ]
        });
    }
    isSubscribed(room) {
        return this.#rooms.has(room);
    }
    async publish(room, payload) {
        this.#assertUsable();
        const result = await this.#rpc({
            op: OP.PUB,
            clientId: this.#clientId,
            namespace: this.#namespace,
            room,
            payload
        });
        return {
            recipients: result.recipients ?? 0
        };
    }
    async broadcast(room, payload) {
        this.#assertUsable();
        const result = await this.#rpc({
            op: OP.BROADCAST,
            clientId: this.#clientId,
            namespace: this.#namespace,
            room,
            payload
        });
        return {
            recipients: result.recipients ?? 0
        };
    }
    #ensureStarted() {
        if (this.#state === "idle" || this.#state === "terminated") {
            void this.connect().catch(()=>{});
        }
    }
    #openStream() {
        if (!this.#setState("connecting")) return;
        const generation = ++this.#generation;
        void this.#buildStreamUrl().then((url)=>{
            if (generation !== this.#generation || this.#state === "disposed") return;
            this.logger?.debug?.(`opening stream (attempt ${this.#attempt})`);
            let es;
            try {
                es = new EventSource(url.href, {
                    withCredentials: this.#withCredentials
                });
            } catch (e) {
                this.#fail(e, "EventSource construction");
                this.#scheduleReconnect();
                return;
            }
            this.#es = es;
            es.onopen = ()=>{
                if (generation !== this.#generation) return;
                this.logger?.debug?.("stream open");
                this.#emit("open", undefined);
                this.#watchdog.start();
            };
            es.onerror = ()=>{
                if (generation !== this.#generation) return;
                this.#onStreamError(es);
            };
            es.addEventListener(SSE_EVENT.HELLO, (ev)=>{
                if (generation !== this.#generation) return;
                this.#watchdog.alive();
                this.#onHello(ev);
            });
            es.addEventListener(SSE_EVENT.MESSAGE, (ev)=>{
                if (generation !== this.#generation) return;
                this.#watchdog.alive();
                this.#onMessage(ev);
            });
            es.addEventListener(SSE_EVENT.HEARTBEAT, (ev)=>{
                if (generation !== this.#generation) return;
                this.#watchdog.alive();
                try {
                    this.#emit("heartbeat", JSON.parse(ev.data));
                } catch  {}
            });
        }).catch((e)=>{
            if (generation !== this.#generation) return;
            this.#fail(e, "stream setup");
            this.#scheduleReconnect();
        });
    }
    async #buildStreamUrl() {
        const url = endpoint(this.#url, "events");
        url.searchParams.set("clientId", this.#clientId);
        url.searchParams.set("namespace", this.#namespace);
        this.#urlRooms = this.#rooms.rooms;
        if (this.#urlRooms.length) {
            url.searchParams.set("rooms", this.#urlRooms.join(","));
        }
        const auth = await this.#authValue();
        if (auth !== null) url.searchParams.set(AUTH_PARAM, auth);
        if (this.#lastEventId) {
            url.searchParams.set(LAST_EVENT_ID_PARAM, this.#lastEventId);
        }
        return url;
    }
    #onHello(ev) {
        let hello;
        try {
            hello = JSON.parse(ev.data);
        } catch (e) {
            return this.#fail(e, "malformed hello");
        }
        this.#hello = hello;
        this.#clientId = hello.clientId;
        this.#namespace = hello.namespace;
        this.#attempt = 0;
        this.#lastError = null;
        if (hello.gap) {
            this.logger?.warn?.("resume point expired — messages were missed, refetch if that matters");
        }
        if (!this.#setState("open")) return;
        this.#clearTimers();
        this.#settleConnect(null);
        this.logger?.debug?.(`connected as ${hello.clientId} in "${hello.namespace}"` + (hello.resumed ? " (resumed)" : ""));
        this.#emit("connected", hello);
        this.#reconcileRooms();
    }
    #reconcileRooms() {
        const local = new Set(this.#rooms.rooms);
        const sent = new Set(this.#urlRooms);
        const missing = [
            ...local
        ].filter((r)=>!sent.has(r));
        const extra = [
            ...sent
        ].filter((r)=>!local.has(r));
        if (missing.length) {
            void this.#rpc({
                op: OP.SUB,
                clientId: this.#clientId,
                namespace: this.#namespace,
                rooms: missing
            }).catch((e)=>this.#fail(e, "reconcile subscribe"));
        }
        if (extra.length) {
            void this.#rpc({
                op: OP.UNSUB,
                clientId: this.#clientId,
                namespace: this.#namespace,
                rooms: extra
            }).catch((e)=>this.#fail(e, "reconcile unsubscribe"));
        }
        this.#urlRooms = [
            ...local
        ];
    }
    #onMessage(ev) {
        if (ev.lastEventId) this.#lastEventId = ev.lastEventId;
        let message;
        try {
            message = JSON.parse(ev.data);
        } catch (e) {
            return this.#fail(e, "malformed message");
        }
        this.#emit("message", message);
        this.#rooms.deliver(message.room, message, (e)=>this.#fail(e, `handler for room "${message.room}"`));
    }
    #onStreamError(es) {
        if (this.#intentional || this.#state === "disposed") return;
        const wasOpen = this.#state === "open";
        this.#watchdog.stop();
        if (es.readyState === READY_STATE.CONNECTING) {
            this.#attempt++;
            if (wasOpen) {
                this.#emit("close", {
                    status: null,
                    reason: "stream interrupted",
                    willReconnect: true
                });
            }
            if (this.#setState("reconnecting")) {
                this.logger?.debug?.(`stream dropped, EventSource retrying (attempt ${this.#attempt})`);
                this.#emit("reconnecting", {
                    attempt: this.#attempt,
                    delay: null
                });
            }
            return;
        }
        this.#closeStream();
        this.#attempt++;
        void this.#probe().then((status)=>{
            if (this.#intentional || this.#state === "disposed") return;
            const terminal = status !== null && this.#terminalStatusCodes.has(status);
            const reason = terminal ? `rejected with ${status}` : `stream failed${status === null ? "" : ` with ${status}`}`;
            this.#emit("close", {
                status,
                reason,
                willReconnect: !terminal
            });
            if (terminal) return this.#terminate(status, reason);
            this.#scheduleReconnect();
        });
    }
    async #probe() {
        try {
            const url = endpoint(this.#url, "ping");
            const auth = await this.#authValue();
            if (auth !== null) url.searchParams.set(AUTH_PARAM, auth);
            const res = await this.#fetch(url, {
                method: "GET"
            });
            return res.status;
        } catch (e) {
            this.logger?.debug?.(`probe failed: ${e}`);
            return null;
        }
    }
    #onStale() {
        if (this.#state !== "open" && this.#state !== "connecting") return;
        this.logger?.warn?.(`no traffic for ${this.#staleTimeout}ms — assuming a dead stream`);
        this.#closeStream();
        this.#attempt++;
        this.#emit("close", {
            status: null,
            reason: "stream went silent",
            willReconnect: true
        });
        this.#scheduleReconnect();
    }
    #scheduleReconnect() {
        if (this.#intentional || this.#state === "disposed") return;
        if (!this.#setState("reconnecting")) return;
        const delay = backoffDelay(this.#attempt, this.#reconnectDelay, this.#reconnectDelayMax);
        this.logger?.debug?.(`reconnecting in ${delay}ms (attempt ${this.#attempt})`);
        this.#emit("reconnecting", {
            attempt: this.#attempt,
            delay
        });
        clearTimeout(this.#reconnectTimer);
        this.#reconnectTimer = setTimeout(()=>this.#openStream(), delay);
    }
    #terminate(status, reason) {
        this.logger?.error?.(`giving up: ${reason}`);
        this.#clearTimers();
        this.#closeStream();
        this.#setState("terminated");
        const error = new SSETerminatedError(status, reason);
        this.#lastError = error;
        this.#settleConnect(error);
        this.#emit("terminated", {
            status,
            reason
        });
    }
    #closeStream() {
        this.#watchdog.stop();
        this.#generation++;
        const es = this.#es;
        this.#es = null;
        try {
            es?.close();
        } catch  {}
    }
    async #rpc(op) {
        const url = endpoint(this.#url, "rpc");
        const auth = await this.#authValue();
        const headers = {
            "content-type": "application/json"
        };
        if (auth !== null) headers[AUTH_HEADER] = auth;
        const res = await this.#fetch(url, {
            method: "POST",
            headers,
            body: JSON.stringify(op)
        });
        let body = null;
        try {
            body = await res.json();
        } catch  {}
        if (!res.ok || !body || body.ok !== true) {
            const info = body && body.ok === false && body.error || {
                code: "internal",
                message: `Request failed with ${res.status}`
            };
            throw new SSERemoteError(info, res.status);
        }
        return body;
    }
    async #fetch(url, init) {
        const controller = new AbortController();
        const timer = setTimeout(()=>controller.abort(), this.#fetchTimeout);
        try {
            return await fetch(url.href, {
                ...init,
                signal: controller.signal,
                credentials: this.#withCredentials ? "include" : "same-origin"
            });
        } catch (e) {
            if (controller.signal.aborted) {
                throw new SSETimeoutError(this.#fetchTimeout);
            }
            throw e;
        } finally{
            clearTimeout(timer);
        }
    }
    async #authValue() {
        if (!this.#auth) return null;
        const value = await this.#auth();
        if (value === undefined) return null;
        return JSON.stringify(value);
    }
    #addWakeListeners() {
        if (typeof globalThis.addEventListener !== "function") return;
        const doc = globalThis.document;
        const wake = ()=>{
            if (this.#state !== "reconnecting") return;
            if (doc?.hidden) return;
            this.logger?.debug?.("woke up — retrying now");
            clearTimeout(this.#reconnectTimer);
            this.#openStream();
        };
        for (const event of [
            "online",
            "visibilitychange",
            "focus"
        ]){
            globalThis.addEventListener(event, wake);
            this.#wakeListeners.push(()=>globalThis.removeEventListener(event, wake));
        }
    }
    #removeWakeListeners() {
        for (const remove of this.#wakeListeners)remove();
        this.#wakeListeners = [];
    }
    #clearTimers() {
        clearTimeout(this.#reconnectTimer);
        this.#reconnectTimer = undefined;
        clearTimeout(this.#connectTimer);
        this.#connectTimer = undefined;
    }
    #settleConnect(error) {
        const deferred = this.#connectDeferred;
        if (!deferred) return;
        this.#connectDeferred = null;
        clearTimeout(this.#connectTimer);
        this.#connectTimer = undefined;
        if (error) deferred.reject(error);
        else deferred.resolve();
    }
    #assertUsable() {
        if (this.#state === "disposed") throw new SSEDisposedError();
    }
    #setState(next) {
        if (this.#state === next) return true;
        if (!TRANSITIONS[this.#state].includes(next)) {
            this.logger?.warn?.(`ignoring illegal state transition ${this.#state} -> ${next}`);
            return false;
        }
        this.#state = next;
        this.#publishState();
        return true;
    }
    #snapshot() {
        return {
            state: this.#state,
            connected: this.#state === "open",
            connecting: this.#state === "connecting" || this.#state === "reconnecting",
            attempt: this.#attempt,
            lastError: this.#lastError
        };
    }
    #publishState() {
        this.#stateBus.publish("state", this.#snapshot());
    }
    #emit(event, data) {
        this.#bus.publish(event, data);
    }
    #fail(error, context) {
        const err = error instanceof Error ? error : new SSEError(String(error));
        this.#lastError = err;
        this.logger?.error?.(`${context}: ${err.message}`);
        this.#emit("error", err);
    }
}
function createSSEClient(options) {
    return new SSEClient(options);
}
const nameOf = (clientId)=>clientId ? clientId.split("#")[0] : "pipeline";
let entrySeq = 0;
const localKey = ()=>`local-${++entrySeq}`;
function messageKey(msg) {
    const { payload } = msg;
    if (isEventPayload(payload) || isCommentPayload(payload)) {
        if (payload.id) return payload.id;
    }
    return `${msg.from ?? "server"}:${msg.timestamp}`;
}
function createFeedStore(session, onTerminated) {
    const tab = Math.random().toString(36).slice(2, 8);
    const sse = createSSEClient({
        url: "/sse",
        namespace: session.project,
        auth: ()=>({
                name: session.name,
                project: session.project,
                tab
            }),
        connectTimeout: 8_000,
        staleTimeout: 30_000
    });
    const connection = observable({
        state: "idle",
        connected: false,
        connecting: false,
        attempt: 0,
        lastError: null
    });
    const room = observable(DEFAULT_ROOM);
    const entries = observable([]);
    const resume = observable({
        resumed: false,
        gap: false,
        from: null
    });
    const paused = observable(false);
    const maintenance = observable(false);
    const joining = observable(false);
    let seen = new Set();
    let connectedAt = 0;
    let markReplayed = false;
    sse.state.subscribe((s)=>connection.set(s));
    sse.on("terminated", ({ status, reason })=>{
        onTerminated(`${reason || "connection refused"} (HTTP ${status})`);
    });
    sse.on("connected", (hello)=>{
        connectedAt = Date.now();
        markReplayed = hello.resumed;
        resume.set({
            resumed: hello.resumed,
            gap: hello.gap,
            from: sse.lastEventId
        });
        if (hello.resumed) {
            divider(hello.gap ? "resumed with a gap — the window overflowed" : "resumed — nothing was missed");
        }
        if (hello.gap) void loadHistory(room.get(), {
            append: false
        });
    });
    function push(entry) {
        if (seen.has(entry.key)) return;
        seen.add(entry.key);
        entries.update((list)=>{
            const next = [
                ...list,
                entry
            ];
            return next.length > 300 ? next.slice(-300) : next;
        });
    }
    function notice(text, kind = "notice") {
        push({
            key: localKey(),
            kind,
            level: kind === "error" ? "error" : "info",
            who: "",
            text,
            ts: Date.now(),
            own: false,
            replayed: false,
            recipients: null
        });
    }
    function divider(text) {
        push({
            key: localKey(),
            kind: "divider",
            level: "info",
            who: "",
            text,
            ts: Date.now(),
            own: false,
            replayed: false,
            recipients: null
        });
    }
    function toEntry(msg) {
        const key = messageKey(msg);
        const fromServer = msg.from === null;
        const replayed = markReplayed && msg.timestamp < connectedAt;
        if (isCommentPayload(msg.payload)) {
            return {
                key,
                kind: "comment",
                level: "info",
                who: fromServer ? msg.payload.who : nameOf(msg.from),
                text: msg.payload.text,
                ts: msg.timestamp,
                own: !fromServer && msg.from === sse.clientId,
                replayed,
                recipients: null
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
                recipients: null
            };
        }
        return {
            key,
            kind: "system",
            level: "info",
            who: fromServer ? "server" : nameOf(msg.from),
            text: JSON.stringify(msg.payload),
            ts: msg.timestamp,
            own: false,
            replayed,
            recipients: null
        };
    }
    const onMessage = (msg)=>push(toEntry(msg));
    async function loadHistory(target, { append }) {
        const ns = sse.namespace;
        try {
            const res = await fetch(`/api/history/${encodeURIComponent(ns)}/${encodeURIComponent(target)}`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const { messages } = await res.json();
            if (room.get() !== target) return;
            const backlog = [];
            for (const msg of messages){
                const key = messageKey(msg);
                if (seen.has(key)) continue;
                seen.add(key);
                backlog.push({
                    ...toEntry(msg),
                    replayed: false
                });
            }
            if (!backlog.length) return;
            if (append) {
                entries.update((list)=>[
                        ...list,
                        ...backlog
                    ]);
            } else {
                entries.update((list)=>{
                    const next = [
                        ...backlog,
                        ...list
                    ];
                    return next.length > 300 ? next.slice(-300) : next;
                });
            }
        } catch (err) {
            notice(`could not load history: ${err.message}`, "error");
        }
    }
    async function join(target) {
        const previous = room.get();
        joining.set(true);
        try {
            try {
                await sse.connect();
            } catch (err) {
                notice(`not connected yet — ${err.message}`);
            }
            if (target !== previous && sse.isSubscribed(previous)) {
                await sse.unsubscribe(previous);
            }
            room.set(target);
            entries.set([]);
            seen = new Set();
            await sse.subscribe(target, onMessage);
            await loadHistory(target, {
                append: false
            });
        } catch (err) {
            notice(`could not join #${target}: ${err.message}`, "error");
        } finally{
            joining.set(false);
        }
    }
    async function comment(text, broadcast) {
        const trimmed = text.trim();
        if (!trimmed) return;
        const target = room.get();
        const payload = {
            kind: "comment",
            id: crypto.randomUUID(),
            who: session.name,
            text: trimmed
        };
        try {
            const { recipients } = broadcast ? await sse.broadcast(target, payload) : await sse.publish(target, payload);
            entries.update((list)=>list.map((e)=>e.key === payload.id ? {
                        ...e,
                        recipients
                    } : e));
            if (!sse.connected) {
                notice(`sent to ${recipients} recipient(s) with the stream down`);
            }
        } catch (err) {
            notice(`✗ ${err.message}`, "error");
        }
    }
    function pause() {
        if (paused.get()) return;
        paused.set(true);
        sse.disconnect();
        divider(`paused at ${sse.lastEventId ?? "the start"}`);
    }
    function unpause() {
        if (!paused.get()) return;
        paused.set(false);
        sse.connect().catch(()=>{});
    }
    async function setMaintenance(on) {
        try {
            const res = await fetch("/api/maintenance", {
                method: "POST",
                headers: {
                    "content-type": "application/json"
                },
                body: JSON.stringify({
                    on
                })
            });
            const body = await res.json();
            maintenance.set(body.on);
            notice(body.on ? "server is now answering /sse with 503 — watch it retry, not give up" : "maintenance over");
        } catch (err) {
            notice(`could not toggle maintenance: ${err.message}`, "error");
        }
    }
    async function burst(count) {
        try {
            await fetch("/api/burst", {
                method: "POST",
                headers: {
                    "content-type": "application/json"
                },
                body: JSON.stringify({
                    count
                })
            });
        } catch (err) {
            notice(`could not burst: ${err.message}`, "error");
        }
    }
    function leave() {
        sse.dispose();
    }
    fetch("/api/maintenance").then((r)=>r.json()).then((b)=>maintenance.set(!!b.on)).catch(()=>{});
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
        leave
    };
}
const SESSION_KEY = "sse-feed-session";
const root = document.getElementById("app");
const time = new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
});
function createLogin({ initial, error, onSubmit }) {
    return createView((track)=>{
        const el = fromTemplate("tpl-login");
        const r = refs(el);
        r.name.value = initial.name;
        r.project.value = initial.project;
        r.error.textContent = error;
        r.error.hidden = !error;
        track(delegate(el, {
            submit: (e)=>{
                e.preventDefault();
                const name = r.name.value.trim();
                const project = r.project.value.trim() || DEFAULT_PROJECT;
                if (!NAME_RE.test(name)) {
                    return fail("2–16 chars: letters, digits, _ or -");
                }
                if (!PROJECT_RE.test(project)) {
                    return fail("project: 2–24 chars, same alphabet");
                }
                onSubmit({
                    name,
                    project
                });
            }
        }));
        function fail(message) {
            r.error.textContent = message;
            r.error.hidden = false;
        }
        queueMicrotask(()=>r.name.focus());
        return {
            el
        };
    });
}
function createFeed({ store, onLeave }) {
    return createView((track)=>{
        const el = fromTemplate("tpl-feed");
        const r = refs(el);
        const broadcast = observable(false);
        r.you.textContent = store.session.name;
        r.project.textContent = store.session.project;
        r.replaySize.textContent = String(REPLAY_BUFFER_SIZE);
        for (const name of ROOMS){
            const button = document.createElement("button");
            button.type = "button";
            button.dataset.on = "click:pickRoom";
            button.dataset.room = name;
            button.textContent = `#${name}`;
            r.rooms.appendChild(button);
        }
        track(delegate(el, {
            pickRoom: (_e, target)=>{
                const next = target.dataset.room;
                if (next !== store.room.get()) void store.join(next);
            },
            send: (e)=>{
                e.preventDefault();
                const input = r.input;
                const text = input.value;
                input.value = "";
                input.focus();
                void store.comment(text, broadcast.get());
            },
            toggleBroadcast: (_e, target)=>{
                broadcast.set(target.checked);
            },
            togglePause: ()=>{
                if (store.paused.get()) store.unpause();
                else store.pause();
            },
            toggleMaintenance: ()=>{
                void store.setMaintenance(!store.maintenance.get());
            },
            burst: ()=>void store.burst(20 + 10),
            leave: onLeave
        }));
        track(store.connection.subscribe(({ state, connected, attempt, lastError })=>{
            r.status.dataset.state = connected ? "open" : state;
            r.status.textContent = connected ? "connected" : attempt > 0 ? `reconnecting… (attempt ${attempt})` : state;
            r.status.title = lastError?.message ?? "";
        }));
        track(store.resume.subscribe(({ resumed, gap, from })=>{
            const badge = r.resume;
            badge.dataset.state = gap ? "gap" : resumed ? "resumed" : "live";
            badge.textContent = gap ? "gap — refetched" : resumed ? "resumed" : "live";
            badge.title = from ? `last event id: ${from}` : "no resume point yet";
        }));
        track(reactTo([
            store.paused,
            store.maintenance
        ], ()=>{
            const paused = store.paused.get();
            r.pause.textContent = paused ? "▶ Resume" : "⏸ Pause";
            r.pause.setAttribute("aria-pressed", String(paused));
            const down = store.maintenance.get();
            r.maintenance.textContent = down ? "🔧 503 is ON" : "🔧 Simulate 503";
            r.maintenance.setAttribute("aria-pressed", String(down));
        }));
        track(reactTo([
            store.room,
            store.joining
        ], ()=>{
            const current = store.room.get();
            const busy = store.joining.get();
            r.roomName.textContent = `#${current}`;
            el.querySelectorAll("[data-room]").forEach((b)=>{
                b.setAttribute("aria-pressed", String(b.dataset.room === current));
                b.disabled = busy;
            });
            r.broadcastHint.textContent = current === BROADCAST_ROOM ? "allowed here" : `denied outside #${BROADCAST_ROOM}`;
        }));
        track(store.entries.subscribe((list)=>{
            const atBottom = r.log.scrollHeight - r.log.scrollTop - r.log.clientHeight < 40;
            r.log.replaceChildren(...list.map(renderEntry));
            r.empty.hidden = list.length > 0;
            if (atBottom) r.log.scrollTop = r.log.scrollHeight;
        }));
        function renderEntry(entry) {
            const li = fromTemplate("tpl-entry");
            li.dataset.kind = entry.kind;
            li.dataset.level = entry.level;
            li.classList.toggle("own", entry.own);
            li.classList.toggle("replayed", entry.replayed);
            applyBindings(li, {
                who: entry.who,
                text: entry.text,
                when: time.format(entry.ts),
                noWho: !entry.who,
                ack: entry.recipients === null ? "" : `✓ ${entry.recipients}`,
                noAck: entry.recipients === null,
                noTag: !entry.replayed
            });
            return li;
        }
        void store.join(DEFAULT_ROOM);
        queueMicrotask(()=>r.input.focus());
        return {
            el
        };
    });
}
const current = observable(null);
let store = null;
function show(view) {
    current.get()?.destroy();
    current.set(view);
    root.replaceChildren(view.el);
}
function loadSession() {
    try {
        const saved = JSON.parse(localStorage.getItem(SESSION_KEY) || "null");
        if (saved && typeof saved.name === "string") {
            return {
                name: saved.name,
                project: saved.project || DEFAULT_PROJECT
            };
        }
    } catch  {}
    return {
        name: "",
        project: DEFAULT_PROJECT
    };
}
function showLogin(error = "") {
    store = null;
    show(createLogin({
        initial: loadSession(),
        error,
        onSubmit: startFeed
    }));
}
function startFeed(session) {
    try {
        localStorage.setItem(SESSION_KEY, JSON.stringify(session));
    } catch  {}
    store = createFeedStore(session, (reason)=>{
        store?.leave();
        showLogin(`Server refused the connection: ${reason}`);
    });
    show(createFeed({
        store,
        onLeave: ()=>{
            store?.leave();
            showLogin();
        }
    }));
}
showLogin();
Object.defineProperty(globalThis, "feed", {
    get: ()=>store
});
