import Transport from 'winston-transport';

/**
 * A winston transport that batches log lines and pushes them to Grafana Loki.
 *
 * Every line is emitted with the same field names regardless of which service produced it,
 * so a single set of Grafana queries covers all of them. Console output is untouched: attach
 * this alongside a Console transport and CloudWatch keeps receiving what it received before.
 *
 * Services differ in the details of that shape, so the parts that vary are options rather
 * than assumptions - see {@link LokiLineFormatOptions} and the {@link LEGACY_LINE_FORMAT}
 * preset.
 */
/**
 * Controls the exact shape of the JSON pushed to Loki.
 *
 * The defaults produce the leaner shape: unset fields are dropped, `nestedErrorStack` walks
 * the standard `cause` chain, and only recognised fields are emitted. Pass
 * {@link LEGACY_LINE_FORMAT} to reproduce the older line format byte for byte.
 */
type LokiLineFormatOptions = {
    /**
     * What to emit for `undefined` values. Left unset, the key is dropped as `JSON.stringify`
     * would. The legacy format emits the literal `'__undefined__'`, which marks a field as
     * deliberately blanked rather than never set.
     */
    undefinedPlaceholder?: string;
    /**
     * Properties walked to build `nestedErrorStack`, in order of preference. Defaults to the
     * standard `['cause']`; error classes that chain through a different property need it named
     * here. The walk is cycle-guarded and depth-capped either way.
     */
    errorCauseKeys?: Array<string>;
    /**
     * Blank out error-shaped keys in `payload` when they hold the same object as the top-level
     * `error`, so one stack trace is not serialized twice in the same line.
     */
    dedupePayloadErrors?: boolean;
    /** Copy unrecognised `info` fields onto the line rather than dropping them. */
    passthroughFields?: boolean;
    /** `info` fields to drop before passthrough. */
    stripFields?: Array<string>;
};
/**
 * Reproduces the line format used before this transport was extracted into a package. Pass
 * this from a service whose existing Loki lines and Grafana queries must not change.
 */
declare const LEGACY_LINE_FORMAT: LokiLineFormatOptions;
/** Options accepted by {@link LokiTransport}. */
type LokiTransportOptions = {
    /** Base URL of the Loki write endpoint, e.g. `http://loki.internal:3100`. Trailing slashes are stripped. */
    host: string;
    /**
     * Basic-auth username. Optional, and only sent when both this and `lokiToken` are given -
     * a Loki running with `auth_enabled: false` behind a private endpoint ignores the header
     * entirely, so many deployments have nothing meaningful to put here.
     */
    lokiUser?: string;
    /** Basic-auth password/token. See {@link LokiTransportOptions.lokiUser}. */
    lokiToken?: string;
    /** Minimum winston level this transport accepts. Pass the same level as the logger to mirror it exactly. */
    level: string;
    /** Stream labels applied to every line. Keep cardinality low; put high-cardinality data in the log body. */
    labels: Record<string, string>;
    /** Batching interval in ms. Defaults to 1000; use ~100 for short-lived processes such as Lambda. */
    interval?: number;
    /** Enables per-line and per-flush console logging for debugging the transport itself. */
    debug?: boolean;
    /**
     * Stable per-process id stamped on every line. Defaults to `process.env.LOGGER_ID`, or a
     * fresh uuid which is written back to that variable so child processes inherit it.
     */
    loggerId?: string;
    /**
     * Resolves per-request context merged into each line as `context`. Omit it and no `context`
     * key is emitted at all. Wire this to an AsyncLocalStorage store.
     */
    getContext?: () => Record<string, unknown> | undefined;
    /**
     * Resolves fields spread onto every line, for values only discoverable at runtime - ECS
     * task and container ids, for instance. Called once per line, so keep it cheap.
     */
    getExtraFields?: () => Record<string, unknown> | undefined;
    /** See {@link LokiLineFormatOptions}. */
    lineFormat?: LokiLineFormatOptions;
    /**
     * Flush on winston's `logger.end()` by implementing `_final`. Off by default: when one
     * transport instance is shared across several loggers, the first `end()` would close it for
     * all of them. Enable only where the process owns the transport and ends it deliberately.
     */
    flushOnEnd?: boolean;
};
/**
 * The fields this transport reads off a winston info object.
 *
 * Two naming conventions are accepted for the same four values, because the services feeding
 * this transport built their winston formats independently. The emitted line always uses the
 * canonical name, so the difference is invisible in Grafana.
 *
 * | Emitted as | Canonical | Also accepted |
 * | ---------- | --------- | ------------- |
 * | `date`     | `date`    | `timestamp`   |
 * | `category` | `category`| `label`       |
 * | `error`    | `error`   | `cause`       |
 * | `payload`  | `payload` | `metadata`    |
 */
type WinstonLogInfo = {
    level?: string;
    message?: unknown;
    namespace?: string;
    service?: string;
    date?: string;
    timestamp?: string;
    category?: string;
    label?: string;
    error?: unknown;
    cause?: unknown;
    payload?: unknown;
    metadata?: unknown;
    [key: string]: unknown;
};
declare class LokiTransport extends Transport {
    private buffer;
    private timer;
    private labels;
    private host;
    private authHeader;
    private debug;
    private loggerId;
    private flushPromise;
    private closePromise;
    private droppedDueToRetries;
    private droppedDueToBacklog;
    private lastDropLogAtMs;
    private isClosing;
    private readonly getContext?;
    private readonly getExtraFields?;
    private readonly lineFormat;
    private readonly causeKeys;
    private readonly stripFields;
    private readonly flushOnEnd;
    /** Configured by lineFormat.undefinedPlaceholder; see makeSafeStringify. */
    private readonly stringify;
    private readonly MAX_BUFFER_SIZE;
    private readonly CHUNK_SIZE;
    private readonly FLUSH_TIMEOUT_MS;
    constructor(opts: LokiTransportOptions);
    log(info: WinstonLogInfo, callback: () => void): void;
    /**
     * Bounded stand-in for a line over MAX_LINE_BYTES. Keeps the fields that identify the line
     * and drops `payload` and any passthrough, which are what make a line oversized.
     *
     * The message is capped too. Copying it verbatim meant that when the bulk of an oversized
     * line WAS the message, the replacement came out larger than the original - preview bytes
     * on top of the full message - so the line sailed straight past the cap it exists to
     * enforce.
     */
    private buildTruncatedLine;
    /** Lines still queued. Drain against this, since a failed flush requeues its entries and still resolves. */
    get pendingCount(): number;
    /** Drops the oldest N lines and reports it, throttled so a sustained overflow can't spam stdout. */
    private dropOldest;
    /**
     * Ships the batch currently buffered. Single-flight: a concurrent call gets the in-flight
     * promise rather than a continuation, so log() hitting MAX_BUFFER_SIZE on every line can't
     * accumulate one pending promise per line.
     *
     * That promise only covers the batch flushInternal() detached, so callers needing
     * everything gone should loop until pendingCount is 0; close() does exactly that.
     */
    flush(): Promise<void>;
    private flushInternal;
    private postChunkWithAutoSplit;
    private postGzipped;
    close(): Promise<void>;
    /**
     * Flushes on stream finalization, so winston's `logger.end()` ships everything buffered.
     * https://nodejs.org/api/stream.html#writable_finalcallback
     *
     * Opt-in via `flushOnEnd`, and deliberately so. When one transport instance is shared by
     * several loggers, honouring finalization lets any single logger ending set `isClosing` and
     * clear the timer permanently, silently cutting Loki off for all the others. Services that
     * share a transport should leave this off and rely on the SIGTERM/SIGINT handler, or await
     * `flush()` directly.
     */
    _final(callback: (error?: Error | null) => void): void;
}

export { LEGACY_LINE_FORMAT, type LokiLineFormatOptions, LokiTransport, type LokiTransportOptions, type WinstonLogInfo };
