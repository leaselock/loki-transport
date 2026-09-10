import http from 'node:http';
import https from 'node:https';
import zlib from 'node:zlib';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';

import Transport from 'winston-transport';
import axios from 'axios';
import jsonSafeStringify from 'json-stringify-safe';

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
export type LokiLineFormatOptions = {
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
export const LEGACY_LINE_FORMAT: LokiLineFormatOptions = {
  undefinedPlaceholder: '__undefined__',
  errorCauseKeys: ['nested'],
  dedupePayloadErrors: true,
  passthroughFields: true,
  stripFields: ['stdoutMethod', 'stderrMethod'],
};

/** Options accepted by {@link LokiTransport}. */
export type LokiTransportOptions = {
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

type LogLine = {
  timestamp: number;
  level: string;
  line: string; // JSON string
  retries: number;
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
export type WinstonLogInfo = {
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
  // Anything else winston attached; emitted only when passthroughFields is on.
  [key: string]: unknown;
};

/** Thrown by flushInternal to carry the entries that still need requeueing. */
class FlushError extends Error {
  constructor(readonly failedEntries: Array<LogLine>) {
    super(`Failed to push ${failedEntries.length} log entries to Loki`);
    this.name = 'FlushError';
  }
}

/** Shape of the parts of an axios error we report on. */
type HttpErrorLike = { response?: { status?: number; statusText?: string; data?: unknown } };

const httpAgent = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });

const MAX_LINE_BYTES = 128 * 1024; // 128 KiB per log line (ALB-safe + avoids Loki parsing edge cases)
const MAX_GZIPPED_PUSH_BYTES = 900 * 1024; // 900 KiB, safely under ALB ~1MiB hard limit
const MAX_RETRIES = 3;
const MAX_BACKLOG_LINES = 50_000;
const TRUNCATION_PREVIEW_BYTES = 16 * 1024; // 16 KiB preview in truncated payloads
const DROP_LOG_THROTTLE_MS = 10_000;
const MAX_CAUSE_DEPTH = 10; // Bound the error `cause` walk in buildNestedErrorStack

const gzipAsync = promisify(zlib.gzip);

function utf8ByteLength(str: string): number {
  return Buffer.byteLength(str, 'utf8');
}

function truncateUtf8(str: string, maxBytes: number): string {
  const buf = Buffer.from(str, 'utf8');
  if (buf.length <= maxBytes) return str;
  return buf.subarray(0, maxBytes).toString('utf8');
}

function splitEvery<T>(size: number, list: Array<T>): Array<Array<T>> {
  const chunks: Array<Array<T>> = [];
  for (let i = 0; i < list.length; i += size) {
    chunks.push(list.slice(i, i + size));
  }
  return chunks;
}

/**
 * Runs a caller-supplied resolver and swallows anything it throws.
 *
 * `getContext` and `getExtraFields` are arbitrary consumer functions called on every line.
 * Left unguarded, one of them throwing would propagate out of `log()` and into whatever
 * called `logger.info(...)` - so a logging statement could take down application code. Every
 * other failure path here is contained; these have to be too.
 */
function resolveSafely(
  resolve: (() => Record<string, unknown> | undefined) | undefined,
  optionName: string
): Record<string, unknown> | undefined {
  if (!resolve) return undefined;
  try {
    return resolve();
  } catch (error) {
    console.error(`Loki transport: ${optionName} threw; continuing without it`, {
      reason: error instanceof Error ? error.message : 'unknown',
    });
    return undefined;
  }
}

/** First of `keys` that holds an Error on `error`, or undefined. */
function nextCause(error: Error, keys: Array<string>): unknown {
  for (const key of keys) {
    const candidate = (error as unknown as Record<string, unknown>)[key];
    if (candidate instanceof Error) return candidate;
  }
  return undefined;
}

function buildNestedErrorStack(error: unknown, causeKeys: Array<string>): string | undefined {
  if (!(error instanceof Error) || !error.stack) return undefined;

  // Only emit when there is genuinely a nested error, otherwise this just duplicates
  // error.stack, which the `error` field already carries.
  let current = nextCause(error, causeKeys);
  if (!(current instanceof Error)) return undefined;

  // The chain is application-supplied, so it can contain a cycle. Track what we have seen and
  // cap the depth: an unbounded walk here would hang log() and grow the string until memory
  // ran out.
  let nestedStack = error.stack;
  const seen = new Set<unknown>([error]);
  while (current instanceof Error && !seen.has(current) && seen.size <= MAX_CAUSE_DEPTH) {
    seen.add(current);
    nestedStack += `\nCaused By: ${current.stack}`;
    current = nextCause(current, causeKeys);
  }
  return nestedStack;
}

/**
 * Builds a stringifier. `undefinedPlaceholder` decides whether unset keys are dropped or
 * rendered, which is the one serialization difference between the supported line formats.
 */
function makeSafeStringify(undefinedPlaceholder?: string) {
  return (obj: unknown): string =>
    // json-stringify-safe handles circular refs; this replacer handles the rest.
    jsonSafeStringify(obj, (_key, value) => {
      if (value instanceof Error) {
        return {
          name: value.name,
          message: value.message,
          stack: value.stack,
          code: (value as Error & { code?: unknown }).code,
        };
      }
      if (value === undefined) {
        // Returning undefined makes JSON.stringify drop the key entirely.
        return undefinedPlaceholder;
      }
      if (value instanceof Function) {
        return '[Function]';
      }
      return typeof value === 'bigint' ? value.toString() : value;
    });
}

/** Keys in `payload` that may hold a copy of the top-level error. */
const PAYLOAD_ERROR_KEYS = ['error', 'err', 'exception', 'cause'] as const;

/**
 * Blanks error-shaped keys in `payload` that hold the same object as the top-level `error`.
 * Without this a single line can carry the same stack trace two or three times, which is how
 * lines end up over the size cap in the first place.
 *
 * The keys are kept and set to `undefined` rather than deleted, so a format using
 * `undefinedPlaceholder` shows that de-duplication happened.
 */
function dedupePayloadErrors(payload: unknown, error: unknown): unknown {
  if (!payload || !(error instanceof Error)) return payload;
  if (payload === error) return undefined;
  if (typeof payload !== 'object' || Array.isArray(payload)) return payload;

  const source = payload as Record<string, unknown>;
  const cleaned: Record<string, unknown> = { ...source };
  for (const key of PAYLOAD_ERROR_KEYS) {
    if (source[key] === error) cleaned[key] = undefined;
  }
  return cleaned;
}

export class LokiTransport extends Transport {
  private buffer: Array<LogLine> = [];
  private timer: NodeJS.Timeout;
  private labels: Record<string, string>;
  private host: string;
  private authHeader: string | undefined;
  private debug: boolean;
  private loggerId: string;
  private flushPromise: Promise<void> | null = null;
  private closePromise: Promise<void> | null = null;
  private droppedDueToRetries = 0;
  private droppedDueToBacklog = 0;
  private lastDropLogAtMs = 0;
  private isClosing = false;
  private readonly getContext?: () => Record<string, unknown> | undefined;
  private readonly getExtraFields?: () => Record<string, unknown> | undefined;
  private readonly lineFormat: LokiLineFormatOptions;
  private readonly causeKeys: Array<string>;
  private readonly stripFields: Set<string>;
  private readonly flushOnEnd: boolean;
  /** Configured by lineFormat.undefinedPlaceholder; see makeSafeStringify. */
  private readonly stringify: (obj: unknown) => string;
  private readonly MAX_BUFFER_SIZE = 10000;
  // Smaller chunks tend to produce faster requests and reduce shutdown tail-loss risk.
  private readonly CHUNK_SIZE = 100;
  // Keep this below any caller-side flush budget, but long enough for normal ingestion.
  private readonly FLUSH_TIMEOUT_MS = 5000;

  constructor(opts: LokiTransportOptions) {
    super(opts);
    if (!opts.host) {
      throw new Error('Loki host is required');
    }

    // One transport is shared by every logger, and winston pipes each one into it. With 20+
    // loggers that exceeds Node's default cap of 10 and emits MaxListenersExceededWarnings.
    this.setMaxListeners(0);

    // Strip trailing slashes so a host set with one doesn't produce '//loki/api/v1/push'.
    this.host = opts.host.replace(/\/+$/, '');
    this.labels = opts.labels;
    this.debug = opts.debug ?? false;
    if (opts.lokiUser && opts.lokiToken) {
      this.authHeader = `Basic ${Buffer.from(`${opts.lokiUser}:${opts.lokiToken}`).toString('base64')}`;
    } else if (opts.lokiUser || opts.lokiToken) {
      console.warn('Loki transport: only one of lokiUser/lokiToken was supplied; sending no auth header');
    }

    // Keep a stable ID per container/process; child processes inherit it via env var.
    this.loggerId = opts.loggerId ?? process.env.LOGGER_ID ?? randomUUID();
    process.env.LOGGER_ID = this.loggerId;

    this.getContext = opts.getContext;
    this.getExtraFields = opts.getExtraFields;
    this.lineFormat = opts.lineFormat ?? {};
    this.causeKeys = this.lineFormat.errorCauseKeys ?? ['cause'];
    this.stripFields = new Set(this.lineFormat.stripFields ?? []);
    this.flushOnEnd = opts.flushOnEnd ?? false;
    this.stringify = makeSafeStringify(this.lineFormat.undefinedPlaceholder);

    // Opportunistic batching. unref()'d so it can't hold the process (or a Lambda
    // invocation) open; a caller wanting a guarantee must await flush().
    this.timer = setInterval(() => {
      this.flush().catch((err) => {
        console.error('Unexpected flush error:', err);
      });
    }, opts.interval ?? 1000);
    this.timer.unref();

    // Graceful shutdown handler that actually waits for flush to complete.
    // Use process.once to avoid double-flush / re-entrancy.
    const cleanup = (signal?: string) => {
      if (this.isClosing) return;
      // Set isClosing immediately to prevent new logs from being enqueued
      // during the window between cleanup starting and close() executing.
      this.isClosing = true;
      if (this.debug) {
        console.log('Cleanup triggered', { signal });
      }

      // Stop new periodic flushes
      clearInterval(this.timer);

      // Give ourselves a hard deadline so we don't hang forever if Loki is unreachable.
      // unref() so the timeout doesn't keep the process alive if flush finishes early.
      const deadline = new Promise<void>((resolve) => {
        setTimeout(resolve, this.FLUSH_TIMEOUT_MS + 2000).unref();
      });

      // Use close() so the shutdown path is single-flight via closePromise.
      Promise.race([this.close(), deadline]).catch((err) => {
        console.error('Cleanup flush failed', err);
      });
      // Note: we don't call process.exit() here. Let the application decide when to exit.
    };

    process.once('SIGINT', () => cleanup('SIGINT'));
    process.once('SIGTERM', () => cleanup('SIGTERM'));

    // One line per container start is the cheapest way to confirm from CloudWatch that
    // Loki is wired up. Per-line and per-flush logging stays behind LOKI_DEBUG_ENABLED.
    console.log('Initialized Loki logger', {
      host: this.host,
      labels: this.labels,
      level: opts.level,
      interval: opts.interval ?? 1000,
    });
  }

  override log(info: WinstonLogInfo, callback: () => void) {
    // Stop accepting new log lines once closing starts to prevent late logs
    // from being enqueued after flush completes.
    if (this.isClosing) {
      callback();
      return;
    }

    setImmediate(() => this.emit('logged', info));

    // Both naming conventions are accepted for the same value; see WinstonLogInfo.
    const {
      level,
      message,
      namespace,
      service,
      date,
      timestamp,
      category,
      label,
      error,
      cause,
      payload,
      metadata,
      ...rest
    } = info;

    const lineDate = date ?? timestamp ?? new Date().toISOString();
    const lineCategory = category ?? label;
    const lineError = error ?? cause;
    const rawPayload = payload ?? metadata;

    let time = new Date(lineDate).getTime();
    if (Number.isNaN(time)) {
      console.warn('Invalid timestamp provided, using current time.');
      time = Date.now();
    }

    const linePayload = this.lineFormat.dedupePayloadErrors ? dedupePayloadErrors(rawPayload, lineError) : rawPayload;

    let passthrough: Record<string, unknown> | undefined;
    if (this.lineFormat.passthroughFields) {
      passthrough = {};
      for (const [key, value] of Object.entries(rest)) {
        if (!this.stripFields.has(key)) passthrough[key] = value;
      }
    }

    // Both are consumer-supplied; see resolveSafely. A resolver that throws must not be able
    // to break the caller's logging statement, so `context` still gets its `{}` and
    // `extraFields` is simply omitted.
    const context = this.getContext ? { context: resolveSafely(this.getContext, 'getContext') ?? {} } : undefined;
    const extraFields = resolveSafely(this.getExtraFields, 'getExtraFields');
    const nestedErrorStack = buildNestedErrorStack(lineError, this.causeKeys);

    // One field order for every service, so a single set of Grafana queries covers all of
    // them. Keys absent from `info` are either dropped or rendered as the configured
    // placeholder - that choice is the whole of lineFormat.undefinedPlaceholder.
    const fullLineObj = {
      level,
      message,
      ...passthrough,
      date: lineDate,
      category: lineCategory,
      namespace,
      // Only emitted by services that set it, so it can't add a placeholder key elsewhere.
      ...(service === undefined ? {} : { service }),
      loggerId: this.loggerId,
      ...extraFields,
      ...context,
      error: lineError,
      nestedErrorStack,
      payload: linePayload,
    };

    const fullLine = this.stringify(fullLineObj);
    const fullLineBytes = utf8ByteLength(fullLine);
    const line =
      fullLineBytes <= MAX_LINE_BYTES ? fullLine : this.buildTruncatedLine(fullLineObj, fullLine, fullLineBytes);

    const logLine: LogLine = {
      timestamp: time * 1e6, // Loki expects nanoseconds
      level: String(level),
      line,
      retries: 0,
    };

    this.buffer.push(logLine);
    if (this.debug) {
      console.log('Added log line to buffer', logLine);
    }

    // Cap on the ingest path too, not just after a failed flush. While a flush is in
    // flight, flush() chains instead of detaching, so sustained logging against a slow
    // Loki would otherwise grow the buffer - and the promise chain - without bound.
    if (this.buffer.length > MAX_BACKLOG_LINES) {
      this.dropOldest(this.buffer.length - MAX_BACKLOG_LINES);
    }

    // Flush immediately if buffer exceeds size
    if (this.buffer.length >= this.MAX_BUFFER_SIZE) {
      this.flush().catch((err) => {
        console.error('Flush error during buffer overflow:', err);
      });
    }

    callback();
  }

  /**
   * Bounded stand-in for a line over MAX_LINE_BYTES. Keeps the fields that identify the line
   * and drops `payload` and any passthrough, which are what make a line oversized.
   *
   * The message is capped too. Copying it verbatim meant that when the bulk of an oversized
   * line WAS the message, the replacement came out larger than the original - preview bytes
   * on top of the full message - so the line sailed straight past the cap it exists to
   * enforce.
   */
  private buildTruncatedLine(full: Record<string, unknown>, fullLine: string, fullLineBytes: number): string {
    const { level, message, date, category, namespace, service, loggerId, context, error, nestedErrorStack } = full;

    const cappedMessage = truncateUtf8(
      typeof message === 'string' ? message : this.stringify(message),
      TRUNCATION_PREVIEW_BYTES
    );

    const truncated: Record<string, unknown> = {
      level,
      message: cappedMessage,
      date,
      category,
      namespace,
      ...(service === undefined ? {} : { service }),
      loggerId,
      ...(context === undefined ? {} : { context }),
      error,
      nestedErrorStack,
      truncated: true,
      originalBytes: fullLineBytes,
      preview: truncateUtf8(fullLine, TRUNCATION_PREVIEW_BYTES),
    };

    let out = this.stringify(truncated);
    if (utf8ByteLength(out) <= MAX_LINE_BYTES) return out;

    // A large error stack can still push it over. Shrink the preview, then drop it.
    truncated.preview = truncateUtf8(fullLine, 1024);
    out = this.stringify(truncated);
    if (utf8ByteLength(out) <= MAX_LINE_BYTES) return out;

    delete truncated.preview;
    out = this.stringify(truncated);
    if (utf8ByteLength(out) <= MAX_LINE_BYTES) return out;

    // Last resort, so the cap holds no matter how pathological the input: identity only.
    return this.stringify({
      level,
      message: truncateUtf8(cappedMessage, 1024),
      date,
      category,
      namespace,
      loggerId,
      truncated: true,
      originalBytes: fullLineBytes,
    });
  }

  /** Lines still queued. Drain against this, since a failed flush requeues its entries and still resolves. */
  get pendingCount(): number {
    return this.buffer.length;
  }

  /** Drops the oldest N lines and reports it, throttled so a sustained overflow can't spam stdout. */
  private dropOldest(count: number) {
    if (count <= 0) return;
    this.buffer.splice(0, count);
    this.droppedDueToBacklog += count;
    const now = Date.now();
    if (now - this.lastDropLogAtMs >= DROP_LOG_THROTTLE_MS) {
      this.lastDropLogAtMs = now;
      console.warn('Dropping oldest buffered log lines due to backlog cap', {
        toDrop: count,
        droppedTotal: this.droppedDueToBacklog,
        maxBacklogLines: MAX_BACKLOG_LINES,
      });
    }
  }

  /**
   * Ships the batch currently buffered. Single-flight: a concurrent call gets the in-flight
   * promise rather than a continuation, so log() hitting MAX_BUFFER_SIZE on every line can't
   * accumulate one pending promise per line.
   *
   * That promise only covers the batch flushInternal() detached, so callers needing
   * everything gone should loop until pendingCount is 0; close() does exactly that.
   */
  flush(): Promise<void> {
    if (this.flushPromise) return this.flushPromise;
    if (this.buffer.length === 0) return Promise.resolve();

    this.flushPromise = this.flushInternal().finally(() => {
      this.flushPromise = null;
    });

    return this.flushPromise;
  }

  private async flushInternal(): Promise<void> {
    if (this.debug) {
      console.log('Attempting to flush logs to Loki');
    }

    // Detach buffer
    const bufferToSend = this.buffer;
    this.buffer = [];

    // Loki is strict about per-stream chronological ordering. Sort globally before chunking;
    // we also split into per-level streams later which preserves order within each stream.
    bufferToSend.sort((a, b) => a.timestamp - b.timestamp);

    const chunks = splitEvery(this.CHUNK_SIZE, bufferToSend);

    try {
      for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        if (!chunk) continue;
        const failedFromChunk = await this.postChunkWithAutoSplit(chunk);
        if (failedFromChunk.length > 0) {
          // Stop sending to avoid cascading partial success/duplication; requeue failed + remaining.
          const remaining = chunks.slice(i + 1).flat();
          throw new FlushError([...failedFromChunk, ...remaining]);
        }
      }
      if (this.debug) {
        console.log('Flushed logs to Loki', { numChunks: chunks.length });
      }
    } catch (error) {
      // If we have chunk-level failure info, only requeue those entries + anything not attempted yet.
      const requeued = error instanceof FlushError ? error.failedEntries : undefined;
      const failedEntries: Array<LogLine> = requeued ?? bufferToSend;

      // Log a count, never the entries: they hold the full serialized log lines, and at a
      // 100ms flush interval an unreachable Loki would flood stdout with the very payloads
      // batching exists to avoid.
      console.error('Failed to send logs to Loki', {
        failedEntryCount: failedEntries.length,
        reason: error instanceof Error ? error.message : 'unknown',
      });

      // Re-queue with bounds to avoid unbounded memory growth if Loki/ALB rejects traffic.
      const retryable = failedEntries
        .map((log) => ({ ...log, retries: (log.retries ?? 0) + 1 }))
        .filter((log) => log.retries <= MAX_RETRIES);

      const droppedThisRound = failedEntries.length - retryable.length;
      if (droppedThisRound > 0) {
        this.droppedDueToRetries += droppedThisRound;
        const now = Date.now();
        if (now - this.lastDropLogAtMs >= DROP_LOG_THROTTLE_MS) {
          this.lastDropLogAtMs = now;
          console.warn('Dropping log lines due to retry limit exceeded', {
            droppedThisRound,
            droppedTotal: this.droppedDueToRetries,
            maxRetries: MAX_RETRIES,
          });
        }
      }

      this.buffer = [...retryable, ...this.buffer];
      if (this.buffer.length > MAX_BACKLOG_LINES) {
        this.dropOldest(this.buffer.length - MAX_BACKLOG_LINES);
      }
    }
  }

  private async postChunkWithAutoSplit(chunk: Array<LogLine>): Promise<Array<LogLine>> {
    // https://grafana.com/docs/loki/latest/reference/loki-http-api/#ingest-logs
    // Add `level` as a label by grouping entries into per-level streams.
    const byLevel = new Map<string, Array<LogLine>>();
    for (const entry of chunk) {
      const lvl = entry.level || 'unknown';
      const existing = byLevel.get(lvl);
      if (existing) {
        existing.push(entry);
      } else {
        byLevel.set(lvl, [entry]);
      }
    }

    const streams = [...byLevel.entries()].map(([level, entries]) => ({
      stream: { ...this.labels, level },
      values: entries.map((entry) => [entry.timestamp.toString(), entry.line]),
    }));

    const payload = this.stringify({ streams });
    let gzipped: Buffer;
    try {
      gzipped = (await gzipAsync(payload)) as Buffer;
    } catch {
      // If compression fails, treat the entire chunk as failed.
      return chunk;
    }

    // Stay under the ALB request-body hard limit. If too large, split and retry.
    if (gzipped.length > MAX_GZIPPED_PUSH_BYTES && chunk.length > 1) {
      const mid = Math.ceil(chunk.length / 2);
      const failedA = await this.postChunkWithAutoSplit(chunk.slice(0, mid));
      const failedB = await this.postChunkWithAutoSplit(chunk.slice(mid));
      return [...failedA, ...failedB];
    }

    // Extremely defensive fallback: if a single entry still produces an oversized request,
    // send a tiny truncated placeholder instead of looping forever.
    if (gzipped.length > MAX_GZIPPED_PUSH_BYTES && chunk.length === 1) {
      const entry = chunk[0];
      if (!entry) return [];
      const tinyLine = this.stringify({
        level: 'error',
        message: 'Log line exceeded max push size even after truncation; dropping content',
        truncated: true,
        originalLineBytes: utf8ByteLength(entry.line),
      });
      const tinyStream = {
        stream: { ...this.labels, level: entry.level || 'unknown' },
        values: [[entry.timestamp.toString(), tinyLine]],
      };
      const tinyPayload = this.stringify({ streams: [tinyStream] });
      try {
        const tinyGzipped = (await gzipAsync(tinyPayload)) as Buffer;
        await this.postGzipped(tinyGzipped);
        return [];
      } catch {
        return [entry];
      }
    }

    try {
      await this.postGzipped(gzipped);
      return [];
    } catch {
      return chunk;
    }
  }

  private async postGzipped(gzippedBody: Buffer): Promise<void> {
    try {
      await axios.post(`${this.host}/loki/api/v1/push`, gzippedBody, {
        headers: {
          'Content-Type': 'application/json',
          'Content-Encoding': 'gzip',
          ...(this.authHeader && { Authorization: this.authHeader }),
        },
        timeout: this.FLUSH_TIMEOUT_MS,
        httpAgent,
        httpsAgent,
        maxBodyLength: Number.POSITIVE_INFINITY,
        maxContentLength: Number.POSITIVE_INFINITY,
      });
    } catch (err: unknown) {
      // Log HTTP errors with status/response details for debugging network/Loki issues
      const response = (err as HttpErrorLike | undefined)?.response;
      if (response) {
        console.error('Loki push HTTP error', {
          status: response.status,
          statusText: response.statusText,
          data: typeof response.data === 'string' ? response.data.slice(0, 500) : response.data,
        });
      }
      throw err;
    }
  }

  override async close() {
    // Make close() single-flight to avoid duplicate concurrent flushes.
    if (this.closePromise) {
      return this.closePromise;
    }

    this.closePromise = (async () => {
      this.isClosing = true;
      clearInterval(this.timer);

      // flush() only ships the batch it detached: lines logged while it ran, and lines it
      // requeued after a failure, both stay behind. Shutdown has no second chance, so drain
      // within the budget the cleanup deadline allows.
      const drainDeadline = Date.now() + this.FLUSH_TIMEOUT_MS;
      do {
        await this.flush();
      } while (this.buffer.length > 0 && Date.now() < drainDeadline);

      if (this.buffer.length > 0) {
        console.warn('Loki transport closed with unflushed log lines', { unflushed: this.buffer.length });
      }

      if (this.debug || this.droppedDueToRetries > 0 || this.droppedDueToBacklog > 0) {
        console.log('Cleared and flushed Loki logger', {
          droppedDueToRetries: this.droppedDueToRetries,
          droppedDueToBacklog: this.droppedDueToBacklog,
        });
      }
    })().finally(() => {
      this.closePromise = null;
    });

    return this.closePromise;
  }

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
  override _final(callback: (error?: Error | null) => void) {
    if (!this.flushOnEnd) {
      callback(null);
      return;
    }
    this.close()
      .then(() => callback(null))
      .catch((error) => callback(error as Error));
  }
}
