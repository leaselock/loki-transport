'use strict';

var http = require('http');
var https = require('https');
var zlib = require('zlib');
var util = require('util');
var crypto = require('crypto');
var Transport = require('winston-transport');
var axios = require('axios');
var jsonSafeStringify = require('json-stringify-safe');

function _interopDefault (e) { return e && e.__esModule ? e : { default: e }; }

var http__default = /*#__PURE__*/_interopDefault(http);
var https__default = /*#__PURE__*/_interopDefault(https);
var zlib__default = /*#__PURE__*/_interopDefault(zlib);
var Transport__default = /*#__PURE__*/_interopDefault(Transport);
var axios__default = /*#__PURE__*/_interopDefault(axios);
var jsonSafeStringify__default = /*#__PURE__*/_interopDefault(jsonSafeStringify);

// src/loki_transport.ts
var LEGACY_LINE_FORMAT = {
  undefinedPlaceholder: "__undefined__",
  errorCauseKeys: ["nested"],
  dedupePayloadErrors: true,
  passthroughFields: true,
  stripFields: ["stdoutMethod", "stderrMethod"]
};
var FlushError = class extends Error {
  constructor(failedEntries) {
    super(`Failed to push ${failedEntries.length} log entries to Loki`);
    this.failedEntries = failedEntries;
    this.name = "FlushError";
  }
  failedEntries;
};
var httpAgent = new http__default.default.Agent({ keepAlive: true });
var httpsAgent = new https__default.default.Agent({ keepAlive: true });
var MAX_LINE_BYTES = 128 * 1024;
var MAX_GZIPPED_PUSH_BYTES = 900 * 1024;
var MAX_RETRIES = 3;
var MAX_BACKLOG_LINES = 5e4;
var TRUNCATION_PREVIEW_BYTES = 16 * 1024;
var DROP_LOG_THROTTLE_MS = 1e4;
var MAX_CAUSE_DEPTH = 10;
var gzipAsync = util.promisify(zlib__default.default.gzip);
function utf8ByteLength(str) {
  return Buffer.byteLength(str, "utf8");
}
function truncateUtf8(str, maxBytes) {
  const buf = Buffer.from(str, "utf8");
  if (buf.length <= maxBytes) return str;
  return buf.subarray(0, maxBytes).toString("utf8");
}
function splitEvery(size, list) {
  const chunks = [];
  for (let i = 0; i < list.length; i += size) {
    chunks.push(list.slice(i, i + size));
  }
  return chunks;
}
function resolveSafely(resolve, optionName) {
  if (!resolve) return void 0;
  try {
    return resolve();
  } catch (error) {
    console.error(`Loki transport: ${optionName} threw; continuing without it`, {
      reason: error instanceof Error ? error.message : "unknown"
    });
    return void 0;
  }
}
function nextCause(error, keys) {
  for (const key of keys) {
    const candidate = error[key];
    if (candidate instanceof Error) return candidate;
  }
  return void 0;
}
function buildNestedErrorStack(error, causeKeys) {
  if (!(error instanceof Error) || !error.stack) return void 0;
  let current = nextCause(error, causeKeys);
  if (!(current instanceof Error)) return void 0;
  let nestedStack = error.stack;
  const seen = /* @__PURE__ */ new Set([error]);
  while (current instanceof Error && !seen.has(current) && seen.size <= MAX_CAUSE_DEPTH) {
    seen.add(current);
    nestedStack += `
Caused By: ${current.stack}`;
    current = nextCause(current, causeKeys);
  }
  return nestedStack;
}
function makeSafeStringify(undefinedPlaceholder) {
  return (obj) => (
    // json-stringify-safe handles circular refs; this replacer handles the rest.
    jsonSafeStringify__default.default(obj, (_key, value) => {
      if (value instanceof Error) {
        return {
          name: value.name,
          message: value.message,
          stack: value.stack,
          code: value.code
        };
      }
      if (value === void 0) {
        return undefinedPlaceholder;
      }
      if (value instanceof Function) {
        return "[Function]";
      }
      return typeof value === "bigint" ? value.toString() : value;
    })
  );
}
var PAYLOAD_ERROR_KEYS = ["error", "err", "exception", "cause"];
function dedupePayloadErrors(payload, error) {
  if (!payload || !(error instanceof Error)) return payload;
  if (payload === error) return void 0;
  if (typeof payload !== "object" || Array.isArray(payload)) return payload;
  const source = payload;
  const cleaned = { ...source };
  for (const key of PAYLOAD_ERROR_KEYS) {
    if (source[key] === error) cleaned[key] = void 0;
  }
  return cleaned;
}
var LokiTransport = class extends Transport__default.default {
  buffer = [];
  timer;
  labels;
  host;
  authHeader;
  debug;
  loggerId;
  flushPromise = null;
  closePromise = null;
  droppedDueToRetries = 0;
  droppedDueToBacklog = 0;
  lastDropLogAtMs = 0;
  isClosing = false;
  getContext;
  getExtraFields;
  lineFormat;
  causeKeys;
  stripFields;
  flushOnEnd;
  /** Configured by lineFormat.undefinedPlaceholder; see makeSafeStringify. */
  stringify;
  MAX_BUFFER_SIZE = 1e4;
  // Smaller chunks tend to produce faster requests and reduce shutdown tail-loss risk.
  CHUNK_SIZE = 100;
  // Keep this below any caller-side flush budget, but long enough for normal ingestion.
  FLUSH_TIMEOUT_MS = 5e3;
  constructor(opts) {
    super(opts);
    if (!opts.host) {
      throw new Error("Loki host is required");
    }
    this.setMaxListeners(0);
    this.host = opts.host.replace(/\/+$/, "");
    this.labels = opts.labels;
    this.debug = opts.debug ?? false;
    if (opts.lokiUser && opts.lokiToken) {
      this.authHeader = `Basic ${Buffer.from(`${opts.lokiUser}:${opts.lokiToken}`).toString("base64")}`;
    } else if (opts.lokiUser || opts.lokiToken) {
      console.warn("Loki transport: only one of lokiUser/lokiToken was supplied; sending no auth header");
    }
    this.loggerId = opts.loggerId ?? process.env.LOGGER_ID ?? crypto.randomUUID();
    process.env.LOGGER_ID = this.loggerId;
    this.getContext = opts.getContext;
    this.getExtraFields = opts.getExtraFields;
    this.lineFormat = opts.lineFormat ?? {};
    this.causeKeys = this.lineFormat.errorCauseKeys ?? ["cause"];
    this.stripFields = new Set(this.lineFormat.stripFields ?? []);
    this.flushOnEnd = opts.flushOnEnd ?? false;
    this.stringify = makeSafeStringify(this.lineFormat.undefinedPlaceholder);
    this.timer = setInterval(() => {
      this.flush().catch((err) => {
        console.error("Unexpected flush error:", err);
      });
    }, opts.interval ?? 1e3);
    this.timer.unref();
    const cleanup = (signal) => {
      if (this.isClosing) return;
      this.isClosing = true;
      if (this.debug) {
        console.log("Cleanup triggered", { signal });
      }
      clearInterval(this.timer);
      const deadline = new Promise((resolve) => {
        setTimeout(resolve, this.FLUSH_TIMEOUT_MS + 2e3).unref();
      });
      Promise.race([this.close(), deadline]).catch((err) => {
        console.error("Cleanup flush failed", err);
      });
    };
    process.once("SIGINT", () => cleanup("SIGINT"));
    process.once("SIGTERM", () => cleanup("SIGTERM"));
    console.log("Initialized Loki logger", {
      host: this.host,
      labels: this.labels,
      level: opts.level,
      interval: opts.interval ?? 1e3
    });
  }
  log(info, callback) {
    if (this.isClosing) {
      callback();
      return;
    }
    setImmediate(() => this.emit("logged", info));
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
    const lineDate = date ?? timestamp ?? (/* @__PURE__ */ new Date()).toISOString();
    const lineCategory = category ?? label;
    const lineError = error ?? cause;
    const rawPayload = payload ?? metadata;
    let time = new Date(lineDate).getTime();
    if (Number.isNaN(time)) {
      console.warn("Invalid timestamp provided, using current time.");
      time = Date.now();
    }
    const linePayload = this.lineFormat.dedupePayloadErrors ? dedupePayloadErrors(rawPayload, lineError) : rawPayload;
    let passthrough;
    if (this.lineFormat.passthroughFields) {
      passthrough = {};
      for (const [key, value] of Object.entries(rest)) {
        if (!this.stripFields.has(key)) passthrough[key] = value;
      }
    }
    const context = this.getContext ? { context: resolveSafely(this.getContext, "getContext") ?? {} } : void 0;
    const extraFields = resolveSafely(this.getExtraFields, "getExtraFields");
    const nestedErrorStack = buildNestedErrorStack(lineError, this.causeKeys);
    const fullLineObj = {
      level,
      message,
      ...passthrough,
      date: lineDate,
      category: lineCategory,
      namespace,
      // Only emitted by services that set it, so it can't add a placeholder key elsewhere.
      ...service === void 0 ? {} : { service },
      loggerId: this.loggerId,
      ...extraFields,
      ...context,
      error: lineError,
      nestedErrorStack,
      payload: linePayload
    };
    const fullLine = this.stringify(fullLineObj);
    const fullLineBytes = utf8ByteLength(fullLine);
    const line = fullLineBytes <= MAX_LINE_BYTES ? fullLine : this.buildTruncatedLine(fullLineObj, fullLine, fullLineBytes);
    const logLine = {
      timestamp: time * 1e6,
      // Loki expects nanoseconds
      level: String(level),
      line,
      retries: 0
    };
    this.buffer.push(logLine);
    if (this.debug) {
      console.log("Added log line to buffer", logLine);
    }
    if (this.buffer.length > MAX_BACKLOG_LINES) {
      this.dropOldest(this.buffer.length - MAX_BACKLOG_LINES);
    }
    if (this.buffer.length >= this.MAX_BUFFER_SIZE) {
      this.flush().catch((err) => {
        console.error("Flush error during buffer overflow:", err);
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
  buildTruncatedLine(full, fullLine, fullLineBytes) {
    const { level, message, date, category, namespace, service, loggerId, context, error, nestedErrorStack } = full;
    const cappedMessage = truncateUtf8(
      typeof message === "string" ? message : this.stringify(message),
      TRUNCATION_PREVIEW_BYTES
    );
    const truncated = {
      level,
      message: cappedMessage,
      date,
      category,
      namespace,
      ...service === void 0 ? {} : { service },
      loggerId,
      ...context === void 0 ? {} : { context },
      error,
      nestedErrorStack,
      truncated: true,
      originalBytes: fullLineBytes,
      preview: truncateUtf8(fullLine, TRUNCATION_PREVIEW_BYTES)
    };
    let out = this.stringify(truncated);
    if (utf8ByteLength(out) <= MAX_LINE_BYTES) return out;
    truncated.preview = truncateUtf8(fullLine, 1024);
    out = this.stringify(truncated);
    if (utf8ByteLength(out) <= MAX_LINE_BYTES) return out;
    delete truncated.preview;
    out = this.stringify(truncated);
    if (utf8ByteLength(out) <= MAX_LINE_BYTES) return out;
    return this.stringify({
      level,
      message: truncateUtf8(cappedMessage, 1024),
      date,
      category,
      namespace,
      loggerId,
      truncated: true,
      originalBytes: fullLineBytes
    });
  }
  /** Lines still queued. Drain against this, since a failed flush requeues its entries and still resolves. */
  get pendingCount() {
    return this.buffer.length;
  }
  /** Drops the oldest N lines and reports it, throttled so a sustained overflow can't spam stdout. */
  dropOldest(count) {
    if (count <= 0) return;
    this.buffer.splice(0, count);
    this.droppedDueToBacklog += count;
    const now = Date.now();
    if (now - this.lastDropLogAtMs >= DROP_LOG_THROTTLE_MS) {
      this.lastDropLogAtMs = now;
      console.warn("Dropping oldest buffered log lines due to backlog cap", {
        toDrop: count,
        droppedTotal: this.droppedDueToBacklog,
        maxBacklogLines: MAX_BACKLOG_LINES
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
  flush() {
    if (this.flushPromise) return this.flushPromise;
    if (this.buffer.length === 0) return Promise.resolve();
    this.flushPromise = this.flushInternal().finally(() => {
      this.flushPromise = null;
    });
    return this.flushPromise;
  }
  async flushInternal() {
    if (this.debug) {
      console.log("Attempting to flush logs to Loki");
    }
    const bufferToSend = this.buffer;
    this.buffer = [];
    bufferToSend.sort((a, b) => a.timestamp - b.timestamp);
    const chunks = splitEvery(this.CHUNK_SIZE, bufferToSend);
    try {
      for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        if (!chunk) continue;
        const failedFromChunk = await this.postChunkWithAutoSplit(chunk);
        if (failedFromChunk.length > 0) {
          const remaining = chunks.slice(i + 1).flat();
          throw new FlushError([...failedFromChunk, ...remaining]);
        }
      }
      if (this.debug) {
        console.log("Flushed logs to Loki", { numChunks: chunks.length });
      }
    } catch (error) {
      const requeued = error instanceof FlushError ? error.failedEntries : void 0;
      const failedEntries = requeued ?? bufferToSend;
      console.error("Failed to send logs to Loki", {
        failedEntryCount: failedEntries.length,
        reason: error instanceof Error ? error.message : "unknown"
      });
      const retryable = failedEntries.map((log) => ({ ...log, retries: (log.retries ?? 0) + 1 })).filter((log) => log.retries <= MAX_RETRIES);
      const droppedThisRound = failedEntries.length - retryable.length;
      if (droppedThisRound > 0) {
        this.droppedDueToRetries += droppedThisRound;
        const now = Date.now();
        if (now - this.lastDropLogAtMs >= DROP_LOG_THROTTLE_MS) {
          this.lastDropLogAtMs = now;
          console.warn("Dropping log lines due to retry limit exceeded", {
            droppedThisRound,
            droppedTotal: this.droppedDueToRetries,
            maxRetries: MAX_RETRIES
          });
        }
      }
      this.buffer = [...retryable, ...this.buffer];
      if (this.buffer.length > MAX_BACKLOG_LINES) {
        this.dropOldest(this.buffer.length - MAX_BACKLOG_LINES);
      }
    }
  }
  async postChunkWithAutoSplit(chunk) {
    const byLevel = /* @__PURE__ */ new Map();
    for (const entry of chunk) {
      const lvl = entry.level || "unknown";
      const existing = byLevel.get(lvl);
      if (existing) {
        existing.push(entry);
      } else {
        byLevel.set(lvl, [entry]);
      }
    }
    const streams = [...byLevel.entries()].map(([level, entries]) => ({
      stream: { ...this.labels, level },
      values: entries.map((entry) => [entry.timestamp.toString(), entry.line])
    }));
    const payload = this.stringify({ streams });
    let gzipped;
    try {
      gzipped = await gzipAsync(payload);
    } catch {
      return chunk;
    }
    if (gzipped.length > MAX_GZIPPED_PUSH_BYTES && chunk.length > 1) {
      const mid = Math.ceil(chunk.length / 2);
      const failedA = await this.postChunkWithAutoSplit(chunk.slice(0, mid));
      const failedB = await this.postChunkWithAutoSplit(chunk.slice(mid));
      return [...failedA, ...failedB];
    }
    if (gzipped.length > MAX_GZIPPED_PUSH_BYTES && chunk.length === 1) {
      const entry = chunk[0];
      if (!entry) return [];
      const tinyLine = this.stringify({
        level: "error",
        message: "Log line exceeded max push size even after truncation; dropping content",
        truncated: true,
        originalLineBytes: utf8ByteLength(entry.line)
      });
      const tinyStream = {
        stream: { ...this.labels, level: entry.level || "unknown" },
        values: [[entry.timestamp.toString(), tinyLine]]
      };
      const tinyPayload = this.stringify({ streams: [tinyStream] });
      try {
        const tinyGzipped = await gzipAsync(tinyPayload);
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
  async postGzipped(gzippedBody) {
    try {
      await axios__default.default.post(`${this.host}/loki/api/v1/push`, gzippedBody, {
        headers: {
          "Content-Type": "application/json",
          "Content-Encoding": "gzip",
          ...this.authHeader && { Authorization: this.authHeader }
        },
        timeout: this.FLUSH_TIMEOUT_MS,
        httpAgent,
        httpsAgent,
        maxBodyLength: Number.POSITIVE_INFINITY,
        maxContentLength: Number.POSITIVE_INFINITY
      });
    } catch (err) {
      const response = err?.response;
      if (response) {
        console.error("Loki push HTTP error", {
          status: response.status,
          statusText: response.statusText,
          data: typeof response.data === "string" ? response.data.slice(0, 500) : response.data
        });
      }
      throw err;
    }
  }
  async close() {
    if (this.closePromise) {
      return this.closePromise;
    }
    this.closePromise = (async () => {
      this.isClosing = true;
      clearInterval(this.timer);
      const drainDeadline = Date.now() + this.FLUSH_TIMEOUT_MS;
      do {
        await this.flush();
      } while (this.buffer.length > 0 && Date.now() < drainDeadline);
      if (this.buffer.length > 0) {
        console.warn("Loki transport closed with unflushed log lines", { unflushed: this.buffer.length });
      }
      if (this.debug || this.droppedDueToRetries > 0 || this.droppedDueToBacklog > 0) {
        console.log("Cleared and flushed Loki logger", {
          droppedDueToRetries: this.droppedDueToRetries,
          droppedDueToBacklog: this.droppedDueToBacklog
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
  _final(callback) {
    if (!this.flushOnEnd) {
      callback(null);
      return;
    }
    this.close().then(() => callback(null)).catch((error) => callback(error));
  }
};

exports.LEGACY_LINE_FORMAT = LEGACY_LINE_FORMAT;
exports.LokiTransport = LokiTransport;
//# sourceMappingURL=index.js.map
//# sourceMappingURL=index.js.map