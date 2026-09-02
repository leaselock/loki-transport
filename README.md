# @leaselock/loki-transport

A [winston](https://github.com/winstonjs/winston) transport that batches log lines and pushes
them to [Grafana Loki](https://grafana.com/oss/loki/).

Attach it **alongside** your Console transport. Console output is untouched, so whatever
already collects stdout — CloudWatch, `docker logs`, your terminal — keeps receiving exactly
what it received before.

```sh
npm install @leaselock/loki-transport winston
```

Ships CommonJS and ESM builds, so it works from either module system without an interop shim.

## Usage

```ts
import winston from 'winston';
import { LokiTransport } from '@leaselock/loki-transport';

const lokiTransport = new LokiTransport({
  host: process.env.LOKI_HOST!,
  lokiUser: process.env.LOKI_USER!,
  lokiToken: process.env.LOKI_TOKEN!,
  // Match the logger's level so Loki receives the same lines stdout does.
  level: 'debug',
  labels: {
    service_name: 'my-service',
    environment: 'production',
  },
});

const logger = winston.createLogger({
  level: 'debug',
  transports: [new winston.transports.Console(), lokiTransport],
});
```

### Options

| Option           | Required | Default                           | Notes                                               |
| ---------------- | -------- | --------------------------------- | --------------------------------------------------- |
| `host`           | yes      | —                                 | Loki write base URL. Trailing slashes are stripped. |
| `lokiUser`       | yes      | —                                 | Basic-auth username.                                |
| `lokiToken`      | yes      | —                                 | Basic-auth password.                                |
| `level`          | yes      | —                                 | Minimum winston level accepted.                     |
| `labels`         | yes      | —                                 | Stream labels on every line. Keep cardinality low.  |
| `interval`       | no       | `1000`                            | Batching interval in ms. Use ~`100` for Lambda.     |
| `debug`          | no       | `false`                           | Per-line and per-flush console logging.             |
| `loggerId`       | no       | `process.env.LOGGER_ID` or a uuid | Stable per-process id on every line.                |
| `getContext`     | no       | —                                 | Returns per-request context, emitted as `context`.  |
| `getExtraFields` | no       | —                                 | Returns fields spread onto every line.              |
| `lineFormat`     | no       | `{}`                              | See [Line format](#line-format).                    |
| `flushOnEnd`     | no       | `false`                           | Flush on `logger.end()`. See [Flushing](#flushing). |

The constructor throws if `host`, `lokiUser` or `lokiToken` is missing, so gate construction on
your own env checks rather than passing empty strings.

### Labels and cardinality

Every distinct label combination is a separate Loki stream with its own rate limit. Keep labels
to a small, bounded set — service, environment, compute type. Anything unbounded (request ids,
user ids, URLs) belongs in the log body, which stays queryable with LogQL filters.

## Line format

Each line is pushed as JSON with a stable field order:

```json
{
  "level": "info",
  "message": "Fetched properties",
  "date": "2026-01-01T00:00:00.000Z",
  "category": "property_service",
  "namespace": "getProperties",
  "service": "my-service",
  "loggerId": "3f2b…",
  "error": { "name": "Error", "message": "…", "stack": "…" },
  "nestedErrorStack": "Error: outer\n… \nCaused By: Error: inner\n…",
  "payload": { "propertyId": 42 }
}
```

Four of those accept two input names, because winston formats in the wild disagree on them.
Whichever you set, the output uses the canonical name:

| Emitted as | Canonical  | Also accepted |
| ---------- | ---------- | ------------- |
| `date`     | `date`     | `timestamp`   |
| `category` | `category` | `label`       |
| `error`    | `error`    | `cause`       |
| `payload`  | `payload`  | `metadata`    |

By default, unset fields are dropped and `nestedErrorStack` walks the standard `cause` chain.
`lineFormat` changes that where a service needs different behaviour:

| Option                 | Default     | Effect                                                                |
| ---------------------- | ----------- | --------------------------------------------------------------------- |
| `undefinedPlaceholder` | —           | Render unset fields as this string instead of dropping the key.       |
| `errorCauseKeys`       | `['cause']` | Properties walked to build `nestedErrorStack`.                        |
| `dedupePayloadErrors`  | `false`     | Blank error-shaped `payload` keys holding the same object as `error`. |
| `passthroughFields`    | `false`     | Copy unrecognised `info` fields onto the line.                        |
| `stripFields`          | `[]`        | Drop these `info` fields before passthrough.                          |

A `LEGACY_LINE_FORMAT` preset is exported for services with existing Grafana queries that must
not change:

```ts
import { LokiTransport, LEGACY_LINE_FORMAT } from '@leaselock/loki-transport';

new LokiTransport({
  /* … */
  lineFormat: LEGACY_LINE_FORMAT,
  getContext: () => asyncLocalStorage.getStore() ?? {},
  flushOnEnd: true,
});
```

## Flushing

Lines are buffered and pushed on an interval. The timer is `unref()`'d, so it never holds a
process open by itself.

**In a short-lived process you must flush explicitly.** AWS Lambda freezes the container the
moment the handler resolves, so anything still buffered is lost:

```ts
try {
  return await handler(event);
} finally {
  await flushLogs();
}
```

`flush()` is single-flight — concurrent callers share the in-flight push — and covers only the
batch it detached. A failed push requeues its entries and still resolves, so a caller needing
the queue genuinely empty should loop against `pendingCount`:

```ts
const deadlineAt = Date.now() + 8000;
do {
  await lokiTransport.flush();
} while (lokiTransport.pendingCount > 0 && Date.now() < deadlineAt);
```

Bound that loop. Without a deadline, an unreachable Loki becomes billed Lambda duration on
every invocation.

`close()` drains and then stops the transport. `flushOnEnd` wires that to winston's
`logger.end()`, but it is **off by default**: when one transport instance is shared across
several loggers, the first `end()` would close it for all of them. Enable it only where the
process owns the transport.

The transport also registers `process.once` handlers for `SIGINT` and `SIGTERM` that drain on
shutdown. It never calls `process.exit` — the application decides when to exit.

## Behaviour under load and failure

| Concern                          | Behaviour                                                                                     |
| -------------------------------- | --------------------------------------------------------------------------------------------- |
| Oversized line                   | Over 128 KiB, replaced by a bounded summary with a preview and `truncated: true`.             |
| Oversized batch                  | Split until the gzipped body fits under 900 KiB.                                              |
| Push failure                     | Entries requeue and retry up to 3 times, then drop with a throttled warning.                  |
| Backlog                          | Capped at 50,000 lines; oldest dropped first.                                                 |
| Circular refs, BigInt, functions | Handled by the serializer.                                                                    |
| Cyclic error chains              | Cycle-guarded and depth-capped at 10.                                                         |
| Errors                           | Never thrown at the caller — failures go to `console.error`, so logging cannot break the app. |

Requests are gzipped, sent over keep-alive agents, and grouped into one stream per level so
`level` is queryable as a Loki label.

## Development

```sh
npm install
npm run verify      # format:check + type_check + test + build
```

The test suite mocks the network entirely, so it needs no Loki instance.

## License

UNLICENSED - internal use only. Publicly visible for install convenience; no rights to
use, copy or distribute are granted.
