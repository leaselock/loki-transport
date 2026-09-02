import zlib from 'node:zlib';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import axios from 'axios';

import { LEGACY_LINE_FORMAT, LokiTransport, type LokiTransportOptions } from './loki_transport';

vi.mock('axios');

const OPTS = {
  host: 'http://loki.test:3100',
  lokiUser: 'user',
  lokiToken: 'token',
  level: 'debug',
  labels: { service_name: 'test-service', environment: 'test' },
  interval: 100_000, // effectively disable the timer; tests flush explicitly
};

/** Decodes the gzipped body of a captured axios.post call back into a Loki push payload. */
const decodePush = (call: Array<unknown>) => {
  const body = call[1] as Buffer;
  return JSON.parse(zlib.gunzipSync(body).toString('utf8')) as {
    streams: Array<{ stream: Record<string, string>; values: Array<[string, string]> }>;
  };
};

const logSync = (transport: LokiTransport, info: Record<string, unknown>) =>
  new Promise<void>((resolve) => transport.log(info, resolve));

let transports: Array<LokiTransport> = [];

const makeTransport = (overrides: Partial<LokiTransportOptions> = {}) => {
  const transport = new LokiTransport({ ...OPTS, ...overrides });
  transports.push(transport);
  return transport;
};

/** Logs one info object, flushes, and returns the parsed line Loki received. */
const lineFor = async (transport: LokiTransport, info: Record<string, unknown>) => {
  await logSync(transport, info);
  await transport.flush();
  const payload = decodePush(vi.mocked(axios.post).mock.calls[0]!);
  return JSON.parse(payload.streams[0]!.values[0]![1]) as Record<string, unknown>;
};

beforeEach(() => {
  vi.mocked(axios.post).mockReset();
  vi.mocked(axios.post).mockResolvedValue({ status: 204, data: '' } as never);
  // The transport logs an init line and registers SIGINT/SIGTERM handlers per instance.
  vi.spyOn(console, 'log').mockImplementation(() => {});
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.spyOn(console, 'error').mockImplementation(() => {});
  process.setMaxListeners(0);
});

afterEach(async () => {
  for (const transport of transports) {
    transport.destroy();
  }
  transports = [];
  vi.restoreAllMocks();
});

describe('constructor', () => {
  it.each([
    ['host', { host: '' }],
    ['lokiUser', { lokiUser: '' }],
    ['lokiToken', { lokiToken: '' }],
  ])('throws when %s is missing', (_name, override) => {
    expect(() => makeTransport(override)).toThrow('Loki host, user, and token are required');
  });

  it('strips trailing slashes from the host so the push URL has no double slash', async () => {
    const transport = makeTransport({ host: 'http://loki.test:3100///' });
    await logSync(transport, { level: 'info', message: 'hello' });
    await transport.flush();

    expect(vi.mocked(axios.post).mock.calls[0]?.[0]).toBe('http://loki.test:3100/loki/api/v1/push');
  });

  it('sends basic auth and gzip headers', async () => {
    const transport = makeTransport();
    await logSync(transport, { level: 'info', message: 'hello' });
    await transport.flush();

    const config = vi.mocked(axios.post).mock.calls[0]?.[2] as { headers: Record<string, string> };
    expect(config.headers['Content-Encoding']).toBe('gzip');
    expect(config.headers.Authorization).toBe(`Basic ${Buffer.from('user:token').toString('base64')}`);
  });
});

describe('buffering', () => {
  it('queues lines without pushing until flushed', async () => {
    const transport = makeTransport();

    await logSync(transport, { level: 'info', message: 'one' });
    await logSync(transport, { level: 'warn', message: 'two' });

    expect(transport.pendingCount).toBe(2);
    expect(axios.post).not.toHaveBeenCalled();

    await transport.flush();

    expect(transport.pendingCount).toBe(0);
    expect(axios.post).toHaveBeenCalledOnce();
  });

  it('applies the configured labels to the stream', async () => {
    const transport = makeTransport();
    await logSync(transport, { level: 'info', message: 'hello' });
    await transport.flush();

    const payload = decodePush(vi.mocked(axios.post).mock.calls[0]!);
    expect(payload.streams[0]!.stream).toMatchObject({
      service_name: 'test-service',
      environment: 'test',
    });
  });

  it('flush() on an empty buffer does not call Loki', async () => {
    const transport = makeTransport();
    await transport.flush();
    expect(axios.post).not.toHaveBeenCalled();
  });
});

describe('flush()', () => {
  it('is single-flight: concurrent callers share one in-flight push', async () => {
    const transport = makeTransport();
    await logSync(transport, { level: 'info', message: 'hello' });

    const [a, b] = [transport.flush(), transport.flush()];
    await Promise.all([a, b]);

    expect(axios.post).toHaveBeenCalledOnce();
  });

  it('requeues entries when the push fails, so nothing is silently lost', async () => {
    vi.mocked(axios.post).mockRejectedValue(new Error('connection refused') as never);
    const transport = makeTransport();

    await logSync(transport, { level: 'info', message: 'hello' });
    await transport.flush();

    expect(transport.pendingCount).toBe(1);
  });

  it('drops an entry after MAX_RETRIES rather than requeueing forever', async () => {
    vi.mocked(axios.post).mockRejectedValue(new Error('connection refused') as never);
    const transport = makeTransport();

    await logSync(transport, { level: 'info', message: 'hello' });
    for (let attempt = 0; attempt < 5; attempt += 1) {
      await transport.flush();
    }

    expect(transport.pendingCount).toBe(0);
  });
});

describe('error cause chains', () => {
  it('walks the cause chain into the pushed line', async () => {
    const transport = makeTransport();
    const error = new Error('outer', { cause: new Error('inner') });

    await logSync(transport, { level: 'error', message: 'boom', cause: error });
    await transport.flush();

    const payload = decodePush(vi.mocked(axios.post).mock.calls[0]!);
    expect(payload.streams[0]!.values[0]![1]).toContain('Caused By:');
  });

  it('terminates on a cyclic cause chain instead of hanging', async () => {
    const transport = makeTransport();
    const a = new Error('a');
    const b = new Error('b', { cause: a });
    (a as Error & { cause?: unknown }).cause = b; // cycle

    await logSync(transport, { level: 'error', message: 'cyclic', cause: b });
    await transport.flush();

    expect(axios.post).toHaveBeenCalledOnce();
  });
});

describe('oversized payloads', () => {
  // MAX_LINE_BYTES in the transport. Kept in the test so a change to the cap has to be
  // made deliberately in both places.
  const MAX_LINE_BYTES = 128 * 1024;

  it.each([
    ['the message', (huge: string) => ({ level: 'info', message: huge })],
    ['the metadata', (huge: string) => ({ level: 'info', message: 'small', metadata: { huge } })],
  ])('keeps a line under the cap when the bulk is in %s', async (_where, build) => {
    const transport = makeTransport();
    const huge = 'x'.repeat(300 * 1024);

    await logSync(transport, build(huge));
    await transport.flush();

    const payload = decodePush(vi.mocked(axios.post).mock.calls[0]!);
    const line = payload.streams[0]!.values[0]![1];

    expect(Buffer.byteLength(line, 'utf8')).toBeLessThanOrEqual(MAX_LINE_BYTES);
    expect(JSON.parse(line)).toMatchObject({ truncated: true });
  });

  it('chunks a large batch into multiple pushes', async () => {
    const transport = makeTransport();
    // CHUNK_SIZE is 100, so 250 lines must span at least 3 requests.
    for (let i = 0; i < 250; i += 1) {
      await logSync(transport, { level: 'info', message: `line ${i}` });
    }
    await transport.flush();

    expect(vi.mocked(axios.post).mock.calls.length).toBeGreaterThanOrEqual(3);
  });
});

describe('close()', () => {
  it('drains the buffer', async () => {
    const transport = makeTransport();
    await logSync(transport, { level: 'info', message: 'hello' });

    await transport.close();

    expect(transport.pendingCount).toBe(0);
    expect(axios.post).toHaveBeenCalledOnce();
  });

  it('stops accepting new lines once closing', async () => {
    const transport = makeTransport();
    await transport.close();

    await logSync(transport, { level: 'info', message: 'after close' });

    expect(transport.pendingCount).toBe(0);
  });
});

// ---------------------------------------------------------------------------------------
// Line format. These lock in the exact JSON each service already sends to Loki: a change
// here silently breaks existing Grafana queries and dashboards, so the key order and the
// treatment of unset fields are both asserted.
// ---------------------------------------------------------------------------------------

describe('default line format', () => {
  it('emits the canonical key order and drops unset fields', async () => {
    const transport = makeTransport({ loggerId: 'fixed-id' });

    const line = await lineFor(transport, {
      level: 'info',
      message: 'hello',
      timestamp: '2026-01-01T00:00:00.000Z',
      label: 'my_module',
      namespace: 'ns',
      service: 'my-service',
    });

    expect(Object.keys(line)).toEqual(['level', 'message', 'date', 'category', 'namespace', 'service', 'loggerId']);
    expect(line).toEqual({
      level: 'info',
      message: 'hello',
      date: '2026-01-01T00:00:00.000Z',
      category: 'my_module',
      namespace: 'ns',
      service: 'my-service',
      loggerId: 'fixed-id',
    });
  });

  it('maps the alternative field names onto the canonical ones', async () => {
    const transport = makeTransport({ loggerId: 'fixed-id' });
    const line = await lineFor(transport, {
      level: 'info',
      message: 'hello',
      timestamp: '2026-01-01T00:00:00.000Z',
      label: 'from_label',
      metadata: { a: 1 },
      cause: new Error('boom'),
    });

    expect(line.category).toBe('from_label');
    expect(line.payload).toEqual({ a: 1 });
    expect((line.error as Record<string, unknown>).message).toBe('boom');
  });

  it('does not emit context or passthrough fields', async () => {
    const transport = makeTransport();
    const line = await lineFor(transport, { level: 'info', message: 'x', requestId: 'abc' });

    expect(line).not.toHaveProperty('context');
    expect(line).not.toHaveProperty('requestId');
  });

  it('walks the standard cause chain', async () => {
    const transport = makeTransport();
    const line = await lineFor(transport, {
      level: 'error',
      message: 'x',
      cause: new Error('outer', { cause: new Error('inner') }),
    });

    expect(line.nestedErrorStack).toContain('Caused By:');
  });
});

describe('legacy line format', () => {
  const legacy = (extra: Partial<LokiTransportOptions> = {}) =>
    makeTransport({
      loggerId: 'fixed-id',
      lineFormat: LEGACY_LINE_FORMAT,
      getContext: () => ({ requestId: 'req-1' }),
      ...extra,
    });

  it('emits the legacy key order, rendering unset fields as __undefined__', async () => {
    const line = await lineFor(legacy(), {
      level: 'info',
      message: 'hello',
      date: '2026-01-01T00:00:00.000Z',
      category: 'my_module',
    });

    expect(Object.keys(line)).toEqual([
      'level',
      'message',
      'date',
      'category',
      'namespace',
      'loggerId',
      'context',
      'error',
      'nestedErrorStack',
      'payload',
    ]);
    // The distinguishing behaviour: absent fields are rendered, not dropped.
    expect(line.namespace).toBe('__undefined__');
    expect(line.error).toBe('__undefined__');
    expect(line.payload).toBe('__undefined__');
    expect(line.context).toEqual({ requestId: 'req-1' });
  });

  it('emits an empty context object when the store is empty', async () => {
    const transport = makeTransport({
      loggerId: 'fixed-id',
      lineFormat: LEGACY_LINE_FORMAT,
      getContext: () => undefined,
    });
    const line = await lineFor(transport, { level: 'info', message: 'x' });

    expect(line.context).toEqual({});
  });

  it('passes unrecognised fields through and strips the console-routing ones', async () => {
    const line = await lineFor(legacy(), {
      level: 'info',
      message: 'x',
      requestId: 'abc',
      stdoutMethod: 'log',
      stderrMethod: 'error',
    });

    expect(line.requestId).toBe('abc');
    expect(line).not.toHaveProperty('stdoutMethod');
    expect(line).not.toHaveProperty('stderrMethod');
  });

  it('spreads getExtraFields onto every line', async () => {
    const line = await lineFor(legacy({ getExtraFields: () => ({ ecsTaskArn: 'arn:task/abc' }) }), {
      level: 'info',
      message: 'x',
    });

    expect(line.ecsTaskArn).toBe('arn:task/abc');
  });

  it('walks the nested chain rather than cause', async () => {
    const outer = new Error('outer') as Error & { nested?: Error };
    outer.nested = new Error('inner');

    const line = await lineFor(legacy(), { level: 'error', message: 'x', error: outer });
    expect(line.nestedErrorStack).toContain('Caused By:');

    // A standard `cause` chain is deliberately NOT walked under this preset.
    vi.mocked(axios.post).mockClear();
    const other = await lineFor(legacy(), {
      level: 'error',
      message: 'x',
      error: new Error('outer', { cause: new Error('inner') }),
    });
    expect(other.nestedErrorStack).toBe('__undefined__');
  });

  it('de-duplicates an error repeated inside payload', async () => {
    const error = new Error('boom');
    const line = await lineFor(legacy(), {
      level: 'error',
      message: 'x',
      error,
      payload: { error, keepMe: 1 },
    });

    const payload = line.payload as Record<string, unknown>;
    expect(payload.keepMe).toBe(1);
    // Blanked rather than deleted, so the de-duplication is visible in the line.
    expect(payload.error).toBe('__undefined__');
  });

  it('drops payload entirely when it IS the error object', async () => {
    const error = new Error('boom');
    const line = await lineFor(legacy(), { level: 'error', message: 'x', error, payload: error });

    expect(line.payload).toBe('__undefined__');
  });

  it('keeps oversized lines under the cap', async () => {
    const line = await lineFor(legacy(), {
      level: 'info',
      message: 'x'.repeat(300 * 1024),
      payload: { blob: 'y'.repeat(300 * 1024) },
    });

    expect(Buffer.byteLength(JSON.stringify(line), 'utf8')).toBeLessThanOrEqual(128 * 1024);
    expect(line.truncated).toBe(true);
  });
});

describe('flushOnEnd', () => {
  it('does not flush on stream finalization by default', async () => {
    const transport = makeTransport();
    await logSync(transport, { level: 'info', message: 'x' });

    await new Promise<void>((resolve) => transport.end(() => resolve()));

    expect(axios.post).not.toHaveBeenCalled();
    expect(transport.pendingCount).toBe(1);
  });

  it('flushes on stream finalization when enabled', async () => {
    const transport = makeTransport({ flushOnEnd: true });
    await logSync(transport, { level: 'info', message: 'x' });

    await new Promise<void>((resolve) => transport.end(() => resolve()));

    expect(axios.post).toHaveBeenCalledOnce();
    expect(transport.pendingCount).toBe(0);
  });
});
