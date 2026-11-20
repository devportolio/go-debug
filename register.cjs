// GoDebug, a local-only debug tool: records the HTTP requests a service receives and, for each one,
// the Postgres statements it ran and the HTTP requests it sent out.
// Load it when starting a service, with no code change:
//   NODE_OPTIONS="--require @dotcom-dev/go-debug/register" yarn start:dev wishlists
// or use the bundled command:  go-debug yarn start:dev wishlists
// Run the command with nothing after it to serve the page on its own, before any service is started.
//
// Env: GO_DEBUG_PORT       port of the page (default: 4600); also read from the .env of the folder the
//                          service starts in. With several services running, the rest take the next ports.
//      GO_DEBUG_DIR        where captured requests are saved (default: ~/.go-debug)
//      GO_DEBUG_FORCE      set to 1 to run even when NODE_ENV is not a local environment
const { AsyncLocalStorage } = require('node:async_hooks');
const { execFile } = require('node:child_process');
const diagnostics = require('node:diagnostics_channel');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');
const zlib = require('node:zlib');

const LOCAL_ENVIRONMENTS = ['local', 'development', 'dev', 'test'];

// "wishlists" from ".../dist/apps/wishlists/main"; falls back to the folder the process started in.
const SERVICE =
  (process.argv[1] ?? '').match(/[\\/]apps[\\/]([^\\/]+)[\\/]/)?.[1] ??
  process.env.SERVICE_NAME ??
  path.basename(process.cwd());
// Hub: serves the page without being a service itself. Set by the command when it is given nothing to run.
const HUB = process.env.GO_DEBUG_HUB === '1';
// True for "node dist/apps/<service>/main", false for the yarn, nest and compiler processes around it.
const IS_SERVICE_PROCESS = /[\\/]apps[\\/][^\\/]+[\\/]/.test(process.argv[1] ?? '');
const DEFAULT_PORT = 4600;
// The preload runs before the app loads its .env, so the file is read here. The environment wins.
function configuredPort() {
  let value = process.env.GO_DEBUG_PORT;
  let source = 'the environment';
  if (!value) {
    try {
      const line = fs
        .readFileSync(path.join(process.cwd(), '.env'), 'utf8')
        .match(/^[ \t]*(?:export[ \t]+)?GO_DEBUG_PORT[ \t]*=(.*)$/m);
      value = line?.[1].replace(/\s+#.*$/, '').trim().replace(/^(["'])(.*)\1$/, '$2');
      source = '.env';
    } catch {
      // No .env file.
    }
  }
  if (!value) {
    return DEFAULT_PORT;
  }
  const port = Number(value);
  if (!Number.isInteger(port) || port < 1024 || port > 65535) {
    console.warn(`[go-debug] GO_DEBUG_PORT "${value}" from ${source} is not a usable port; using ${DEFAULT_PORT}.`);
    return DEFAULT_PORT;
  }
  return port;
}
const MAIN_PORT = configuredPort();
const PORT_ATTEMPTS = 40;
const TAKEOVER_INTERVAL_MS = 2000;
const DATA_DIR = process.env.GO_DEBUG_DIR || path.join(os.homedir(), '.go-debug');
const MAX_REQUESTS = 100;
const MAX_BACKGROUND = 300;
const MAX_LIST = 500;
const MAX_BODY_BYTES = 512 * 1024;
const MAX_PARAMS = 100;
const MAX_PARAM_LENGTH = 300;
// Per outgoing request or response body. Kept small because bodies are saved to disk with the request.
const MAX_CAPTURED_BODY = 20000;
const TEXT_BODY = /json|text|xml|graphql|x-www-form-urlencoded|javascript/i;
const SENSITIVE_HEADER = /authorization|cookie|token|secret|api-?key|password/i;
const fromApp = (name) => require(require.resolve(name, { paths: [process.cwd()] }));

let pg;
try {
  pg = HUB ? null : fromApp('pg');
} catch {
  pg = null;
}

const environment = process.env.NODE_ENV;
const enabled =
  process.env.GO_DEBUG_FORCE === '1' || !environment || LOCAL_ENVIRONMENTS.includes(environment);
if (!enabled) {
  console.warn(`[go-debug] not started: NODE_ENV is "${environment}". It only runs in local environments.`);
}

if (enabled) {
  const requestContext = new AsyncLocalStorage();
  let requests = [];
  let nextRequestId = 1;
  let nextEntryId = 1;
  let nextTransactionId = 1;
  // The open transaction of each database connection, so every statement can be tied to its transaction.
  const openTransactions = new WeakMap();
  const transactionStep = (text) => {
    const start = String(text).trimStart().slice(0, 40).toLowerCase();
    if (/^(begin|start\s+transaction)\b/.test(start)) {
      return 'begin';
    }
    if (/^(commit|end)\b/.test(start)) {
      return 'committed';
    }
    // "rollback to savepoint" undoes part of a transaction and leaves it open.
    if (/^(rollback|abort)\b/.test(start) && !/^rollback\s+(work\s+|transaction\s+)?to\b/.test(start)) {
      return 'rolled back';
    }
    return null;
  };
  // Work done outside any incoming request (polling, startup). Kept in memory only.
  const background = { calls: [], http: [] };
  let server = null;
  // The page's own servers; requests to them are not the service's traffic.
  const ownServers = new WeakSet();

  // Timers and batchers created while a request is handled keep its context after it has responded
  // (a log exporter flushing every few seconds, for example). That later work is not the request's.
  const currentRequest = () => {
    const request = requestContext.getStore();
    return request && request.status === null ? request : undefined;
  };
  // The service's own telemetry exports are noise in a list of calls to other services.
  const isTelemetryExport = (url) => /\/v1\/(logs|traces|metrics)(\?|$)/.test(url) || /\/\/pyroscope[^/]*\/ingest/.test(url) || url.includes('/api/local/');

  // Captured requests are kept on disk so they survive service restarts (watch mode restarts often).
  const safeName = (name) => name.replace(/[^a-z0-9_-]/gi, '_');
  const dataPath = path.join(DATA_DIR, `${safeName(SERVICE)}.json`);
  try {
    const saved = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
    if (Array.isArray(saved.requests)) {
      requests = saved.requests.slice(-MAX_REQUESTS).map((request) => ({ calls: [], http: [], ...request }));
      nextRequestId = requests.reduce((max, request) => Math.max(max, request.id), 0) + 1;
      nextEntryId =
        requests.reduce(
          (max, request) => Math.max(max, ...request.calls.map((c) => c.uid ?? 0), ...request.http.map((h) => h.uid ?? 0)),
          0,
        ) + 1;
    }
  } catch {
    // No saved data yet, or it is unreadable; start empty.
  }
  const saveRequests = () => {
    try {
      fs.mkdirSync(DATA_DIR, { recursive: true });
      fs.writeFileSync(dataPath, JSON.stringify({ requests }));
    } catch (error) {
      console.warn(`[go-debug] could not save requests: ${error.message}`);
    }
  };

  const round = (ms) => (ms === null || ms === undefined ? null : Math.round(ms * 10) / 10);
  const pushCapped = (list, item, max) => {
    list.push(item);
    if (list.length > max) {
      list.shift();
    }
  };

  const normalize = (text) =>
    String(text)
      .replace(/\s+/g, ' ')
      .replace(/\(\s*(\$\d+\s*,\s*)+\$\d+\s*\)/g, '($…)')
      .trim();

  const describeParams = (values) => {
    if (!Array.isArray(values)) {
      return [];
    }
    return values.slice(0, MAX_PARAMS).map((value) => {
      let text;
      try {
        text = value instanceof Date ? value.toISOString() : JSON.stringify(value);
      } catch {
        text = String(value);
      }
      text = text === undefined ? String(value) : text;
      return text.length > MAX_PARAM_LENGTH ? `${text.slice(0, MAX_PARAM_LENGTH)}…` : text;
    });
  };

  const describeHeaders = (headers) => {
    const result = {};
    for (const [name, value] of Object.entries(headers ?? {})) {
      const text = Array.isArray(value) ? value.join(', ') : String(value);
      result[name] = SENSITIVE_HEADER.test(name) ? '[hidden]' : text.length > 500 ? `${text.slice(0, 500)}…` : text;
    }
    return result;
  };

  // undici hands headers over as a flat [name, value, name, value] list or as one raw string.
  const describeRawHeaders = (raw) => {
    const result = {};
    if (Array.isArray(raw)) {
      for (let i = 0; i + 1 < raw.length; i += 2) {
        result[String(raw[i]).toLowerCase()] = String(raw[i + 1]);
      }
    } else if (typeof raw === 'string') {
      for (const line of raw.split('\r\n')) {
        const at = line.indexOf(':');
        if (at > 0) {
          result[line.slice(0, at).trim().toLowerCase()] = line.slice(at + 1).trim();
        }
      }
    }
    return describeHeaders(result);
  };

  const operationOf = (request) =>
    request
      ? { requestId: request.id, operation: request.operationName ?? `${request.method} ${request.url}` }
      : { requestId: null, operation: null };

  // Keep in sync with verbOf in page.html, which labels the same statements.
  const isWrite = (text) => {
    const start = String(text).replace(/^[\s(]+/, '');
    if (/^with\b/i.test(start)) {
      return /\b(insert\s+into|update\s+\S+\s+set|delete\s+from)\b/i.test(start);
    }
    return /^(insert|update|delete|merge|truncate)\b/i.test(start);
  };
  // The gateway sends one client operation to a service as several requests; they share a trace id.
  const traceOf = (headers) => {
    const fromTraceparent = /^[0-9a-f]{2}-([0-9a-f]{32})-/i.exec(headers?.traceparent ?? '');
    const fromSentry = /^([0-9a-f]{32})-/i.exec(headers?.['sentry-trace'] ?? '');
    return (fromTraceparent ?? fromSentry)?.[1].toLowerCase() ?? null;
  };

  const summarize = (request) => {
    const counts = new Map();
    for (const call of request.calls) {
      counts.set(call.sql, (counts.get(call.sql) ?? 0) + 1);
    }
    return {
      id: request.id,
      startedAt: request.startedAt,
      method: request.method,
      url: request.url,
      operationName: request.operationName,
      operationType: request.operationType,
      status: request.status,
      durationMs: request.durationMs,
      trace: traceOf(request.headers),
      dbCalls: request.calls.length,
      writes: request.calls.filter((call) => isWrite(call.raw)).length,
      dbMs: round(request.calls.reduce((sum, call) => sum + (call.ms ?? 0), 0)),
      repeated: [...counts.values()].filter((n) => n > 1).reduce((sum, n) => sum + n, 0),
      httpCalls: request.http.length,
      httpMs: round(request.http.reduce((sum, call) => sum + (call.ms ?? 0), 0)),
    };
  };

  // One entry per transaction the request ran, in the order they started.
  function transactionsOf(calls) {
    const found = new Map();
    for (const call of calls) {
      if (!call.tx) {
        continue;
      }
      const entry = found.get(call.tx) ?? { id: call.tx, statements: 0, ms: 0, outcome: 'not finished' };
      entry.statements++;
      entry.ms += call.ms ?? 0;
      const step = transactionStep(call.raw);
      if (step === 'committed' || step === 'rolled back') {
        entry.outcome = step;
      }
      found.set(call.tx, entry);
    }
    return [...found.values()].map((entry) => ({ ...entry, ms: round(entry.ms) }));
  }

  function requestDetail(id, store) {
    const request = store.requests.find((candidate) => candidate.id === id);
    if (!request) {
      return null;
    }
    const grouped = new Map();
    request.calls.forEach((call, index) => {
      const entry = grouped.get(call.sql) ?? { sql: call.sql, raw: call.raw, calls: 0, ms: 0, executions: [] };
      entry.calls++;
      entry.ms += call.ms ?? 0;
      entry.executions.push({
        uid: call.uid,
        order: index + 1,
        ms: round(call.ms),
        params: call.params,
        // Lists of different lengths are grouped as one statement, so a call's SQL can differ from the group's.
        ...(call.raw !== entry.raw && { raw: call.raw }),
        ...(call.tx && { tx: call.tx }),
      });
      grouped.set(call.sql, entry);
    });
    return {
      ...summarize(request),
      headers: request.headers,
      query: request.query,
      variables: request.variables,
      statements: [...grouped.values()]
        .map((entry) => ({ ...entry, ms: round(entry.ms) }))
        .sort((a, b) => b.calls - a.calls || b.ms - a.ms),
      timeline: request.calls.map((call, index) => ({
        uid: call.uid,
        order: index + 1,
        raw: call.raw,
        ms: round(call.ms),
        ...(call.tx && { tx: call.tx }),
      })),
      transactions: transactionsOf(request.calls),
      http: request.http.map(({ requestBody, responseBody, ...call }) => ({ ...call, ms: round(call.ms) })),
    };
  }

  // Newest first, across all requests and background work.
  function listEntries(kind, store) {
    const rows = [];
    for (const call of store.background[kind]) {
      rows.push({ call, request: null });
    }
    for (const request of store.requests) {
      for (const call of request[kind]) {
        rows.push({ call, request });
      }
    }
    return rows.sort((a, b) => b.call.uid - a.call.uid).slice(0, MAX_LIST);
  }

  function findEntry(kind, uid, store) {
    for (const call of store.background[kind]) {
      if (call.uid === uid) {
        return { call, request: null };
      }
    }
    for (const request of store.requests) {
      const call = request[kind].find((candidate) => candidate.uid === uid);
      if (call) {
        return { call, request };
      }
    }
    return null;
  }

  const readPage = () => fs.readFileSync(path.join(__dirname, 'page.html'), 'utf8');

  // Answers for one service's data: this process's live data, or a stopped service's saved file.
  function serviceApi(method, url, store) {
    let match;
    if (method === 'GET' && url === '/requests') {
      return store.requests.slice().reverse().map(summarize);
    }
    if (method === 'GET' && (match = url.match(/^\/requests\/(\d+)$/))) {
      return requestDetail(Number(match[1]), store);
    }
    if (method === 'GET' && url === '/queries') {
      return listEntries('calls', store).map(({ call, request }) => ({
        uid: call.uid,
        at: call.at,
        sql: call.raw.length > 400 ? `${call.raw.slice(0, 400)}…` : call.raw,
        ms: round(call.ms),
        ...operationOf(request),
      }));
    }
    if (method === 'GET' && (match = url.match(/^\/queries\/(\d+)$/))) {
      const found = findEntry('calls', Number(match[1]), store);
      return (
        found && {
          uid: found.call.uid,
          at: found.call.at,
          raw: found.call.raw,
          params: found.call.params,
          ms: round(found.call.ms),
          timesInRequest: found.request
            ? found.request.calls.filter((call) => call.sql === found.call.sql).length
            : null,
          ...operationOf(found.request),
        }
      );
    }
    if (method === 'GET' && url === '/http') {
      return listEntries('http', store).map(({ call, request }) => ({
        uid: call.uid,
        at: call.at,
        method: call.method,
        url: call.url,
        status: call.status,
        error: call.error,
        ms: round(call.ms),
        ...operationOf(request),
      }));
    }
    if (method === 'GET' && (match = url.match(/^\/http\/(\d+)$/))) {
      const found = findEntry('http', Number(match[1]), store);
      return found && { ...found.call, ms: round(found.call.ms), ...operationOf(found.request) };
    }
    return undefined;
  }

  function localApi(method, url) {
    if (method === 'POST' && url === '/clear') {
      requests = [];
      background.calls = [];
      background.http = [];
      saveRequests();
      return { ok: true };
    }
    return serviceApi(method, url, { requests, background });
  }

  // ---- combining services ----
  // Every running service announces its page here, so any one page can show them all.
  const instancesDir = path.join(DATA_DIR, 'instances');
  const instancePath = path.join(instancesDir, `${safeName(SERVICE)}.json`);
  const isAlive = (pid) => {
    try {
      process.kill(pid, 0);
      return true;
    } catch {
      return false;
    }
  };
  function announce(port) {
    if (HUB) {
      return;
    }
    try {
      fs.mkdirSync(instancesDir, { recursive: true });
      fs.writeFileSync(instancePath, JSON.stringify({ service: SERVICE, port, pid: process.pid }));
      process.once('exit', () => {
        try {
          if (JSON.parse(fs.readFileSync(instancePath, 'utf8')).pid === process.pid) {
            fs.unlinkSync(instancePath);
          }
        } catch {
          // Already gone.
        }
      });
    } catch (error) {
      console.warn(`[go-debug] could not announce this service: ${error.message}`);
    }
  }
  // Running services first, then services that are stopped but have saved requests.
  function discover() {
    const services = new Map(HUB ? [] : [[SERVICE, { service: SERVICE, live: true, port: null, self: true }]]);
    const readDir = (dir) => {
      try {
        return fs.readdirSync(dir).filter((name) => name.endsWith('.json'));
      } catch {
        return [];
      }
    };
    for (const name of readDir(instancesDir)) {
      try {
        const info = JSON.parse(fs.readFileSync(path.join(instancesDir, name), 'utf8'));
        if ((HUB || info.service !== SERVICE) && isAlive(info.pid)) {
          services.set(info.service, { service: info.service, live: true, port: info.port, self: false });
        }
      } catch {
        // Half-written or stale file; skip it.
      }
    }
    for (const name of readDir(DATA_DIR)) {
      const service = name.slice(0, -5);
      if (!services.has(service)) {
        services.set(service, { service, live: false, port: null, self: false, file: path.join(DATA_DIR, name) });
      }
    }
    return [...services.values()];
  }
  const fetchPeer = (port, method, url) =>
    new Promise((resolve) => {
      const peer = http.request(
        { host: '127.0.0.1', port, path: `/api/local${url}`, method, timeout: 1500 },
        (res) => {
          let text = '';
          res.on('data', (chunk) => {
            text += chunk;
          });
          res.on('end', () => {
            try {
              resolve(res.statusCode === 200 ? JSON.parse(text) : undefined);
            } catch {
              resolve(undefined);
            }
          });
        },
      );
      peer.on('timeout', () => peer.destroy());
      peer.on('error', () => resolve(undefined));
      peer.end();
    });
  async function askService(target, method, url) {
    if (target.self) {
      return localApi(method, url);
    }
    if (target.live) {
      return fetchPeer(target.port, method, url);
    }
    try {
      if (method === 'POST' && url === '/clear') {
        fs.writeFileSync(target.file, JSON.stringify({ requests: [] }));
        return { ok: true };
      }
      const saved = JSON.parse(fs.readFileSync(target.file, 'utf8'));
      const savedRequests = (saved.requests ?? []).map((request) => ({ calls: [], http: [], ...request }));
      return serviceApi(method, url, { requests: savedRequests, background: { calls: [], http: [] } });
    } catch {
      return undefined;
    }
  }
  // Ids are only unique within a service, so the combined view prefixes them: "wishlists-124".
  function tag(service, value, top = true) {
    if (Array.isArray(value)) {
      return value.map((item) => tag(service, item, top));
    }
    if (!value || typeof value !== 'object') {
      return value;
    }
    const result = {};
    for (const [key, inner] of Object.entries(value)) {
      const isId = key === 'uid' || key === 'requestId' || (top && key === 'id');
      result[key] = isId && inner !== null && inner !== undefined ? `${service}-${inner}` : tag(service, inner, false);
    }
    if (top) {
      result.service = service;
    }
    return result;
  }

  // Services whose process is running, whether or not it was started with GoDebug. Found by
  // looking at running commands for ".../apps/<service>/main", so nothing is listed by hand.
  let runningCache = { at: 0, names: new Set() };
  const runningServices = () =>
    new Promise((resolve) => {
      if (Date.now() - runningCache.at < 3000) {
        resolve(runningCache.names);
        return;
      }
      execFile('ps', ['-axo', 'command='], { maxBuffer: 8 * 1024 * 1024 }, (error, stdout) => {
        const names = new Set();
        if (!error) {
          for (const line of stdout.split('\n')) {
            const match = line.match(/[\\/]apps[\\/]([^\\/\s]+)[\\/]main(\.js)?(\s|$)/);
            if (match) {
              names.add(match[1]);
            }
          }
        }
        runningCache = { at: Date.now(), names };
        resolve(names);
      });
    });
  // connected: running with GoDebug. started: running without it, so nothing is captured.
  // stopped: not running, but it has saved requests.
  async function serviceStatuses(services) {
    const statuses = services.map(({ service, live }) => ({
      service,
      live,
      status: live ? 'connected' : 'stopped',
    }));
    for (const name of await runningServices()) {
      const known = statuses.find((entry) => entry.service === name);
      if (!known) {
        statuses.push({ service: name, live: false, status: 'started' });
      } else if (!known.live) {
        known.status = 'started';
      }
    }
    const order = { connected: 0, started: 1, stopped: 2 };
    return statuses.sort((a, b) => order[a.status] - order[b.status] || a.service.localeCompare(b.service));
  }

  // ---- one client operation, several requests ----
  // The gateway sends one client operation to the services as several requests (a mutation that
  // only opens a namespace, then an _entities request that does the writing). They share a trace
  // id, and are shown as one request so the operation's database work is in one place.
  const mergedCache = new Map();
  const MAX_MERGED_CACHE = 300;
  // "createOrder__orders__3" is the gateway's name for step 3 of createOrder, sent to the orders service.
  const clientOperation = (name) => (name ? String(name).replace(/__[A-Za-z0-9-]+__\d+$/, '') : name);

  async function allSummaries(services) {
    const lists = await Promise.all(
      services.map(async (target) => tag(target.service, (await askService(target, 'GET', '/requests')) ?? [])),
    );
    return lists.flat();
  }
  // A browser can keep one trace id for a whole page, so the trace alone is not enough: the requests
  // must also carry the same operation name and follow each other without a pause.
  const MAX_STEP_GAP_MS = 3000;
  function groupOperations(summaries) {
    const candidates = new Map();
    const groups = [];
    for (const summary of summaries) {
      const name = clientOperation(summary.operationName);
      if (!summary.trace || !name) {
        groups.push([summary]);
        continue;
      }
      const key = `${summary.trace}|${name}`;
      candidates.set(key, [...(candidates.get(key) ?? []), summary]);
    }
    const startOf = (summary) => new Date(summary.startedAt).getTime();
    for (const list of candidates.values()) {
      list.sort((a, b) => startOf(a) - startOf(b));
      let run = [];
      let runEnd = 0;
      for (const summary of list) {
        if (run.length && startOf(summary) - runEnd > MAX_STEP_GAP_MS) {
          groups.push(run);
          run = [];
        }
        run.push(summary);
        runEnd = Math.max(runEnd, startOf(summary) + (summary.durationMs ?? 0));
      }
      groups.push(run);
    }
    return groups;
  }
  function mergeDetails(parts) {
    const [root] = parts;
    const sum = (key) => parts.reduce((total, part) => total + (part[key] ?? 0), 0);
    const start = new Date(root.startedAt).getTime();
    const ends = parts.filter((part) => part.durationMs !== null).map((part) => new Date(part.startedAt).getTime() + part.durationMs);
    const statements = new Map();
    const timeline = [];
    const transactions = [];
    parts.forEach((part, index) => {
      // Transaction numbers restart in every service, so they are made unique here.
      const txId = (tx) => (tx ? `${part.id}:${tx}` : undefined);
      const offset = timeline.length;
      for (const entry of part.timeline) {
        timeline.push({ ...entry, order: offset + entry.order, part: index, ...(entry.tx && { tx: txId(entry.tx) }) });
      }
      for (const statement of part.statements) {
        const merged = statements.get(statement.sql) ?? { sql: statement.sql, raw: statement.raw, calls: 0, ms: 0, executions: [] };
        merged.calls += statement.calls;
        merged.ms += statement.ms;
        for (const execution of statement.executions) {
          const raw = execution.raw ?? statement.raw;
          merged.executions.push({
            ...execution,
            order: offset + execution.order,
            ...(raw !== merged.raw && { raw }),
            ...(execution.tx && { tx: txId(execution.tx) }),
          });
        }
        statements.set(statement.sql, merged);
      }
      for (const transaction of part.transactions ?? []) {
        transactions.push({ ...transaction, id: txId(transaction.id) });
      }
    });
    const grouped = [...statements.values()]
      .map((statement) => ({ ...statement, ms: round(statement.ms), executions: statement.executions.sort((a, b) => a.order - b.order) }))
      .sort((a, b) => b.calls - a.calls || b.ms - a.ms);
    return {
      id: root.id,
      service: root.service,
      services: [...new Set(parts.map((part) => part.service))],
      startedAt: root.startedAt,
      method: root.method,
      url: root.url,
      operationName: clientOperation(root.operationName),
      operationType: parts.some((part) => part.operationType === 'mutation') ? 'mutation' : root.operationType,
      status: parts.some((part) => part.status === null) ? null : Math.max(...parts.map((part) => part.status)),
      durationMs: ends.length === parts.length ? Math.max(...ends) - start : null,
      trace: root.trace,
      dbCalls: sum('dbCalls'),
      writes: sum('writes'),
      dbMs: round(sum('dbMs')),
      repeated: grouped.filter((statement) => statement.calls > 1).reduce((total, statement) => total + statement.calls, 0),
      httpCalls: sum('httpCalls'),
      httpMs: round(sum('httpMs')),
      headers: root.headers,
      query: root.query,
      variables: root.variables,
      statements: grouped,
      timeline,
      transactions,
      http: parts.flatMap((part) => part.http),
      parts: parts.map((part) => ({
        id: part.id,
        service: part.service,
        operationName: part.operationName,
        operationType: part.operationType,
        status: part.status,
        durationMs: part.durationMs,
        dbCalls: part.dbCalls,
        writes: part.writes,
        query: part.query,
        variables: part.variables,
      })),
    };
  }
  // The whole operation that the given requests belong to. Finished requests never change, so the result is kept.
  async function mergedOperation(group, services) {
    const key = group.map((summary) => summary.id).join(',');
    if (mergedCache.has(key)) {
      return mergedCache.get(key);
    }
    const details = await Promise.all(
      group.map(async (summary) => {
        const local = summary.id.slice(summary.service.length + 1);
        const target = services.find((candidate) => candidate.service === summary.service);
        const found = target && (await askService(target, 'GET', `/requests/${local}`));
        return found ? tag(summary.service, found) : null;
      }),
    );
    if (details.some((detail) => detail === null)) {
      return null;
    }
    const merged = mergeDetails(details);
    mergedCache.set(key, merged);
    if (mergedCache.size > MAX_MERGED_CACHE) {
      mergedCache.delete(mergedCache.keys().next().value);
    }
    return merged;
  }
  const withoutDetail = ({ headers, query, variables, statements, timeline, transactions, http, parts, ...summary }) => ({
    ...summary,
    parts: parts.length,
  });

  async function api(method, url) {
    if (url.startsWith('/api/local/')) {
      return localApi(method, url.slice('/api/local'.length));
    }
    const services = discover();
    let match;
    if (method === 'GET' && url === '/api/meta') {
      return {
        service: HUB ? null : SERVICE,
        pid: process.pid,
        capturesQueries: pg !== null,
        services: await serviceStatuses(services),
      };
    }
    if (method === 'GET' && url === '/api/requests') {
      const rows = await Promise.all(
        groupOperations(await allSummaries(services)).map(async (group) => {
          const merged = group.length > 1 ? await mergedOperation(group, services) : null;
          return merged ? [withoutDetail(merged)] : group;
        }),
      );
      return rows.flat().sort((a, b) => new Date(b.startedAt) - new Date(a.startedAt));
    }
    if (method === 'GET' && (match = url.match(/^\/api\/requests\/(.+)-(\d+)$/))) {
      // Any request of an operation opens the whole operation.
      const id = `${match[1]}-${match[2]}`;
      const group = groupOperations(await allSummaries(services)).find((candidates) => candidates.some((summary) => summary.id === id));
      const merged = group && group.length > 1 ? await mergedOperation(group, services) : null;
      if (merged) {
        return merged;
      }
    }
    if (method === 'POST' && url === '/api/clear') {
      mergedCache.clear();
    }
    if (method === 'GET' && (match = url.match(/^\/api\/(requests|queries|http)\/(.+)-(\d+)$/))) {
      const target = services.find((candidate) => candidate.service === match[2]);
      const found = target && (await askService(target, 'GET', `/${match[1]}/${match[3]}`));
      return found ? tag(target.service, found) : undefined;
    }
    if (method === 'POST' && url === '/api/clear') {
      await Promise.all(services.map((target) => askService(target, 'POST', '/clear')));
      return { ok: true };
    }
    return undefined;
  }

  function startServer() {
    const handler = async (req, res) => {
      if (req.url.startsWith('/api/')) {
        let result;
        try {
          result = await api(req.method, req.url);
        } catch {
          result = undefined;
        }
        res.writeHead(result ? 200 : 404, { 'content-type': 'application/json' });
        return res.end(JSON.stringify(result ?? { error: 'Not found. It may have been cleared or pushed out of the list.' }));
      }
      // The page handles its own routes, so any page address can be opened or reloaded directly.
      if (req.method === 'GET') {
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        return res.end(readPage());
      }
      res.writeHead(404);
      return res.end();
    };
    server = http.createServer(handler);
    ownServers.add(server);
    const name = HUB ? 'page' : SERVICE;
    // Whoever starts first gets the main port; the others walk forward until one is free.
    let port = MAIN_PORT;
    server.on('error', (error) => {
      if (error.code === 'EADDRINUSE' && port < MAIN_PORT + PORT_ATTEMPTS) {
        port++;
        server.listen(port, '127.0.0.1');
        return;
      }
      console.warn(`[go-debug] ${name}: page not available: ${error.message}`);
    });
    server.on('listening', async () => {
      announce(port);
      if (port === MAIN_PORT) {
        console.log(`[go-debug] ${name}: http://127.0.0.1:${port} (shows every service running GoDebug)`);
        return;
      }
      keepMainPortServed(handler);
      const heldByPeer = Array.isArray(await fetchPeer(MAIN_PORT, 'GET', '/requests'));
      console.log(
        heldByPeer
          ? `[go-debug] ${name}: connected. Open http://127.0.0.1:${MAIN_PORT} (also answers on port ${port})`
          : `[go-debug] ${name}: http://127.0.0.1:${port} (port ${MAIN_PORT} is used by something else)`,
      );
    });
    server.listen(port, '127.0.0.1');
    // A service must never stay alive just for this page; the hub has nothing else to stay alive for.
    if (!HUB) {
      server.unref();
    }
  }
  // When the service holding the main port stops, another one takes it over, so the address stays the same.
  function keepMainPortServed(handler) {
    const attempt = () => {
      const main = http.createServer(handler);
      ownServers.add(main);
      main.on('error', () => setTimeout(attempt, TAKEOVER_INTERVAL_MS).unref());
      main.on('listening', () => {
        console.log(`[go-debug] ${HUB ? 'page' : SERVICE}: now serving http://127.0.0.1:${MAIN_PORT}`);
      });
      main.listen(MAIN_PORT, '127.0.0.1');
      main.unref();
    };
    setTimeout(attempt, TAKEOVER_INTERVAL_MS).unref();
  }
  // Started on first activity so build tools and watchers that load this file stay untouched.
  const ensureServer = () => {
    if (!server) {
      startServer();
    }
  };
  if (HUB) {
    startServer();
    // The hub captures nothing, so none of the patches below are installed.
    return;
  }
  // A real service shows as connected as soon as it boots, before its first request.
  if (IS_SERVICE_PROCESS) {
    setImmediate(ensureServer);
  }

  // ---- incoming requests ----

  function describeBody(request, raw) {
    try {
      const body = JSON.parse(raw);
      const first = Array.isArray(body) ? body[0] : body;
      if (!first || typeof first.query !== 'string') {
        return;
      }
      const header = first.query.match(/^\s*(query|mutation|subscription)\b\s*([A-Za-z_][A-Za-z0-9_]*)?/);
      request.operationType = header?.[1] ?? 'query';
      request.operationName = first.operationName ?? header?.[2] ?? null;
      request.query = first.query.length > 20000 ? `${first.query.slice(0, 20000)}…` : first.query;
      const variables = JSON.stringify(first.variables ?? {}, null, 2);
      request.variables = variables.length > 20000 ? `${variables.slice(0, 20000)}…` : variables;
    } catch {
      // Not JSON; leave the request described by method and URL only.
    }
  }

  // Every incoming HTTP request gets its own context, so its work can be traced back to it.
  const originalServerEmit = http.Server.prototype.emit;
  http.Server.prototype.emit = function patchedServerEmit(event, req, res) {
    if (event !== 'request' || ownServers.has(this)) {
      return originalServerEmit.apply(this, arguments);
    }
    ensureServer();

    const request = {
      id: nextRequestId++,
      startedAt: new Date().toISOString(),
      method: req.method,
      url: req.url,
      headers: describeHeaders(req.headers),
      operationName: null,
      operationType: null,
      query: null,
      variables: null,
      status: null,
      durationMs: null,
      calls: [],
      http: [],
    };
    const started = performance.now();
    const chunks = [];
    let size = 0;

    // Body events fire from the socket, outside the request's context. Re-entering it here keeps
    // everything that continues after the body is read (the resolvers) attributed to this request.
    const originalReqEmit = req.emit;
    req.emit = function patchedReqEmit(reqEvent, chunk) {
      if (reqEvent === 'data' && chunk && size < MAX_BODY_BYTES) {
        size += chunk.length;
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      } else if (reqEvent === 'end' && chunks.length) {
        describeBody(request, Buffer.concat(chunks).toString('utf8'));
      }
      return requestContext.run(request, () => originalReqEmit.apply(this, arguments));
    };

    res.once('finish', () => {
      request.status = res.statusCode;
      request.durationMs = Math.round(performance.now() - started);
      // The gateway polls every subgraph for its schema every few seconds; keeping those would push real requests out.
      if (/^__Apollo/.test(request.operationName ?? '')) {
        return;
      }
      pushCapped(requests, request, MAX_REQUESTS);
      saveRequests();
    });

    return requestContext.run(request, () => originalServerEmit.apply(this, arguments));
  };

  // ---- Postgres ----

  if (pg !== null) {
    // A pooled query waits for a free connection; the pool then calls back from whichever request
    // released one. Pinning the callback to the caller's context keeps the statement with its request.
    const originalConnect = pg.Pool.prototype.connect;
    pg.Pool.prototype.connect = function patchedConnect(callback) {
      if (typeof callback !== 'function') {
        return originalConnect.apply(this, arguments);
      }
      const request = requestContext.getStore();
      return originalConnect.call(this, function pinnedCallback() {
        return requestContext.run(request, () => callback.apply(this, arguments));
      });
    };

    const originalQuery = pg.Client.prototype.query;
    pg.Client.prototype.query = function patchedQuery(config, ...rest) {
      ensureServer();
      const text = typeof config === 'string' ? config : config && config.text;
      if (!text) {
        return originalQuery.call(this, config, ...rest);
      }

      const request = currentRequest();
      const values = config && typeof config === 'object' && Array.isArray(config.values) ? config.values : rest[0];
      const call = {
        uid: nextEntryId++,
        at: Date.now(),
        sql: normalize(text),
        raw: String(text),
        params: describeParams(values),
        ms: null,
      };
      const step = transactionStep(text);
      let open = openTransactions.get(this);
      // A connection handed back without a commit must not lend its transaction to the next request.
      if (open && open.request !== request) {
        open = undefined;
        openTransactions.delete(this);
      }
      if (step === 'begin') {
        open = { id: nextTransactionId++, request };
        openTransactions.set(this, open);
      }
      if (open) {
        call.tx = open.id;
        if (step === 'committed' || step === 'rolled back') {
          openTransactions.delete(this);
        }
      }
      if (request) {
        request.calls.push(call);
      } else {
        pushCapped(background.calls, call, MAX_BACKGROUND);
      }
      const started = performance.now();
      const done = () => {
        call.ms = performance.now() - started;
      };

      const last = rest.length - 1;
      if (last >= 0 && typeof rest[last] === 'function') {
        const callback = rest[last];
        rest[last] = function timedCallback() {
          done();
          return callback.apply(this, arguments);
        };
        return originalQuery.call(this, config, ...rest);
      }

      const result = originalQuery.call(this, config, ...rest);
      if (result && typeof result.then === 'function') {
        result.then(done, done);
      }
      return result;
    };
  }

  // ---- outgoing HTTP ----
  // Calls are detected through events Node publishes itself: one set for the http/https modules
  // (axios, got, most SDKs) and one for fetch and undici. Bodies need small hooks on top of that.

  const newCollector = () => ({ chunks: [], kept: 0, size: 0 });
  const collect = (collector, chunk, encoding) => {
    const buffer = Buffer.isBuffer(chunk)
      ? chunk
      : chunk instanceof Uint8Array
        ? Buffer.from(chunk)
        : Buffer.from(String(chunk), typeof encoding === 'string' ? encoding : 'utf8');
    collector.size += buffer.length;
    if (collector.kept < MAX_BODY_BYTES) {
      collector.chunks.push(buffer);
      collector.kept += buffer.length;
    }
  };
  const describeText = (text, bytes) => ({
    text: text.length > MAX_CAPTURED_BODY ? text.slice(0, MAX_CAPTURED_BODY) : text,
    bytes,
    truncated: text.length > MAX_CAPTURED_BODY,
    note: null,
  });
  const describeCollected = (collector, headers) => {
    if (!collector || collector.size === 0) {
      return null;
    }
    const type = String(headers?.['content-type'] ?? '');
    const encoding = String(headers?.['content-encoding'] ?? '').toLowerCase();
    if (type && !TEXT_BODY.test(type)) {
      return { text: null, bytes: collector.size, truncated: false, note: `${type}, not shown` };
    }
    let data = Buffer.concat(collector.chunks);
    try {
      if (encoding.includes('gzip')) {
        data = zlib.gunzipSync(data);
      } else if (encoding.includes('br')) {
        data = zlib.brotliDecompressSync(data);
      } else if (encoding.includes('deflate')) {
        data = zlib.inflateSync(data);
      }
    } catch {
      return { text: null, bytes: collector.size, truncated: false, note: `${encoding} body could not be decoded` };
    }
    return describeText(data.toString('utf8'), collector.size);
  };

  const pending = new WeakMap();
  const startOutgoing = (key, fields) => {
    if (isTelemetryExport(fields.url)) {
      return undefined;
    }
    const request = currentRequest();
    const call = {
      uid: nextEntryId++,
      at: Date.now(),
      status: null,
      error: null,
      ms: null,
      responseHeaders: {},
      requestBody: null,
      responseBody: null,
      ...fields,
    };
    if (request) {
      request.http.push(call);
    } else {
      pushCapped(background.http, call, MAX_BACKGROUND);
    }
    pending.set(key, { call, started: performance.now(), request });
    return call;
  };
  const saveIfParentDone = (entry) => {
    // The parent request may already be saved if this call outlived it.
    if (entry.request && entry.request.status !== null) {
      saveRequests();
    }
  };
  const finishOutgoing = (key, fields) => {
    const entry = pending.get(key);
    if (!entry) {
      return;
    }
    Object.assign(entry.call, fields, { ms: performance.now() - entry.started });
    saveIfParentDone(entry);
  };
  const subscribe = (name, handler) => {
    diagnostics.subscribe(name, (message) => {
      try {
        handler(message);
      } catch {
        // Capturing must never break the request being observed.
      }
    });
  };

  // http / https modules. The body is written before Node announces the request, so it is collected
  // as it is written and attached once the request is known.
  const writtenBodies = new WeakMap();
  for (const method of ['write', 'end']) {
    const original = http.ClientRequest.prototype[method];
    http.ClientRequest.prototype[method] = function capturedWrite(chunk, encoding) {
      try {
        if (chunk && typeof chunk !== 'function') {
          let collector = writtenBodies.get(this);
          if (!collector) {
            collector = newCollector();
            writtenBodies.set(this, collector);
          }
          collect(collector, chunk, encoding);
        }
      } catch {
        // Never interfere with the request itself.
      }
      return original.apply(this, arguments);
    };
  }

  subscribe('http.client.request.start', ({ request }) => {
    const host = request.getHeader('host') ?? request.host;
    const headers = request.getHeaders();
    startOutgoing(request, {
      via: 'http',
      method: request.method,
      url: `${request.protocol}//${host}${request.path}`,
      requestHeaders: describeHeaders(headers),
      requestBody: describeCollected(writtenBodies.get(request), headers),
    });
  });
  subscribe('http.client.response.finish', ({ request, response }) => {
    finishOutgoing(request, { status: response.statusCode, responseHeaders: describeHeaders(response.headers) });
    const entry = pending.get(request);
    if (!entry) {
      return;
    }
    // Watches the body as Node feeds it to the response stream, without reading the stream itself.
    const collector = newCollector();
    const originalPush = response.push;
    response.push = function capturedPush(chunk) {
      try {
        if (chunk) {
          collect(collector, chunk);
        } else {
          entry.call.responseBody = describeCollected(collector, response.headers);
          entry.call.ms = performance.now() - entry.started;
          saveIfParentDone(entry);
        }
      } catch {
        // Never interfere with the response itself.
      }
      return originalPush.apply(this, arguments);
    };
  });
  subscribe('http.client.request.error', ({ request, error }) => {
    finishOutgoing(request, { error: error?.message ?? 'Request failed' });
  });

  // fetch / undici. The events carry no bodies, so fetch itself is wrapped to read them: the request
  // body from its arguments, the response body from a copy so the caller's response is untouched.
  const fetchContext = new AsyncLocalStorage();
  const describeFetchBody = (body, contentType) => {
    if (body === null || body === undefined) {
      return null;
    }
    if (contentType && !TEXT_BODY.test(contentType)) {
      return { text: null, bytes: null, truncated: false, note: `${contentType}, not shown` };
    }
    if (typeof body === 'string') {
      return describeText(body, Buffer.byteLength(body));
    }
    if (body instanceof URLSearchParams) {
      return describeText(body.toString(), null);
    }
    if (body instanceof Uint8Array) {
      return describeText(Buffer.from(body).toString('utf8'), body.length);
    }
    return { text: null, bytes: null, truncated: false, note: 'stream or form data, not shown' };
  };
  if (typeof globalThis.fetch === 'function') {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = function capturedFetch(input, init) {
      const holder = { calls: [], entries: [], requestBody: null };
      try {
        const contentType = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined)).get('content-type');
        holder.requestBody = describeFetchBody(init?.body, contentType ?? '');
      } catch {
        // Unusual arguments; the call is still recorded without a body.
      }
      const promise = fetchContext.run(holder, () => originalFetch.apply(this, arguments));
      promise.then(
        (response) => {
          try {
            const call = holder.calls.at(-1);
            const type = response.headers.get('content-type') ?? '';
            const length = Number(response.headers.get('content-length') ?? 0);
            if (!call || !response.body || response.bodyUsed) {
              return;
            }
            if ((type && !TEXT_BODY.test(type)) || /event-stream/i.test(type) || length > MAX_BODY_BYTES) {
              call.responseBody = { text: null, bytes: length || null, truncated: false, note: `${type || 'large body'}, not shown` };
              return;
            }
            response
              .clone()
              .text()
              .then(
                (text) => {
                  call.responseBody = describeText(text, Buffer.byteLength(text));
                  const entry = holder.entries.at(-1);
                  if (entry) {
                    saveIfParentDone(entry);
                  }
                },
                () => {},
              );
          } catch {
            // Never interfere with the caller's response.
          }
        },
        () => {},
      );
      return promise;
    };
  }

  subscribe('undici:request:create', ({ request }) => {
    const call = startOutgoing(request, {
      via: 'fetch',
      method: request.method,
      url: `${request.origin}${request.path}`,
      requestHeaders: describeRawHeaders(request.headers),
    });
    const holder = fetchContext.getStore();
    if (call && holder) {
      // A redirect makes one fetch produce several requests: the body went out with the first.
      if (holder.calls.length === 0) {
        call.requestBody = holder.requestBody;
      }
      holder.calls.push(call);
      holder.entries.push(pending.get(request));
    }
  });
  subscribe('undici:request:headers', ({ request, response }) => {
    finishOutgoing(request, {
      status: response.statusCode,
      responseHeaders: describeRawHeaders((response.headers ?? []).map(String)),
    });
  });
  subscribe('undici:request:trailers', ({ request }) => {
    finishOutgoing(request, {});
  });
  subscribe('undici:request:error', ({ request, error }) => {
    finishOutgoing(request, { error: error?.message ?? 'Request failed' });
  });
}
