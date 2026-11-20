// Checks that queries and outgoing HTTP calls are attributed to the request that caused them,
// under concurrency, and that captured requests survive a restart.
//
// Needs a Postgres to talk to and the host project's pg and fastify. Run it from a project that
// has both installed:
//   GO_DEBUG_TEST_DATABASE_URL=postgres://postgres:test@127.0.0.1:55999/test \
//     node --test /path/to/go-debug/test/attribution.test.cjs
const assert = require('node:assert');
const { spawn, spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const databaseUrl = process.env.GO_DEBUG_TEST_DATABASE_URL;
const register = path.join(__dirname, '..', 'register.cjs');
const skip = !databaseUrl && 'GO_DEBUG_TEST_DATABASE_URL is not set';

const service = `
const http = require('node:http');
const { Pool } = require('pg');
const fastify = require('fastify');
(async () => {
  const pool = new Pool({ connectionString: process.env.GO_DEBUG_TEST_DATABASE_URL, max: 3 });
  // Stands in for another service or an external API.
  const zlib = require('node:zlib');
  const upstream = http.createServer((req, res) => {
    let received = '';
    req.on('data', (c) => { received += c; });
    req.on('end', () => {
      const body = JSON.stringify({ path: req.url, received });
      if (req.url.includes('gzip')) {
        res.writeHead(200, { 'x-upstream': 'yes', 'content-type': 'application/json', 'content-encoding': 'gzip' });
        return res.end(zlib.gzipSync(body));
      }
      if (req.url.includes('binary')) { res.writeHead(200, { 'content-type': 'image/png' }); return res.end(Buffer.from([1, 2, 3])); }
      res.writeHead(req.url.includes('missing') ? 404 : 200, { 'x-upstream': 'yes', 'content-type': 'application/json' });
      res.end(body);
    });
  });
  const postHttp = (p, payload) => new Promise((resolve, reject) => {
    const r = http.request(upstreamBase + p, { method: 'POST', headers: { 'content-type': 'application/json' } }, (res) => { let t = ''; res.on('data', (c) => { t += c; }); res.on('end', () => resolve(t)); });
    r.on('error', reject); r.write(payload.slice(0, 5)); r.end(payload.slice(5));
  });
  await new Promise((r) => upstream.listen(0, '127.0.0.1', r));
  const upstreamBase = 'http://127.0.0.1:' + upstream.address().port;
  const viaHttp = (p) => new Promise((resolve, reject) => http.get(upstreamBase + p, { headers: { authorization: 'Bearer secret' } }, (res) => { res.resume(); res.on('end', resolve); }).on('error', reject));

  const app = fastify();
  app.post('/graphql', async (req) => {
    const { n, name } = req.body.variables;
    for (let i = 0; i < n; i++) { await pool.query('select $1::int as seq', [i]); }
    await Promise.all(Array.from({ length: n }, (_, i) => pool.query('select $1::int as par', [i])));
    await viaHttp('/http-module/' + name);
    await (await fetch(upstreamBase + '/fetch/' + name, { method: 'POST', body: 'x' })).text();
    if (name === 'large') { await (await fetch(upstreamBase + '/missing/' + name)).text(); }
    if (name === 'medium') {
      const sent = JSON.stringify({ query: '{ me { id } }', n });
      const viaModule = await postHttp('/gzip/echo', sent);
      const viaFetch = await (await fetch(upstreamBase + '/fetch-json', { method: 'POST', headers: { 'content-type': 'application/json' }, body: sent })).json();
      await (await fetch(upstreamBase + '/binary')).arrayBuffer();
      globalThis.callerSaw = { viaModuleBytes: viaModule.length, viaFetch };
    }
    // Work that starts in this request's context but runs after it has responded, and a telemetry export.
    setTimeout(() => { pool.query('select 7 as after_response'); viaHttp('/after-response/' + name); }, 60);
    await viaHttp('/v1/logs');
    return { data: {} };
  });
  app.get('/health', async () => ({ ok: true }));
  await app.listen({ port: 0, host: '127.0.0.1' });
  const base = 'http://127.0.0.1:' + app.server.address().port;
  await pool.query('select 99 as background');
  const post = (name, n) => fetch(base + '/graphql', { method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ operationName: name, variables: { n, name }, query: 'query ' + name + ' { x }' }) });
  const bar = 'http://127.0.0.1:' + process.env.GO_DEBUG_PORT + '/api';
  const get = async (p) => (await fetch(bar + p)).json();
  const before = (await get('/requests')).length;
  await Promise.all([post('small', 2), post('large', 9), post('medium', 5), fetch(base + '/health'), post('__ApolloGetServiceDefinition__', 0)]);
  await new Promise((r) => setTimeout(r, 150));
  const requests = await get('/requests');
  const mine = requests.filter((r) => r.service === process.env.SERVICE_NAME);
  const large = mine.find((r) => r.operationName === 'large');
  const stopped = requests.find((r) => r.service !== process.env.SERVICE_NAME && r.operationName === 'large');
  const fromStopped = stopped ? await get('/requests/' + stopped.id) : null;
  const stoppedHttp = fromStopped ? await get('/http/' + fromStopped.http[0].uid) : null;
  const detail = await get('/requests/' + large.id);
  const queries = await get('/local/queries');
  const httpList = await get('/local/http');
  const oneQuery = await get('/queries/' + detail.statements[0].executions[0].uid);
  const oneHttp = await get('/http/' + detail.http[0].uid);
  const medium = await get('/requests/' + mine.find((r) => r.operationName === 'medium').id);
  const bodies = [];
  for (const h of medium.http) { bodies.push(await get('/http/' + h.uid)); }
  console.log(JSON.stringify({ fromStopped, stoppedHttp, bodies, mediumListHasBodies: medium.http.some((h) => 'responseBody' in h), callerSaw: globalThis.callerSaw, before, meta: await get('/meta'), upstreamBase, base, requests, detail, queries, httpList, oneQuery, oneHttp, page: (await fetch(bar.replace('/api', '/requests/' + large.id))).status }));
  await app.close(); upstream.close(); await pool.end();
})();
`;

function run(dir, extraEnv = {}) {
  const result = spawnSync(process.execPath, ['--require', register, '-e', service], {
    cwd: process.cwd(),
    encoding: 'utf8',
    env: { ...process.env, NODE_ENV: 'local', SERVICE_NAME: 'alpha', GO_DEBUG_DIR: dir, GO_DEBUG_PORT: '4790', ...extraEnv },
  });
  assert.strictEqual(result.status, 0, result.stderr);
  return JSON.parse(result.stdout.split('\n').filter((line) => line.startsWith('{')).pop());
}

test('attributes queries and outgoing HTTP to their request and keeps them across restarts', { skip }, () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'go-debug-'));
  const first = run(dir);
  assert.strictEqual(first.meta.service, 'alpha');
  assert.ok(first.requests.every((r) => r.service === 'alpha' && /^alpha-\d+$/.test(r.id)), 'ids carry the service');
  const graphql = first.requests.filter((r) => r.operationName);

  assert.strictEqual(first.before, 0);
  assert.deepStrictEqual(
    Object.fromEntries(graphql.map((r) => [r.operationName, [r.dbCalls, r.repeated, r.httpCalls]])),
    { small: [4, 4, 2], large: [18, 18, 3], medium: [10, 10, 5] },
  );
  assert.ok(first.requests.some((r) => r.url === '/health' && r.dbCalls === 0), 'plain HTTP requests are listed too');
  assert.ok(!first.requests.some((r) => /^__Apollo/.test(r.operationName ?? '')), 'schema polls are skipped');

  // Statements with the same call count are ordered by time, which varies run to run.
  const byRaw = Object.fromEntries(first.detail.statements.map((s) => [s.raw, s]));
  assert.deepStrictEqual(Object.keys(byRaw).sort(), ['select $1::int as par', 'select $1::int as seq']);
  assert.strictEqual(byRaw['select $1::int as seq'].calls, 9);
  assert.deepStrictEqual(
    byRaw['select $1::int as seq'].executions.map((e) => e.params[0]),
    ['0', '1', '2', '3', '4', '5', '6', '7', '8'],
  );
  assert.strictEqual(first.detail.timeline.length, 18);
  assert.strictEqual(first.oneQuery.timesInRequest, 9);
  assert.strictEqual(first.oneQuery.operation, 'large');

  assert.deepStrictEqual(
    first.detail.http.map((h) => [h.via, h.method, h.url.replace(first.upstreamBase, ''), h.status]),
    [
      ['http', 'GET', '/http-module/large', 200],
      ['fetch', 'POST', '/fetch/large', 200],
      ['fetch', 'GET', '/missing/large', 404],
    ],
  );
  assert.strictEqual(first.detail.http[0].requestHeaders.authorization, '[hidden]');
  assert.strictEqual(first.detail.http[0].responseHeaders['x-upstream'], 'yes');
  assert.strictEqual(first.detail.http[1].responseHeaders['x-upstream'], 'yes');
  assert.ok(first.detail.http.every((h) => typeof h.ms === 'number'));
  assert.strictEqual(first.oneHttp.operation, 'large');

  // The test's own calls into the service are outgoing requests made outside any request.
  assert.ok(first.httpList.some((h) => h.requestId === null && h.url === first.base + '/graphql'));
  assert.ok(first.queries.some((q) => q.requestId === null && q.sql === 'select 99 as background'));
  // Payload and response of outgoing calls.
  const sent = JSON.stringify({ query: '{ me { id } }', n: 5 });
  const byPath = Object.fromEntries(first.bodies.map((b) => [b.url.replace(first.upstreamBase, ''), b]));
  const gzip = byPath['/gzip/echo'];
  assert.strictEqual(gzip.requestBody.text, sent, 'http module payload, written in two chunks');
  assert.deepStrictEqual(JSON.parse(gzip.responseBody.text), { path: '/gzip/echo', received: sent }, 'gzip response is decoded');
  const fetched = byPath['/fetch-json'];
  assert.strictEqual(fetched.requestBody.text, sent, 'fetch payload');
  assert.deepStrictEqual(JSON.parse(fetched.responseBody.text), { path: '/fetch-json', received: sent });
  assert.deepStrictEqual(first.callerSaw.viaFetch, { path: '/fetch-json', received: sent }, 'the caller still gets its response');
  assert.ok(first.callerSaw.viaModuleBytes > 0);
  assert.strictEqual(byPath['/binary'].responseBody.text, null);
  assert.match(byPath['/binary'].responseBody.note, /image\/png/);
  assert.strictEqual(byPath['/http-module/medium'].requestBody, null);
  assert.strictEqual(first.mediumListHasBodies, false, 'bodies are only sent with the single-call page');

  const late = first.queries.filter((q) => q.sql === 'select 7 as after_response');
  // One per GraphQL call the test makes, including the skipped schema poll.
  assert.strictEqual(late.length, 4);
  assert.ok(late.every((q) => q.requestId === null), 'work after the response is not charged to the request');
  assert.ok(first.httpList.filter((h) => h.url.includes('/after-response/')).every((h) => h.requestId === null));
  assert.ok(!first.httpList.some((h) => h.url.includes('/v1/logs')), 'telemetry exports are skipped');
  assert.strictEqual(first.page, 200);

  const second = run(dir);
  assert.strictEqual(second.before, first.requests.length, 'requests from the first run are loaded on start');
  const ids = second.requests.map((r) => r.id);
  assert.strictEqual(new Set(ids).size, ids.length, 'ids stay unique across restarts');

  // A different service sharing the folder shows the stopped service's saved requests next to its own.
  const other = run(dir, { SERVICE_NAME: 'beta' });
  assert.deepStrictEqual(
    other.meta.services.filter((svc) => ['alpha', 'beta'].includes(svc.service)),
    [
      { service: 'beta', live: true, status: 'connected' },
      { service: 'alpha', live: false, status: 'stopped' },
    ],
  );
  assert.strictEqual(other.before, second.requests.length, 'the stopped service is listed before any traffic');
  const counts = other.requests.reduce((acc, r) => ({ ...acc, [r.service]: (acc[r.service] ?? 0) + 1 }), {});
  assert.strictEqual(counts.alpha, second.requests.length);
  assert.strictEqual(counts.beta, first.requests.length);
  assert.strictEqual(other.detail.service, 'beta');
  assert.strictEqual(other.fromStopped.service, 'alpha');
  assert.strictEqual(other.fromStopped.dbCalls, 18);
  assert.match(other.fromStopped.http[0].uid, /^alpha-\d+$/);
  assert.strictEqual(other.stoppedHttp.status, 200);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('does not start outside local environments', () => {
  const result = spawnSync(process.execPath, ['--require', register, '-e', "console.log('ran')"], {
    cwd: process.cwd(),
    encoding: 'utf8',
    env: { ...process.env, NODE_ENV: 'production', GO_DEBUG_FORCE: '' },
  });
  assert.match(result.stderr, /not started: NODE_ENV is "production"/);
  assert.match(result.stdout, /ran/);
});

// A service with no database: answers one request to itself so the page starts, then stays up.
const idleService = `
const http = require('node:http');
const app = http.createServer((req, res) => res.end('ok'));
app.listen(0, '127.0.0.1', () => { http.get('http://127.0.0.1:' + app.address().port + '/ping', (res) => res.resume()); });
`;
function startIdle(name, dir, cwd, extraEnv = {}) {
  const env = { ...process.env, NODE_ENV: 'local', SERVICE_NAME: name, GO_DEBUG_DIR: dir, ...extraEnv };
  if (!('GO_DEBUG_PORT' in extraEnv)) {
    delete env.GO_DEBUG_PORT;
  }
  const child = spawn(process.execPath, ['--require', register, '-e', idleService], { cwd, env });
  child.output = '';
  child.stdout.on('data', (chunk) => { child.output += chunk; });
  child.stderr.on('data', (chunk) => { child.output += chunk; });
  return child;
}
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function servedBy(port) {
  try {
    return (await (await fetch('http://127.0.0.1:' + port + '/api/meta')).json()).service;
  } catch {
    return null;
  }
}
async function waitFor(check, what) {
  for (let i = 0; i < 60; i++) {
    if (await check()) {
      return;
    }
    await sleep(150);
  }
  assert.fail('timed out waiting for ' + what);
}

test('takes the port from .env, and the environment wins over .env', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'go-debug-'));
  fs.writeFileSync(path.join(dir, '.env'), 'OTHER=1\nGO_DEBUG_PORT="4793" # page port\n');
  const fromFile = startIdle('gamma', dir, dir);
  const fromEnv = startIdle('delta', dir, dir, { GO_DEBUG_PORT: '4794' });
  try {
    await waitFor(async () => (await servedBy(4793)) === 'gamma', 'the .env port');
    await waitFor(async () => (await servedBy(4794)) === 'delta', 'the environment port');
  } finally {
    fromFile.kill();
    fromEnv.kill();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('falls back to the default port when the value is not a port', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'go-debug-'));
  const child = startIdle('epsilon', dir, dir, { GO_DEBUG_PORT: 'abc' });
  try {
    await waitFor(() => /is not a usable port; using 4600/.test(child.output), 'the warning');
  } finally {
    child.kill();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('a second service takes over the main port when the first one stops', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'go-debug-'));
  const first = startIdle('zeta', dir, dir, { GO_DEBUG_PORT: '4796' });
  let second;
  try {
    await waitFor(async () => (await servedBy(4796)) === 'zeta', 'the first service');
    second = startIdle('eta', dir, dir, { GO_DEBUG_PORT: '4796' });
    await waitFor(async () => (await servedBy(4797)) === 'eta', 'the second service on the next port');
    await waitFor(() => /Open http:\/\/127\.0\.0\.1:4796/.test(second.output), 'the second service pointing at the main port');
    first.kill();
    await waitFor(async () => (await servedBy(4796)) === 'eta', 'the takeover');
    const listed = await (await fetch('http://127.0.0.1:4796/api/requests')).json();
    assert.ok(listed.length > 0 && !listed.some((r) => r.url.startsWith('/api/')), 'page traffic on the main port is not recorded');
  } finally {
    first.kill();
    second?.kill();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('the page runs on its own and picks up a service started afterwards', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'go-debug-'));
  const env = { ...process.env, NODE_ENV: 'local', GO_DEBUG_DIR: dir, GO_DEBUG_PORT: '4798' };
  const hub = spawn(process.execPath, [path.join(__dirname, '..', 'bin', 'go-debug.cjs')], { cwd: dir, env });
  let service;
  try {
    const pageStatus = () => fetch('http://127.0.0.1:4798/requests').then((res) => res.status, () => null);
    await waitFor(async () => (await pageStatus()) === 200, 'the page');
    assert.strictEqual(await servedBy(4798), null, 'the hub is not a service itself');
    assert.deepStrictEqual(await (await fetch('http://127.0.0.1:4798/api/requests')).json(), []);
    service = startIdle('theta', dir, dir, { GO_DEBUG_PORT: '4798' });
    const meta = async () => (await (await fetch('http://127.0.0.1:4798/api/meta')).json()).services;
    await waitFor(async () => (await meta()).some((svc) => svc.service === 'theta' && svc.status === 'connected'), 'the service to connect');
    await waitFor(async () => (await (await fetch('http://127.0.0.1:4798/api/requests')).json()).some((r) => r.service === 'theta' && r.url === '/ping'), 'its request');
    service.kill();
    await waitFor(async () => (await meta()).some((svc) => svc.service === 'theta' && svc.status === 'stopped'), 'the service to show as stopped');
  } finally {
    hub.kill();
    service?.kill();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// The page builds the copied SQL in the browser; that code is taken out of page.html and run here.
function loadCopyCode() {
  const page = fs.readFileSync(path.join(__dirname, '..', 'page.html'), 'utf8');
  const code = page.slice(page.indexOf('// ---- copying a query with its values ----'), page.indexOf('async function copyText'));
  return new Function(`${code}\nreturn { queryWithValues, inlineParams };`)();
}

test('copied SQL fills in values without touching strings, comments or quoted names', () => {
  const { queryWithValues, inlineParams } = loadCopyCode();
  assert.strictEqual(
    queryWithValues('select * from t where a = $1 and b = $2 and c = $10', ['1', '"o\'k"', ...Array(7).fill('null'), '"ten"']),
    "select * from t where a = 1 and b = 'o''k' and c = 'ten';",
  );
  const untouched = `select '$1', "col$1", $tag$ $1 $tag$, $$ $2 $$ /* $1 */ from t -- $1\nwhere x = $1`;
  assert.strictEqual(inlineParams(untouched, ['7']).text, untouched.replace(/\$1$/, '7'));
  assert.strictEqual(queryWithValues('select $1::bigint;', ['9007199254740993']), 'select 9007199254740993::bigint;');
  assert.strictEqual(queryWithValues('select $1, $2', ['true']), '-- no value was recorded for $2\nselect true, $2;');
  assert.match(queryWithValues('select $1', [`"${'x'.repeat(299)}…`]), /^-- \$1 was cut off at 300 characters/);
});

function typedCase() {
  return {
    sql: 'select $1::int as n, $2::text as s, $3::int[] as ints, $4::text[] as texts, $5::jsonb as doc, $6::timestamptz as at, $7::bool as flag, $8::text as nothing, $9::bigint as big, $10::text as tenth, $11::int[] as none, $12::uuid = any($13::uuid[]) as found',
    values: [42, 'it\'s "quoted" \\ back', [1, 2, 3], ['a,b', 'c"d', "e'f", null, 'g\\h', '{x}'], { a: [1, { b: "x'y" }] },
      new Date('2026-01-02T03:04:05.678Z'), true, null, '9007199254740993', 'ten', [],
      '6f1c2f0e-8f0b-4c56-9a53-1d2b3c4d5e6f', ['6f1c2f0e-8f0b-4c56-9a53-1d2b3c4d5e6f', '00000000-0000-4000-8000-000000000000']],
  };
}
const typedService = `
const http = require('node:http');
const { Pool } = require('pg');
${typedCase.toString()}
(async () => {
  const pool = new Pool({ connectionString: process.env.GO_DEBUG_TEST_DATABASE_URL });
  const { sql, values } = typedCase();
  const app = http.createServer(async (req, res) => { await pool.query(sql, values); res.end('ok'); });
  await new Promise((resolve) => app.listen(0, '127.0.0.1', resolve));
  await fetch('http://127.0.0.1:' + app.address().port + '/typed');
  await new Promise((resolve) => setTimeout(resolve, 150));
  const bar = 'http://127.0.0.1:' + process.env.GO_DEBUG_PORT + '/api';
  const list = await (await fetch(bar + '/requests')).json();
  const detail = await (await fetch(bar + '/requests/' + list.find((r) => r.url === '/typed').id)).json();
  console.log(JSON.stringify(detail.statements[0]));
  app.close(); await pool.end();
})();
`;

test('copied SQL returns the same row as the query that was recorded', { skip }, async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'go-debug-'));
  const result = spawnSync(process.execPath, ['--require', register, '-e', typedService], {
    cwd: process.cwd(),
    encoding: 'utf8',
    env: { ...process.env, NODE_ENV: 'local', SERVICE_NAME: 'typed', GO_DEBUG_DIR: dir, GO_DEBUG_PORT: '4791' },
  });
  fs.rmSync(dir, { recursive: true, force: true });
  assert.strictEqual(result.status, 0, result.stderr);
  const statement = JSON.parse(result.stdout.split('\n').filter((line) => line.startsWith('{')).pop());
  const copied = loadCopyCode().queryWithValues(statement.raw, statement.executions[0].params);
  assert.ok(!/\$\d/.test(copied), 'no placeholder is left: ' + copied);

  const { Pool } = require(require.resolve('pg', { paths: [process.cwd()] }));
  const pool = new Pool({ connectionString: databaseUrl });
  try {
    const { sql, values } = typedCase();
    const expected = (await pool.query(sql, values)).rows;
    assert.deepStrictEqual((await pool.query(copied)).rows, expected);
    assert.strictEqual(expected[0].found, true);
  } finally {
    await pool.end();
  }
});

const transactionService = `
const http = require('node:http');
const { Pool } = require('pg');
(async () => {
  const pool = new Pool({ connectionString: process.env.GO_DEBUG_TEST_DATABASE_URL, max: 2 });
  await pool.query('create table if not exists go_debug_tx (id int, note text)');
  const app = http.createServer(async (req, res) => {
    const mode = req.url.slice(1);
    await pool.query('select 0 as before_tx');
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query('insert into go_debug_tx (id, note) values ($1, $2)', [1, mode]);
      await client.query('savepoint sp1');
      await client.query('update go_debug_tx set note = $1 where id = $2', ['x', 1]);
      await client.query('rollback to savepoint sp1');
      await client.query('select count(*) from go_debug_tx');
      if (mode === 'abandon') { return; }
      await client.query(mode === 'rollback' ? 'rollback' : 'commit');
    } finally {
      // An abandoned transaction's connection is thrown away, as pg does after an error.
      client.release(mode === 'abandon');
      await pool.query('select 9 as after_tx');
      res.end('ok');
    }
  });
  await new Promise((resolve) => app.listen(0, '127.0.0.1', resolve));
  const base = 'http://127.0.0.1:' + app.address().port;
  // More requests than connections, so transactions wait for and reuse each other's connection.
  // The first five carry the same trace id, as the requests of one gateway operation do.
  const traceparent = '00-' + 'ab'.repeat(16) + '-' + '12'.repeat(8) + '-01';
  await Promise.all(['commit', 'rollback', 'commit', 'rollback', 'abandon'].map((mode) => fetch(base + '/' + mode, { headers: { traceparent } })));
  await fetch(base + '/commit');
  await new Promise((resolve) => setTimeout(resolve, 150));
  const bar = 'http://127.0.0.1:' + process.env.GO_DEBUG_PORT + '/api';
  const list = await (await fetch(bar + '/requests')).json();
  const details = [];
  for (const row of list) { details.push(await (await fetch(bar + '/requests/' + row.id)).json()); }
  console.log(JSON.stringify(details));
  await pool.query('drop table go_debug_tx');
  app.close(); await pool.end();
})();
`;

test('ties each statement to its transaction and reports how the transaction ended', { skip }, () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'go-debug-'));
  const result = spawnSync(process.execPath, ['--require', register, '-e', transactionService], {
    cwd: process.cwd(),
    encoding: 'utf8',
    env: { ...process.env, NODE_ENV: 'local', SERVICE_NAME: 'tx', GO_DEBUG_DIR: dir, GO_DEBUG_PORT: '4792' },
  });
  fs.rmSync(dir, { recursive: true, force: true });
  assert.strictEqual(result.status, 0, result.stderr);
  const details = JSON.parse(result.stdout.split('\n').filter((line) => line.startsWith('[')).pop());
  assert.strictEqual(details.length, 6);
  assert.strictEqual(details.filter((detail) => detail.trace === 'ab'.repeat(16)).length, 5, 'requests of one operation share a trace');
  assert.strictEqual(details.filter((detail) => detail.trace === null).length, 1);
  assert.ok(details.every((detail) => detail.writes === 2), 'the insert and the update are counted as writes');
  const ids = new Set();
  for (const detail of details) {
    const mode = detail.url.slice(1);
    assert.strictEqual(detail.transactions.length, 1, mode);
    const [transaction] = detail.transactions;
    assert.ok(!ids.has(transaction.id), 'each request has its own transaction');
    ids.add(transaction.id);
    assert.strictEqual(transaction.outcome, { commit: 'committed', rollback: 'rolled back', abandon: 'not finished' }[mode]);
    assert.strictEqual(transaction.statements, mode === 'abandon' ? 6 : 7);
    const inside = detail.timeline.filter((q) => q.tx === transaction.id).map((q) => q.raw.split(' ')[0].toLowerCase());
    assert.deepStrictEqual(inside, ['begin', 'insert', 'savepoint', 'update', 'rollback', 'select', ...(mode === 'abandon' ? [] : [mode])]);
    const outside = detail.timeline.filter((q) => !q.tx).map((q) => q.raw);
    assert.deepStrictEqual(outside, ['select 0 as before_tx', 'select 9 as after_tx'], 'statements around the transaction are not part of it');
  }
});

const gatewayStepsService = `
const http = require('node:http');
const { Pool } = require('pg');
(async () => {
  const pool = new Pool({ connectionString: process.env.GO_DEBUG_TEST_DATABASE_URL, max: 2 });
  await pool.query('create table if not exists go_debug_steps (id int)');
  const handlers = {
    // The mutation only opens a namespace; the follow-up entity request does the work.
    createThing__things__0: async () => {},
    createThing__things__3: async () => {
      await pool.query('select 1 as shared');
      await pool.query('insert into go_debug_steps (id) values ($1)', [1]);
    },
    listThings__things__0: async () => { await pool.query('select 1 as shared'); },
  };
  const app = http.createServer((req, res) => {
    let raw = '';
    req.on('data', (chunk) => { raw += chunk; });
    req.on('end', async () => { await handlers[JSON.parse(raw).operationName](); res.end('{}'); });
  });
  await new Promise((resolve) => app.listen(0, '127.0.0.1', resolve));
  const trace = (letter) => '00-' + letter.repeat(32) + '-' + '1'.repeat(16) + '-01';
  const post = (operationName, type, traceparent) => fetch('http://127.0.0.1:' + app.address().port + '/graphql', {
    method: 'POST', headers: { 'content-type': 'application/json', traceparent },
    body: JSON.stringify({ operationName, variables: { step: operationName }, query: type + ' ' + operationName + ' { x }' }) });
  await post('createThing__things__0', 'mutation', trace('a'));
  await post('createThing__things__3', 'query', trace('a'));
  // Same trace as the mutation, as a browser keeps one trace per page, but a different operation.
  await post('listThings__things__0', 'query', trace('a'));
  await post('listThings__things__0', 'query', trace('b'));
  await new Promise((resolve) => setTimeout(resolve, 150));
  const bar = 'http://127.0.0.1:' + process.env.GO_DEBUG_PORT + '/api';
  const get = async (p) => (await fetch(bar + p)).json();
  const list = await get('/requests');
  const local = await get('/local/requests');
  const second = 'steps-' + local.find((r) => r.operationName === 'createThing__things__3').id;
  console.log(JSON.stringify({ list, viaSecond: await get('/requests/' + second), single: await get('/requests/' + list.find((r) => r.operationName === 'listThings__things__0').id) }));
  await pool.query('drop table go_debug_steps');
  app.close(); await pool.end();
})();
`;

test('shows the requests the gateway sends for one operation as a single request', { skip }, () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'go-debug-'));
  const result = spawnSync(process.execPath, ['--require', register, '-e', gatewayStepsService], {
    cwd: process.cwd(),
    encoding: 'utf8',
    env: { ...process.env, NODE_ENV: 'local', SERVICE_NAME: 'steps', GO_DEBUG_DIR: dir, GO_DEBUG_PORT: '4793' },
  });
  fs.rmSync(dir, { recursive: true, force: true });
  assert.strictEqual(result.status, 0, result.stderr);
  const { list, viaSecond, single } = JSON.parse(result.stdout.split('\n').filter((line) => line.startsWith('{')).pop());

  assert.deepStrictEqual(list.map((r) => r.operationName).sort(), ['createThing', 'listThings__things__0', 'listThings__things__0']);
  const merged = list.find((r) => r.operationName === 'createThing');
  assert.deepStrictEqual(
    [merged.operationType, merged.parts, merged.dbCalls, merged.writes, merged.repeated],
    ['mutation', 2, 2, 1, 0],
  );
  assert.strictEqual(viaSecond.id, merged.id, 'any request of the operation opens the whole operation');
  assert.deepStrictEqual(viaSecond.timeline.map((q) => [q.order, q.part, q.raw.split(' ')[0]]), [[1, 1, 'select'], [2, 1, 'insert']]);
  assert.deepStrictEqual(viaSecond.parts.map((part) => [part.operationName, part.operationType, part.dbCalls]), [
    ['createThing__things__0', 'mutation', 0],
    ['createThing__things__3', 'query', 2],
  ]);
  assert.match(viaSecond.parts[1].variables, /createThing__things__3/);
  assert.strictEqual(single.parts, undefined, 'a different operation on the same trace stays on its own');
  assert.strictEqual(single.dbCalls, 1);
});
