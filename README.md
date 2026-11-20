# @dotcom-dev/go-debug

GoDebug is a local-only debug tool for Node services. It records the HTTP requests a service
receives and, for each one, the Postgres statements it ran and the HTTP requests it sent out to
other services and APIs.

It needs no code change in the service. It is loaded when the process starts; it hooks Node's HTTP
server and the `pg` driver, and listens to the events Node publishes for outgoing HTTP.

![The Requests list: one row per request, with its query count, repeated queries and outgoing HTTP calls](docs/requests.png)

The screenshots on this page use a made-up shop, not real data.

## Quick start

1. Install it as a dev dependency. That alone makes `yarn start:go-debug` available; nothing is added
   to the project's own scripts.

       yarn add -D "@dotcom-dev/go-debug@https://github.com/devportolio/go-debug.git#v0.1.0"

2. Start a service with it instead of `yarn start:dev`:

       yarn start:go-debug wishlists

3. Open **http://127.0.0.1:4600**, trigger a query or mutation, and click the request that appears.

`yarn start:go-debug <service>` runs the project's own `yarn start:dev <service>` with GoDebug
loaded, so the service starts exactly as it does with `start:dev`. It is not the same thing as
`yarn start:debug`, which starts the Node inspector.

The package is installed straight from this repository at a version tag; it is not on a package
registry.

## What you see

**Requests** is a live list of every HTTP request the connected services handled, newest first.
GraphQL requests show their operation name. Columns: time, service, status, number of queries, how
many of those were repeats of the same statement (the N+1 signal), number of writes, number of
outgoing HTTP calls, and duration.

- **Filter**: type part of a service name, operation, URL or status. Several words must all match;
  a word starting with `-` excludes.
- **Services** in the sidebar: a green dot means connected, an empty dot means not connected. Hover
  one to see why; click one to filter the list to it.
- **Live / Paused** (top right): stops and resumes the automatic refresh.
- **Clear** (top right): removes the recorded requests of every service, after a confirmation.

Click a request to open its page:

![A request's page: 41 queries, 40 of them repeats, one line per statement with a Copy button](docs/request.png)

- **Queries**: identical statements grouped with their call count and total time. Each statement
  is one line; click it to see the full SQL and the parameters of every call. Untick "Group
  identical statements" to see them in the order they ran.
- **Writes and transactions**: inserts, updates and deletes are labelled, and the request shows
  how many writes and transactions it ran. Each transaction is listed with how it ended (committed,
  rolled back, or not finished), its number of statements and its time. Click one, or untick the
  grouping, to see the statements in the order they ran, with the ones inside each transaction
  marked from `begin` to `commit`.
- **Copy**: each query has a Copy button that copies the SQL with its parameter values filled in,
  ready to paste into a database client. When a statement ran several times with different values,
  a dropdown next to the button chooses which call's values to use.
- **HTTP client**: the outgoing requests made while handling it. Click one for its URL, status,
  payload, response and headers.
- **Payload**: the GraphQL operation and variables.
- **Headers**: the incoming request headers.

![A mutation's page: its inserts labelled, and the transaction they ran in shown as committed](docs/mutation.png)

**Compare two requests**: tick two rows in the Requests list and press "Compare selected". The older
one is the baseline. The page shows DB calls before and after with the change in percent, the same
for repeated calls, DB time, outgoing HTTP calls and duration, and a per-statement table of what was
removed, added or changed. "Swap before and after" flips the baseline.

![Comparing two requests: DB calls went from 41 to 3, a 92.7% drop](docs/compare.png)

## Other ways to start it

For a start command other than `start:dev`, put the command after `go-debug`:

    yarn go-debug yarn start:debug wishlists

or set the flag yourself:

    NODE_OPTIONS="--require @dotcom-dev/go-debug/register" yarn start:dev wishlists

To have the page open before any service is started, run the command with nothing after it, in its
own terminal:

    yarn go-debug

## Port

The page is on port 4600. To use another port, add this to the `.env` of the project you start
services from, or set it in your shell (the shell wins):

    GO_DEBUG_PORT=4700

## Behaviour

- **Local only.** It starts only when `NODE_ENV` is `local`, `development`, `dev`, `test` or unset.
  Anywhere else it prints one line and does nothing.
- **One address, one combined view.** The page shows the requests of every service started with
  GoDebug. The first service started serves the port; the others use the next ports, and one of
  them takes the main port over within a few seconds if the first service stops, so the address
  does not change.
- **Connected or not.** A service started with GoDebug shows as connected as soon as it boots. Any
  other running service is listed as not connected and nothing is captured from it: capturing
  happens inside the service's own process, so GoDebug has to be loaded when that process starts.
  Running services are found from the process list, by looking for `apps/<service>/main`.
- **Survives restarts.** The last 100 requests per service are saved in `~/.go-debug/` and loaded
  on start. While a service is restarting or stopped, an open page says it is offline and keeps
  showing the last list it received, and the requests you had already opened. It reconnects and
  catches up by itself. Services that have stopped stay in the list with their saved requests. The files
  contain query parameters from your local database.
- **One operation, one row.** The gateway can send one client operation to a service as several
  requests. A mutation is the common case: the request typed `mutation` may only open the way,
  while the insert or update runs in a follow-up `_entities` request typed `query`. GoDebug shows
  them as a single row and a single page, named after the client operation, with all their queries,
  writes and outgoing calls together. The page lists the underlying requests in one line, the
  in-order view marks where each begins, and the Payload tab shows each one's operation and
  variables. Requests are joined when they carry the same trace id (`traceparent`) and the same
  operation name and follow each other within three seconds.
- **What is captured.** Every Postgres statement sent through `pg`, reads and writes alike, including
  `begin`, `commit`, `rollback` and savepoints, and outgoing HTTP through the `http`/`https`
  modules (axios and most SDKs) and through `fetch`/undici. Redis, Kafka and gRPC are not captured.
- **Outgoing request bodies.** Text bodies (JSON, form, XML, plain text) are shown, up to 20,000
  characters each; binary bodies are noted but not stored. Gzip, deflate and brotli responses are
  decoded. For `fetch`, a streamed or form-data payload is not shown. Headers that look like
  credentials are shown as `[hidden]`; bodies are stored as sent, so they can contain tokens.
- **Background work is not shown.** Queries and outgoing requests made outside any incoming request
  (polling, startup) are recorded in memory but have no page yet.
- **Noise is left out.** The gateway's `__ApolloGetServiceDefinition__` schema polls and the
  service's own telemetry exports are not listed.

## Settings

| Variable | Meaning |
|---|---|
| `GO_DEBUG_PORT` | Port of the page. Default `4600`. Read from the environment, or from the `.env` of the folder the service is started in; the environment wins. An unusable value falls back to 4600 with a warning. |
| `GO_DEBUG_DIR` | Where captured requests are saved. Default `~/.go-debug`. |
| `GO_DEBUG_FORCE` | Set to `1` to start even when `NODE_ENV` is not a local environment. |

## Requirements and limits

- Node 20 or newer: outgoing HTTP capture relies on Node's diagnostics channels.
- macOS or Linux: the `start:go-debug` command name contains a colon, which Windows does not allow.
- `yarn start:go-debug` expects the project to have a `start:dev` script that takes the service name.
- It works by patching `http.Server` and `pg`, so a major upgrade of either can break it; the test
  below is there to catch that.

## Development

Run the test from a project that has `pg` and `fastify` installed, with a Postgres available:

    GO_DEBUG_TEST_DATABASE_URL=postgres://postgres:test@127.0.0.1:55999/test \
      node --test /path/to/go-debug/test/attribution.test.cjs

To release, set the new version in `package.json`, commit, then push a matching tag. Projects
pick it up by changing the tag at the end of their install address:

    git tag v0.1.1 && git push origin main v0.1.1
