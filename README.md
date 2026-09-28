# Logrift for JavaScript and TypeScript

Buffered client for [Logrift](https://github.com/logrift/server), the self-hosted
log collector. Requires Node.js 18 or newer (uses the built-in `fetch`).

## Install

```sh
npm install @logrift/client
```

Create a project in your Logrift server and use its **project ingest key**. Set
`LOGRIFT_URL` to the instance base URL and `LOGRIFT_API_KEY` to that key. The
client appends `/api/logs` and supports a reverse proxy path prefix.

## Usage

```ts
import { LogriftClient } from "@logrift/client";

const logger = new LogriftClient({
  url: process.env.LOGRIFT_URL!,
  apiKey: process.env.LOGRIFT_API_KEY!,
  service: "worker",
});

logger.info("Job complete", { jobId: 42, durationMs: 120 });
logger.error(new Error("Payment failed"), { orderId: "o-123" });

// On graceful process shutdown, send remaining entries before exiting.
await logger.close();
```

`log`, `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `ingest`, and
`ingestMany` only add entries to an in-memory queue. The default batch size is
100 entries and the default flush interval is one second. Hitting the batch
size starts a delivery immediately in the background. The configured timer
flushes partial batches. `flush()` waits for queued delivery; `close()` stops
the timer and flushes before resolving. Register `close()` with your framework's
graceful shutdown hook.

```ts
const logger = new LogriftClient({
  url: "http://127.0.0.1:8787",
  apiKey: process.env.LOGRIFT_API_KEY!,
  service: "api",
  batchSize: 250,
  flushIntervalMs: 1500,
  maxQueueSize: 20_000,
  requestTimeoutMs: 3000,
  onError: (error) => process.stderr.write(`${error.message}\n`),
});
```

The queue is bounded (10,000 entries by default). When full, new entries are
dropped and `onError` is called. A failed batch is put back at the head of the
queue for the next timer or explicit flush attempt. The default error handler
writes a short message to stderr. Avoid logging credentials or other sensitive
data.

`ingest`/`ingestMany` accept Logrift native entries as supplied. `log` and the
level methods create canonical entries with an ISO timestamp, level, configured
service, message, and `attrs` object. Errors are represented by their message;
pass stack data explicitly in attrs if desired.

## Development

```sh
npm install
npm run build
```

Licensed under MIT.
