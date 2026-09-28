export type LogLevel = "trace" | "debug" | "info" | "warn" | "error" | "fatal";
export type LogEntry = {
  time?: string | number | Date;
  level?: string | number;
  service?: string;
  message?: unknown;
  msg?: unknown;
  attrs?: Record<string, unknown>;
  [key: string]: unknown;
};

export type ClientOptions = {
  url: string;
  apiKey: string;
  service?: string;
  batchSize?: number;
  flushIntervalMs?: number;
  maxQueueSize?: number;
  requestTimeoutMs?: number;
  onError?: (error: Error) => void;
};

export class LogriftError extends Error {}
export class TransportError extends LogriftError {}
export class HTTPError extends LogriftError {
  constructor(public readonly status: number) {
    super(`Logrift rejected logs (HTTP ${status})`);
    this.name = "HTTPError";
  }
}

/**
 * Asynchronous, in-memory buffered client. log() never performs network I/O;
 * entries are sent in batches by the timer or when batchSize is reached.
 */
export class LogriftClient {
  readonly service?: string;
  private readonly endpoint: string;
  private readonly apiKey: string;
  private readonly batchSize: number;
  private readonly flushIntervalMs: number;
  private readonly maxQueueSize: number;
  private readonly requestTimeoutMs: number;
  private readonly onError: (error: Error) => void;
  private queue: LogEntry[] = [];
  private timer?: ReturnType<typeof setInterval>;
  private draining?: Promise<void>;
  private closed = false;

  constructor(options: ClientOptions) {
    let base: URL;
    try { base = new URL(options.url); } catch { throw new TypeError("url must be a valid HTTP(S) base URL"); }
    if (!["http:", "https:"].includes(base.protocol) || base.username || base.password || base.search || base.hash) {
      throw new TypeError("url must be an HTTP(S) base URL without credentials, query or fragment");
    }
    if (!options.apiKey.trim() || /[\r\n]/.test(options.apiKey)) throw new TypeError("apiKey must be present and contain no newlines");
    this.endpoint = `${base.toString().replace(/\/+$/, "")}/api/logs`;
    this.apiKey = options.apiKey;
    this.service = options.service;
    this.batchSize = positiveInt(options.batchSize ?? 100, "batchSize");
    this.flushIntervalMs = positiveInt(options.flushIntervalMs ?? 1000, "flushIntervalMs");
    this.maxQueueSize = positiveInt(options.maxQueueSize ?? 10_000, "maxQueueSize");
    this.requestTimeoutMs = positiveInt(options.requestTimeoutMs ?? 3000, "requestTimeoutMs");
    this.onError = options.onError ?? ((error) => { console.error(`logrift: ${error.message}`); });
    this.timer = setInterval(() => { void this.flush().catch((error: unknown) => this.report(asError(error))); }, this.flushIntervalMs);
    // Do not keep a Node process alive just for the periodic flush.
    (this.timer as unknown as { unref?: () => void }).unref?.();
  }

  /** Queue a canonical/native entry. Returns false if closed or queue is full. */
  ingest(entry: LogEntry): boolean { return this.enqueue(entry); }

  /** Queue multiple native entries. Returns the number accepted. */
  ingestMany(entries: LogEntry[]): number {
    let accepted = 0;
    for (const entry of entries) if (this.enqueue(entry)) accepted++;
    return accepted;
  }

  /** Queue a structured log entry without waiting for network delivery. */
  log(message: unknown, level: LogLevel | string = "info", attrs: Record<string, unknown> = {}): boolean {
    return this.enqueue({ time: new Date().toISOString(), level: level.toLowerCase(), service: this.service,
      message: stringifyMessage(message), attrs });
  }

  trace(message: unknown, attrs?: Record<string, unknown>): boolean { return this.log(message, "trace", attrs); }
  debug(message: unknown, attrs?: Record<string, unknown>): boolean { return this.log(message, "debug", attrs); }
  info(message: unknown, attrs?: Record<string, unknown>): boolean { return this.log(message, "info", attrs); }
  warn(message: unknown, attrs?: Record<string, unknown>): boolean { return this.log(message, "warn", attrs); }
  error(message: unknown, attrs?: Record<string, unknown>): boolean { return this.log(message, "error", attrs); }
  fatal(message: unknown, attrs?: Record<string, unknown>): boolean { return this.log(message, "fatal", attrs); }

  /** Send queued entries now. Resolves after the current queue has been delivered. */
  async flush(): Promise<void> {
    if (this.draining) { await this.draining; if (this.queue.length) return this.flush(); return; }
    if (!this.queue.length) return;
    this.draining = (async () => {
      while (this.queue.length) {
        const batch = this.queue.splice(0, this.batchSize);
        try { await this.deliver(batch); }
        catch (error) {
          // Restore failed batch at the front; cap to the configured memory bound.
          this.queue = [...batch, ...this.queue].slice(0, this.maxQueueSize);
          throw error;
        }
      }
    })().finally(() => { this.draining = undefined; });
    return this.draining;
  }

  /** Stop the timer and make a final delivery attempt. */
  async close(): Promise<void> {
    if (!this.closed) { this.closed = true; if (this.timer) clearInterval(this.timer); }
    await this.flush();
  }

  get pendingCount(): number { return this.queue.length; }

  private enqueue(entry: LogEntry): boolean {
    if (this.closed) return false;
    if (this.queue.length >= this.maxQueueSize) {
      this.report(new LogriftError("Logrift queue is full; entry dropped"));
      return false;
    }
    this.queue.push(entry);
    if (this.queue.length >= this.batchSize) void this.flush().catch((error: unknown) => this.report(asError(error)));
    return true;
  }

  private async deliver(batch: LogEntry[]): Promise<void> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.requestTimeoutMs);
    try {
      const response = await fetch(this.endpoint, {
        method: "POST", headers: { Authorization: `Bearer ${this.apiKey}`, "Content-Type": "application/json", "User-Agent": "logrift-typescript/0.1.0" },
        body: JSON.stringify(batch), signal: controller.signal,
      });
      if (!response.ok) throw new HTTPError(response.status);
    } catch (error) {
      if (error instanceof LogriftError) throw error;
      throw new TransportError(`Logrift delivery failed (${error instanceof Error ? error.name : "unknown error"})`);
    } finally { clearTimeout(timeout); }
  }

  private report(error: Error): void { try { this.onError(error); } catch { /* error callbacks must not break logging */ } }
}

function positiveInt(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new TypeError(`${name} must be a positive integer`);
  return value;
}
function stringifyMessage(value: unknown): string {
  if (value instanceof Error) return value.message;
  if (typeof value === "string") return value;
  try { return JSON.stringify(value) ?? String(value); } catch { return String(value); }
}
function asError(error: unknown): Error { return error instanceof Error ? error : new Error(String(error)); }
