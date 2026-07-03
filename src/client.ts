import { EventEmitter } from "events";
import * as grpc from "@grpc/grpc-js";
import {
  FeedStreamClientCtor,
  type FeedStreamGrpcClient,
  type FeedMessage,
  type SubscribeRequest,
} from "./proto.js";

export interface FeedStreamClientOptions {
  /** "host:443", "host:port", or a full https URL. */
  url: string;
  /** Bearer token sent as gRPC `authorization` metadata. */
  token?: string;
  /** TLS to the gateway. Defaults to true (Cloud Run). Set false for local h2c. */
  tls?: boolean;
}

export interface SubscribeOptions {
  /** Feed ids, or ["*"] for every feed your tier allows. */
  feeds: string[];
  filters?: { driverNumbers?: number[]; constructors?: string[]; eventTypes?: string[] };
  /** Auto-reconnect on drop / server reconnect hint. Default true. */
  reconnect?: boolean;
  /** Max backoff between reconnect attempts (ms). Default 30000. */
  maxBackoffMs?: number;
}

export interface FeedEvent {
  feed: string;
  sessionId: string;
  utc: string;
  seq: number;
  /** Parsed JSON body (same shape as the webhook/SSE payload). */
  payload: unknown;
}

const CONTROL_CONNECTED = "_connected";
const CONTROL_RECONNECT = "_reconnect";

function targetFromUrl(url: string): { target: string; tls: boolean } {
  if (url.startsWith("http://") || url.startsWith("https://")) {
    const u = new URL(url);
    const tls = u.protocol === "https:";
    return { target: `${u.hostname}:${u.port || (tls ? "443" : "80")}`, tls };
  }
  return { target: url, tls: true };
}

/**
 * A single logical subscription. Emits:
 *   "message" (FeedEvent) · "open" · "reconnect" · "error" (Error) · "close"
 *
 * Reconnect contract (mirrors the server):
 *   - On the server's `_reconnect` control frame (~55-min Cloud Run cap), opens a
 *     NEW stream before cancelling the old one (overlap handoff) → zero gap.
 *   - On a transient error/end, reconnects with exponential backoff.
 *   - Both paths resume via `resumeFrom = lastSeq`; duplicates (a message both
 *     replayed and delivered live) are dropped by seq. Webhooks remain the strict
 *     durable path.
 */
export class FeedStream extends EventEmitter {
  private current?: grpc.ClientReadableStream<FeedMessage>;
  private maxSeq = 0;
  private closed = false;
  private readonly reconnect: boolean;
  private readonly maxBackoffMs: number;

  constructor(
    private readonly client: FeedStreamGrpcClient,
    private readonly base: Omit<SubscribeRequest, "resumeFrom">,
    private readonly token: string | undefined,
    opts: Pick<SubscribeOptions, "reconnect" | "maxBackoffMs">
  ) {
    super();
    this.reconnect = opts.reconnect !== false;
    this.maxBackoffMs = opts.maxBackoffMs ?? 30_000;
    this.connect(0);
  }

  /** Stop the subscription. Idempotent. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.cancel(this.current);
    this.emit("close");
  }

  private metadata(): grpc.Metadata {
    const md = new grpc.Metadata();
    if (this.token) md.set("authorization", `Bearer ${this.token}`);
    return md;
  }

  private cancel(call?: grpc.ClientReadableStream<FeedMessage>): void {
    try {
      call?.cancel();
    } catch {
      /* already torn down */
    }
  }

  private connect(attempt: number): void {
    if (this.closed) return;
    const req: SubscribeRequest = { ...this.base, resumeFrom: this.maxSeq };
    const call = this.client.subscribe(req, this.metadata());
    const previous = this.current;
    this.current = call;

    call.on("data", (msg: FeedMessage) => {
      if (call !== this.current) return; // event from a superseded stream
      // Overlap handoff: once the new stream yields, drop the old one (zero gap).
      if (previous) this.cancel(previous);
      this.onMessage(msg, attempt);
    });
    call.on("error", (err: grpc.ServiceError) => {
      if (call !== this.current) return;
      if (err.code === grpc.status.CANCELLED) return; // our own cancel
      this.scheduleReconnect(attempt, err);
    });
    call.on("end", () => {
      if (call !== this.current) return;
      this.scheduleReconnect(attempt);
    });
  }

  private onMessage(msg: FeedMessage, attempt: number): void {
    if (msg.feed === CONTROL_CONNECTED) {
      this.emit("open");
      return;
    }
    if (msg.feed === CONTROL_RECONNECT) {
      this.emit("reconnect");
      this.connect(attempt); // open new before old is cancelled (overlap)
      return;
    }
    const seq = Number(msg.seq) || 0;
    if (seq > 0 && seq <= this.maxSeq) return; // duplicate (resume overlap)
    if (seq > this.maxSeq) this.maxSeq = seq;

    let payload: unknown;
    try {
      const buf = msg.jsonPayload;
      if (buf && buf.length > 0) payload = JSON.parse(Buffer.from(buf).toString("utf8"));
    } catch {
      payload = undefined;
    }
    this.emit("message", { feed: msg.feed, sessionId: msg.sessionId, utc: msg.utc, seq, payload } as FeedEvent);
  }

  private scheduleReconnect(attempt: number, err?: grpc.ServiceError): void {
    if (this.closed) return;
    if (!this.reconnect) {
      if (err) this.emit("error", err);
      this.close();
      return;
    }
    const base = Math.min(1_000 * 2 ** attempt, this.maxBackoffMs);
    const delay = Math.round(base * (0.5 + Math.random() * 0.5)); // ±50% jitter
    const timer = setTimeout(() => this.connect(attempt + 1), delay);
    if (typeof timer.unref === "function") timer.unref();
  }
}

/** Connection to a RaceHooks grpc-gateway. Create one per process; reuse it. */
export class FeedStreamClient {
  private readonly client: FeedStreamGrpcClient;
  private readonly token?: string;

  constructor(opts: FeedStreamClientOptions) {
    const { target, tls } = targetFromUrl(opts.url);
    const useTls = opts.tls ?? tls;
    const creds = useTls ? grpc.credentials.createSsl() : grpc.credentials.createInsecure();
    this.client = new (FeedStreamClientCtor as grpc.ServiceClientConstructor)(
      target,
      creds
    ) as unknown as FeedStreamGrpcClient;
    this.token = opts.token;
  }

  /** Open a feed subscription. Returns an event emitter; call `.close()` to stop. */
  subscribe(opts: SubscribeOptions): FeedStream {
    const base: Omit<SubscribeRequest, "resumeFrom"> = {
      feeds: opts.feeds,
      filters: {
        driverNumbers: opts.filters?.driverNumbers ?? [],
        constructors: opts.filters?.constructors ?? [],
        eventTypes: opts.filters?.eventTypes ?? [],
      },
    };
    return new FeedStream(this.client, base, this.token, opts);
  }

  /** Close the underlying gRPC channel. */
  close(): void {
    this.client.close();
  }
}
