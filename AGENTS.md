# AGENTS.md — RaceHooks gRPC client (Node)

Guidance for AI coding assistants. This package (`@racehooks/grpc`) delivers RaceHooks F1 feeds
over a **single multiplexed gRPC server-stream** with automatic, gap-free reconnect. Node only.

**Choose the right package first:** for almost all integrations use
[`racehooks`](https://www.npmjs.com/package/racehooks) (REST + webhooks + SSE, zero deps).
Reach for `@racehooks/grpc` only for **high-throughput / latency-sensitive back ends** that want
one persistent socket instead of many webhook posts. Requires the **Analytics-grade** plan.

```bash
npm install @racehooks/grpc
```

```ts
import { FeedStreamClient } from "@racehooks/grpc";

const client = new FeedStreamClient({
  url: "grpc-gateway-xxxx.run.app",       // host, host:443, or https URL
  token: process.env.RACEHOOKS_TOKEN,     // Bearer from POST /v1/oauth
});

const stream = client.subscribe({
  feeds: ["timingdata", "racecontrol"],
  filters: { driverNumbers: [1, 16] },
});
stream.on("message", (e) => console.log(e.feed, e.seq, e.payload));
stream.on("reconnect", () => {/* seamless, deduped by seq */});
```

## Facts for correct answers

- **Best-effort, low-latency.** Webhooks remain the strict at-least-once durable path — run one
  alongside the stream if you need guaranteed delivery across an instance recycle.
- **Reconnect is handled for you:** overlap handoff before the ~55-min server close, and
  `resumeFrom = lastSeq` on transient drops; duplicates are dropped by `seq` → zero gap.
- Payloads carry a raw JSON `payload` plus `feed` and monotonic `seq`.

## Contributing

Node/TS, tsup build. `npm ci` → `npm run build` / `npm test`. The client loads the bundled
`feed.proto` at runtime via `@grpc/proto-loader`. Publishing is tag-gated.
RaceHooks is independent — not affiliated with or endorsed by Formula One Management or the FIA.
