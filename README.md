# @racehooks/grpc

Low-latency RaceHooks F1 feed delivery over a **gRPC server-stream**, with automatic
reconnect and gap resume. Node.js only.

Most integrations should use [`racehooks`](https://www.npmjs.com/package/racehooks)
(REST + webhooks + SSE, zero dependencies). Reach for this package when you want a single
multiplexed gRPC socket — typically high-throughput / latency-sensitive back ends.
Requires the **Analytics** tier.

```bash
npm install @racehooks/grpc
```

```ts
import { FeedStreamClient } from "@racehooks/grpc";

const client = new FeedStreamClient({
  url: "grpc-gateway-xxxx.run.app",      // host, host:443, or https URL
  token: process.env.RACEHOOKS_TOKEN,    // Bearer token from POST /v1/oauth
});

const stream = client.subscribe({
  feeds: ["timingdata", "racecontrol"],
  filters: { driverNumbers: [1, 16] },
});

stream.on("open", () => console.log("subscribed"));
stream.on("message", (e) => console.log(e.feed, e.seq, e.payload));
stream.on("reconnect", () => console.log("seamless reconnect"));
stream.on("error", (err) => console.error(err));

// later
stream.close();
client.close();
```

## Delivery semantics

The stream is **best-effort, low-latency**; webhooks remain the strict at-least-once
durable path. Reconnects are handled for you:

- The server closes long streams with a `_reconnect` hint (~55 min). The client opens a
  new stream **before** closing the old (overlap handoff) and dedupes by `seq` → zero gap.
- Transient drops reconnect with exponential backoff and `resumeFrom = lastSeq`; the
  gateway replays the buffered gap.

Run a webhook alongside the stream if you need guaranteed delivery across an instance
recycle.

## Notes

The client loads the bundled `feed.proto` at runtime via `@grpc/proto-loader`. Generating
typed stubs with `ts-proto` (no runtime parse, full types) is the planned hardening step;
the client API is unaffected by that change.
