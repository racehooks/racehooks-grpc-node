import * as grpc from "@grpc/grpc-js";
import { FeedStreamClient } from "../client.js";
import { FeedStreamClientCtor, type FeedMessage } from "../proto.js";

// Spins a minimal in-process gRPC server implementing the FeedStream service and
// drives the real client against it — validates proto load, subscribe, control
// frames, payload parsing, and dedupe.

let server: grpc.Server;
let port: number;

function buildServer(onSubscribe: (call: grpc.ServerWritableStream<unknown, FeedMessage>) => void) {
  const s = new grpc.Server();
  s.addService((FeedStreamClientCtor as grpc.ServiceClientConstructor).service, {
    subscribe: onSubscribe,
    pushFeed: (_call: unknown, cb: (e: null, r: { ok: boolean }) => void) => cb(null, { ok: true }),
  } as grpc.UntypedServiceImplementation);
  return s;
}

function startServer(onSubscribe: (call: grpc.ServerWritableStream<unknown, FeedMessage>) => void): Promise<void> {
  return new Promise((resolve, reject) => {
    server = buildServer(onSubscribe);
    server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, p) => {
      if (err) return reject(err);
      port = p;
      resolve();
    });
  });
}

afterEach((done) => {
  if (server) server.tryShutdown(() => done());
  else done();
});

describe("FeedStreamClient", () => {
  it("emits open then parsed messages from the stream", async () => {
    await startServer((call) => {
      call.write({ feed: "_connected", sessionId: "", utc: "", jsonPayload: Buffer.alloc(0), seq: 0 });
      call.write({
        feed: "timingdata",
        sessionId: "s1",
        utc: "t",
        jsonPayload: Buffer.from(JSON.stringify({ drivers: [{ number: "1" }] }), "utf8"),
        seq: 1,
      });
    });

    const client = new FeedStreamClient({ url: `127.0.0.1:${port}`, tls: false, token: "tok" });
    const events: string[] = [];

    await new Promise<void>((resolve) => {
      const stream = client.subscribe({ feeds: ["timingdata"] });
      stream.on("open", () => events.push("open"));
      stream.on("message", (e) => {
        expect(e.feed).toBe("timingdata");
        expect(e.seq).toBe(1);
        expect(e.payload).toEqual({ drivers: [{ number: "1" }] });
        events.push("message");
        stream.close();
        resolve();
      });
    });

    expect(events).toEqual(["open", "message"]);
    client.close();
  });

  it("forwards the Bearer token and requested feeds to the server", async () => {
    let seenAuth: string | undefined;
    let seenFeeds: string[] | undefined;
    let seenResumeFrom: number | undefined;
    await startServer((call) => {
      seenAuth = call.metadata.get("authorization")[0] as string;
      const req = call.request as { feeds: string[]; resumeFrom: number };
      seenFeeds = req.feeds;
      seenResumeFrom = Number(req.resumeFrom);
      call.write({ feed: "_connected", sessionId: "", utc: "", jsonPayload: Buffer.alloc(0), seq: 0 });
    });

    const client = new FeedStreamClient({ url: `http://127.0.0.1:${port}`, tls: false, token: "tok-123" });

    await new Promise<void>((resolve) => {
      const stream = client.subscribe({ feeds: ["timingdata", "racecontrol"] });
      stream.on("open", () => {
        stream.close();
        resolve();
      });
    });

    expect(seenAuth).toBe("Bearer tok-123");
    expect(seenFeeds).toEqual(["timingdata", "racecontrol"]);
    expect(seenResumeFrom).toBe(0);
    client.close();
  });

  it("dedupes messages with seq <= the last seen seq", async () => {
    await startServer((call) => {
      call.write({ feed: "_connected", sessionId: "", utc: "", jsonPayload: Buffer.alloc(0), seq: 0 });
      const body = Buffer.from(JSON.stringify({ x: 1 }), "utf8");
      call.write({ feed: "timingdata", sessionId: "s", utc: "t", jsonPayload: body, seq: 5 });
      call.write({ feed: "timingdata", sessionId: "s", utc: "t", jsonPayload: body, seq: 5 }); // dup
      call.write({ feed: "timingdata", sessionId: "s", utc: "t", jsonPayload: body, seq: 6 });
    });

    const client = new FeedStreamClient({ url: `127.0.0.1:${port}`, tls: false });
    const seqs: number[] = [];

    await new Promise<void>((resolve) => {
      const stream = client.subscribe({ feeds: ["*"] });
      stream.on("message", (e) => {
        seqs.push(e.seq);
        if (e.seq === 6) {
          stream.close();
          resolve();
        }
      });
    });

    expect(seqs).toEqual([5, 6]); // the duplicate seq 5 was dropped
    client.close();
  });

  it("reconnects and resumes from the last seq after the server ends the stream", async () => {
    const body = Buffer.from(JSON.stringify({ x: 1 }), "utf8");
    let calls = 0;
    let secondResume: number | undefined;
    await startServer((call) => {
      calls += 1;
      if (calls === 1) {
        call.write({ feed: "_connected", sessionId: "", utc: "", jsonPayload: Buffer.alloc(0), seq: 0 });
        call.write({ feed: "timingdata", sessionId: "s", utc: "t", jsonPayload: body, seq: 1 });
        call.end(); // clean end → client reconnects
      } else {
        secondResume = Number((call.request as { resumeFrom: number }).resumeFrom);
        call.write({ feed: "timingdata", sessionId: "s", utc: "t", jsonPayload: body, seq: 2 });
      }
    });

    const client = new FeedStreamClient({ url: `127.0.0.1:${port}`, tls: false });
    const seqs: number[] = [];

    await new Promise<void>((resolve) => {
      const stream = client.subscribe({ feeds: ["*"], maxBackoffMs: 50 });
      stream.on("message", (e) => {
        seqs.push(e.seq);
        if (e.seq === 2) {
          stream.close();
          resolve();
        }
      });
    });

    expect(seqs).toEqual([1, 2]);
    expect(secondResume).toBe(1); // resumed from the last seen seq
    client.close();
  });

  it("resubscribes on a server _reconnect frame (overlap handoff)", async () => {
    const body = Buffer.from(JSON.stringify({ x: 1 }), "utf8");
    let calls = 0;
    await startServer((call) => {
      calls += 1;
      if (calls === 1) {
        call.write({ feed: "_connected", sessionId: "", utc: "", jsonPayload: Buffer.alloc(0), seq: 0 });
        call.write({ feed: "_reconnect", sessionId: "", utc: "", jsonPayload: Buffer.alloc(0), seq: 0 });
        // keep open — the client cancels this once the new stream yields
      } else {
        call.write({ feed: "timingdata", sessionId: "s", utc: "t", jsonPayload: body, seq: 9 });
      }
    });

    const client = new FeedStreamClient({ url: `127.0.0.1:${port}`, tls: false });
    let reconnected = false;

    await new Promise<void>((resolve) => {
      const stream = client.subscribe({ feeds: ["*"] });
      stream.on("reconnect", () => {
        reconnected = true;
      });
      stream.on("message", (e) => {
        expect(e.seq).toBe(9);
        stream.close();
        resolve();
      });
    });

    expect(reconnected).toBe(true);
    expect(calls).toBeGreaterThanOrEqual(2);
    client.close();
  });

  it("emits close (not an error) when reconnect is disabled and the server ends", async () => {
    const body = Buffer.from(JSON.stringify({ x: 1 }), "utf8");
    await startServer((call) => {
      call.write({ feed: "timingdata", sessionId: "s", utc: "t", jsonPayload: body, seq: 1 });
      call.end(); // clean end
    });

    const client = new FeedStreamClient({ url: `127.0.0.1:${port}`, tls: false });
    const seqs: number[] = [];
    let errored = false;

    await new Promise<void>((resolve) => {
      const stream = client.subscribe({ feeds: ["*"], reconnect: false });
      stream.on("message", (e) => seqs.push(e.seq));
      stream.on("error", () => {
        errored = true;
      });
      stream.on("close", () => resolve());
    });

    expect(seqs).toEqual([1]);
    expect(errored).toBe(false); // a clean server end is not an error
    client.close();
  });
});
