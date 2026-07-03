import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

// Runtime-loads the vendored feed.proto. Codegen (ts-proto) is the documented
// hardening step; the client logic does not depend on how the service definition
// is obtained.
//
// ESM-safe: resolves relative to this module via import.meta.url (no __dirname).
// In both the bundled output (dist/index.js) and the source tree (src/proto.ts),
// the vendored proto sits one directory up, at ../proto/.

const here = path.dirname(fileURLToPath(import.meta.url));

function resolveProtoPath(): string {
  const candidates = [
    path.join(here, "../proto/racehooks/v1/feed.proto"),
    path.join(here, "../../proto/racehooks/v1/feed.proto"),
  ];
  return candidates.find((c) => fs.existsSync(c)) ?? candidates[0];
}

const packageDefinition = protoLoader.loadSync(resolveProtoPath(), {
  keepCase: false,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
});

const loaded = grpc.loadPackageDefinition(packageDefinition) as unknown as {
  racehooks: { v1: { FeedStream: grpc.ServiceClientConstructor } };
};

/** gRPC client constructor for the FeedStream service. */
export const FeedStreamClientCtor = loaded.racehooks.v1.FeedStream;

export interface SubscribeRequest {
  feeds: string[];
  filters?: { driverNumbers: number[]; constructors: string[]; eventTypes: string[] };
  resumeFrom: number;
}

export interface FeedMessage {
  feed: string;
  sessionId: string;
  utc: string;
  jsonPayload: Buffer;
  seq: number;
}

/** Minimal typed surface of the generated gRPC client we actually use. */
export interface FeedStreamGrpcClient extends grpc.Client {
  subscribe(req: SubscribeRequest, metadata: grpc.Metadata): grpc.ClientReadableStream<FeedMessage>;
}
