import { createClient, type Client } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";
import { Listo } from "../gen/listo_pb";

// gRPC-Web over HTTP/1.1 — translated by `tonic_web::GrpcWebLayer` on the
// Rust server. Connect-Web sends binary protobuf frames; messages are still
// strongly typed end-to-end via the descriptors generated from
// `proto/listo.proto`.
const baseUrl =
  (import.meta as { env?: { VITE_API_URL?: string } }).env?.VITE_API_URL ??
  "http://localhost:8080";

const transport = createGrpcWebTransport({
  baseUrl,
});

export const api: Client<typeof Listo> = createClient(Listo, transport);
