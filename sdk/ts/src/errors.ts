import type { ConnectError } from "@connectrpc/connect";
import { Code } from "@connectrpc/connect";
import type { JsonValue } from "./function-values";
import { parseJsonValue } from "./function-values";

/** Structured server error fields. */
export interface APIErrorShape {
  status: number;
  code: string;
  message: string;
  retryable?: boolean;
  action?: string | null;
}

/** An error envelope returned by the vmon API. */
export class APIError extends Error implements APIErrorShape {
  override readonly name = "APIError";
  readonly status: number;
  readonly code: string;
  readonly retryable: boolean;
  readonly action: string | null;

  /** Create a structured API error. */
  constructor(error: APIErrorShape) {
    super(error.message);
    this.status = error.status;
    this.code = error.code;
    this.retryable = error.retryable ?? false;
    this.action = error.action ?? null;
  }
}

/** A connection, DNS, or transport timeout failure eligible for failover. */
export class TransportError extends Error {
  override readonly name = "TransportError";
}

/** A malformed wire frame or response payload. */
export class ProtocolError extends Error {
  override readonly name = "ProtocolError";
}

/** Parse a strict JSON response and normalize decoder failures. */
export function parseResponseJson(text: string): JsonValue {
  try {
    return parseJsonValue(text);
  } catch (error) {
    throw new ProtocolError("vmon API returned malformed JSON", { cause: error });
  }
}

/** Decode a non-success HTTP response into an APIError. */
export async function apiError(response: Response): Promise<APIError> {
  const fallback = response.statusText || "request failed";
  let body: unknown = null;
  const contentType = response.headers.get("content-type") ?? "";
  if (contentType.includes("json")) body = await response.json().catch(() => null);
  else {
    const text = await response.text().catch(() => "");
    if (text) return new APIError({ status: response.status, code: "http_error", message: text });
  }
  const record = isRecord(body) ? body : {};
  const detail = isRecord(record.detail) ? record.detail : record;
  const code = typeof detail.code === "string" ? detail.code : "http_error";

  let retryable = false;
  if (typeof record.retryable === "boolean") {
    retryable = record.retryable;
  } else if (typeof record.retryable === "string") {
    retryable = record.retryable.toLowerCase() === "true";
  } else if (typeof detail.retryable === "boolean") {
    retryable = detail.retryable;
  } else if (typeof detail.retryable === "string") {
    retryable = detail.retryable.toLowerCase() === "true";
  } else {
    retryable = ["busy", "ha_unavailable", "unavailable_secret"].includes(code);
  }

  const action =
    typeof record.action === "string"
      ? record.action
      : typeof detail.action === "string"
        ? detail.action
        : null;

  return new APIError({
    status: response.status,
    code,
    message:
      typeof detail.message === "string"
        ? detail.message
        : typeof record.detail === "string"
          ? record.detail
          : fallback,
    retryable,
    action,
  });
}

/** vmond error codes assumed when the vmon-code trailer is absent, by gRPC status. */
const GRPC_FALLBACK_CODE: Partial<Record<Code, string>> = {
  [Code.NotFound]: "not_found",
  [Code.InvalidArgument]: "invalid",
  [Code.Unauthenticated]: "unauthorized",
  [Code.PermissionDenied]: "forbidden",
  [Code.FailedPrecondition]: "not_running",
  [Code.Aborted]: "busy",
  [Code.Unimplemented]: "unsupported",
  [Code.Unavailable]: "engine",
};
/** HTTP-equivalent statuses for vmond error codes (populates APIError.status). */
const VMON_CODE_STATUS: Record<string, number> = {
  not_found: 404,
  invalid: 400,
  unauthorized: 401,
  forbidden: 403,
  not_running: 409,
  busy: 409,
  unsupported: 501,
  engine: 503,
};
/** HTTP-equivalent statuses by gRPC status, for codes outside the vmond set. */
const GRPC_HTTP_STATUS: Partial<Record<Code, number>> = {
  [Code.NotFound]: 404,
  [Code.InvalidArgument]: 400,
  [Code.Unauthenticated]: 401,
  [Code.PermissionDenied]: 403,
  [Code.FailedPrecondition]: 409,
  [Code.Aborted]: 409,
  [Code.Unimplemented]: 501,
  [Code.Unavailable]: 503,
};

/** Decode a failed RPC into an APIError using the vmon-code trailer. */
export function rpcError(error: ConnectError): APIError {
  const code = error.metadata.get("vmon-code") ?? GRPC_FALLBACK_CODE[error.code] ?? "http_error";
  const retryableMetadata = error.metadata.get("vmon-retryable");

  let retryable = false;
  if (retryableMetadata !== undefined && retryableMetadata !== null) {
    retryable = retryableMetadata.toLowerCase() === "true";
  } else {
    retryable = ["busy", "ha_unavailable", "unavailable_secret"].includes(code);
  }

  const action = error.metadata.get("vmon-action") ?? null;

  return new APIError({
    status: VMON_CODE_STATUS[code] ?? GRPC_HTTP_STATUS[error.code] ?? 500,
    code,
    message: error.rawMessage || "request failed",
    retryable,
    action,
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
