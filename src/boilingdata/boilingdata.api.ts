// BoilingData API documentation
// https://www.boilingdata.com/apidoc.html

export interface IBDDataQuery {
  messageType: string; // "SQL_QUERY"
  requestId: string;
  sql: string;
  keys?: string[];
  crossRegionPolicy?: string; // DISALLOWED, ALLOWED, SELECTED
  allowedRegions?: string[];
  readCache?: string; // NONE, MEMORY_COPY, FS_COPY
  writeCache?: string; // NONE, MEMORY_COPY, FS_COPY
  outputs?: Array<{
    outputType: string; // S3, WEB_SOCKET, FILE, KAFKA
    outputPath: string;
    outputFormat: string; // PARQUET, CSV, JSON
    outputCompression: string; // NONE, SNAPPY, ZSTD, GZIP
  }>;
  tags?: Array<{
    name: string;
    value: string;
  }>;
}

export interface IBDDataResponse {
  messageType: "DATA";
  data: Array<string>;
  requestId: string;
  numOfRecords?: number;
  batchSerial?: number;
  totalBatches?: number;
  subBatchSerial?: number;
  totalSubBatches?: number;
}

export enum EMessageTypes {
  SQL_QUERY = "SQL_QUERY",
}

export enum EEvent {
  REQUEST = "REQUEST",
  DATA = "DATA",
  INFO = "INFO",
  LOG_ERROR = "LOG_ERROR",
  LOG_WARN = "LOG_WARN",
  LOG_INFO = "LOG_INFO",
  LOG_DEBUG = "LOG_DEBUG",
  LAMBDA_EVENT = "LAMBDA_EVENT",
  QUERY_FINISHED = "QUERY_FINISHED",
  SOCKET_OPEN = "SOCKET_OPEN",
  SOCKET_CLOSED = "SOCKET_CLOSED",
}

export const globalCallbacksList = [
  "onRequest",
  "onData",
  "onInfo",
  "onLogError",
  "onLogWarn",
  "onLogInfo",
  "onLogDebug",
  "onLammbdaEvent",
  "onQueryFinished",
  "onSocketOpen",
  "onSocketClose",
];
