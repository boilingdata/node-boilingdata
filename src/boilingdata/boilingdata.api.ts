/* eslint-disable @typescript-eslint/no-explicit-any */
// BoilingData API documentation
// https://www.boilingdata.com/apidoc.html

export interface IBDDataQuery {
  messageType: string; // SQL_QUERY
  requestId: string;
  sql: string;
  splitAccess?: boolean;
  splitSizeMB?: number;
  jsHooks?: {
    initFunc?: string; // (sql: string, scanCursor: number) => any;       // The return value is stored in "privCtx" and passed to other hooks as param
    headerFunc?: string; // (privCtx: any, firstRow: any) => [any, any];  // The 1st return param is the "updated privCtx"
    batchFunc?: string; // (privCtx: any, rows: any[]) => [any, any[]];   // The 1st return param is the "updated privCtx"
    footerFunc?: string; // (privCtx: any, total: number) => any;         // The 1st return param is the "updated privCtx"
    finalFunc?: string; // (privCtx: any, allRows: any[]) => any[];       // Function for transforming the whole return batch
  };
  scanCursor?: number; // offset for rows to deliver
  engine?: string; // DUCKDB (default), SQLITE
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
  data: Array<any>;
  requestId: string;
  numOfRecords?: number;
  batchSerial?: number;
  totalBatches?: number;
  splitSerial: number;
  totalSplitSerials: number;
  subBatchSerial?: number;
  totalSubBatches?: number;
}

export enum EMessageTypes {
  SQL_QUERY = "SQL_QUERY",
}

export enum EEngineTypes {
  DUCKDB = "DUCKDB",
  SQLITE = "SQLITE",
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
