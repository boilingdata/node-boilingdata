import { BDCredentials, getBoilingDataCredentials } from "../common/identity";
import { EEngineTypes, EEvent, EMessageTypes, IBDDataQuery, IBDDataResponse } from "./boilingdata.api";
import { v4 as uuidv4 } from "uuid";
import WebSocket from "isomorphic-ws";
import { MessageEvent } from "isomorphic-ws";
import jsonBigInt from "json-bigint";

export interface IBDCallbacks {
  onData?: (data: unknown) => void;
  onInfo?: (data: unknown) => void;
  onRequest?: (data: unknown) => void;
  onQueryFinished?: (data: unknown) => void;
  onLogError?: (data: unknown) => void;
  onLogWarn?: (data: unknown) => void;
  onLogInfo?: (data: unknown) => void;
  onLogDebug?: (data: unknown) => void;
  onLambdaEvent?: (data: unknown) => void;
  onSocketOpen?: () => void;
  onSocketClose?: () => void;
  unknown?: (data: unknown) => void;
}

export type BDAWSRegion =
  | "eu-west-1"
  | "eu-west-2"
  | "eu-west-3"
  | "eu-north-1"
  | "eu-south-1"
  | "eu-central-1"
  | "us-east-1"
  | "us-east-2"
  | "us-west-1"
  | "us-west-2"
  | "ca-central-1";

export interface IBoilingData {
  username?: string;
  password?: string;
  mfa?: number;
  authcontext?: { idToken?: any; accessToken?: any; refreshToken?: any };
  logLevel?: "trace" | "debug" | "info" | "warn" | "error" | "fatal"; // Match with Bunyan
  globalCallbacks?: IBDCallbacks;
  region?: BDAWSRegion;
  endpointUrl?: string;
}

export interface IJsHooks {
  initFunc?: (sql: string, scanCursor: number) => any; // The return value is stored in "privCtx" and passed to other hooks as param
  headerFunc?: (privCtx: any, firstRow: any) => [any, any]; // The 1st return param is the "updated privCtx"
  batchFunc?: (privCtx: any, rows: any[]) => [any, any[]]; // The 1st return param is the "updated privCtx"
  footerFunc?: (privCtx: any, total: number) => any; // The 1st return param is the "updated privCtx"
  finalFunc?: (privCtx: any, allRows: any[]) => any[]; // Function for transforming the whole return batch
}

export interface IBDQuery {
  sql: string;
  jsHooks?: IJsHooks;
  scanCursor?: number; // row number to start deliverying from
  engine?: EEngineTypes.DUCKDB | EEngineTypes.SQLITE;
  splitAccess?: boolean;
  splitSizeMB?: number;
  requestId?: string;
  callbacks?: IBDCallbacks;
  bdStsToken?: string; // access token
  shareInfo?: any; // share information
}

export interface ISocketInstance {
  lastActivity: number;
  queries: Map<
    string,
    {
      receivedBatches: Set<number>;
      receivedSplitBatches: Map<number, Set<number>>;
      receivedSubBatches: Map<number, Set<number>>;
    }
  >;
  send: (payload: IBDDataQuery) => Promise<void>;
  query: (params: IBDQuery) => Promise<void>;
  bumpActivity: () => void;
  socket?: WebSocket;
  queryCallbacks: Map<string, IBDCallbacks>;
}

interface IEvent {
  requestId?: string;
  eventType: EEvent | string;
  payload: unknown;
}

enum ECallbackNames {
  REQUEST = "onRequest",
  ERROR = "onLogError",
  LOG_INFO = "onLogInfo",
  LOG_ERROR = "onLogError",
  LOG_WARN = "onLogWarn",
  LOG_DEBUG = "onLogDebug",
  DATA = "onData",
  INFO = "onInfo",
  LAMBDA_EVENT = "onLambdaEvent",
  QUERY_FINISHED = "onQueryFinished",
  UNKNOWN = "unknown",
}

const createLogger = (props: any): Console => {
  const logLevel = props.logLevel ?? "info";

  // logLevel == "error"
  // ==> log.info("") --> NA
  return {
    ...console,
    debug: (a, ...rest) => (["debug"].includes(logLevel) ? console.log(a, ...rest) : undefined),
    log: (a, ...rest) => (["debug", "info"].includes(logLevel) ? console.log(a, ...rest) : undefined),
    info: (a, ...rest) => (["debug", "info"].includes(logLevel) ? console.log(a, ...rest) : undefined),
    warn: (a, ...rest) => (["debug", "info", "warn"].includes(logLevel) ? console.log(a, ...rest) : undefined),
    error: (a, ...rest) => console.log(a, ...rest),
  };
};

function mapEventToCallbackName(event: IEvent): ECallbackNames {
  const entry = Object.entries(ECallbackNames).find(([key, _value]) => key === event.eventType);
  if (!entry) return ECallbackNames.UNKNOWN; // throw new Error(`Mapping event type "${JSON.stringify(event)}" to callback name failed!`);
  return entry[1];
}

export function isDataResponse(data: IBDDataResponse | unknown): data is IBDDataResponse {
  return (data as IBDDataResponse).messageType !== undefined && (data as IBDDataResponse).messageType === "DATA";
}

export class BoilingData {
  private region: BDAWSRegion;
  private creds?: BDCredentials;
  private socketInstance: ISocketInstance;
  private logger: Console;
  private closedPromise?: Promise<void>;

  constructor(public props: IBoilingData) {
    this.logger = createLogger({ name: "boilingdata", logLevel: this.props.logLevel ?? "info" });
    this.region = this.props.region ? this.props.region : "eu-west-1";
    this.socketInstance = {
      queries: new Map(), // no queries yet
      queryCallbacks: new Map(), // no queries yet, so no query specific callbacks either
      lastActivity: Date.now(),
      send: async (payload: IBDDataQuery) => {
        this.logger.debug("PAYLOAD(send):\n", JSON.stringify(payload));
        try {
          await new Promise<void>((resolve, reject) => {
            if (!this.socketInstance.socket) {
              return reject({ message: "No socket instance, need to connect." });
            }
            if (this.socketInstance.socket.readyState != WebSocket.OPEN) {
              return reject({
                message: `Socket is not OPEN(1) (${this.socketInstance.socket.readyState}), need to re-connect`,
              });
            }
            this.socketInstance.socket.send(JSON.stringify(payload), err => {
              if (err) reject(err);
              resolve();
            });
          });
          this.execEventCallback({ eventType: EEvent.REQUEST, requestId: payload.requestId, payload });
        } catch (error) {
          console.error(error);
          this.execEventCallback({ eventType: EEvent.LOG_ERROR, requestId: payload.requestId, payload: { error } });
          return;
        }
      },
      bumpActivity: () => {
        this.socketInstance.lastActivity = Date.now();
      },
      query: (params: IBDQuery) => this.execQuery(params),
    };
  }

  public async close(): Promise<void> {
    this.socketInstance.socket?.close(1000);
    if (this.closedPromise) await this.closedPromise;
  }

  public getCachedAuthContext(): { idToken: any } | undefined {
    return this.creds?.idToken;
  }

  public async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const sock = this.socketInstance;
      const cbs = this.props.globalCallbacks;
      this.closedPromise = new Promise<void>(closeResolve => {
        getBoilingDataCredentials(
          this.props.username,
          this.props.password,
          this.region,
          this.props.endpointUrl,
          this.props.mfa,
          this.props.authcontext,
          this.logger,
        )
          .then(creds => {
            this.creds = creds;
            sock.socket = new WebSocket(this.creds.signedWebsocketUrl);
            sock.socket!.onclose = () => {
              if (cbs?.onSocketClose) cbs.onSocketClose();
              closeResolve();
            };
            sock.socket!.onopen = () => {
              if (cbs?.onSocketOpen) cbs.onSocketOpen();
              resolve();
            };
            sock.socket!.onerror = (err: any) => {
              this.logger.error(err);
              reject(err);
            };
            sock.socket!.onmessage = (msg: MessageEvent) => {
              return this.handleSocketMessage(msg);
            };
          })
          .catch(err => {
            console.error(err);
            reject(err);
          });
      });
    });
  }

  // FIXME: the ordering of e.g. subBatches is not guaranteed
  public execQueryPromise(params: IBDQuery): Promise<any[]> {
    this.logger.info("execQueryPromise:", params);
    const r: any[] = [];
    return new Promise((resolve, reject) => {
      this.execQuery({
        ...params,
        sql: params.sql,
        scanCursor: params.scanCursor ?? 0,
        engine: params.engine ?? EEngineTypes.DUCKDB,
        callbacks: {
          onData: (data: IBDDataResponse | unknown) => {
            if (isDataResponse(data))
              data.data.map((row, rowNum) => r.push([data.batchSerial, data.subBatchSerial, rowNum, row]));
          },
          onQueryFinished: () =>
            resolve(
              r
                .sort((a, b) => {
                  return a[0] == b[0] ? (a[1] == b[1] ? a[2] - b[2] : a[1] - b[1]) : a[0] - b[0];
                })
                .map(r => r[3]),
            ),
          onLogError: (data: any) => reject(data),
        },
      });
    });
  }

  private validateJsHooks(params: IBDQuery): void {
    if (!params.jsHooks) return;
    const initFunc =
      params.jsHooks?.initFunc !== undefined
        ? new Function('"use strict"; return ' + params.jsHooks.initFunc.toString())()
        : undefined;
    const headerFunc =
      params.jsHooks?.headerFunc !== undefined
        ? new Function('"use strict"; return ' + params.jsHooks.headerFunc.toString())()
        : undefined;
    const batchFunc =
      params.jsHooks?.batchFunc !== undefined
        ? new Function('"use strict"; return ' + params.jsHooks.batchFunc.toString())()
        : undefined;
    const footerFunc =
      params.jsHooks?.footerFunc !== undefined
        ? new Function('"use strict"; return ' + params.jsHooks.footerFunc.toString())()
        : undefined;
    const finalFunc =
      params.jsHooks?.finalFunc !== undefined
        ? new Function('"use strict"; return ' + params.jsHooks.finalFunc.toString())()
        : undefined;
    try {
      /*
      initFunc?: (sql: string, webSocketUrl: string, scanCursor: number) => any; // The return value is stored in "privCtx" and passed to other hooks as param
      headerFunc?: (privCtx: any, firstRow: any) => [any, any]; // The 1st return param is the "updated privCtx"
      batchFunc?: (privCtx: any, rows: any[]) => [any, any[]]; // The 1st return param is the "updated privCtx"
      footerFunc?: (privCtx: any, total: number) => any; // The 1st return param is the "updated privCtx"
      finalFunc?: (privCtx: any, allRows: any[]) => any; // Function for transforming the whole return batch
      */
      const initFuncResp = initFunc ? initFunc("SELECT 42;", "wss://dummy", 0) : {};
      if (initFuncResp === undefined) throw new Error("initFunc() did not return valid response");
      const headerFuncResp = headerFunc ? headerFunc({}, { test: 1, foo: "bar" }) : [1, 2];
      if (headerFuncResp.length != 2) throw new Error("headerFunc() did not return valid response");
      const batchFuncResp = batchFunc ? batchFunc({}, [{ test: 2, foo: "bar2" }]) : [1, 2];
      if (batchFuncResp.length != 2) throw new Error("batchFunc() did not return valid response");
      const footerFuncResp = footerFunc ? footerFunc({}, 42) : "";
      if (footerFuncResp === undefined) throw new Error("footerFunc() did not return valid response");
      const finalFuncResp = finalFunc ? finalFunc({}, [42, 3, 2, "testing"]) : [];
      if (!Array.isArray(finalFuncResp)) throw new Error("finalFunc() did not return valid response");
    } catch (err) {
      console.error(err);
      throw err;
    }
  }

  private getSocketReadyStateString(
    readyState:
      | typeof WebSocket.CONNECTING
      | typeof WebSocket.OPEN
      | typeof WebSocket.CLOSING
      | typeof WebSocket.CLOSED
      | undefined,
  ): string {
    switch (readyState) {
      case WebSocket.CONNECTING:
        return "CONNECTING";
      case WebSocket.OPEN:
        return "OPEN";
      case WebSocket.CLOSED:
        return "CLOSED";
      case WebSocket.CLOSING:
        return "CLOSING";
      default:
        return "UNKNOWN";
    }
  }

  public async execQuery(params: IBDQuery): Promise<void> {
    this.validateJsHooks(params);
    this.logger.debug("execQuery:", params);
    this.socketInstance.bumpActivity();
    const requestId = params.requestId ?? uuidv4();
    const payload: IBDDataQuery = {
      ...params,
      splitAccess: params.splitAccess !== undefined ? params.splitAccess == true : false, // TODO: set true by default
      splitSizeMB: params.splitSizeMB !== undefined ? params.splitSizeMB : 500,
      messageType: EMessageTypes.SQL_QUERY,
      sql: params.sql,
      jsHooks: {
        initFunc: params.jsHooks?.initFunc?.toString(),
        headerFunc: params.jsHooks?.headerFunc?.toString(),
        batchFunc: params.jsHooks?.batchFunc?.toString(),
        footerFunc: params.jsHooks?.footerFunc?.toString(),
      },
      scanCursor: params.scanCursor ?? 0,
      engine: params.engine ?? EEngineTypes.DUCKDB,
      requestId,
    };
    this.socketInstance.queries.set(requestId, {
      receivedBatches: new Set(),
      receivedSplitBatches: new Map(),
      receivedSubBatches: new Map(),
    });
    this.socketInstance.queryCallbacks.set(requestId, {
      onData: params.callbacks?.onData,
      onInfo: params.callbacks?.onInfo,
      onRequest: params.callbacks?.onRequest,
      onLogError: params.callbacks?.onLogError,
      onLogWarn: params.callbacks?.onLogWarn,
      onLogInfo: params.callbacks?.onLogInfo,
      onLogDebug: params.callbacks?.onLogDebug,
      onLambdaEvent: params.callbacks?.onLambdaEvent,
      onQueryFinished: params.callbacks?.onQueryFinished,
    });
    this.logger.debug("PAYLOAD:\n", payload);
    this.logger.debug("WebSocket.readyState:", this.getSocketReadyStateString(this.socketInstance.socket?.readyState));
    if (this.socketInstance.socket?.readyState != WebSocket.OPEN) await this.connect();
    await this.socketInstance.send(payload);
  }

  private processBatchInfo(message: unknown): void {
    if (!isDataResponse(message)) return;
    // Keeps track of the recieved batches, executes event when all batches have been recieved.
    if (!message.requestId || !message.batchSerial || !message.totalBatches || message.batchSerial <= 0) return;
    const queryInfo = this.socketInstance.queries.get(message?.requestId);
    if (!queryInfo) return;
    queryInfo.receivedBatches.add(message.batchSerial);
    // split bath check (optional, parent is batchSerial)
    if (message.splitSerial && message.totalSplitSerials) {
      if (!queryInfo.receivedSplitBatches.has(message.batchSerial)) {
        queryInfo.receivedSplitBatches.set(message.batchSerial, new Set());
      }
      const receivedSplitSerials = queryInfo.receivedSplitBatches.get(message.batchSerial);
      if (receivedSplitSerials) {
        receivedSplitSerials.add(message.splitSerial);
        if (receivedSplitSerials.size < message.totalSplitSerials) return;
      }
    }
    // sub batch check (optional, parent is splitSerial if exists, otherwise batchSerial)
    const parentBatchSerial =
      message.splitSerial && message.totalSplitSerials ? message.splitSerial : message.batchSerial;
    if (message.subBatchSerial && message.totalSubBatches) {
      if (!queryInfo.receivedSubBatches.has(parentBatchSerial)) {
        queryInfo.receivedSubBatches.set(parentBatchSerial, new Set());
      }
      const receivedSubBatches = queryInfo.receivedSubBatches.get(parentBatchSerial);
      if (receivedSubBatches) {
        receivedSubBatches.add(message.subBatchSerial);
        if (receivedSubBatches.size < message.totalSubBatches) return;
      }
    }
    if (queryInfo.receivedBatches.size < message.totalBatches) return;
    this.execEventCallback({ eventType: EEvent.QUERY_FINISHED, requestId: message.requestId, payload: message });
  }

  private execEventCallback(event: IEvent): void {
    const cbName = mapEventToCallbackName(event);
    if (this.props?.globalCallbacks) {
      const f = this.props.globalCallbacks[cbName];
      if (f) f(event.payload);
    }
    if (!event.requestId || !this.socketInstance.queryCallbacks.has(event.requestId)) return;
    const cbs = this.socketInstance.queryCallbacks.get(event.requestId);
    if (cbs && Object.prototype.hasOwnProperty.call(cbs, cbName)) {
      const f = cbs[cbName];
      if (f) f(event.payload);
    }
    if (event.eventType == EEvent.DATA) {
      this.processBatchInfo(event.payload);
    }
  }

  private handleSocketMessage(result: MessageEvent): void {
    const data = result?.data?.toString();
    if (data.length <= 0) return this.logger.info("No data on WebSocket incoming message");
    let message;
    try {
      message = jsonBigInt.parse(data);
      const eventType = message?.messageType == "LOG_MESSAGE" ? message?.logLevel : message?.messageType;
      this.execEventCallback({ eventType, requestId: message.requestId, payload: message });
    } catch (error) {
      console.error(error);
      const payload = { error, ...message };
      this.execEventCallback({ eventType: EEvent.LOG_ERROR, requestId: message?.requestId, payload });
    }
  }
}
