import { createLogger } from "bunyan";
import { BDCredentials, getBoilingDataCredentials } from "../common/identity";
import { EEngineTypes, EEvent, EMessageTypes, IBDDataQuery, IBDDataResponse } from "./boilingdata.api";
import { v4 as uuidv4 } from "uuid";
import { WebSocket, MessageEvent } from "ws";
import { inspect } from "util";

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
  username: string;
  password: string;
  logLevel?: "trace" | "debug" | "info" | "warn" | "error" | "fatal"; // Match with Bunyan
  globalCallbacks?: IBDCallbacks;
  region?: BDAWSRegion;
}

export interface IBDQuery {
  sql: string;
  engine?: EEngineTypes.DUCKDB | EEngineTypes.SQLITE;
  keys?: string[];
  requestId?: string;
  callbacks?: IBDCallbacks;
}

export interface ISocketInstance {
  lastActivity: number;
  queries: Map<string, { recievedBatches: Set<number> }>;
  send: (payload: IBDDataQuery) => void;
  query: (params: IBDQuery) => void;
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
  LOG_INFO = "onLogInfo",
  LOG_ERROR = "onLogError",
  LOG_WARN = "onLogWarn",
  LOG_DEBUG = "onLogDebug",
  DATA = "onData",
  INFO = "onInfo",
  LAMBDA_EVENT = "onLambdaEvent",
  QUERY_FINISHED = "onQueryFinished",
}

function mapEventToCallbackName(event: IEvent): ECallbackNames {
  const entry = Object.entries(ECallbackNames).find(([key, _value]) => key === event.eventType);
  if (!entry) throw new Error(`Mapping event type "${inspect(event, false, 7)}" to callback name failed!`);
  return entry[1];
}

export function isDataResponse(data: IBDDataResponse | unknown): data is IBDDataResponse {
  return (data as IBDDataResponse).messageType !== undefined && (data as IBDDataResponse).messageType === "DATA";
}

export class BoilingData {
  private statusTimer?: NodeJS.Timeout;
  private region: BDAWSRegion;
  private creds?: BDCredentials;
  private socketInstance: ISocketInstance;
  private logger = createLogger({ name: "boilingdata", level: this.props.logLevel ?? "info" });

  constructor(public props: IBoilingData) {
    this.region = this.props.region ? this.props.region : "eu-west-1";
    this.socketInstance = {
      queries: new Map(), // no queries yet
      queryCallbacks: new Map(), // no queries yet, so no query specific callbacks either
      lastActivity: Date.now(),
      send: (payload: IBDDataQuery) => {
        this.socketInstance.socket?.send(JSON.stringify(payload));
        this.execEventCallback({ eventType: EEvent.REQUEST, requestId: payload.requestId, payload });
      },
      bumpActivity: () => {
        this.socketInstance.lastActivity = Date.now();
      },
      query: (params: IBDQuery) => this.execQuery(params),
    };
  }

  public async close(): Promise<void> {
    if (this.statusTimer) clearTimeout(this.statusTimer);
    this.socketInstance.socket?.close(1000);
  }

  public async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      const sock = this.socketInstance;
      const cbs = this.props.globalCallbacks;
      getBoilingDataCredentials(this.props.username, this.props.password, this.region).then(creds => {
        this.creds = creds;
        sock.socket = new WebSocket(this.creds.signedWebsocketUrl);
        sock.socket.onclose = () => {
          if (cbs?.onSocketClose) cbs.onSocketClose();
        };
        sock.socket.onopen = () => {
          this.getStatus();
          if (cbs?.onSocketOpen) cbs.onSocketOpen();
          resolve();
        };
        sock.socket.onerror = (err: any) => {
          this.logger.error(err);
          reject(err);
        };
        sock.socket.onmessage = (msg: MessageEvent) => {
          return this.handleSocketMessage(msg);
        };
      });
    });
  }

  public execQuery(params: IBDQuery): void {
    this.logger.info("runQuery:", params);
    this.socketInstance.bumpActivity();
    const requestId = uuidv4();
    const payload: IBDDataQuery = {
      messageType: EMessageTypes.SQL_QUERY,
      sql: params.sql,
      keys: params.keys || [],
      engine: params.engine ?? EEngineTypes.DUCKDB,
      requestId,
    };
    this.socketInstance.queries.set(requestId, {
      recievedBatches: new Set(),
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
    this.socketInstance.send(payload);
  }

  private getStatus(): void {
    // Once a minute fetch full status if fetched status is older than 5 mins
    const { lastActivity } = this.socketInstance;
    const fiveMinsMs = 5 * 60 * 1000;
    if (Date.now() - lastActivity < fiveMinsMs) this.execQuery({ sql: "SELECT * FROM status;" });
    this.statusTimer = setTimeout(() => this.getStatus(), 60000);
  }

  private processBatchInfo(message: unknown): void {
    if (!isDataResponse(message)) return;
    // Keeps track of the recieved batches, executes event when all batches have been recieved.
    if (!message.requestId || !message.batchSerial || !message.totalBatches || message.batchSerial <= 0) return;
    const queryInfo = this.socketInstance.queries.get(message?.requestId);
    if (!queryInfo) return;
    queryInfo.recievedBatches.add(message.batchSerial);
    if (queryInfo.recievedBatches.size < message.totalBatches) return;
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
      message = JSON.parse(data);
      const eventType = message?.messageType == "LOG_MESSAGE" ? message?.logLevel : message?.messageType;
      this.execEventCallback({ eventType, requestId: message.requestId, payload: message });
    } catch (error) {
      console.error(error);
      const payload = { error, ...message };
      this.execEventCallback({ eventType: EEvent.LOG_ERROR, requestId: message?.requestId, payload });
    }
  }
}
