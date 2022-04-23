import { createLogger } from "bunyan";
import { BDCredentials, getBoilingDataCredentials } from "../common/identity";
import { EEvent, EMessageTypes, IQuery } from "./boilingdata.api";
import { v4 as uuidv4 } from "uuid";
import { WebSocket } from "ws";

export interface IBDCallbacks {
  onData?: (event: IEvent) => void;
  onInfo?: (event: IEvent) => void;
  onRequest?: (event: IEvent) => void;
  onQueryFinished?: (event: IEvent) => void;
  onError?: (event: IEvent) => void;
  onLogError?: (event: IEvent) => void;
  onLogWarn?: (event: IEvent) => void;
  onLogInfo?: (event: IEvent) => void;
  onLogDebug?: (event: IEvent) => void;
  onLambdaEvent?: (event: IEvent) => void;
  onSocketOpen?: (socketInstance: ISocketInstance) => void;
  onSocketClose?: () => void;
}

export interface IBoilingData {
  username: string;
  password: string;
  logLevel?: "trace" | "debug" | "info" | "warn" | "error" | "fatal"; // Match with Bunyan
  globalCallbacks?: IBDCallbacks;
}

export interface IBDQuery {
  sql: string;
  keys?: string[];
  requestId?: string;
  callbacks?: IBDCallbacks;
}

interface ISocketInstance {
  lastActivity: number;
  queries: Map<string, { recievedBatches: string[] }>;
  sendQuery: (payload: IQuery) => void;
  query: (params: any) => Promise<void>;
  bumpActivity: () => void;
  socket?: WebSocket;
  queryCallbacks: Map<string, IBDCallbacks>;
}

interface IEvent {
  requestId: string;
  eventType: EEvent;
  payload: any;
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
}

function mapEventToCallbackName(eventType: EEvent): ECallbackNames {
  const entry = Object.entries(ECallbackNames).find(([key, _value]) => key === eventType);
  if (!entry) throw new Error(`Mapping event type "${eventType}" to callback name failed!`);
  return entry[1];
}

export class BoilingData {
  private statusTimer?: NodeJS.Timeout;
  private creds?: BDCredentials;
  private socketInstance: ISocketInstance;
  private logger = createLogger({ name: "boilingdata", level: this.props.logLevel ?? "info" });

  constructor(public props: IBoilingData) {
    this.socketInstance = {
      queries: new Map(), // no queries yet
      queryCallbacks: new Map(), // no queries yet, so no query specific callbacks either
      lastActivity: Date.now(),
      sendQuery: (payload: IQuery) => {
        this.logger.info(payload);
        this.execEventCallback({ eventType: EEvent.REQUEST, requestId: payload.requestId, payload });
        return this.socketInstance.socket?.send(JSON.stringify(payload));
      },
      bumpActivity: () => {
        this.socketInstance.lastActivity = Date.now();
      },
      query: (params: IQuery) => this.runQuery(params),
    };
  }

  public async close(): Promise<void> {
    if (this.statusTimer) clearTimeout(this.statusTimer);
    this.socketInstance.socket?.close();
  }

  public async connect(): Promise<ISocketInstance> {
    return new Promise(async (resolve, reject) => {
      const sock = this.socketInstance;
      const cbs = this.props.globalCallbacks;
      this.creds = await getBoilingDataCredentials(this.props.username, this.props.password);
      this.logger.debug(this.creds);
      sock.socket = new WebSocket(this.creds.signedWebsocketUrl);
      sock.socket.onclose = () => {
        if (!!cbs?.onSocketClose) cbs.onSocketClose();
      };
      sock.socket.onopen = () => {
        this.getStatus();
        if (!!cbs?.onSocketOpen) cbs.onSocketOpen(this.socketInstance);
        resolve(this.socketInstance);
      };
      sock.socket.onerror = (err: any) => {
        this.logger.error(err);
        reject(err);
      };
      sock.socket.onmessage = (msg: any) => {
        return this.handleSocketMessage(msg);
      };
    });
  }

  public async runQuery(params: IBDQuery) {
    this.logger.info("runQuery:", params);
    this.socketInstance.bumpActivity();
    const requestId = uuidv4();
    const payload: IQuery = {
      messageType: EMessageTypes.SQL_QUERY,
      sql: params.sql,
      keys: params.keys || [],
      requestId,
    };
    this.socketInstance.queries.set(requestId, {
      recievedBatches: [],
    });
    this.socketInstance.queryCallbacks.set(requestId, {
      onData: params.callbacks?.onData,
      onInfo: params.callbacks?.onInfo,
      onRequest: params.callbacks?.onRequest,
      onQueryFinished: params.callbacks?.onQueryFinished,
      onError: params.callbacks?.onError,
      onLogError: params.callbacks?.onError,
      onLogWarn: params.callbacks?.onLogWarn,
      onLogInfo: params.callbacks?.onLogInfo,
      onLogDebug: params.callbacks?.onLogDebug,
      onLambdaEvent: params.callbacks?.onLambdaEvent,
    });
    this.socketInstance.sendQuery(payload);
  }

  private getStatus() {
    if (Date.now() - this.socketInstance.lastActivity < 5 * 60 * 1000) {
      this.socketInstance.query({ sql: "SELECT * FROM status;" });
    }
    this.statusTimer = setTimeout(() => this.getStatus(), 60000); // call me again after 1 min
  }

  private execEventCallback(event: IEvent) {
    const cbName = mapEventToCallbackName(event.eventType);
    // this.logger.info("CALLBACK:", cbName, event);
    if (this.props?.globalCallbacks && this.props.globalCallbacks[cbName]) {
      const f = this.props.globalCallbacks[cbName]; // (event.payload);
      if (f) f(event.payload);
    }
    if (!!event.requestId && !!this.socketInstance.queryCallbacks.has(event.requestId)) {
      const cbs = this.socketInstance.queryCallbacks.get(event.requestId);
      if (cbs && cbs[cbName]) {
        const f = cbs[cbName];
        if (f) f(event.payload);
      }
    }
  }

  private handleSocketMessage(result: any) {
    if (result.data.length <= 0) return;
    const message = JSON.parse(result.data);
    try {
      switch (message?.messageType) {
        case "LAMBDA_EVENT":
          this.execEventCallback({ eventType: EEvent.LAMBDA_EVENT, requestId: message.requestId, payload: message });
          break;
        case "INFO":
          this.execEventCallback({ eventType: EEvent.INFO, requestId: message.requestId, payload: message });
          break;
        case "DATA":
          this.execEventCallback({ eventType: EEvent.DATA, requestId: message.requestId, payload: message });
          //processBatchInfo(message);
          break;
        case "LOG_MESSAGE":
          if (message.logLevel == "ERROR") {
            this.execEventCallback({ eventType: EEvent.LOG_ERROR, requestId: message.requestId, payload: message });
          } else if (message.logLevel == "WARN") {
            this.execEventCallback({ eventType: EEvent.LOG_WARN, requestId: message.requestId, payload: message });
          } else if (message.logLevel == "INFO") {
            this.execEventCallback({ eventType: EEvent.LOG_INFO, requestId: message.requestId, payload: message });
          } else if (message.logLevel == "DEBUG") {
            this.execEventCallback({ eventType: EEvent.LOG_DEBUG, requestId: message.requestId, payload: message });
          }
          break;
        default:
          this.execEventCallback({ eventType: EEvent.INFO, requestId: message.requestId, payload: message });
      }
    } catch (err) {
      console.error(err);
      this.execEventCallback({
        eventType: EEvent.LOG_ERROR,
        requestId: message.requestId,
        payload: { error: err, ...message },
      });
    }
  }
}
