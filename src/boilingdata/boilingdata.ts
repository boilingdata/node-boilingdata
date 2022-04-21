import { createLogger } from "bunyan";
import { BDCredentials, getBoilingDataCredentials } from "common/identity";
import { resolve } from "path";

export interface IBDCallbacks {
  onData?: () => void;
  onInfo?: () => void;
  onRequest?: () => void;
  onQueryFinished?: () => void;
  onError?: () => void;
  onLogError?: () => void;
  onlogWarn?: () => void;
  onLogInfo?: () => void;
  onLogDebug?: () => void;
  onLambdaEvent?: () => void;
  onSocketOpen?: (socketInstance: ISocketInstance) => void;
  onSocketClose?: () => void;
}

export interface IBoilingData {
  username: string;
  password: string;
  logLevel?: "trace" | "debug" | "info" | "warn" | "error" | "fatal"; // Match with Bunyan
  callbacks?: IBDCallbacks;
}

interface ISocketInstance {
  lastActivity: number;
  queries: {};
  sendQuery: (payload: any) => void;
  query: (params: any) => Promise<void>;
  bumpActivity: () => void;
  socket?: WebSocket;
  callbacks?: IBDCallbacks;
}

export class BoilingData {
  private creds?: BDCredentials;
  private socketInstance: ISocketInstance;
  private logger = createLogger({ name: "boilingdata", level: this.props.logLevel ?? "info" });

  constructor(public props: IBoilingData) {
    this.socketInstance = {
      lastActivity: Date.now(),
      callbacks: this.props.callbacks,
      sendQuery: (payload: any) => {
        this.logger.info(payload);
        // execEvent({event: "onRequest", requestId: payload.requestId, payload }, socketInstance)
        return this.socketInstance.socket?.send(JSON.stringify(payload));
      },
      bumpActivity: () => {
        this.socketInstance.lastActivity = Date.now();
      },
      queries: {},
      query: (params: any) => this.runQuery(params, this.socketInstance),
    };
  }

  public async init(): Promise<ISocketInstance> {
    return new Promise(async (resolve, _reject) => {
      const cb = this.socketInstance.callbacks;
      this.creds = await getBoilingDataCredentials(this.props.username, this.props.password);
      this.logger.debug(this.creds);
      this.socketInstance.socket = new WebSocket(this.creds.signedWebsocketUrl);
      this.socketInstance.socket.onclose = () => {
        if (!!cb?.onSocketClose) cb.onSocketClose();
      };
      this.socketInstance.socket.onopen = () => {
        if (!!cb?.onSocketOpen) cb.onSocketOpen(this.socketInstance);
        resolve(this.socketInstance);
      };
      this.socketInstance.socket.onerror = (err: any) => this.logger.error(err);
      this.socketInstance.socket.onmessage = (msg: any) => this.logger.info(msg);
    });
  }

  public async runQuery(params: any, socketInstance: ISocketInstance) {
    this.logger.info("runQuery:", params, socketInstance);
  }
}
