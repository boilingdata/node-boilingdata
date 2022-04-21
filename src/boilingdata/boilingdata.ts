import { createLogger } from "bunyan";
import { BoilingDataCredentials, getBoilingDataCredentials } from "common/identity";

export interface IBoilingData {
  username: string;
  password: string;
  logLevel?: "trace" | "debug" | "info" | "warn" | "error" | "fatal"; // Match with Bunyan
}

export class BoilingData {
  private creds?: BoilingDataCredentials;
  private logger = createLogger({ name: "boilingdata", level: this.props.logLevel ?? "info" });

  constructor(public props: IBoilingData) {}

  public async login(): Promise<void> {
    this.creds = await getBoilingDataCredentials(this.props.username, this.props.password);
    this.logger.debug(this.creds);
  }

  public async runQuery() {
    this.logger.info("runQuery");
  }
}
