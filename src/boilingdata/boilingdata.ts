import { createLogger } from "bunyan";

export interface IBoilingData {
  logLevel?: "trace" | "debug" | "info" | "warn" | "error" | "fatal"; // Match with Bunyan
}

export class BoilingData {
  private logger = createLogger({ name: "boilingdata", level: this.props.logLevel ?? "info" });

  constructor(public props: IBoilingData) {}

  public async runQuery() {
    this.logger.info("runQuery");
  }
}
