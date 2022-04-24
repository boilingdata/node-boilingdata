import { BoilingData } from "../boilingdata/boilingdata";
import { createLogger } from "bunyan";

jest.setTimeout(30000);
const logger = createLogger({ name: "TEST", level: "info" });
const username = process.env["BD_USERNAME"];
const password = process.env["BD_PASSWORD"];
if (!password || !username) throw new Error("Set BD_USERNAME and BD_PASSWORD envs");
const bdInstance = new BoilingData({
  username,
  password,
  globalCallbacks: {
    onData: (d: any) => logger.info(d),
    onError: (d: any) => logger.info(d),
    onInfo: (d: any) => logger.info(d),
    onLambdaEvent: (d: any) => logger.info(d),
    onLogDebug: (d: any) => logger.info(d),
    onLogError: (d: any) => logger.error(d),
    onLogInfo: (d: any) => logger.info(d),
    onLogWarn: (d: any) => logger.warn(d),
    onQueryFinished: (d: any) => logger.info(d),
    onRequest: (d: any) => logger.info(d),
    onSocketClose: () => logger.info("socket closed"),
    onSocketOpen: () => logger.info("socket opened"),
  },
});

describe("boilingdata", () => {
  beforeAll(async () => {
    await bdInstance.connect();
    logger.info("CONNECTED.");
  });

  afterAll(async () => {
    await bdInstance.close();
    logger.info("SOCKET CLOSED.");
  });

  it("run single query", async () => {
    await new Promise((resolve, reject) => {
      bdInstance.execQuery({
        sql: `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet:m=0') LIMIT 1;`,
        keys: [],
        callbacks: {
          onData: (data: any) => resolve(data),
          onError: (data: any) => reject(data),
        },
      });
    });
    logger.info("QUERY DONE.");
  });

  it("run multi-key query", async () => {
    await new Promise((resolve, reject) => {
      bdInstance.execQuery({
        sql: `SELECT 's3://KEY' AS key, COUNT(*) AS count FROM parquet_scan('s3://KEY') LIMIT 1;`,
        keys: ["s3://boilingdata-demo/demo.parquet", "s3://boilingdata-demo/demo2.parquet"],
        callbacks: {
          onData: (data: any) => resolve(data),
          onError: (data: any) => reject(data),
        },
      });
    });
    logger.info("QUERY DONE.");
  });
});
