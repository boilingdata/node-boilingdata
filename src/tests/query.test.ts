import { globalCallbacksList, IBDDataResponse } from "../boilingdata/boilingdata.api";
import { BoilingData, isDataResponse } from "../boilingdata/boilingdata";
import { createLogger } from "bunyan";

jest.setTimeout(30000);

const logLevel = "error";
const logger = createLogger({ name: "TEST", level: logLevel });
console.log(process.env);
const username = process.env["BD_USERNAME"];
const password = process.env["BD_PASSWORD"];
if (!password || !username) throw new Error("Set BD_USERNAME and BD_PASSWORD envs");

const globalCallbacks = globalCallbacksList
  .map((cb: string) => ({ [cb]: (d: unknown) => logger.info(d) }))
  .reduce((obj, item) => ({ ...obj, ...item }), {});
globalCallbacks.onSocketOpen = () => {
  logger.info("socket open");
  return undefined;
};
globalCallbacks.onSocketClose = () => {
  logger.info("socket closed");
  return undefined;
};
const bdInstance = new BoilingData({ username, password, globalCallbacks, logLevel });

describe("boilingdata", () => {
  beforeAll(async () => {
    await bdInstance.connect();
    logger.info("connected.");
  });

  afterAll(async () => {
    await bdInstance.close();
    logger.info("connection closed.");
  });

  it("run single query", async () => {
    const rows = await new Promise<any[]>((resolve, reject) => {
      const r: any[] = [];
      bdInstance.execQuery({
        sql: `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet:m=0') LIMIT 2;`,
        keys: [],
        callbacks: {
          onData: (data: IBDDataResponse | unknown) => {
            if (isDataResponse(data)) data.data.map(row => r.push(row));
            resolve(r);
          },
          onLogError: (data: any) => reject(data),
        },
      });
    });
    console.log(rows);
  });

  it("run multi-key query", async () => {
    const rows = await new Promise<any[]>((resolve, reject) => {
      const r: any[] = [];
      bdInstance.execQuery({
        sql: `SELECT 's3://KEY' AS key, COUNT(*) AS count FROM parquet_scan('s3://KEY');`,
        keys: ["s3://boilingdata-demo/demo.parquet", "s3://boilingdata-demo/demo2.parquet"],
        callbacks: {
          onData: (data: IBDDataResponse | unknown) => {
            if (isDataResponse(data)) data.data.map(row => r.push(row));
          },
          onQueryFinished: () => resolve(r),
          onLogError: (data: any) => reject(data),
        },
      });
    });
    console.log(rows);
  });
});
