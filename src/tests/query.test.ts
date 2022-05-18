import { EEngineTypes, globalCallbacksList, IBDDataResponse } from "../boilingdata/boilingdata.api";
import { BoilingData, isDataResponse } from "../boilingdata/boilingdata";
import { createLogger } from "bunyan";

jest.setTimeout(30000);

const logLevel = "error";
const logger = createLogger({ name: "TEST", level: logLevel });
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

describe("boilingdata with DuckDB", () => {
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
        // engine: EEngineTypes.DUCKDB, // DuckDB is the default
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
    expect(rows.sort()).toMatchSnapshot();
  });

  it("run multi-key query", async () => {
    const rows = await new Promise<any[]>((resolve, reject) => {
      const r: any[] = [];
      bdInstance.execQuery({
        sql: `SELECT 's3://KEY' AS key, COUNT(*) AS count FROM parquet_scan('s3://KEY');`,
        engine: EEngineTypes.DUCKDB,
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
    expect(rows.sort((a, b) => a.key.localeCompare(b.key))).toMatchSnapshot();
  });

  it("run all meta queries", async () => {
    const metaQueries = [
      "SELECT * FROM list('s3://');",
      "SELECT * FROM list('s3://boilingdata-demo/');",
      "SELECT * FROM boilingdata;",
      "SELECT * FROM pragmas;",
      "SELECT * FROM status;",
    ];
    const rows = await Promise.all(
      metaQueries.map(sql => {
        return new Promise<any[]>((resolve, reject) => {
          const r: any[] = [];
          bdInstance.execQuery({
            sql,
            keys: [],
            engine: EEngineTypes.DUCKDB,
            callbacks: {
              onData: (data: IBDDataResponse | unknown) => {
                if (isDataResponse(data)) data.data.map(row => r.push(row));
              },
              onQueryFinished: () => resolve(r),
              onLogError: (data: any) => reject(data),
            },
          });
        });
      }),
    );
    expect(rows.sort()).toMatchSnapshot();
  });
});

describe("boilingdata with SQLite3", () => {
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
        sql: `SELECT * FROM sqlite('s3://boilingdata-demo/uploads/userdata1.sqlite3','userdata1') LIMIT 2;`,
        engine: EEngineTypes.SQLITE,
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
    expect(rows.sort()).toMatchSnapshot();
  });
});

describe("boilingdata with Glue Tables", () => {
  beforeAll(async () => {
    await bdInstance.connect();
    logger.info("connected.");
  });

  afterAll(async () => {
    await bdInstance.close();
    logger.info("connection closed.");
  });

  it("can read S3 Keys from Glue Table", async () => {
    const rows = await new Promise<any[]>((resolve, _reject) => {
      const r: any[] = [];
      bdInstance.execQuery({
        sql: `SELECT 's3://KEY' AS s3key, COUNT(*) AS count FROM parquet_scan('s3://KEY');`,
        engine: EEngineTypes.DUCKDB,
        keys: ["glue.default.nyctaxis"],
        callbacks: {
          onData: (data: IBDDataResponse | unknown) => {
            if (isDataResponse(data)) data.data.map(row => r.push(row));
          },
          onQueryFinished: () => resolve(r),
          // onLogError: (data: any) => reject(data),
        },
      });
    });
    expect(rows.sort((a, b) => a.s3key.localeCompare(b.s3key))).toMatchSnapshot();
  });

  it("can do partition filter push down", async () => {
    const rows = await new Promise<any[]>((resolve, _reject) => {
      const r: any[] = [];
      bdInstance.execQuery({
        sql: `SELECT 's3://KEY' AS s3key, COUNT(*) AS count FROM parquet_scan('s3://KEY') WHERE year=2009 AND month=8;`,
        engine: EEngineTypes.DUCKDB,
        keys: ["glue.default.nyctaxis"],
        callbacks: {
          onData: (data: IBDDataResponse | unknown) => {
            if (isDataResponse(data)) data.data.map(row => r.push(row));
          },
          onQueryFinished: () => resolve(r),
          // onLogError: (data: any) => reject(data),
        },
      });
    });
    expect(rows.sort((a, b) => a.s3key.localeCompare(b.s3key))).toMatchSnapshot();
  });
});
