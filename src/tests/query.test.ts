import { EEngineTypes, globalCallbacksList, IBDDataResponse } from "../boilingdata/boilingdata.api";
import { BDAWSRegion, BoilingData, isDataResponse } from "../boilingdata/boilingdata";
import { createLogger } from "bunyan";

jest.setTimeout(60000);

const logLevel = "info";
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
let bdInstance: BoilingData; //  = new BoilingData({ username, password, globalCallbacks, logLevel });

describe("boilingdata with DuckDB", () => {
  beforeAll(async () => {
    bdInstance = new BoilingData({ username, password, globalCallbacks, logLevel });
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

  it("run single query with scan cursor (offset)", async () => {
    const rows = await new Promise<any[]>((resolve, reject) => {
      const r: any[] = [];
      bdInstance.execQuery({
        sql: `SELECT * FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test.parquet:m=0') LIMIT 10;`,
        scanCursor: 3,
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

  it("run single query with scan cursor over the size", async () => {
    const rows = await new Promise<any[]>((resolve, reject) => {
      const r: any[] = [];
      bdInstance.execQuery({
        sql: `SELECT * FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test.parquet:m=0');`,
        scanCursor: 10,
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
    bdInstance = new BoilingData({ username, password, globalCallbacks, logLevel });
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

  it("run single query, same one, 2nd time", async () => {
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

describe("boilingdata with promise method", () => {
  beforeAll(async () => {
    bdInstance = new BoilingData({ username, password, globalCallbacks, logLevel });
    await bdInstance.connect();
    logger.info("connected.");
  });

  afterAll(async () => {
    await bdInstance.close();
    logger.info("connection closed.");
  });

  it("can run simple promise based query", async () => {
    const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet:m=0') LIMIT 2;`;
    const results = await bdInstance.execQueryPromise({ sql });
    expect(results).toMatchSnapshot();
  });
});

describe("boilingdata with Glue Tables", () => {
  beforeAll(async () => {
    bdInstance = new BoilingData({ username, password, globalCallbacks, logLevel });
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

describe("BoilingData in all North-America and Europe AWS Regions", () => {
  const regions: BDAWSRegion[] = [
    "eu-north-1",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-south-1",
    "eu-central-1",
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "ca-central-1",
  ];

  it("runs query succesfully in other regions too", async () => {
    await Promise.all(
      regions.map(async region => {
        const bdInstance = new BoilingData({ username, password, globalCallbacks, logLevel, region });
        await bdInstance.connect();
        logger.info(`connected to region ${region}`);
        const rows = await new Promise<any[]>((resolve, reject) => {
          const r: any[] = [];
          const bucket = region == "eu-west-1" ? "boilingdata-demo" : `${region}-boilingdata-demo`;
          bdInstance.execQuery({
            sql: `SELECT * FROM parquet_scan('s3://${bucket}/test.parquet:m=0') LIMIT 1;`,
            engine: EEngineTypes.DUCKDB,
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
        const sorted = rows.sort();
        console.log(sorted);
        expect(sorted).toMatchSnapshot();

        await bdInstance.close();
        logger.info(`connection closed to region ${region}`);
      }),
    );
  });

  it("can query cross-region", async () => {
    const sourceRegion = "eu-west-3";
    const bdInstance = new BoilingData({ username, password, globalCallbacks, logLevel, region: sourceRegion });
    await bdInstance.connect();
    const allKeys = [
      "s3://boilingdata-demo/test.parquet",
      "s3://eu-west-2-boilingdata-demo/test.parquet",
      "s3://eu-west-3-boilingdata-demo/test.parquet",
      "s3://eu-north-1-boilingdata-demo/test.parquet",
      "s3://eu-south-1-boilingdata-demo/test.parquet",
      "s3://eu-central-1-boilingdata-demo/test.parquet",
      "s3://us-east-1-boilingdata-demo/test.parquet",
      "s3://us-east-2-boilingdata-demo/test.parquet",
      "s3://us-west-1-boilingdata-demo/test.parquet",
      "s3://us-west-2-boilingdata-demo/test.parquet",
      "s3://ca-central-1-boilingdata-demo/test.parquet",
    ];
    const totalCount = allKeys.length;
    let count = 0;
    logger.info(`connected to region ${sourceRegion}`);
    const rows = await new Promise<any[]>((resolve, reject) => {
      const r: any[] = [];
      while (allKeys.length) {
        const keys = allKeys.splice(0, 5);
        console.log(totalCount, keys);
        bdInstance.execQuery({
          sql: `SELECT 's3://KEY' AS key, * FROM parquet_scan('s3://KEY') LIMIT 1;`,
          engine: EEngineTypes.DUCKDB,
          keys,
          callbacks: {
            onData: (data: IBDDataResponse | unknown) => {
              if (isDataResponse(data)) {
                data.data.map(row => r.push(row));
                count = count + 1;
                console.log("---------------------- ", count, "/", totalCount, data?.data[0].key);
                if (count >= totalCount) resolve(r);
              }
            },
            onLogError: (data: any) => reject(data),
          },
        });
      }
    });
    const sorted = rows.sort((a, b) => a.key.localeCompare(b.key));
    console.log(sorted);
    expect(sorted).toMatchSnapshot();

    await bdInstance.close();
    logger.info(`connection closed to region ${sourceRegion}`);
  });
});

describe("BoilingData with S3 folders", () => {
  beforeAll(async () => {
    bdInstance = new BoilingData({ username, password, globalCallbacks, logLevel });
    await bdInstance.connect();
    logger.info("connected.");
  });

  afterAll(async () => {
    await bdInstance.close();
    logger.info("connection closed.");
  });

  it("run single query on S3 folder with 2 copies of the same parquet file", async () => {
    const rows = await new Promise<any[]>((resolve, reject) => {
      const r: any[] = [];
      bdInstance.execQuery({
        sql: `SELECT * FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test_folder/') a ORDER BY email LIMIT 12;`,
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

  it("query over folder with 80 gz.parquet files (no mem caching)", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const rows = await new Promise<any[]>((resolve, reject) => {
      const r: any[] = [];
      bdInstance.execQuery({
        sql: `SELECT COUNT(*) FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test_folder2/:m=0') a;`,
        callbacks: {
          onData: (data: IBDDataResponse | unknown) => {
            if (isDataResponse(data)) data.data.map(row => r.push(row));
            resolve(r);
          },
          onLogError: (data: any) => reject(data),
        },
      });
    });
    expect(rows.sort()).toMatchInlineSnapshot(`
      Array [
        Object {
          "count_star()": 29166808,
        },
      ]
    `);
  });

  it("query over folder with 80 gz.parquet files (mem cached)", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const rows = await new Promise<any[]>((resolve, reject) => {
      const r: any[] = [];
      bdInstance.execQuery({
        sql: `SELECT COUNT(*) FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test_folder2/') a;`,
        callbacks: {
          onData: (data: IBDDataResponse | unknown) => {
            if (isDataResponse(data)) data.data.map(row => r.push(row));
            resolve(r);
          },
          onLogError: (data: any) => reject(data),
        },
      });
    });
    expect(rows.sort()).toMatchInlineSnapshot(`
      Array [
        Object {
          "count_star()": 29166808,
        },
      ]
    `);
  });
});
