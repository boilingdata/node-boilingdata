import { EEngineTypes, globalCallbacksList, IBDDataResponse } from "../boilingdata/boilingdata.api";
import { BDAWSRegion, BoilingData, IJsHooks, isDataResponse } from "../boilingdata/boilingdata";
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
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet') LIMIT 2;`,
      // engine: EEngineTypes.DUCKDB, // DuckDB is the default
      keys: [],
    });
    expect(rows.sort()).toMatchSnapshot();
  });

  it("run single query with scan cursor (offset)", async () => {
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT * FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test.parquet') LIMIT 10;`,
      scanCursor: 3,
      keys: [],
    });
    expect(rows.sort()).toMatchSnapshot();
  });

  it("run single query with scan cursor over the size", async () => {
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT * FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test.parquet');`,
      scanCursor: 10,
      keys: [],
    });
    expect(rows.sort()).toMatchSnapshot();
  });

  it("run multi-key query", async () => {
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT 's3://KEY' AS key, COUNT(*) AS count FROM parquet_scan('s3://KEY');`,
      engine: EEngineTypes.DUCKDB,
      keys: ["s3://boilingdata-demo/demo.parquet", "s3://boilingdata-demo/demo2.parquet"],
    });
    expect(rows.sort((a, b) => a.key?.localeCompare(b.key))).toMatchSnapshot();
  });

  it("run all meta queries", async () => {
    const metaQueries = [
      "SELECT * FROM list('s3://');",
      "SELECT * FROM list('s3://boilingdata-demo/');",
      "SELECT * FROM boilingdata;",
      "SELECT * FROM pragmas;",
    ];
    const rows = await Promise.all(
      metaQueries.map(sql => {
        return bdInstance.execQueryPromise({
          sql,
          keys: [],
          engine: EEngineTypes.DUCKDB,
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
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT * FROM sqlite('s3://boilingdata-demo/uploads/userdata1.sqlite3','userdata1') LIMIT 2;`,
      engine: EEngineTypes.SQLITE,
      keys: [],
    });
    expect(rows.sort()).toMatchSnapshot();
  });

  it("run single query, same one, 2nd time", async () => {
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT * FROM sqlite('s3://boilingdata-demo/uploads/userdata1.sqlite3','userdata1') LIMIT 2;`,
      engine: EEngineTypes.SQLITE,
      keys: [],
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
    const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet') LIMIT 2;`;
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
    const rows: any[] = [];
    await Promise.all(
      regions.map(async region => {
        const bdInstance = new BoilingData({ username, password, globalCallbacks, logLevel, region });
        await bdInstance.connect();
        logger.info(`connected to region ${region}`);
        const bucket = region == "eu-west-1" ? "boilingdata-demo" : `${region}-boilingdata-demo`;
        rows.push(
          await bdInstance.execQueryPromise({
            sql: `SELECT * FROM parquet_scan('s3://${bucket}/test.parquet') LIMIT 1;`,
            engine: EEngineTypes.DUCKDB,
            keys: [],
          }),
        );
        await bdInstance.close();
        logger.info(`connection closed to region ${region}`);
      }),
    );
    const sorted = rows.sort();
    console.log(sorted);
    expect(sorted).toMatchSnapshot();
  });

  it("can query cross-region", async () => {
    const sourceRegion = "eu-west-1";
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
    logger.info(`connected to region ${sourceRegion}`);
    const rows: any[] = [];
    while (allKeys.length) {
      const keys = allKeys.splice(0, 5);
      console.log(totalCount, keys);
      const newRows = await bdInstance.execQueryPromise({
        sql: `SELECT 's3://KEY' AS key, * FROM parquet_scan('s3://KEY') LIMIT 1;`,
        engine: EEngineTypes.DUCKDB,
        keys,
      });
      rows.push(...newRows);
    }
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
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT * FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test_folder/') ORDER BY email LIMIT 12;`,
    });
    expect(rows).toMatchSnapshot();
  });

  it("query over folder with 80 gz.parquet files (no mem caching, local region)", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT COUNT(*) AS totalCount FROM parquet_scan('s3://boilingdata-demo/test_folder2/');`,
    });
    expect(rows).toMatchInlineSnapshot(`
      Array [
        Object {
          "totalcount": 29166808,
        },
      ]
    `);
  });

  it("query over folder with 80 gz.parquet files (no mem caching)", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT COUNT(*) FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test_folder2/');`,
    });
    expect(rows).toMatchInlineSnapshot(`
      Array [
        Object {
          "count_star()": 29166808,
        },
      ]
    `);
  });

  it("query over example file", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT COUNT(*) FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test_folder2/part-r-00426-6e222bd6-47be-424a-a29a-606961a23de1.gz.parquet');`,
    });
    expect(rows.sort()).toMatchInlineSnapshot(`
      Array [
        Object {
          "count_star()": 365016,
        },
      ]
    `);
  });

  it("query over example file without splitAccess", async () => {
    const rows = await bdInstance.execQueryPromise({
      splitAccess: false,
      splitSizeMB: 300,
      sql: `SELECT COUNT(*) AS splitAccess FROM parquet_scan('s3://boilingdata-demo/demo2.parquet');`,
    });
    expect(rows.sort()).toMatchInlineSnapshot(`
      Array [
        Object {
          "splitaccess": 28160000,
        },
      ]
    `);
  });

  it("query over example file with splitAccess but no actual splitting", async () => {
    const rows = await bdInstance.execQueryPromise({
      splitAccess: true,
      splitSizeMB: 500,
      sql: `SELECT COUNT(*) AS splitAccess FROM parquet_scan('s3://boilingdata-demo/demo2.parquet');`,
    });
    expect(rows.sort()).toMatchInlineSnapshot(`
      Array [
        Object {
          "splitaccess": 28160000,
        },
      ]
    `);
  });

  it("query over example file with explicit splitAccess and splitSize", async () => {
    const rows = await bdInstance.execQueryPromise({
      splitAccess: true,
      splitSizeMB: 300,
      sql: `SELECT COUNT(*) AS splitAccess FROM parquet_scan('s3://boilingdata-demo/demo2.parquet');`,
    });
    // NOTE: If splitting happens, query results need to be combined.
    //       In this case it would be 14010368 + 14149632 = 28160000 ==> OK
    console.log(rows);
    expect(rows.sort((a, b) => a.splitaccess - b.splitaccess)).toMatchInlineSnapshot(`
      Array [
        Object {
          "splitaccess": 14010368,
        },
        Object {
          "splitaccess": 14149632,
        },
      ]
    `);
  });

  it("query over example file with smaller splitSize", async () => {
    const rows = await bdInstance.execQueryPromise({
      splitAccess: true,
      splitSizeMB: 100,
      sql: `SELECT COUNT(*) AS splitaccess FROM parquet_scan('s3://boilingdata-demo/demo.parquet');`,
    });
    // NOTE: If splitting happens, query results need to be combined.
    //       In this case it would be 5619712 + 5580800 + 5619712 + 5619712 + 5720064 = 28160000 ==> OK.
    expect(rows.sort((a, b) => a.splitaccess - b.splitaccess)).toMatchInlineSnapshot(`
      Array [
        Object {
          "splitaccess": 5580800,
        },
        Object {
          "splitaccess": 5619712,
        },
        Object {
          "splitaccess": 5619712,
        },
        Object {
          "splitaccess": 5619712,
        },
        Object {
          "splitaccess": 5720064,
        },
      ]
    `);
  });

  it("2x query over same example file", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT COUNT(*) FROM ( SELECT * FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test_folder2/part-r-00426-6e222bd6-47be-424a-a29a-606961a23de1.gz.parquet') UNION ALL SELECT * FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test_folder2/part-r-00426-6e222bd6-47be-424a-a29a-606961a23de1.gz.parquet')) a;`,
    });
    expect(rows.sort()).toMatchInlineSnapshot(`
      Array [
        Object {
          "count_star()": 730032,
        },
      ]
    `);
  });

  it.skip("2x query over folder with 80 gz.parquet files (mem caching)", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT COUNT(*) FROM ( SELECT * FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test_folder2/') UNION ALL SELECT * FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test_folder2/')) a;`,
    });
    expect(rows.sort()).toMatchInlineSnapshot(`
      Array [
        Object {
          "count_star()": 58333616,
        },
      ]
    `);
  });

  it.skip("query over folder with 80 gz.parquet files (mem cached)", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT COUNT(*) FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test_folder2/');`,
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

describe("BoilingData JS query hooks", () => {
  beforeAll(async () => {
    bdInstance = new BoilingData({ username, password, globalCallbacks, logLevel });
    await bdInstance.connect();
    logger.info("connected.");
  });

  afterAll(async () => {
    await bdInstance.close();
    logger.info("connection closed.");
  });

  it("transforms query results to (naive) CSV", async () => {
    const jsHooks: IJsHooks = {
      initFunc: (_sql: string, _scanCursor: number) => "",
      headerFunc: (c: any, first: any) => [c, Object.keys(first).join(",")],
      batchFunc: (c: any, rows: any[]) => [c, rows.map(r => Object.values(r).join(","))],
      footerFunc: (c: any, total: number) => `total rows: ${total}`,
      finalFunc: (c: any, allRows: any[]) => allRows, // identity func
    };
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT * FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test.parquet') LIMIT 5;`,
      jsHooks: jsHooks,
      keys: [],
    });
    expect(rows.join("\n")).toMatchInlineSnapshot(`
      "registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments
      1454486129000,1,Amanda,Jordan,ajordan0@com.com,Female,1.197.201.2,6759521864920116,Indonesia,3/8/1971,49756.53,Internal Auditor,1E+02
      1454519043000,2,Albert,Freeman,afreeman1@is.gd,Male,218.111.175.34,,Canada,1/16/1968,150280.17,Accountant IV,
      1454461771000,3,Evelyn,Morgan,emorgan2@altervista.org,Female,7.161.136.94,6767119071901597,Russia,2/1/1960,144972.51,Structural Engineer,
      1454459781000,4,Denise,Riley,driley3@gmpg.org,Female,140.35.109.83,3576031598965625,China,4/8/1997,90263.05,Senior Cost Accountant,
      1454475931000,5,Carlos,Burns,cburns4@miitbeian.gov.cn,,169.113.235.40,5602256255204850,South Africa,,,,
      total rows: 5"
    `);
  });

  it("transforms query results to simple HTML Table", async () => {
    const jsHooks: IJsHooks = {
      initFunc: (_sql: string, _scanCursor: number) => "",
      headerFunc: (c: any, first: any) => [
        c,
        "<table><tr><th>".concat(Object.keys(first).join("</th><th>")).concat("</th></tr>"),
      ],
      batchFunc: (c: any, rows: any[]) => [
        c,
        rows.map(r => "<tr><td>".concat(Object.values(r).join("</td><td>")).concat("</td></tr>")),
      ],
      footerFunc: (c: any, total: number) => `</table><br><b>Total rows: ${total}<br>`,
      finalFunc: (c: any, allRows: any[]) => allRows, // identity func
    };
    const rows = await bdInstance.execQueryPromise({
      sql: `SELECT * FROM parquet_scan('s3://eu-north-1-boilingdata-demo/test.parquet') LIMIT 5;`,
      jsHooks: jsHooks,
      keys: [],
    });
    expect(rows.join("\n")).toMatchInlineSnapshot(`
      "<table><tr><th>registration_dttm</th><th>id</th><th>first_name</th><th>last_name</th><th>email</th><th>gender</th><th>ip_address</th><th>cc</th><th>country</th><th>birthdate</th><th>salary</th><th>title</th><th>comments</th></tr>
      <tr><td>1454486129000</td><td>1</td><td>Amanda</td><td>Jordan</td><td>ajordan0@com.com</td><td>Female</td><td>1.197.201.2</td><td>6759521864920116</td><td>Indonesia</td><td>3/8/1971</td><td>49756.53</td><td>Internal Auditor</td><td>1E+02</td></tr>
      <tr><td>1454519043000</td><td>2</td><td>Albert</td><td>Freeman</td><td>afreeman1@is.gd</td><td>Male</td><td>218.111.175.34</td><td></td><td>Canada</td><td>1/16/1968</td><td>150280.17</td><td>Accountant IV</td><td></td></tr>
      <tr><td>1454461771000</td><td>3</td><td>Evelyn</td><td>Morgan</td><td>emorgan2@altervista.org</td><td>Female</td><td>7.161.136.94</td><td>6767119071901597</td><td>Russia</td><td>2/1/1960</td><td>144972.51</td><td>Structural Engineer</td><td></td></tr>
      <tr><td>1454459781000</td><td>4</td><td>Denise</td><td>Riley</td><td>driley3@gmpg.org</td><td>Female</td><td>140.35.109.83</td><td>3576031598965625</td><td>China</td><td>4/8/1997</td><td>90263.05</td><td>Senior Cost Accountant</td><td></td></tr>
      <tr><td>1454475931000</td><td>5</td><td>Carlos</td><td>Burns</td><td>cburns4@miitbeian.gov.cn</td><td></td><td>169.113.235.40</td><td>5602256255204850</td><td>South Africa</td><td></td><td></td><td></td><td></td></tr>
      </table><br><b>Total rows: 5<br>"
    `);
  });
});
