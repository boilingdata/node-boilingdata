import { EEngineTypes, globalCallbacksList, IBDDataResponse } from "../boilingdata/boilingdata.api";
import { BoilingData, IJsHooks, isDataResponse } from "../boilingdata/boilingdata";

const createLogger = (_props: any): Console => console;

jest.setTimeout(30000);

const logLevel = "info";
const logger = createLogger({ name: "TEST", level: logLevel });
const username = process.env["BD_USERNAME"];
const password = process.env["BD_PASSWORD"];
if (!password || !username) throw new Error("Set BD_USERNAME and BD_PASSWORD envs");

const globalCallbacks = globalCallbacksList
  .map((cb: string) => ({
    [cb]: (d: unknown) => {
      //logger.info(d);
    },
  }))
  .reduce((obj, item) => ({ ...obj, ...item }), {});
globalCallbacks.onSocketOpen = () => logger.info("connected.");
globalCallbacks.onSocketClose = () => logger.info("connection closed.");

describe("error handling", () => {
  let bdInstance: BoilingData = new BoilingData({ username, password, globalCallbacks, logLevel, region: "eu-west-1" });

  beforeAll(async () => {
    bdInstance = new BoilingData({ username, password, globalCallbacks, logLevel });
    await bdInstance.connect();
    logger.info("connected.");
  });

  afterAll(async () => {
    await bdInstance.close();
    logger.info("connection closed.");
  });

  it("missing catalog", async () => {
    const sql = "SELECT * FROM taxi_locations_limited;";
    try {
      const rows = await bdInstance.execQueryPromise({ sql });
      console.log({ rows });
    } catch (err) {
      // console.error(err);
      // console.log(err.logMessage);
      expect(err.logMessage).toContain("Catalog Error: Table with name taxi_locations_limited does not exist");
    }
  });

  it("random test queries", async () => {
    try {
      const sql = "DESCRIBE taxi_locations; -- Database: boiling.sharedtome";
      const rows = await bdInstance.execQueryPromise({ sql });
      console.log({ rows });
    } catch (err) {
      //console.error(err);
      //console.log(err.logMessage);
      expect(err.logMessage).toContain('Error: syntax error at or near "taxi_locations"');
    }
  });
});

describe("boilingdata with DuckDB", () => {
  let bdInstance: BoilingData = new BoilingData({ username, password, globalCallbacks, logLevel, region: "eu-west-1" });

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
    const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet') ORDER BY VendorID, DOLocationID, PULocationID, RatecodeID, tip_amount, total_amount, trip_distance, tpep_dropoff_datetime LIMIT 2;`;
    const rows = await bdInstance.execQueryPromise({ sql });
    console.log({ rows });
    expect(rows.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))).toMatchSnapshot();
  });

  it("run single query (2nd time with cacheHit)", async () => {
    const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet') ORDER BY VendorID, DOLocationID, PULocationID, RatecodeID, tip_amount, total_amount, trip_distance, tpep_dropoff_datetime LIMIT 2;`;
    const rows = await bdInstance.execQueryPromise({ sql });
    expect(rows.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))).toMatchSnapshot();
  });

  it("run single query with scan cursor (offset)", async () => {
    const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/test.parquet');`;
    const rows = await bdInstance.execQueryPromise({ sql, scanCursor: 3 });
    expect(rows.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))).toMatchSnapshot();
  });

  it("run single query with scan cursor over the size", async () => {
    const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/test.parquet');`;
    const rows = await bdInstance.execQueryPromise({ sql, scanCursor: 10 });
    expect(rows.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))).toMatchSnapshot();
  });

  it("run all meta queries", async () => {
    const metaQueries = [
      "SELECT * FROM list('s3://');",
      "SELECT * FROM list('s3://boilingdata-demo/');",
      "SELECT * FROM boilingdata;",
      "SELECT * FROM pragmas;",
      "SELECT * FROM status;",
    ];
    metaQueries.forEach(sql => {
      expect(() => bdInstance.execQueryPromise({ sql })).resolves;
    });
  });
});

describe("boilingdata with promise method", () => {
  let bdInstance: BoilingData = new BoilingData({ username, password, globalCallbacks, logLevel, region: "eu-west-1" });

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
    const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet') ORDER BY VendorID, DOLocationID, PULocationID, RatecodeID, tip_amount, total_amount, trip_distance, tpep_dropoff_datetime LIMIT 2;`;
    const results = await bdInstance.execQueryPromise({ sql });
    expect(results).toMatchSnapshot();
  });
});

describe("boilingdata with subBatches ordering", () => {
  let bdInstance: BoilingData = new BoilingData({ username, password, globalCallbacks, logLevel, region: "eu-west-1" });

  beforeAll(async () => await bdInstance.connect());
  afterAll(async () => await bdInstance.close());

  it("can order sub batches", async () => {
    const sql = `SELECT passenger_count, vendorid, COUNT(*) AS total_trips, MIN(fare_amount), MAX(trip_distance) FROM parquet_scan('s3://boilingdata-demo/demo.parquet') GROUP BY passenger_count, vendorid ORDER BY total_trips DESC, passenger_count DESC, vendorid DESC;`;
    const rows = await bdInstance.execQueryPromise({ sql });
    expect(rows.map(r => r.total_trips)).toEqual([
      11815973, 7790025, 2745186, 1520307, 1174025, 834914, 699251, 516910, 370343, 359760, 199327, 116169, 7294, 4458,
      2779, 1882, 692, 388, 123, 83, 80, 13, 9, 5, 4,
    ]);
  });
});

describe("splits", () => {
  let bdInstance: BoilingData = new BoilingData({ username, password, globalCallbacks, logLevel, region: "eu-west-1" });

  beforeAll(async () => await bdInstance.connect());
  afterAll(async () => await bdInstance.close());

  it("run single query", async () => {
    const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet') LIMIT 200;`;
    const rows = await bdInstance.execQueryPromise({ sql });
    expect(rows.length).toEqual(800); // 4 workers, cloud side does not "collect"
    // - the snapshot is random ordered, becuase the 4 workers are racing..
    // - DuckDB also returns random 200 rows as it does not need to order them..
    // ==> hard to do results verification testing
    //expect(rows.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))).toMatchSnapshot();
  });
});

describe("boilingdata with Glue Tables", () => {
  let bdInstance: BoilingData = new BoilingData({ username, password, globalCallbacks, logLevel, region: "eu-west-1" });

  beforeAll(async () => {
    bdInstance = new BoilingData({ username, password, globalCallbacks, logLevel });
    await bdInstance.connect();
    logger.info("connected.");
  });

  afterAll(async () => {
    await bdInstance.close();
    logger.info("connection closed.");
  });

  it("can do partition filter push down - year", async () => {
    let rows: any[];
    rows = await bdInstance.execQueryPromise({
      sql: `SELECT COUNT(*) AS count FROM glue('default.flights_parquet') WHERE year=1987;`,
    });
    console.log(JSON.parse(JSON.stringify(rows)));
    expect(rows[0]?.count).toEqual(1311826);
  });
  it("can do partition filter push down - year and month", async () => {
    let rows: any[];
    rows = await bdInstance.execQueryPromise({
      sql: `SELECT COUNT(*) AS count FROM glue('default.flights_parquet') WHERE year=2009 AND month=8;`,
    });
    console.log(JSON.parse(JSON.stringify(rows)));
    expect(rows).toEqual([{ count: 568301 }]);
  });
  it("can do partition filter push down - range", async () => {
    let rows: any[];
    rows = await bdInstance.execQueryPromise({
      sql: `SELECT COUNT(*) AS count FROM glue('default.flights_parquet') WHERE year=2015 AND month>=3 AND month<=11;`,
    });
    console.log(JSON.parse(JSON.stringify(rows)));
    expect(rows[0]?.count).toEqual(4440690);
  });
});

describe("BoilingData with S3 folders", () => {
  let bdInstance: BoilingData = new BoilingData({ username, password, globalCallbacks, logLevel, region: "eu-west-1" });

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
    const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/test_folder/') ORDER BY email LIMIT 12;`;
    const rows = await bdInstance.execQueryPromise({ sql });
    expect(rows).toMatchSnapshot();
  });

  it("query over folder with 80 gz.parquet files", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const sql = `SELECT COUNT(*) FROM parquet_scan('s3://boilingdata-demo/test_folder2/');`;
    const rows = await bdInstance.execQueryPromise({ sql });
    expect(rows).toMatchInlineSnapshot(`
      Array [
        Object {
          "count": 29166808,
        },
      ]
    `);
  });

  it("query over example file", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const sql = `SELECT COUNT(*) FROM parquet_scan('s3://boilingdata-demo/test_folder2/part-r-00426-6e222bd6-47be-424a-a29a-606961a23de1.gz.parquet');`;
    const rows = await bdInstance.execQueryPromise({ sql });
    expect(rows.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))).toMatchInlineSnapshot(`
      Array [
        Object {
          "count_star()": 365016,
        },
      ]
    `);
  });

  it("2x query over same example file", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const sql = `SELECT COUNT(*) FROM ( SELECT * FROM parquet_scan('s3://boilingdata-demo/test_folder2/part-r-00426-6e222bd6-47be-424a-a29a-606961a23de1.gz.parquet') UNION ALL SELECT * FROM parquet_scan('s3://boilingdata-demo/test_folder2/part-r-00426-6e222bd6-47be-424a-a29a-606961a23de1.gz.parquet')) a;`;
    const rows = await bdInstance.execQueryPromise({ sql });
    expect(rows.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))).toMatchInlineSnapshot(`
      Array [
        Object {
          "count_star()": 730032,
        },
      ]
    `);
  });

  it("2x query over folder with 80 gz.parquet files (mem caching)", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const sql = `SELECT COUNT(*) FROM ( SELECT * FROM parquet_scan('s3://boilingdata-demo/test_folder2/') UNION ALL SELECT * FROM parquet_scan('s3://boilingdata-demo/test_folder2/')) a;`;
    const rows = await bdInstance.execQueryPromise({ sql });
    expect(rows.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))).toMatchInlineSnapshot(`
      Array [
        Object {
          "count": 58333616,
        },
      ]
    `);
  });

  it("query over folder with 80 gz.parquet files (mem cached)", async () => {
    // s3://isecurefi-serverless-analytics/NY-Pub/year=2009/month=12/type=yellow/
    const sql = `SELECT COUNT(*) FROM parquet_scan('s3://boilingdata-demo/test_folder2/');`;
    const rows = await bdInstance.execQueryPromise({ sql });
    expect(rows.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))).toMatchInlineSnapshot(`
      Array [
        Object {
          "count": 29166808,
        },
      ]
    `);
  });
});

describe("BoilingData JS query hooks", () => {
  let bdInstance: BoilingData = new BoilingData({ username, password, globalCallbacks, logLevel, region: "eu-west-1" });

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
    const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/test.parquet') LIMIT 5;`;
    const rows = await bdInstance.execQueryPromise({ sql, jsHooks });
    expect(rows.join("\n")).toMatchInlineSnapshot(`
      "registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments
      2016-02-03 07:55:29+00,1,Amanda,Jordan,ajordan0@com.com,Female,1.197.201.2,6759521864920116,Indonesia,3/8/1971,49756.53,Internal Auditor,1E+02
      2016-02-03 17:04:03+00,2,Albert,Freeman,afreeman1@is.gd,Male,218.111.175.34,,Canada,1/16/1968,150280.17,Accountant IV,
      2016-02-03 01:09:31+00,3,Evelyn,Morgan,emorgan2@altervista.org,Female,7.161.136.94,6767119071901597,Russia,2/1/1960,144972.51,Structural Engineer,
      2016-02-03 00:36:21+00,4,Denise,Riley,driley3@gmpg.org,Female,140.35.109.83,3576031598965625,China,4/8/1997,90263.05,Senior Cost Accountant,
      2016-02-03 05:05:31+00,5,Carlos,Burns,cburns4@miitbeian.gov.cn,,169.113.235.40,5602256255204850,South Africa,,,,
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
    const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/test.parquet') LIMIT 5;`;
    const rows = await bdInstance.execQueryPromise({ sql, jsHooks });
    expect(rows.join("\n")).toMatchInlineSnapshot(`
      "<table><tr><th>registration_dttm</th><th>id</th><th>first_name</th><th>last_name</th><th>email</th><th>gender</th><th>ip_address</th><th>cc</th><th>country</th><th>birthdate</th><th>salary</th><th>title</th><th>comments</th></tr>
      <tr><td>2016-02-03 07:55:29+00</td><td>1</td><td>Amanda</td><td>Jordan</td><td>ajordan0@com.com</td><td>Female</td><td>1.197.201.2</td><td>6759521864920116</td><td>Indonesia</td><td>3/8/1971</td><td>49756.53</td><td>Internal Auditor</td><td>1E+02</td></tr>
      <tr><td>2016-02-03 17:04:03+00</td><td>2</td><td>Albert</td><td>Freeman</td><td>afreeman1@is.gd</td><td>Male</td><td>218.111.175.34</td><td></td><td>Canada</td><td>1/16/1968</td><td>150280.17</td><td>Accountant IV</td><td></td></tr>
      <tr><td>2016-02-03 01:09:31+00</td><td>3</td><td>Evelyn</td><td>Morgan</td><td>emorgan2@altervista.org</td><td>Female</td><td>7.161.136.94</td><td>6767119071901597</td><td>Russia</td><td>2/1/1960</td><td>144972.51</td><td>Structural Engineer</td><td></td></tr>
      <tr><td>2016-02-03 00:36:21+00</td><td>4</td><td>Denise</td><td>Riley</td><td>driley3@gmpg.org</td><td>Female</td><td>140.35.109.83</td><td>3576031598965625</td><td>China</td><td>4/8/1997</td><td>90263.05</td><td>Senior Cost Accountant</td><td></td></tr>
      <tr><td>2016-02-03 05:05:31+00</td><td>5</td><td>Carlos</td><td>Burns</td><td>cburns4@miitbeian.gov.cn</td><td></td><td>169.113.235.40</td><td>5602256255204850</td><td>South Africa</td><td></td><td></td><td></td><td></td></tr>
      </table><br><b>Total rows: 5<br>"
    `);
  });
});
