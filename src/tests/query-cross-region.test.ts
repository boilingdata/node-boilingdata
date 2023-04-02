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

describe("BD can switch the WebSocket connection endpoint dynamically", () => {
  it("run single query from 2 different regions", async () => {
    const expected = [
      {
        DOLocationID: 145,
        PULocationID: 145,
        RatecodeID: 1,
        VendorID: 1,
        congestion_surcharge: 0,
        extra: 0.5,
        fare_amount: 3,
        improvement_surcharge: 0.3,
        mta_tax: 0.5,
        passenger_count: 1,
        payment_type: 2,
        store_and_fwd_flag: "N",
        tip_amount: 0,
        tolls_amount: 0,
        total_amount: 4.3,
        tpep_dropoff_datetime: 1556669808000,
        tpep_pickup_datetime: 1556669690000,
        trip_distance: 0,
      },
      {
        DOLocationID: 145,
        PULocationID: 145,
        RatecodeID: 1,
        VendorID: 1,
        congestion_surcharge: 0,
        extra: 0.5,
        fare_amount: 3,
        improvement_surcharge: 0.3,
        mta_tax: 0.5,
        passenger_count: 1,
        payment_type: 2,
        store_and_fwd_flag: "N",
        tip_amount: 0,
        tolls_amount: 0,
        total_amount: 4.3,
        tpep_dropoff_datetime: 1556671047000,
        tpep_pickup_datetime: 1556670954000,
        trip_distance: 1.5,
      },
    ];
    let bd = new BoilingData({
      username,
      password,
      globalCallbacks,
      logLevel,
      region: "eu-west-1",
    });
    await bd.connect();
    const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet') LIMIT 2;`;
    let rows = await bd.execQueryPromise({ sql });
    expect(rows.sort()).toEqual(expected);
    await bd.close();
    // New connection from different region, web socket endpoint changes (like ConnectionId too).
    // But the bucket is still in eu-west-1 and the request is routed there to the same Lambda.
    bd = new BoilingData({
      username,
      password,
      globalCallbacks,
      logLevel,
      region: "eu-north-1",
    });
    await bd.connect();
    rows = await bd.execQueryPromise({ sql });
    expect(rows.sort()).toEqual(expected);
    await bd.close();
  });
});

describe("BoilingData in all North-America and Europe AWS Regions", () => {
  const regions: BDAWSRegion[] = [
    "eu-west-1",
    "eu-north-1",
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
        const bdInstance = new BoilingData({
          username,
          password,
          globalCallbacks,
          logLevel,
          region,
        });
        await bdInstance.connect();
        logger.info(`connected to region ${region}`);
        const bucket = region == "eu-west-1" ? "boilingdata-demo" : `${region}-boilingdata-demo`;
        const sql = `SELECT * FROM parquet_scan('s3://${bucket}/test.parquet') LIMIT 1;`;
        rows.push(await bdInstance.execQueryPromise({ sql }));
        await bdInstance.close();
        logger.info(`connection closed to region ${region}`);
      }),
    );
    const sorted = rows.sort();
    console.log(sorted);
    expect(sorted).toMatchSnapshot();
  });

  it("can query cross-region", async () => {
    const bdInstance = new BoilingData({
      username,
      password,
      globalCallbacks,
      logLevel,
    });
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
    const rows: any[] = [];
    while (allKeys.length) {
      const keys = allKeys.splice(0, 5);
      console.log(totalCount, keys);
      const sql = `SELECT 's3://KEY' AS key, * FROM parquet_scan('s3://KEY') LIMIT 1;`;
      const newRows = await bdInstance.execQueryPromise({ sql, keys });
      rows.push(...newRows);
    }
    const sorted = rows.sort((a, b) => a.key.localeCompare(b.key));
    console.log(sorted);
    expect(sorted).toMatchSnapshot();

    await bdInstance.close();
    logger.info(`connection closed`);
  });
});
