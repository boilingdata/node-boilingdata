import { globalCallbacksList } from "../boilingdata/boilingdata.api";
import { BDAWSRegion, BoilingData } from "../boilingdata/boilingdata";

const createLogger = (_props: any): Console => console;

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

const regions: BDAWSRegion[] = [
  "eu-west-1",
  "eu-north-1",
  // "eu-west-2",
  // "eu-west-3",
  // "eu-south-1",
  // "eu-central-1",
  // "us-east-1",
  "us-east-2",
  // "us-west-1",
  "us-west-2",
  // "ca-central-1",
];

// Create connection to each region
const bdAllRegions = regions.map(region => ({
  region,
  bd: new BoilingData({ username, password, globalCallbacks, logLevel, region }),
}));
const connMap = new Map<string, BoilingData>();
bdAllRegions.forEach(conn => connMap.set(conn.region, conn.bd));

describe("BD can switch the WebSocket connection endpoint dynamically", () => {
  const expected = [
    {
      congestion_surcharge: 0,
      dolocationid: 1,
      extra: 0,
      fare_amount: 121,
      improvement_surcharge: 0.3,
      mta_tax: 0,
      passenger_count: 2,
      payment_type: 1,
      pulocationid: 1,
      ratecodeid: 5,
      store_and_fwd_flag: "N",
      tip_amount: 0,
      tolls_amount: 0,
      total_amount: 121.3,
      tpep_dropoff_datetime: 1551410222000,
      tpep_pickup_datetime: 1551410199000,
      trip_distance: 0,
      vendorid: 1,
    },
    {
      congestion_surcharge: 0,
      dolocationid: 1,
      extra: 0,
      fare_amount: 110,
      improvement_surcharge: 0.3,
      mta_tax: 0,
      passenger_count: 1,
      payment_type: 1,
      pulocationid: 1,
      ratecodeid: 5,
      store_and_fwd_flag: "N",
      tip_amount: 10,
      tolls_amount: 0,
      total_amount: 120.3,
      tpep_dropoff_datetime: 1551415659000,
      tpep_pickup_datetime: 1551415597000,
      trip_distance: 18.4,
      vendorid: 1,
    },
  ];

  beforeAll(async () => await Promise.all(bdAllRegions.map(conn => conn.bd.connect())));

  afterAll(async () => await Promise.all(bdAllRegions.map(conn => conn.bd.close())));

  it("run a local query in all supported regions", async () => {
    // Run all in parallel
    await Promise.all(
      regions.map(async region => {
        const rows: any[] = [];
        const bucket = region == "eu-west-1" ? "boilingdata-demo" : `${region}-boilingdata-demo`;
        const sql = `SELECT * FROM parquet_scan('s3://${bucket}/test.parquet') LIMIT 1;`;
        rows.push(await connMap.get(region)?.execQueryPromise({ sql }));
        const sorted = rows.sort();
        console.log(sorted);
        expect(sorted).toMatchSnapshot();
      }),
    );
  });
  it("run single query to eu-west-1 from all supported regions", async () => {
    // Run all queries in parallel
    await Promise.all(
      regions.map(async region => {
        const sql = `SELECT * FROM parquet_scan('s3://boilingdata-demo/demo2.parquet') ORDER BY DOLocationID, PULocationID, tpep_dropoff_datetime, tpep_pickup_datetime, trip_distance LIMIT 2;`;
        // The JSON is bigint JSON, thus converting back to "normal", to get Object properties inherited, like toString()
        const rows = JSON.parse(JSON.stringify(await connMap.get(region)?.execQueryPromise({ sql })));
        expect(rows.sort()).toEqual(expected);
      }),
    );
  });
  it("run single query to us-east-2 from all supported regions", async () => {
    // Run all queries in parallel
    await Promise.all(
      regions.map(async region => {
        const sql = `SELECT * FROM parquet_scan('s3://us-east-2-boilingdata-demo/demo2.parquet') ORDER BY DOLocationID, PULocationID, tpep_dropoff_datetime, tpep_pickup_datetime, trip_distance LIMIT 2;`;
        // The JSON is bigint JSON, thus converting back to "normal", to get Object properties inherited, like toString()
        const rows = JSON.parse(JSON.stringify(await connMap.get(region)?.execQueryPromise({ sql })));
        expect(rows.sort()).toEqual(expected);
      }),
    );
  });
});
