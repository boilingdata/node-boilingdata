# BoilingData JS/TS SDK

![CI](https://github.com/boilingdata/node-boilingdata/workflows/CI/badge.svg?branch=main)
![BuiltBy](https://img.shields.io/badge/TypeScript-Lovers-black.svg "img.shields.io")

## Installing the SDK

```shell
yarn add @boilingdata/node-boilingdata
```

## Basic Examples

`execQueryPromise()` method can be used to await for the results directly.

```typescript
import { BoilingData, isDataResponse } from "@boilingdata/node-boilingdata";

async function main() {
  const bdInstance = new BoilingData({ username: process.env["BD_USERNAME"], password: process.env["BD_PASSWORD"] });
  await bdInstance.connect();
  const sql = `SELECT 's3://KEY' AS key, COUNT(*) AS count FROM parquet_scan('s3://KEY');`;
  const keys = ["s3://boilingdata-demo/demo.parquet", "s3://boilingdata-demo/demo2.parquet"];
  const rows = await bdInstance.execQueryPromise({ sql, keys });
  console.log(rows);
  await bdInstance.close();
}
```

`execQuery()` uses callbacks.

```typescript
import { BoilingData, isDataResponse } from "@boilingdata/node-boilingdata";

async function main() {
  const bdInstance = new BoilingData({ username: process.env["BD_USERNAME"], password: process.env["BD_PASSWORD"] });
  await bdInstance.connect();
  const sql = `SELECT 's3://KEY' AS key, COUNT(*) AS count FROM parquet_scan('s3://KEY');`;
  const keys = ["s3://boilingdata-demo/demo.parquet", "s3://boilingdata-demo/demo2.parquet"];
  const rows = await new Promise<any[]>((resolve, reject) => {
    let r: any[] = [];
    bdInstance.execQuery({
      sql,
      keys,
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
  await bdInstance.close();
}
```

This repository contains JS/TS BoilingData client SDK that can be used both with NodeJS and in browser. Please see the integration tests on `tests/query.test.ts` for for more examples.

### Callbacks

The SDK uses the BoilingData Websocket API in the background, meaning that events can arrive at any time. We use a range of global and query-specific callbacks to allow you to hook into the events that you care about.

All callbacks work in both the global scope and the query scope; i.e. global callbacks will always be executed when a message arrives, query callbacks will only be executed when messages relating to that query arrive.

- onRequest - This event happens when your application sends a request to BoilingData
- onData - Query data response. A single query may have many onData events as processing is parallelised in the background.
- onQueryFinished - The processing of data has completed, and you should not expect any further onData events (although more info messages may arrive)
- onLambdaEvent - the status of your datasets, i.e. warm, warmingUp, shutdown
- onSocketOpen - executed when the socket API successfully opens (so it is safe to start sending SQL queries)
- onSocketClose - executed when the socket API has closed (intentionally or not)
- onInfo - information about a query - connection time, query time, execution time, etc.
- onLogError - Log Errors, such as SQL syntax errors.
- onLogWarn - Log warning messages
- onLogInfo - Log info messages
- onLogDebug - Log debug messsages

#### Setting Global Callbacks

Global callbacks can be set when creating the BoilingData instance.

```typescript
new BoilingData({
  username,
  password,
  globalCallbacks: {
    onRequest: req => {
      console.log("A new request has been made with ID", req.requestId);
    },
    onQueryFinished: req => {
      console.log("Request complete!", req.requestId);
    },
    onLogError: message => {
      console.error("LogError", message);
    },
    onSocketOpen: socketInstance => {
      console.log("The socket has opened!");
    },
    onLambdaEvent: message => {
      console.log("Change in status of dataset: ", message);
    },
  },
});
```

#### Setting Query-level Callbacks

Query callbacks are set when creating the query

```typescript
bdInstance.execQuery({
  sql: `SELECT COUNT(*) AS count FROM parquet_scan('s3://boilingdata-demo/demo2.parquet');`,
  callbacks: {
    onData: data => {
      console.log("Some data for this query arrived", data);
    },
    onQueryFinished: () => resolve(r),
    onLogError: (data: any) => reject(data),
  },
});
```

## Using `keys`

BoilingData works best for running the same query against many files (for example, creating a historical trend from a dataset that is partitioned by date). To achieve this, you can use the `keys` array to specify a list of files to query, and the string `s3://KEY` in place of the file location in the SQL query:

```typescript
bdInstance.execQuery(
  sql: `SELECT 's3://KEY' as fileLocation, COUNT(*) as rowCount FROM parquet_scan('s3://KEY');`,
  keys: [
    "s3://bucket/data/2022-01-01.parquet",
    "s3://bucket/data/2022-01-02.parquet",
    "s3://bucket/data/2022-01-03.parquet",
  ])
```

Results are streamed as soon as they are ready, so it is unlikely that you will recieve results in the same order that you specified the files.

If you do not need to query multiple files, then you do not need to specify the keys, for instance `SELECT COUNT(*) as rowCount FROM parquet_scan('s3://bucket/data/2022-01-01.parquet');`.

You can also now query Glue (Hive) Tables:

```typescript
bdInstance.execQuery(
  sql: `SELECT 's3://KEY' as fileLocation, COUNT(*) as rowCount FROM parquet_scan('s3://KEY');`,
  keys: [ "glue.default.nyctaxis" ]
)
```
