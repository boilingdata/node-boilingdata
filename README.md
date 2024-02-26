# BoilingData WebSocket client JS/TS SDK

![CI](https://github.com/boilingdata/node-boilingdata/workflows/CI/badge.svg?branch=main)
![BuiltBy](https://img.shields.io/badge/TypeScript-Lovers-black.svg "img.shields.io")

You can use this SDK both on browser and with NodeJS.

> See also BoilingData command line client tool: [https://github.com/boilingdata/boilingdata-bdcli](https://github.com/boilingdata/boilingdata-bdcli).

## Installing the SDK

```shell
yarn add @boilingdata/node-boilingdata
```

## Browser

Copy and add `browser/boilingdata.min.js` script to your HTML.

```html
<script src="boilingdata.min.js"></script>
<script>
  const bdInstance = new BoilingData({ username: "myUsername", password: "myPw" });
  let isConnected = false;
  async function connectAndRunQuery() {
    if (!isConnected) {
      await bdInstance.connect();
      isConnected = true;
    }
    const rows = await bdInstance.execQueryPromise({ sql: "SELECT 42;" });
    console.log({ rows });
  }
  connectAndRunQuery();
</script>
```

## Basic Examples

`execQueryPromise()` method can be used to await for the results directly.

```shell
yarn install @boilingdata/node-boilingdata
# copy paste the example to example.mjs file.
BD_USERNAME=<yourBoilingEmail> BD_PASSWORD=<yourBoilingPw> node example.mjs
```

```typescript
import { BoilingData } from "@boilingdata/node-boilingdata";

async function main() {
  const bdInstance = new BoilingData({ username: process.env["BD_USERNAME"], password: process.env["BD_PASSWORD"] });
  await bdInstance.connect();
  const sql = `SELECT COUNT(*) FROM parquet_scan('s3://boilingdata-demo/demo.parquet');`;
  const rows = await bdInstance.execQueryPromise({ sql });
  console.log(JSON.parse(JSON.stringify(rows)));
  await bdInstance.close();
}

main();
```

`execQuery()` uses callbacks.

```typescript
import { BoilingData, isDataResponse } from "@boilingdata/node-boilingdata";

async function main() {
  const bdInstance = new BoilingData({ username: process.env["BD_USERNAME"], password: process.env["BD_PASSWORD"] });
  await bdInstance.connect();
  const sql = `SELECT COUNT(*) FROM parquet_scan('s3://boilingdata-demo/demo.parquet');`;
  const rows = await new Promise<any[]>((resolve, reject) => {
    let r: any[] = [];
    bdInstance.execQuery({
      sql,
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
