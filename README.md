# BoilingData JS/TS SDK

![CI](https://github.com/boilingdata/node-boilingdata/workflows/CI/badge.svg?branch=main)
![BuiltBy](https://img.shields.io/badge/TypeScript-Lovers-black.svg "img.shields.io")

```shell
yarn add @boilingdata/node-boilingdata
```

```typescript
import { BoilingData, isDataResponse } from "../boilingdata/boilingdata";

async function main() {
  const bdInstance = new BoilingData({ process.env["BD_USERNAME"], process.env["BD_PASSWORD"] });
  await bdInstance.connect();
  const rows = await new Promise<any[]>((resolve, reject) => {
    let r: any[] = [];
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
  await bdInstance.close();
}
```

This repository contains JS/TS BoilingData SDK. Please see the integration tests on `tests/query.test.ts` for for more examples.
