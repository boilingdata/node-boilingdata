import { BoilingData } from "../boilingdata/boilingdata";

const username = process.env["BD_USERNAME"];
const password = process.env["BD_PASSWORD"];
if (!password || !username) throw new Error("Set BD_USERNAME and BD_PASSWORD envs");

describe("boilingdata with DuckDB", () => {
  const bdInstance: BoilingData = new BoilingData({ username, password, region: "eu-west-1" });

  it("can get client TapToken with default arguments", async () => {
    const tapClientToken = await bdInstance.getTapClientToken();
    console.log({ tapClientToken });
    expect(tapClientToken.length).toBeGreaterThan(0);
  });

  it("can get client TapToken with default arguments", async () => {
    const tapClientToken = await bdInstance.getTapClientToken("24h", "dforsber@gmail.com");
    console.log({ tapClientToken });
    expect(tapClientToken.length).toBeGreaterThan(0);
  });
});
