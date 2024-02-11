const path = require("path");

module.exports = {
  entry: "./src/browser.ts",
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: "ts-loader",
        exclude: /node_modules/,
      },
    ],
  },
  resolve: {
    extensions: [".tsx", ".ts", ".js"],
  },
  target: "web",
  mode: "production",
  output: {
    path: path.resolve(__dirname, "./browser"),
    filename: "boilingdata.min.js",
    libraryTarget: "umd",
    globalObject: "this",
    umdNamedDefine: true,
    libraryExport: "default",
    library: "BoilingData",
  },
};
