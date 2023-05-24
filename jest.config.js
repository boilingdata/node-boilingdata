module.exports = {
  transform: {
    "^.+\\.(t|j)sx?$": ["@swc/jest"],
  },
  testRegex: "(/src/.*(\\.)(test|spec))\\.(jsx?|tsx?)$",
  moduleFileExtensions: ["js", "ts"],
  collectCoverage: false,
  collectCoverageFrom: ["src/**/{!(index),}.ts"],
  coverageReporters: [], // ["json-summary", "text", "lcov"],
};
