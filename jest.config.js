module.exports = {
  transform: {
    "^.+\\.ts$": "ts-jest",
  },
  testRegex: "(/src/.*(\\.)(test|spec))\\.(jsx?|tsx?)$",
  moduleFileExtensions: ["js", "ts"],
  collectCoverage: true,
  collectCoverageFrom: ["src/**/{!(index),}.ts"],
  coverageReporters: ["json-summary", "text", "lcov"],
};
