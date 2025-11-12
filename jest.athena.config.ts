module.exports = {
  ...require('./jest.config.ts'),
  roots: [
    '<rootDir>/packages/malloy-db-athena/src/',
    '<rootDir>/test/src/databases/all/',
    '<rootDir>/test/src/databases/presto-trino/',
  ],
};
