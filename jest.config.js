module.exports = {
  testEnvironment: 'node',
  testMatch: ['**/tests/**/*.test.js'],
  collectCoverageFrom: [
    '*.js',
    '!jest.config.js',
    '!start.sh',
    '!stop.sh'
  ],
  coverageDirectory: 'coverage',
  coverageReporters: ['text', 'lcov', 'html'],
  testTimeout: 10000, // 10 seconds for integration tests
  setupFilesAfterEnv: ['<rootDir>/tests/setup.js'],
  verbose: true
};