// Jest setup file for common test utilities and configurations

// Increase timeout for integration tests
jest.setTimeout(10000);

// Global test helpers
global.sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Clean up any hanging processes after tests
afterAll(async () => {
  // Give time for any async operations to complete
  await sleep(100);
});

// Handle unhandled promise rejections in tests
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

// Suppress console.log in tests unless explicitly needed
const originalConsoleLog = console.log;
const originalConsoleError = console.error;

beforeEach(() => {
  // Suppress logs unless in verbose mode
  if (!process.env.VERBOSE_TESTS) {
    console.log = jest.fn();
    console.error = jest.fn();
  }
});

afterEach(() => {
  // Restore console functions
  console.log = originalConsoleLog;
  console.error = originalConsoleError;
});