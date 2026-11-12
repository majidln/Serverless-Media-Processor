/*
 * Live tests run against the deployed stack, using your AWS credentials.
 * They are not part of `npm test`. See `test:live` and `test:live:dlq` in package.json.
 */

export default {
    transform: {
        '^.+\\.ts?$': 'ts-jest',
    },
    setupFiles: ['<rootDir>/tests/live/setup-env.ts'],
    testMatch: ['**/tests/live/*.test.ts'],
    testTimeout: 120000,
    maxWorkers: 1,
};
