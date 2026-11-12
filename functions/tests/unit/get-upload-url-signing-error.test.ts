import { APIGatewayProxyEventV2WithJWTAuthorizer } from 'aws-lambda';
import { describe, expect, it, jest } from '@jest/globals';

jest.mock('@aws-sdk/s3-request-presigner', () => ({
    getSignedUrl: jest.fn(async () => {
        throw new Error('signing failed');
    }),
}));

import { handler } from '../../src/get-upload-url';

describe('upload URL signing failure (get-upload-url.ts)', () => {
    it('returns 500 without leaking the error', async () => {
        jest.spyOn(console, 'error').mockImplementation(() => undefined);

        const result = await handler({
            body: JSON.stringify({ filename: 'photo.jpg', contentType: 'image/jpeg', size: 2048 }),
            requestContext: { authorizer: { jwt: { claims: { sub: 'user-1' } } } },
        } as unknown as APIGatewayProxyEventV2WithJWTAuthorizer);

        expect(result.statusCode).toBe(500);
        expect(result.body).toBe(JSON.stringify({ message: 'Could not generate upload URL' }));
    });
});
