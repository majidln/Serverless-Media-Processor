import { APIGatewayProxyEventV2 } from 'aws-lambda';
import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { handler } from '../../get-upload-url';

const apiEvent = (body?: string): APIGatewayProxyEventV2 => ({ body }) as unknown as APIGatewayProxyEventV2;

// The handler's return type is a union that includes plain strings; these tests always get an object.
const call = async (body?: string) => {
    const result = await handler(apiEvent(body));
    if (typeof result === 'string') throw new Error('expected a structured response');
    return result;
};

describe('upload URL (get-upload-url.ts)', () => {
    beforeEach(() => {
        jest.spyOn(console, 'error').mockImplementation(() => undefined);
    });

    it('returns a presigned PUT URL for the requested filename', async () => {
        const result = await call(JSON.stringify({ filename: 'photo.jpg' }));

        expect(result.statusCode).toBe(200);
        const body = JSON.parse(result.body as string);
        expect(body.key).toBe('photo.jpg');

        const url = new URL(body.uploadUrl);
        expect(url.hostname).toContain('test-upload-bucket');
        expect(url.pathname).toBe('/photo.jpg');
        expect(url.searchParams.get('X-Amz-Expires')).toBe('300');
    });

    it('generates a filename when none is given', async () => {
        const result = await call();

        expect(result.statusCode).toBe(200);
        expect(JSON.parse(result.body as string).key).toMatch(/^upload-\d+\.jpg$/);
    });

    // Current behavior: malformed JSON is reported as a server error. Step 2 changes this to 400.
    it('returns 500 for a malformed body', async () => {
        const result = await call('{not json');

        expect(result.statusCode).toBe(500);
    });
});
