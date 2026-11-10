import { APIGatewayProxyEventV2WithJWTAuthorizer } from 'aws-lambda';
import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { handler } from '../../get-upload-url';

const USER_ID = '11111111-2222-3333-4444-555555555555';
const KEY_PATTERN = new RegExp(`^uploads/${USER_ID}/[0-9a-f-]{36}\\.(jpg|png)$`);

const validBody = { filename: 'photo.jpg', contentType: 'image/jpeg', size: 2048 };

const call = (body?: unknown, claims: object = { sub: USER_ID }) =>
    handler({
        body: typeof body === 'string' || body === undefined ? body : JSON.stringify(body),
        requestContext: { authorizer: { jwt: { claims } } },
    } as unknown as APIGatewayProxyEventV2WithJWTAuthorizer);

const messageOf = (result: { body?: string }) => JSON.parse(result.body as string).message as string;

describe('upload URL (get-upload-url.ts)', () => {
    beforeEach(() => {
        jest.spyOn(console, 'error').mockImplementation(() => undefined);
    });

    describe('valid requests', () => {
        it('returns a presigned PUT URL for a key under the caller', async () => {
            const result = await call(validBody);

            expect(result.statusCode).toBe(200);
            const body = JSON.parse(result.body as string);
            expect(body.key).toMatch(KEY_PATTERN);

            const url = new URL(body.uploadUrl);
            expect(url.hostname).toContain('test-upload-bucket');
            expect(decodeURIComponent(url.pathname)).toBe(`/${body.key}`);
            expect(url.searchParams.get('X-Amz-Expires')).toBe('300');
        });

        it('signs the content type and length so S3 enforces them', async () => {
            const result = await call(validBody);

            const url = new URL(JSON.parse(result.body as string).uploadUrl);
            expect(url.searchParams.get('X-Amz-SignedHeaders')).toBe('content-length;content-type;host');
        });

        it('gives two uploads of the same filename two different keys (SC-2)', async () => {
            const first = JSON.parse((await call(validBody)).body as string).key;
            const second = JSON.parse((await call(validBody)).body as string).key;

            expect(first).not.toBe(second);
        });

        it('keeps different users apart', async () => {
            const key = JSON.parse((await call(validBody, { sub: 'other-user' })).body as string).key;

            expect(key.startsWith('uploads/other-user/')).toBe(true);
        });

        it('never uses the client filename in the key', async () => {
            const result = await call({ ...validBody, filename: '../../etc/passwd.jpg' });

            expect(result.statusCode).toBe(200);
            expect(JSON.parse(result.body as string).key).toMatch(KEY_PATTERN);
        });

        it('accepts png, and .jpeg or upper-case extensions', async () => {
            const png = await call({ filename: 'shot.png', contentType: 'image/png', size: 10 });
            expect(JSON.parse(png.body as string).key).toMatch(/\.png$/);

            const jpeg = await call({ filename: 'PHOTO.JPEG', contentType: 'image/jpeg', size: 10 });
            expect(JSON.parse(jpeg.body as string).key).toMatch(/\.jpg$/);
        });

        it('accepts a file of exactly the maximum size', async () => {
            const result = await call({ ...validBody, size: 10 * 1024 * 1024 });

            expect(result.statusCode).toBe(200);
        });
    });

    describe('rejected requests (SC-3)', () => {
        it.each([
            ['an unsupported content type', { ...validBody, contentType: 'image/gif', filename: 'a.gif' }, /contentType/],
            ['a prototype property as content type', { ...validBody, contentType: 'constructor' }, /contentType/],
            ['a missing content type', { filename: 'a.jpg', size: 10 }, /contentType/],
            ['a missing filename', { contentType: 'image/jpeg', size: 10 }, /filename is required/],
            ['an empty filename', { ...validBody, filename: '' }, /filename is required/],
            ['a filename with no extension', { ...validBody, filename: 'photo' }, /extension/],
            ['an extension that does not match the type', { ...validBody, filename: 'photo.png' }, /extension/],
            ['a missing size', { filename: 'a.jpg', contentType: 'image/jpeg' }, /size/],
            ['a size given as a string', { ...validBody, size: '2048' }, /size/],
            ['a size of zero', { ...validBody, size: 0 }, /size/],
            ['a negative size', { ...validBody, size: -5 }, /size/],
            ['a fractional size', { ...validBody, size: 10.5 }, /size/],
            ['a file over the maximum size', { ...validBody, size: 10 * 1024 * 1024 + 1 }, /size/],
        ])('returns 400 for %s', async (_name, body, expected) => {
            const result = await call(body);

            expect(result.statusCode).toBe(400);
            expect(messageOf(result)).toMatch(expected);
        });

        it('returns 400 for a malformed body', async () => {
            const result = await call('{not json');

            expect(result.statusCode).toBe(400);
            expect(messageOf(result)).toMatch(/valid JSON/);
        });

        it('returns 400 when the body is not an object', async () => {
            const result = await call('null');

            expect(result.statusCode).toBe(400);
        });

        it('returns 400 when there is no body', async () => {
            const result = await call();

            expect(result.statusCode).toBe(400);
        });
    });

    describe('identity', () => {
        it.each([
            ['missing', {}],
            ['empty', { sub: '' }],
            ['not a string', { sub: 42 }],
        ])('returns 401 when the sub claim is %s', async (_name, claims) => {
            const result = await call(validBody, claims);

            expect(result.statusCode).toBe(401);
        });

        it('returns 401 when the request has no authorizer context at all', async () => {
            const result = await handler({ body: JSON.stringify(validBody) } as unknown as APIGatewayProxyEventV2WithJWTAuthorizer);

            expect(result.statusCode).toBe(401);
        });
    });
});
