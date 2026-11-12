import { readFileSync } from 'fs';
import { join } from 'path';
import { DeleteObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DeleteCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { afterAll, beforeAll, describe, expect, it } from '@jest/globals';
import { Stack, getItem, loadStack, pause, requestUploadUrl, waitFor } from './stack';

// Fast live suite (about a minute): the upload API and the whole upload -> label pipeline.
// Run with `npm run test:live`. Put your own photo in LIVE_TEST_IMAGE for real labels.

const s3 = new S3Client({});
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const SUB = `live-test-${Date.now()}`;
const image = readFileSync(process.env.LIVE_TEST_IMAGE ?? join(__dirname, 'fixtures', 'sample.jpg'));
const declared = { filename: 'sample.jpg', contentType: 'image/jpeg', size: image.length };

const PIPELINE_TIMEOUT_MS = 90_000;
// Long enough for a wrongly-handled event to show up, short enough to keep the suite quick.
const QUIET_PERIOD_MS = 20_000;

const keysToClean = new Set<string>();
const objectsToClean = new Set<string>();

let stack: Stack;

const putTo = (uploadUrl: string, body: Buffer, contentType = 'image/jpeg') =>
    fetch(uploadUrl, { method: 'PUT', headers: { 'Content-Type': contentType }, body });

const newUpload = async (body: object = declared) => {
    const result = await requestUploadUrl(stack, SUB, body);
    expect(result.statusCode).toBe(200);
    keysToClean.add(result.body.key);
    return result.body as { uploadUrl: string; key: string };
};

describe('live pipeline', () => {
    beforeAll(async () => {
        stack = await loadStack();
    });

    afterAll(async () => {
        for (const id of keysToClean) {
            await s3.send(new DeleteObjectCommand({ Bucket: stack.bucket, Key: id }));
            await ddb.send(new DeleteCommand({ TableName: stack.table, Key: { id } }));
        }
        for (const key of objectsToClean) {
            await s3.send(new DeleteObjectCommand({ Bucket: stack.bucket, Key: key }));
        }
    });

    describe('upload API', () => {
        it('rejects a request without a token with 401', async () => {
            const response = await fetch(`${stack.apiUrl}/upload`, { method: 'POST', body: JSON.stringify(declared) });

            expect(response.status).toBe(401);
        });

        it('returns a key under the caller, and a different key each time (SC-2)', async () => {
            const first = await newUpload();
            const second = await newUpload();

            expect(first.key).toMatch(new RegExp(`^uploads/${SUB}/[0-9a-f-]{36}\\.jpg$`));
            expect(second.key).not.toBe(first.key);
        });

        it('rejects an unsupported type with 400 (SC-3)', async () => {
            const result = await requestUploadUrl(stack, SUB, {
                filename: 'a.gif',
                contentType: 'image/gif',
                size: 10,
            });

            expect(result.statusCode).toBe(400);
        });

        it('rejects an oversized file with 400 (SC-3)', async () => {
            const result = await requestUploadUrl(stack, SUB, { ...declared, size: 11 * 1024 * 1024 });

            expect(result.statusCode).toBe(400);
        });
    });

    describe('S3 enforces what was signed (SC-3)', () => {
        it('rejects a PUT with a different content type', async () => {
            const { uploadUrl } = await newUpload();

            const response = await putTo(uploadUrl, image, 'image/png');

            expect(response.status).toBe(403);
        });

        it('rejects a PUT whose length differs from the declared size', async () => {
            const { uploadUrl } = await newUpload();

            const response = await putTo(uploadUrl, Buffer.concat([image, Buffer.from('extra')]));

            expect(response.status).toBe(403);
        });
    });

    describe('upload -> dispatcher -> SNS -> SQS -> analyzer -> DynamoDB', () => {
        let key: string;
        let uploadUrl: string;
        let analyzedAt: string;
        let createdAt: string;

        it('completes the analysis of a valid image', async () => {
            ({ key, uploadUrl } = await newUpload());

            const response = await putTo(uploadUrl, image);
            expect(response.status).toBe(200);

            const item = await waitFor(
                `analysis of ${key} to complete`,
                async () => {
                    const found = await getItem(stack, key);
                    return found?.analysis?.status === 'COMPLETE' ? found : undefined;
                },
                PIPELINE_TIMEOUT_MS,
            );

            expect(item).toMatchObject({ id: key, bucket: stack.bucket, size: image.length });
            expect(Array.isArray(item.analysis.labels)).toBe(true);
            analyzedAt = item.analysis.at;
            createdAt = item.createdAt;
        });

        it(
            'leaves a finished item alone when the same upload event arrives again (SC-7)',
            async () => {
                // The signed URL is still valid, and a second PUT fires a second ObjectCreated event.
                const response = await putTo(uploadUrl, image);
                expect(response.status).toBe(200);

                await pause(QUIET_PERIOD_MS);

                const item = await getItem(stack, key);
                expect(item?.analysis.status).toBe('COMPLETE');
                expect(item?.analysis.at).toBe(analyzedAt);
                expect(item?.createdAt).toBe(createdAt);
            },
            QUIET_PERIOD_MS + 60_000,
        );
    });

    describe('failure handling', () => {
        it('records FAILED, without retrying, for a file Rekognition cannot read (SC-1)', async () => {
            const notAnImage = Buffer.from('this is not an image');
            const { uploadUrl, key } = await newUpload({ ...declared, size: notAnImage.length });

            const response = await putTo(uploadUrl, notAnImage);
            expect(response.status).toBe(200);

            const item = await waitFor(
                `analysis of ${key} to fail`,
                async () => {
                    const found = await getItem(stack, key);
                    return found?.analysis?.status === 'FAILED' ? found : undefined;
                },
                PIPELINE_TIMEOUT_MS,
            );

            expect(item.analysis.error).toBe('InvalidImageFormatException');
        });

        it(
            'ignores objects outside the uploads/ prefix (SC-6)',
            async () => {
                const key = `not-uploads/${SUB}/ignored.jpg`;
                objectsToClean.add(key);
                keysToClean.add(key);

                await s3.send(
                    new PutObjectCommand({ Bucket: stack.bucket, Key: key, Body: image, ContentType: 'image/jpeg' }),
                );
                await pause(QUIET_PERIOD_MS);

                expect(await getItem(stack, key)).toBeUndefined();
            },
            QUIET_PERIOD_MS + 60_000,
        );
    });
});
