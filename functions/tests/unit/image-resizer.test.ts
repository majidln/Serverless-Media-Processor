import { Readable } from 'stream';
import { SQSEvent } from 'aws-lambda';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { sdkStreamMixin } from '@smithy/util-stream';
import { mockClient } from 'aws-sdk-client-mock';
import sharp from 'sharp';
import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { handler } from '../../src/image-resizer';

const s3Mock = mockClient(S3Client);
const ddbMock = mockClient(DynamoDBDocumentClient);

const KEY = 'uploads/user-1/abc-123.jpg';

const message = (key = KEY) => JSON.stringify({ bucket: 'test-upload-bucket', key });

const sqsEvent = (...bodies: string[]): SQSEvent =>
    ({
        Records: bodies.map((body, i) => ({ messageId: `msg-${i + 1}`, body })),
    } as unknown as SQSEvent);

const solid = (width: number, height: number) => sharp({ create: { width, height, channels: 3, background: '#3a7' } });

const jpeg = (width: number, height: number) => solid(width, height).jpeg().toBuffer();

// Serves `original` as the uploaded object. A fresh stream per call, because a stream reads once.
const uploadedObject = (original: Buffer) =>
    s3Mock.on(GetObjectCommand).callsFake(async () => ({ Body: sdkStreamMixin(Readable.from([original])) } as never));

const writtenCopies = () =>
    s3Mock.commandCalls(PutObjectCommand).map((call) => {
        const input = call.args[0].input;
        return { key: input.Key as string, body: input.Body as Buffer, input };
    });

const dimensionsOf = async (body: Buffer) => {
    const { width, height, format } = await sharp(body).metadata();
    return { width, height, format };
};

describe('resizer (image-resizer.ts)', () => {
    beforeEach(() => {
        s3Mock.reset();
        ddbMock.reset();
        s3Mock.on(PutObjectCommand).resolves({});
        ddbMock.on(UpdateCommand).resolves({});
        jest.spyOn(console, 'log').mockImplementation(() => undefined);
        jest.spyOn(console, 'error').mockImplementation(() => undefined);
    });

    describe('resizing', () => {
        it('writes a thumb and a medium WebP copy to the processed bucket, and records them', async () => {
            uploadedObject(await jpeg(2000, 1000));

            const response = await handler(sqsEvent(message()));

            expect(response).toEqual({ batchItemFailures: [] });

            const get = s3Mock.commandCalls(GetObjectCommand)[0].args[0].input;
            expect(get).toEqual({ Bucket: 'test-upload-bucket', Key: KEY });

            const copies = writtenCopies();
            expect(copies.map((c) => c.key)).toEqual(['user-1/abc-123_thumb.webp', 'user-1/abc-123_medium.webp']);
            for (const copy of copies) {
                expect(copy.input.Bucket).toBe('test-processed-bucket');
                expect(copy.input.ContentType).toBe('image/webp');
            }

            const update = ddbMock.commandCalls(UpdateCommand)[0].args[0].input;
            expect(update.Key).toEqual({ id: KEY });
            expect(update.UpdateExpression).toBe('SET resize = :a');
            expect(update.ConditionExpression).toBe('attribute_exists(id)');
            expect(update.ExpressionAttributeValues?.[':a']).toMatchObject({
                status: 'COMPLETE',
                variants: ['user-1/abc-123_thumb.webp', 'user-1/abc-123_medium.webp'],
            });
        });

        it('fits the longest edge to 256 and 1024 and keeps the aspect ratio (SC-5)', async () => {
            uploadedObject(await jpeg(3000, 2000));

            await handler(sqsEvent(message()));

            const [thumb, medium] = await Promise.all(writtenCopies().map((c) => dimensionsOf(c.body)));
            expect(thumb).toEqual({ width: 256, height: 171, format: 'webp' });
            expect(medium).toEqual({ width: 1024, height: 683, format: 'webp' });
        });

        it('fits a portrait image by its height', async () => {
            uploadedObject(await jpeg(1000, 3000));

            await handler(sqsEvent(message()));

            const [thumb, medium] = await Promise.all(writtenCopies().map((c) => dimensionsOf(c.body)));
            expect(thumb).toEqual({ width: 85, height: 256, format: 'webp' });
            expect(medium).toEqual({ width: 341, height: 1024, format: 'webp' });
        });

        it('never enlarges a small image (SC-5)', async () => {
            uploadedObject(await jpeg(100, 50));

            await handler(sqsEvent(message()));

            const [thumb, medium] = await Promise.all(writtenCopies().map((c) => dimensionsOf(c.body)));
            expect(thumb).toEqual({ width: 100, height: 50, format: 'webp' });
            expect(medium).toEqual({ width: 100, height: 50, format: 'webp' });
        });

        it('only shrinks the thumb when the original is between the two sizes', async () => {
            uploadedObject(await jpeg(600, 400));

            await handler(sqsEvent(message()));

            const [thumb, medium] = await Promise.all(writtenCopies().map((c) => dimensionsOf(c.body)));
            expect(thumb).toEqual({ width: 256, height: 171, format: 'webp' });
            expect(medium).toEqual({ width: 600, height: 400, format: 'webp' });
        });

        it('applies the EXIF orientation, so a phone photo is not left sideways', async () => {
            // Stored as 2000x1000 with orientation 6 (rotate 90 degrees), so it displays as 1000x2000.
            const sideways = await solid(2000, 1000).jpeg().withMetadata({ orientation: 6 }).toBuffer();
            uploadedObject(sideways);

            await handler(sqsEvent(message()));

            const thumb = await dimensionsOf(writtenCopies()[0].body);
            expect(thumb).toEqual({ width: 128, height: 256, format: 'webp' });
        });

        it('reads PNG uploads and builds the key from any extension', async () => {
            uploadedObject(await solid(800, 600).png().toBuffer());

            await handler(sqsEvent(message('uploads/user-1/some-id.png')));

            expect(writtenCopies().map((c) => c.key)).toEqual([
                'user-1/some-id_thumb.webp',
                'user-1/some-id_medium.webp',
            ]);
        });
    });

    describe('a file sharp can never read (SC-1)', () => {
        it('records FAILED instead of retrying', async () => {
            uploadedObject(Buffer.from('this is not an image'));

            const response = await handler(sqsEvent(message()));

            expect(response).toEqual({ batchItemFailures: [] });
            expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(0);
            const update = ddbMock.commandCalls(UpdateCommand)[0].args[0].input;
            expect(update.ExpressionAttributeValues?.[':a']).toMatchObject({
                status: 'FAILED',
                error: 'UnreadableImage',
            });
        });

        it('refuses an image with more pixels than the limit', async () => {
            // Tiny as a file, 56 megapixels once decoded.
            uploadedObject(await solid(8000, 7000).png({ compressionLevel: 9 }).toBuffer());

            const response = await handler(sqsEvent(message()));

            expect(response).toEqual({ batchItemFailures: [] });
            expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(0);
            const update = ddbMock.commandCalls(UpdateCommand)[0].args[0].input;
            expect(update.ExpressionAttributeValues?.[':a']).toMatchObject({ status: 'FAILED' });
        }, 30_000);
    });

    describe('transient failures are retried', () => {
        it('returns the record when the original cannot be read', async () => {
            s3Mock.on(GetObjectCommand).rejects(new Error('s3 down'));

            const response = await handler(sqsEvent(message()));

            expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'msg-1' }] });
            expect(ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
        });

        it('returns the record when the object has no body', async () => {
            s3Mock.on(GetObjectCommand).resolves({});

            const response = await handler(sqsEvent(message()));

            expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'msg-1' }] });
            expect(ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
        });

        it('returns the record when a copy cannot be written, and does not mark it complete', async () => {
            uploadedObject(await jpeg(800, 600));
            s3Mock.on(PutObjectCommand).rejects(new Error('s3 write failed'));

            const response = await handler(sqsEvent(message()));

            expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'msg-1' }] });
            expect(ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
        });

        it('returns the record when the item does not exist', async () => {
            uploadedObject(await jpeg(800, 600));
            ddbMock
                .on(UpdateCommand)
                .rejects(new ConditionalCheckFailedException({ message: 'no item', $metadata: {} }));

            const response = await handler(sqsEvent(message()));

            expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'msg-1' }] });
        });

        it('returns a malformed message, without calling AWS', async () => {
            const response = await handler(sqsEvent('not json'));

            expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'msg-1' }] });
            expect(s3Mock.commandCalls(GetObjectCommand)).toHaveLength(0);
            expect(ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
        });
    });
});
