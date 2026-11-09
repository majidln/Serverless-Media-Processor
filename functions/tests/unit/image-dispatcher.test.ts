import { S3Event } from 'aws-lambda';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { PublishCommand, SNSClient } from '@aws-sdk/client-sns';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { handler } from '../../image-dispatcher';

const ddbMock = mockClient(DynamoDBDocumentClient);
const snsMock = mockClient(SNSClient);

const s3Event = (key: string, size = 1024): S3Event =>
    ({
        Records: [
            {
                eventTime: '2026-01-01T00:00:00.000Z',
                s3: { bucket: { name: 'test-upload-bucket' }, object: { key, size } },
            },
        ],
    }) as unknown as S3Event;

describe('dispatcher (image-dispatcher.ts)', () => {
    beforeEach(() => {
        ddbMock.reset();
        snsMock.reset();
        jest.spyOn(console, 'log').mockImplementation(() => undefined);
        jest.spyOn(console, 'error').mockImplementation(() => undefined);
    });

    it('saves the item and publishes the upload event', async () => {
        ddbMock.on(PutCommand).resolves({});
        snsMock.on(PublishCommand).resolves({});

        await handler(s3Event('photo.jpg', 2048));

        const put = ddbMock.commandCalls(PutCommand)[0].args[0].input;
        expect(put.TableName).toBe('test-table');
        expect(put.Item).toMatchObject({
            id: 'photo.jpg',
            bucket: 'test-upload-bucket',
            size: 2048,
            status: 'INITIALIZED',
        });

        const publish = snsMock.commandCalls(PublishCommand)[0].args[0].input;
        expect(publish.TopicArn).toBe('arn:aws:sns:us-east-1:123456789012:test-topic');
        expect(JSON.parse(publish.Message as string)).toEqual({
            bucket: 'test-upload-bucket',
            key: 'photo.jpg',
        });
    });

    it('decodes the URL-encoded S3 key', async () => {
        ddbMock.on(PutCommand).resolves({});
        snsMock.on(PublishCommand).resolves({});

        await handler(s3Event('my+photo%281%29.jpg'));

        const put = ddbMock.commandCalls(PutCommand)[0].args[0].input;
        expect(put.Item?.id).toBe('my photo(1).jpg');
    });

    // Current behavior: errors are swallowed, so Lambda reports success. Step 1 changes this.
    it('does not publish when the DynamoDB write fails, and resolves anyway', async () => {
        ddbMock.on(PutCommand).rejects(new Error('ddb down'));

        await expect(handler(s3Event('photo.jpg'))).resolves.toBeUndefined();
        expect(snsMock.commandCalls(PublishCommand)).toHaveLength(0);
    });
});
