import { S3Event } from 'aws-lambda';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { PublishCommand, SNSClient } from '@aws-sdk/client-sns';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { handler } from '../../image-dispatcher';

const ddbMock = mockClient(DynamoDBDocumentClient);
const snsMock = mockClient(SNSClient);

const s3Event = (...keys: string[]): S3Event =>
    ({
        Records: keys.map((key) => ({
            eventTime: '2026-01-01T00:00:00.000Z',
            s3: { bucket: { name: 'test-upload-bucket' }, object: { key, size: 2048 } },
        })),
    }) as unknown as S3Event;

const alreadyExists = () => new ConditionalCheckFailedException({ message: 'exists', $metadata: {} });

describe('dispatcher (image-dispatcher.ts)', () => {
    beforeEach(() => {
        ddbMock.reset();
        snsMock.reset();
        jest.spyOn(console, 'log').mockImplementation(() => undefined);
        jest.spyOn(console, 'error').mockImplementation(() => undefined);
    });

    it('saves a pending item and publishes the upload event', async () => {
        ddbMock.on(PutCommand).resolves({});
        snsMock.on(PublishCommand).resolves({});

        await handler(s3Event('photo.jpg'));

        const put = ddbMock.commandCalls(PutCommand)[0].args[0].input;
        expect(put.TableName).toBe('test-table');
        expect(put.Item).toMatchObject({
            id: 'photo.jpg',
            bucket: 'test-upload-bucket',
            size: 2048,
            analysis: { status: 'PENDING' },
        });
        expect(put.ConditionExpression).toBe('attribute_not_exists(id)');

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

    it('handles every record in the event', async () => {
        ddbMock.on(PutCommand).resolves({});
        snsMock.on(PublishCommand).resolves({});

        await handler(s3Event('a.jpg', 'b.jpg'));

        expect(ddbMock.commandCalls(PutCommand)).toHaveLength(2);
        expect(snsMock.commandCalls(PublishCommand)).toHaveLength(2);
    });

    it('fails the invocation when the DynamoDB write fails, and does not publish', async () => {
        ddbMock.on(PutCommand).rejects(new Error('ddb down'));

        await expect(handler(s3Event('photo.jpg'))).rejects.toThrow('ddb down');
        expect(snsMock.commandCalls(PublishCommand)).toHaveLength(0);
    });

    it('fails the invocation when publishing fails', async () => {
        ddbMock.on(PutCommand).resolves({});
        snsMock.on(PublishCommand).rejects(new Error('sns down'));

        await expect(handler(s3Event('photo.jpg'))).rejects.toThrow('sns down');
    });

    it('ignores a duplicate event once the analysis has finished', async () => {
        ddbMock.on(PutCommand).rejects(alreadyExists());
        ddbMock.on(GetCommand).resolves({ Item: { id: 'photo.jpg', analysis: { status: 'COMPLETE' } } });

        await handler(s3Event('photo.jpg'));

        expect(snsMock.commandCalls(PublishCommand)).toHaveLength(0);
    });

    it('publishes again on a retry when the analysis is still pending', async () => {
        ddbMock.on(PutCommand).rejects(alreadyExists());
        ddbMock.on(GetCommand).resolves({ Item: { id: 'photo.jpg', analysis: { status: 'PENDING' } } });
        snsMock.on(PublishCommand).resolves({});

        await handler(s3Event('photo.jpg'));

        expect(snsMock.commandCalls(PublishCommand)).toHaveLength(1);
    });
});
