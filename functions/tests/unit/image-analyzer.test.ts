import { SNSEvent } from 'aws-lambda';
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import {
    DetectLabelsCommand,
    InvalidImageFormatException,
    RekognitionClient,
    ThrottlingException,
} from '@aws-sdk/client-rekognition';
import { mockClient } from 'aws-sdk-client-mock';
import { beforeEach, describe, expect, it, jest } from '@jest/globals';
import { handler } from '../../image-analyzer';

const ddbMock = mockClient(DynamoDBDocumentClient);
const rekogMock = mockClient(RekognitionClient);

const snsEvent = (message: string): SNSEvent =>
    ({ Records: [{ Sns: { Message: message } }] }) as unknown as SNSEvent;

const uploaded = JSON.stringify({ bucket: 'test-upload-bucket', key: 'photo.jpg' });

describe('analyzer (image-analyzer.ts)', () => {
    beforeEach(() => {
        ddbMock.reset();
        rekogMock.reset();
        jest.spyOn(console, 'log').mockImplementation(() => undefined);
        jest.spyOn(console, 'error').mockImplementation(() => undefined);
    });

    it('detects labels and stores them in the analysis attribute', async () => {
        rekogMock.on(DetectLabelsCommand).resolves({ Labels: [{ Name: 'Dog' }, { Name: 'Grass' }] });
        ddbMock.on(UpdateCommand).resolves({});

        await handler(snsEvent(uploaded));

        const detect = rekogMock.commandCalls(DetectLabelsCommand)[0].args[0].input;
        expect(detect.Image?.S3Object).toEqual({ Bucket: 'test-upload-bucket', Name: 'photo.jpg' });
        expect(detect.MaxLabels).toBe(5);
        expect(detect.MinConfidence).toBe(70);

        const update = ddbMock.commandCalls(UpdateCommand)[0].args[0].input;
        expect(update.TableName).toBe('test-table');
        expect(update.Key).toEqual({ id: 'photo.jpg' });
        expect(update.UpdateExpression).toBe('SET analysis = :a');
        expect(update.ConditionExpression).toBe('attribute_exists(id)');
        expect(update.ExpressionAttributeValues?.[':a']).toMatchObject({
            status: 'COMPLETE',
            labels: ['Dog', 'Grass'],
        });
    });

    it('stores an empty label list when Rekognition finds nothing', async () => {
        rekogMock.on(DetectLabelsCommand).resolves({});
        ddbMock.on(UpdateCommand).resolves({});

        await handler(snsEvent(uploaded));

        const update = ddbMock.commandCalls(UpdateCommand)[0].args[0].input;
        expect(update.ExpressionAttributeValues?.[':a']).toMatchObject({ status: 'COMPLETE', labels: [] });
    });

    it('fails the invocation on a transient Rekognition error, and leaves the item alone', async () => {
        rekogMock.on(DetectLabelsCommand).rejects(
            new ThrottlingException({ message: 'slow down', $metadata: {} }),
        );

        await expect(handler(snsEvent(uploaded))).rejects.toThrow('slow down');
        expect(ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
    });

    it('records FAILED instead of retrying an image Rekognition can never read', async () => {
        rekogMock.on(DetectLabelsCommand).rejects(
            new InvalidImageFormatException({ message: 'bad format', $metadata: {} }),
        );
        ddbMock.on(UpdateCommand).resolves({});

        await expect(handler(snsEvent(uploaded))).resolves.toBeUndefined();

        const update = ddbMock.commandCalls(UpdateCommand)[0].args[0].input;
        expect(update.ExpressionAttributeValues?.[':a']).toMatchObject({
            status: 'FAILED',
            error: 'InvalidImageFormatException',
        });
    });

    it('fails the invocation when the item does not exist', async () => {
        rekogMock.on(DetectLabelsCommand).resolves({ Labels: [{ Name: 'Dog' }] });
        ddbMock.on(UpdateCommand).rejects(
            new ConditionalCheckFailedException({ message: 'no item', $metadata: {} }),
        );

        await expect(handler(snsEvent(uploaded))).rejects.toThrow('no item');
    });

    it('fails the invocation on a malformed message, without calling AWS', async () => {
        await expect(handler(snsEvent('not json'))).rejects.toThrow();
        expect(rekogMock.commandCalls(DetectLabelsCommand)).toHaveLength(0);
        expect(ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
    });
});
