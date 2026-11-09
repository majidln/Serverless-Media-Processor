import { SNSEvent } from 'aws-lambda';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { DetectLabelsCommand, RekognitionClient } from '@aws-sdk/client-rekognition';
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

    it('detects labels and stores them on the item', async () => {
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
        expect(update.ExpressionAttributeValues).toMatchObject({
            ':l': ['Dog', 'Grass'],
            ':status': 'ANALYSIS_COMPLETE',
        });
    });

    it('stores an empty label list when Rekognition finds nothing', async () => {
        rekogMock.on(DetectLabelsCommand).resolves({});
        ddbMock.on(UpdateCommand).resolves({});

        await handler(snsEvent(uploaded));

        const update = ddbMock.commandCalls(UpdateCommand)[0].args[0].input;
        expect(update.ExpressionAttributeValues?.[':l']).toEqual([]);
    });

    // Current behavior: errors are swallowed, so Lambda reports success. Step 1 changes this.
    it('does not update the item when Rekognition fails, and resolves anyway', async () => {
        rekogMock.on(DetectLabelsCommand).rejects(new Error('unsupported image'));

        await expect(handler(snsEvent(uploaded))).resolves.toBeUndefined();
        expect(ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
    });

    it('skips a malformed message without calling AWS', async () => {
        await expect(handler(snsEvent('not json'))).resolves.toBeUndefined();
        expect(rekogMock.commandCalls(DetectLabelsCommand)).toHaveLength(0);
        expect(ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
    });
});
