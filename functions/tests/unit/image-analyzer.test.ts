import { SQSEvent } from 'aws-lambda';
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
import { handler } from '../../src/image-analyzer';

const ddbMock = mockClient(DynamoDBDocumentClient);
const rekogMock = mockClient(RekognitionClient);

// Raw SNS delivery: the SQS body is the published message itself.
const upload = (key: string) => JSON.stringify({ bucket: 'test-upload-bucket', key });

const sqsEvent = (...bodies: string[]): SQSEvent =>
    ({
        Records: bodies.map((body, i) => ({ messageId: `msg-${i + 1}`, body })),
    } as unknown as SQSEvent);

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

        const response = await handler(sqsEvent(upload('photo.jpg')));

        expect(response).toEqual({ batchItemFailures: [] });

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

        await handler(sqsEvent(upload('photo.jpg')));

        const update = ddbMock.commandCalls(UpdateCommand)[0].args[0].input;
        expect(update.ExpressionAttributeValues?.[':a']).toMatchObject({ status: 'COMPLETE', labels: [] });
    });

    it('returns the record for retry on a transient Rekognition error, and leaves the item alone', async () => {
        rekogMock.on(DetectLabelsCommand).rejects(new ThrottlingException({ message: 'slow down', $metadata: {} }));

        const response = await handler(sqsEvent(upload('photo.jpg')));

        expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'msg-1' }] });
        expect(ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
    });

    it('records FAILED instead of retrying an image Rekognition can never read', async () => {
        rekogMock
            .on(DetectLabelsCommand)
            .rejects(new InvalidImageFormatException({ message: 'bad format', $metadata: {} }));
        ddbMock.on(UpdateCommand).resolves({});

        const response = await handler(sqsEvent(upload('photo.jpg')));

        expect(response).toEqual({ batchItemFailures: [] });
        const update = ddbMock.commandCalls(UpdateCommand)[0].args[0].input;
        expect(update.ExpressionAttributeValues?.[':a']).toMatchObject({
            status: 'FAILED',
            error: 'InvalidImageFormatException',
        });
    });

    it('returns the record for retry when the item does not exist', async () => {
        rekogMock.on(DetectLabelsCommand).resolves({ Labels: [{ Name: 'Dog' }] });
        ddbMock.on(UpdateCommand).rejects(new ConditionalCheckFailedException({ message: 'no item', $metadata: {} }));

        const response = await handler(sqsEvent(upload('photo.jpg')));

        expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'msg-1' }] });
    });

    it('returns a malformed message for retry, without calling AWS', async () => {
        const response = await handler(sqsEvent('not json'));

        expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'msg-1' }] });
        expect(rekogMock.commandCalls(DetectLabelsCommand)).toHaveLength(0);
        expect(ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
    });

    it('processes the rest of a batch exactly once when one record is bad', async () => {
        rekogMock.on(DetectLabelsCommand).resolves({ Labels: [{ Name: 'Dog' }] });
        ddbMock.on(UpdateCommand).resolves({});

        const response = await handler(sqsEvent(upload('a.jpg'), 'not json', upload('c.jpg')));

        expect(response).toEqual({ batchItemFailures: [{ itemIdentifier: 'msg-2' }] });

        const updatedKeys = ddbMock.commandCalls(UpdateCommand).map((call) => call.args[0].input.Key?.id);
        expect(updatedKeys).toEqual(['a.jpg', 'c.jpg']);
        expect(rekogMock.commandCalls(DetectLabelsCommand)).toHaveLength(2);
    });

    it('reports every record that fails, and only those', async () => {
        // The later matcher wins, so the general one goes first and the specific one overrides it.
        rekogMock
            .on(DetectLabelsCommand)
            .resolves({ Labels: [{ Name: 'Dog' }] })
            .on(DetectLabelsCommand, { Image: { S3Object: { Bucket: 'test-upload-bucket', Name: 'bad.jpg' } } })
            .rejects(new ThrottlingException({ message: 'slow down', $metadata: {} }));
        ddbMock.on(UpdateCommand).resolves({});

        const response = await handler(sqsEvent(upload('bad.jpg'), upload('ok.jpg'), upload('bad.jpg')));

        expect(response.batchItemFailures).toEqual([{ itemIdentifier: 'msg-1' }, { itemIdentifier: 'msg-3' }]);
        expect(ddbMock.commandCalls(UpdateCommand)).toHaveLength(1);
    });
});
