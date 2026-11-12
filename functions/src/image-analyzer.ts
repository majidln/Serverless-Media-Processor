import { SQSBatchItemFailure, SQSBatchResponse, SQSEvent, SQSRecord } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import {
    DetectLabelsCommand,
    ImageTooLargeException,
    InvalidImageFormatException,
    RekognitionClient,
} from '@aws-sdk/client-rekognition';

const ddbClient = new DynamoDBClient({});
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);
const rekogClient = new RekognitionClient({});

const tableName = process.env.TABLE_NAME;

// This function owns the `analysis` attribute and writes nothing else.
// The condition keeps it from creating an item the dispatcher never wrote.
const saveAnalysis = async (key: string, analysis: Record<string, unknown>): Promise<void> => {
    await ddbDocClient.send(
        new UpdateCommand({
            TableName: tableName,
            Key: { id: key },
            UpdateExpression: 'SET analysis = :a',
            ConditionExpression: 'attribute_exists(id)',
            ExpressionAttributeValues: { ':a': { ...analysis, at: new Date().toISOString() } },
        }),
    );
};

// Throws on any failure, so the caller can report the record for retry.
// The one exception is an image Rekognition can never read; retrying would not help,
// so it is recorded as FAILED on the item.
const processRecord = async (record: SQSRecord): Promise<void> => {
    // The SNS subscription uses raw delivery, so the body is the message itself.
    const { bucket, key } = JSON.parse(record.body);

    console.log(`Analyzer processing: ${key} from ${bucket}`);

    let labels: (string | undefined)[];
    try {
        const rekogResponse = await rekogClient.send(
            new DetectLabelsCommand({
                Image: { S3Object: { Bucket: bucket, Name: key } },
                MaxLabels: 5,
                MinConfidence: 70,
            }),
        );
        labels = rekogResponse.Labels?.map((label) => label.Name) || [];
    } catch (error) {
        if (error instanceof InvalidImageFormatException || error instanceof ImageTooLargeException) {
            console.error(`Image ${key} cannot be analyzed: ${error.name}`);
            await saveAnalysis(key, { status: 'FAILED', error: error.name });
            return;
        }
        throw error;
    }

    await saveAnalysis(key, { status: 'COMPLETE', labels });

    console.log(`Successfully updated labels for ${key}`);
};

// Partial batch response: only the records that failed go back to the queue (and to the DLQ
// after maxReceiveCount), so one bad record doesn't make the rest of the batch run again.
export const handler = async (event: SQSEvent): Promise<SQSBatchResponse> => {
    const batchItemFailures: SQSBatchItemFailure[] = [];

    for (const record of event.Records) {
        try {
            await processRecord(record);
        } catch (error) {
            console.error(`Record ${record.messageId} failed, returning it to the queue:`, error);
            batchItemFailures.push({ itemIdentifier: record.messageId });
        }
    }

    return { batchItemFailures };
};
