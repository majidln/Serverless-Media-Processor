import { SQSEvent, SQSRecord } from 'aws-lambda';
import {
    DetectLabelsCommand,
    ImageTooLargeException,
    InvalidImageFormatException,
    RekognitionClient,
} from '@aws-sdk/client-rekognition';
import { saveItemAttribute } from './item-store';
import { processBatch } from './sqs-batch';

const rekogClient = new RekognitionClient({});

// Throws on any failure, so the batch can report the record for retry.
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
            await saveItemAttribute(key, 'analysis', { status: 'FAILED', error: error.name });
            return;
        }
        throw error;
    }

    await saveItemAttribute(key, 'analysis', { status: 'COMPLETE', labels });

    console.log(`Successfully updated labels for ${key}`);
};

export const handler = (event: SQSEvent) => processBatch(event, processRecord);
