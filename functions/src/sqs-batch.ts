import { SQSBatchItemFailure, SQSBatchResponse, SQSEvent, SQSRecord } from 'aws-lambda';

// Partial batch response: only the records that failed go back to the queue (and to the DLQ
// after maxReceiveCount), so one bad record doesn't make the rest of the batch run again.
export const processBatch = async (
    event: SQSEvent,
    processRecord: (record: SQSRecord) => Promise<void>,
): Promise<SQSBatchResponse> => {
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
