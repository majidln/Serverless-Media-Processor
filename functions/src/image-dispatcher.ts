import { S3Event } from 'aws-lambda';
import { ConditionalCheckFailedException, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { PublishCommand, SNSClient } from '@aws-sdk/client-sns';

const ddbClient = new DynamoDBClient({});
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);
const snsClient = new SNSClient({});

const tableName = process.env.TABLE_NAME;
const topicArn = process.env.TOPIC_ARN;

const publishUploadEvent = async (bucket: string, key: string): Promise<void> => {
    await snsClient.send(
        new PublishCommand({
            TopicArn: topicArn,
            Message: JSON.stringify({ bucket, key }),
            MessageAttributes: {
                eventType: {
                    DataType: 'String',
                    StringValue: 'ObjectCreated',
                },
            },
        }),
    );
};

// Errors are not caught here: a failure must fail the invocation so S3 retries it.
// Records already handled are safe to replay because the item write is conditional.
export const handler = async (event: S3Event): Promise<void> => {
    for (const record of event.Records) {
        const bucketName = record.s3.bucket.name;
        const fileKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));

        console.log(`NEW FILE DETECTED: [${fileKey}] in bucket [${bucketName}]`);

        try {
            // Never overwrite an existing item: a duplicate event must not reset finished work.
            await ddbDocClient.send(
                new PutCommand({
                    TableName: tableName,
                    Item: {
                        id: fileKey,
                        bucket: bucketName,
                        size: record.s3.object.size,
                        timestamp: record.eventTime,
                        createdAt: new Date().toISOString(),
                        analysis: { status: 'PENDING' },
                        resize: { status: 'PENDING' },
                    },
                    ConditionExpression: 'attribute_not_exists(id)',
                }),
            );

            console.log(`Metadata for ${fileKey} saved to table ${tableName}`);
        } catch (error) {
            if (!(error instanceof ConditionalCheckFailedException)) {
                throw error;
            }

            // The item exists: either a duplicate event, or a retry after the publish below failed.
            // Publish again only while a consumer hasn't finished, so a retry can't lose the event.
            const { Item } = await ddbDocClient.send(
                new GetCommand({
                    TableName: tableName,
                    Key: { id: fileKey },
                    ConsistentRead: true,
                }),
            );

            const unfinished = Item?.analysis?.status === 'PENDING' || Item?.resize?.status === 'PENDING';
            if (!unfinished) {
                console.log(`Duplicate event for ${fileKey}, already processed`);
                continue;
            }

            console.log(`Item for ${fileKey} exists but is still being processed, publishing again`);
        }

        await publishUploadEvent(bucketName, fileKey);

        console.log(`SNS notification sent for ${fileKey}`);
    }
};
