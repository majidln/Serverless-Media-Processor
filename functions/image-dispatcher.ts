import { S3Event } from 'aws-lambda';
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { PublishCommand, SNSClient } from '@aws-sdk/client-sns';

const ddbClient = new DynamoDBClient({});
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);
const snsClient = new SNSClient({});

const tableName = process.env.TABLE_NAME;
const topicArn = process.env.TOPIC_ARN;

export const handler =async (event: S3Event): Promise<void> => {
    // This log helps you see the raw data coming from S3
    console.log("EVENT RECEIVED:", JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        try {
            const bucketName = record.s3.bucket.name;
            const fileKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
            const fileSize = record.s3.object.size;
            const eventTime = record.eventTime;

            console.log(`NEW FILE DETECTED: [${fileKey}] in bucket [${bucketName}]`);

            // Save metadata to DynamoDB
            const putParams = {
                TableName: tableName,
                Item: {
                    id: fileKey,
                    bucket: bucketName,
                    size: fileSize,
                    timestamp: eventTime,
                    status: "INITIALIZED",
                    createdAt: new Date().toISOString()
                }
            };

            await ddbDocClient.send(new PutCommand(putParams));

            console.log(`Metadata for ${fileKey} saved to table ${tableName}`);

            const messagePayload = {
                bucket: bucketName,
                key: fileKey
            }

            await snsClient.send(new PublishCommand({
                TopicArn: topicArn,
                Message: JSON.stringify(messagePayload),
                MessageAttributes: {
                    eventType: {
                        DataType: 'String',
                        StringValue: 'ObjectCreated'
                    }
                }
            }))

            console.log(`NS Notification sent for ${fileKey}`);

        } catch (error) {
            console.error("ERROR PROCESSING RECORD:", error);
        }
    }
};
