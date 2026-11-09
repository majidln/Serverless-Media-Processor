import { SNSEvent } from 'aws-lambda';
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { DetectLabelsCommand, RekognitionClient } from '@aws-sdk/client-rekognition';

const ddbClient = new DynamoDBClient({});
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);
const rekogClient = new RekognitionClient({});

const tableName = process.env.TABLE_NAME;

export const handler = async (event: SNSEvent): Promise<void> => {
    // This log helps you see the raw data coming from S3
    console.log("SNS EVENT RECEIVED:", JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        try {
            const message = JSON.parse(record.Sns.Message);
            const { bucket, key } = message;

            console.log(`Analyzer processing: ${key} from ${bucket}`);

            const detectParams = new DetectLabelsCommand({
                Image: { S3Object: { Bucket: bucket, Name: key } },
                MaxLabels: 5,
                MinConfidence: 70,
            });

            const rekogResponse = await rekogClient.send(detectParams);
            const labels = rekogResponse.Labels?.map(label => label.Name) || [];


            // Save metadata to DynamoDB
            const updateParams = {
                TableName: tableName,
                Key: { id: key },
                UpdateExpression: "set labels = :l, #s = :status, analyzedAt = :t",
                ExpressionAttributeNames: { "#s": "status" },
                ExpressionAttributeValues: {
                    ":l": labels,
                    ":status": "ANALYSIS_COMPLETE",
                    ":t": new Date().toISOString()
                }
            };

            await ddbDocClient.send(new UpdateCommand(updateParams));

            console.log(`Successfully updated labels for ${key}`);

        } catch (error) {
            console.error("ERROR PROCESSING RECORD:", error);
        }
    }
};
