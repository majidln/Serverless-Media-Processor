import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';

const ddbDocClient = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const tableName = process.env.TABLE_NAME;

// Each consumer owns one attribute of the item and writes nothing else, so consumers never
// overwrite each other. The condition keeps a consumer from creating an item the dispatcher
// never wrote.
export type ConsumerAttribute = 'analysis' | 'resize';

export const saveItemAttribute = async (
    key: string,
    attribute: ConsumerAttribute,
    value: Record<string, unknown>,
): Promise<void> => {
    await ddbDocClient.send(
        new UpdateCommand({
            TableName: tableName,
            Key: { id: key },
            UpdateExpression: `SET ${attribute} = :a`,
            ConditionExpression: 'attribute_exists(id)',
            ExpressionAttributeValues: { ':a': { ...value, at: new Date().toISOString() } },
        }),
    );
};
