import {
    CloudFormationClient,
    DescribeStackResourcesCommand,
    DescribeStacksCommand,
} from '@aws-sdk/client-cloudformation';
import { InvokeCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb';

export interface Stack {
    apiUrl: string;
    bucket: string;
    processedBucket: string;
    table: string;
    uploadFunction: string;
    analyzerQueueUrl: string;
    resizerQueueUrl: string;
    analyzerDeadLetterQueueUrl: string;
    resizerDeadLetterQueueUrl: string;
    analyzerDeadLetterAlarm: string;
    resizerDeadLetterAlarm: string;
}

const lambda = new LambdaClient({});
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));

// Everything is looked up from the deployed stack, so the tests never hold resource names.
export const loadStack = async (): Promise<Stack> => {
    const stackName = process.env.STACK_NAME ?? 'darkroom';
    const cfn = new CloudFormationClient({});

    const { StackResources = [] } = await cfn.send(new DescribeStackResourcesCommand({ StackName: stackName }));
    const physicalId = (logicalId: string): string => {
        const id = StackResources.find((r) => r.LogicalResourceId === logicalId)?.PhysicalResourceId;
        if (!id) throw new Error(`Resource ${logicalId} not found in stack ${stackName}`);
        return id;
    };

    const { Stacks = [] } = await cfn.send(new DescribeStacksCommand({ StackName: stackName }));
    const apiUrl = Stacks[0]?.Outputs?.find((o) => o.OutputKey === 'ApiUrl')?.OutputValue;
    if (!apiUrl) throw new Error(`Output ApiUrl not found in stack ${stackName}`);

    return {
        apiUrl,
        bucket: physicalId('UploadBucket'),
        processedBucket: physicalId('ProcessedBucket'),
        table: physicalId('ImageMetadataTable'),
        uploadFunction: physicalId('GetUploadUrlFunction'),
        analyzerQueueUrl: physicalId('AnalyzerQueue'),
        resizerQueueUrl: physicalId('ResizerQueue'),
        analyzerDeadLetterQueueUrl: physicalId('AnalyzerDeadLetterQueue'),
        resizerDeadLetterQueueUrl: physicalId('ResizerDeadLetterQueue'),
        analyzerDeadLetterAlarm: physicalId('AnalyzerDeadLetterQueueAlarm'),
        resizerDeadLetterAlarm: physicalId('ResizerDeadLetterQueueAlarm'),
    };
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Polls until `check` returns something other than undefined, or fails with a readable message.
export const waitFor = async <T>(
    description: string,
    check: () => Promise<T | undefined>,
    timeoutMs: number,
    intervalMs = 3000,
): Promise<T> => {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
        const value = await check();
        if (value !== undefined) return value;
        if (Date.now() > deadline) {
            throw new Error(`Timed out after ${Math.round(timeoutMs / 1000)}s waiting for ${description}`);
        }
        await sleep(intervalMs);
    }
};

export const pause = sleep;

// The upload Lambda is called directly with the event API Gateway would send after the JWT
// authorizer passed. That tests the deployed code without needing a Cognito user.
export const requestUploadUrl = async (stack: Stack, sub: string, body: object) => {
    const response = await lambda.send(
        new InvokeCommand({
            FunctionName: stack.uploadFunction,
            Payload: Buffer.from(
                JSON.stringify({
                    body: JSON.stringify(body),
                    requestContext: { authorizer: { jwt: { claims: { sub } } } },
                }),
            ),
        }),
    );
    if (response.FunctionError) {
        throw new Error(`Upload function failed: ${Buffer.from(response.Payload ?? []).toString()}`);
    }
    const result = JSON.parse(Buffer.from(response.Payload ?? []).toString());
    return { statusCode: result.statusCode as number, body: JSON.parse(result.body) };
};

export const getItem = async (stack: Stack, id: string): Promise<Record<string, any> | undefined> => {
    const { Item } = await ddb.send(new GetCommand({ TableName: stack.table, Key: { id }, ConsistentRead: true }));
    return Item;
};
