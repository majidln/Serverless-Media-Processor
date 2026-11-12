import {
    DeleteMessageCommand,
    GetQueueAttributesCommand,
    ReceiveMessageCommand,
    SQSClient,
    SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { CloudWatchClient, DescribeAlarmsCommand } from '@aws-sdk/client-cloudwatch';
import { beforeAll, describe, expect, it } from '@jest/globals';
import { Stack, loadStack, waitFor } from './stack';

// Slow live suite (about 8 to 10 minutes): a poison message must be retried 3 times, land in the
// DLQ, and raise the alarm. Retries are spaced by the queue's 180 s visibility timeout, which is
// why this is separate from the fast suite. Run with `npm run test:live:dlq`.

const sqs = new SQSClient({});
const cloudwatch = new CloudWatchClient({});

const POISON = `live-test-poison ${Date.now()} (not json)`;

let stack: Stack;

const visibleMessages = async (queueUrl: string): Promise<number> => {
    const { Attributes } = await sqs.send(
        new GetQueueAttributesCommand({ QueueUrl: queueUrl, AttributeNames: ['ApproximateNumberOfMessages'] }),
    );
    return Number(Attributes?.ApproximateNumberOfMessages ?? 0);
};

describe('live DLQ and alarm', () => {
    beforeAll(async () => {
        stack = await loadStack();

        // A leftover message would make the checks below ambiguous.
        expect(await visibleMessages(stack.deadLetterQueueUrl)).toBe(0);
    });

    it(
        'moves a message the analyzer cannot process to the DLQ after 3 attempts (SC-1)',
        async () => {
            await sqs.send(new SendMessageCommand({ QueueUrl: stack.analyzerQueueUrl, MessageBody: POISON }));

            await waitFor(
                'the poison message to reach the DLQ',
                async () => ((await visibleMessages(stack.deadLetterQueueUrl)) > 0 ? true : undefined),
                15 * 60_000,
                15_000,
            );
        },
        16 * 60_000,
    );

    it(
        'puts the DLQ alarm into ALARM',
        async () => {
            await waitFor(
                `alarm ${stack.deadLetterAlarm} to be in ALARM`,
                async () => {
                    const { MetricAlarms } = await cloudwatch.send(
                        new DescribeAlarmsCommand({ AlarmNames: [stack.deadLetterAlarm] }),
                    );
                    return MetricAlarms?.[0]?.StateValue === 'ALARM' ? true : undefined;
                },
                8 * 60_000,
                15_000,
            );
        },
        9 * 60_000,
    );

    it('keeps the original message in the DLQ, then removes it so the stack is left clean', async () => {
        const { Messages = [] } = await sqs.send(
            new ReceiveMessageCommand({
                QueueUrl: stack.deadLetterQueueUrl,
                MaxNumberOfMessages: 10,
                WaitTimeSeconds: 10,
            }),
        );

        expect(Messages.map((m) => m.Body)).toContain(POISON);

        for (const message of Messages.filter((m) => m.Body === POISON)) {
            await sqs.send(
                new DeleteMessageCommand({ QueueUrl: stack.deadLetterQueueUrl, ReceiptHandle: message.ReceiptHandle }),
            );
        }
    });
});
