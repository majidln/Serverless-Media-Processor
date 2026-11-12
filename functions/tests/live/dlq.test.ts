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

// Slow live suite (about 25 minutes): a poison message must be retried 3 times, land in the DLQ,
// and raise the alarm, for each consumer. Retries are spaced by the queue's visibility timeout
// (180 s for the analyzer, 360 s for the resizer), which is why this is separate from the fast
// suite. Both consumers run side by side, so it takes as long as the slower one.
// Run with `npm run test:live:dlq`.

const sqs = new SQSClient({});
const cloudwatch = new CloudWatchClient({});

const RUN = Date.now();

let stack: Stack;

interface Consumer {
    name: string;
    queueUrl: string;
    deadLetterQueueUrl: string;
    alarm: string;
    poison: string;
}

let consumers: Consumer[];

const visibleMessages = async (queueUrl: string): Promise<number> => {
    const { Attributes } = await sqs.send(
        new GetQueueAttributesCommand({ QueueUrl: queueUrl, AttributeNames: ['ApproximateNumberOfMessages'] }),
    );
    return Number(Attributes?.ApproximateNumberOfMessages ?? 0);
};

describe('live DLQ and alarm', () => {
    beforeAll(async () => {
        stack = await loadStack();
        consumers = [
            {
                name: 'analyzer',
                queueUrl: stack.analyzerQueueUrl,
                deadLetterQueueUrl: stack.analyzerDeadLetterQueueUrl,
                alarm: stack.analyzerDeadLetterAlarm,
                poison: `live-test-poison analyzer ${RUN} (not json)`,
            },
            {
                name: 'resizer',
                queueUrl: stack.resizerQueueUrl,
                deadLetterQueueUrl: stack.resizerDeadLetterQueueUrl,
                alarm: stack.resizerDeadLetterAlarm,
                poison: `live-test-poison resizer ${RUN} (not json)`,
            },
        ];

        // A leftover message would make the checks below ambiguous.
        for (const consumer of consumers) {
            expect(await visibleMessages(consumer.deadLetterQueueUrl)).toBe(0);
        }
    });

    it(
        'moves a message a consumer cannot process to its DLQ after 3 attempts (SC-1)',
        async () => {
            await Promise.all(
                consumers.map((c) => sqs.send(new SendMessageCommand({ QueueUrl: c.queueUrl, MessageBody: c.poison }))),
            );

            await Promise.all(
                consumers.map((c) =>
                    waitFor(
                        `the ${c.name} poison message to reach its DLQ`,
                        async () => ((await visibleMessages(c.deadLetterQueueUrl)) > 0 ? true : undefined),
                        28 * 60_000,
                        15_000,
                    ),
                ),
            );
        },
        30 * 60_000,
    );

    it(
        'puts each DLQ alarm into ALARM',
        async () => {
            await Promise.all(
                consumers.map((c) =>
                    waitFor(
                        `alarm ${c.alarm} to be in ALARM`,
                        async () => {
                            const { MetricAlarms } = await cloudwatch.send(
                                new DescribeAlarmsCommand({ AlarmNames: [c.alarm] }),
                            );
                            return MetricAlarms?.[0]?.StateValue === 'ALARM' ? true : undefined;
                        },
                        8 * 60_000,
                        15_000,
                    ),
                ),
            );
        },
        9 * 60_000,
    );

    it('keeps each original message in its DLQ, then removes it so the stack is left clean', async () => {
        for (const c of consumers) {
            const { Messages = [] } = await sqs.send(
                new ReceiveMessageCommand({
                    QueueUrl: c.deadLetterQueueUrl,
                    MaxNumberOfMessages: 10,
                    WaitTimeSeconds: 10,
                }),
            );

            expect(Messages.map((m) => m.Body)).toContain(c.poison);

            for (const message of Messages.filter((m) => m.Body === c.poison)) {
                await sqs.send(
                    new DeleteMessageCommand({ QueueUrl: c.deadLetterQueueUrl, ReceiptHandle: message.ReceiptHandle }),
                );
            }
        }
    });
});
