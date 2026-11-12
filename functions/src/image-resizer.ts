import { SQSEvent, SQSRecord } from 'aws-lambda';
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import sharp from 'sharp';
import { saveItemAttribute } from './item-store';
import { processBatch } from './sqs-batch';

const s3Client = new S3Client({});

const processedBucket = process.env.PROCESSED_BUCKET;

// The longest edge of each copy. Images are never enlarged: a small original keeps its size.
const VARIANTS = [
    { name: 'thumb', size: 256 },
    { name: 'medium', size: 1024 },
] as const;

// Refuses decompression bombs: a small file can describe a huge image. 50 megapixels is a large photo.
const MAX_INPUT_PIXELS = 50_000_000;

// uploads/{userId}/{id}.jpg -> {userId}/{id}_thumb.webp
const processedKey = (uploadKey: string, variant: string): string =>
    `${uploadKey.replace(/^uploads\//, '').replace(/\.[^./]+$/, '')}_${variant}.webp`;

const render = (original: Buffer, size: number): Promise<Buffer> =>
    sharp(original, { limitInputPixels: MAX_INPUT_PIXELS })
        .rotate() // apply the EXIF orientation, so phone photos aren't sideways
        .resize({ width: size, height: size, fit: 'inside', withoutEnlargement: true })
        .webp({ quality: 80 })
        .toBuffer();

// Throws on any failure, so the batch can report the record for retry.
// The one exception is a file sharp can never read; retrying would not help, so it is recorded
// as FAILED on the item. Only the decoding step is treated that way: a failure in S3 or
// DynamoDB is transient and is retried.
const processRecord = async (record: SQSRecord): Promise<void> => {
    // The SNS subscription uses raw delivery, so the body is the message itself.
    const { bucket, key } = JSON.parse(record.body);

    console.log(`Resizer processing: ${key} from ${bucket}`);

    const object = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    if (!object.Body) {
        throw new Error(`Object ${key} has no body`);
    }
    const original = Buffer.from(await object.Body.transformToByteArray());

    let copies: { key: string; body: Buffer }[];
    try {
        copies = await Promise.all(
            VARIANTS.map(async ({ name, size }) => ({
                key: processedKey(key, name),
                body: await render(original, size),
            })),
        );
    } catch (error) {
        console.error(`Image ${key} cannot be resized:`, error);
        await saveItemAttribute(key, 'resize', { status: 'FAILED', error: 'UnreadableImage' });
        return;
    }

    // Uploading overwrites, so a retry after a partial failure is safe.
    for (const copy of copies) {
        await s3Client.send(
            new PutObjectCommand({
                Bucket: processedBucket,
                Key: copy.key,
                Body: copy.body,
                ContentType: 'image/webp',
            }),
        );
    }

    await saveItemAttribute(key, 'resize', { status: 'COMPLETE', variants: copies.map((copy) => copy.key) });

    console.log(`Successfully resized ${key}`);
};

export const handler = (event: SQSEvent) => processBatch(event, processRecord);
