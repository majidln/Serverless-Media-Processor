import { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from "aws-lambda";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

const s3Client = new S3Client({});
const BUCKET_NAME = process.env.UPLOAD_BUCKET;

export const handler = async (event: APIGatewayProxyEventV2): Promise<APIGatewayProxyResultV2> => {
    try {
        const body = event.body ? JSON.parse(event.body) : {};
        const fileName = body.filename || `upload-${Date.now()}.jpg`;
        const key = fileName;

        const command = new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: key,
        });

        const uploadUrl = await getSignedUrl(s3Client, command, { expiresIn: 300 });

        return {
            statusCode: 200,
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                uploadUrl,
                key,
                message: "Use a PUT request to upload your file to the uploadUrl"
            }),
        };

    } catch (error) {
        console.error("Error generating presigned URL:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: "Could not generate upload URL" }),
        };
    }
}