import { randomUUID } from "crypto";
import { APIGatewayProxyEventV2WithJWTAuthorizer, APIGatewayProxyStructuredResultV2 } from "aws-lambda";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

const s3Client = new S3Client({});
const BUCKET_NAME = process.env.UPLOAD_BUCKET;

// Rekognition only reads JPEG and PNG, so nothing else is accepted.
const ALLOWED_TYPES: Record<string, { extension: string; filenameExtensions: string[] }> = {
    "image/jpeg": { extension: "jpg", filenameExtensions: ["jpg", "jpeg"] },
    "image/png": { extension: "png", filenameExtensions: ["png"] },
};
const MAX_SIZE_BYTES = 10 * 1024 * 1024;
const URL_EXPIRY_SECONDS = 300;

const respond = (statusCode: number, body: object): APIGatewayProxyStructuredResultV2 => ({
    statusCode,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
});

const badRequest = (message: string) => respond(400, { message });

export const handler = async (
    event: APIGatewayProxyEventV2WithJWTAuthorizer,
): Promise<APIGatewayProxyStructuredResultV2> => {
    // The JWT authorizer guarantees a token, but the key is built from this claim, so never assume it.
    const userId = event.requestContext?.authorizer?.jwt?.claims?.sub;
    if (typeof userId !== "string" || userId === "") {
        return respond(401, { message: "Missing user identity" });
    }

    let body: { filename?: unknown; contentType?: unknown; size?: unknown };
    try {
        body = event.body ? JSON.parse(event.body) : {};
    } catch {
        return badRequest("Request body must be valid JSON");
    }
    if (body === null || typeof body !== "object") {
        return badRequest("Request body must be a JSON object");
    }

    const { filename, contentType, size } = body;

    // hasOwnProperty, not `in`: "constructor" and "toString" are on every object.
    if (typeof contentType !== "string" || !Object.prototype.hasOwnProperty.call(ALLOWED_TYPES, contentType)) {
        return badRequest(`contentType must be one of: ${Object.keys(ALLOWED_TYPES).join(", ")}`);
    }
    const allowed = ALLOWED_TYPES[contentType];

    if (typeof filename !== "string" || filename === "") {
        return badRequest("filename is required");
    }
    const filenameExtension = filename.slice(filename.lastIndexOf(".") + 1).toLowerCase();
    if (!allowed.filenameExtensions.includes(filenameExtension)) {
        return badRequest(`filename extension must match contentType ${contentType}`);
    }

    if (typeof size !== "number" || !Number.isInteger(size) || size <= 0 || size > MAX_SIZE_BYTES) {
        return badRequest(`size must be a whole number of bytes between 1 and ${MAX_SIZE_BYTES}`);
    }

    // The filename is only validated, never used: the key comes from the user and a random id.
    const key = `uploads/${userId}/${randomUUID()}.${allowed.extension}`;

    try {
        const command = new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: key,
            ContentType: contentType,
            ContentLength: size,
        });

        // Signing both headers means S3 rejects a PUT whose type or length differs from what was declared.
        // content-type is not signed unless it is listed here.
        const uploadUrl = await getSignedUrl(s3Client, command, {
            expiresIn: URL_EXPIRY_SECONDS,
            signableHeaders: new Set(["content-type", "content-length"]),
        });

        return respond(200, {
            uploadUrl,
            key,
            message: "Use a PUT request with the same Content-Type and body size to upload your file to the uploadUrl",
        });
    } catch (error) {
        console.error("Error generating presigned URL:", error);
        return respond(500, { message: "Could not generate upload URL" });
    }
};
