// Runs before each test file, so handlers read these when their module loads.
process.env.TABLE_NAME = 'test-table';
process.env.TOPIC_ARN = 'arn:aws:sns:us-east-1:123456789012:test-topic';
process.env.UPLOAD_BUCKET = 'test-upload-bucket';
process.env.AWS_REGION = 'us-east-1';
process.env.AWS_ACCESS_KEY_ID = 'test';
process.env.AWS_SECRET_ACCESS_KEY = 'test';
process.env.PROCESSED_BUCKET = 'test-processed-bucket';
