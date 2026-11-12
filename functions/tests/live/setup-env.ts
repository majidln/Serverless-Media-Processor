// Same region as samconfig.toml, unless the caller sets one.
process.env.AWS_REGION ??= 'us-east-1';
