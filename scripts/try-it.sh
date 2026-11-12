#!/usr/bin/env bash
#
# Runs the whole Darkroom flow against the deployed stack with one image:
# sign in, ask for an upload URL, upload, wait for both consumers, list the copies,
# then check that bad requests are rejected.
#
# Usage:   scripts/try-it.sh <image.jpg|image.png> [--no-failure-cases]
# Needs:   aws CLI (with credentials for the stack's account), curl, jq
# Config (environment variables, all optional except the password):
#   DARKROOM_PASSWORD  password of the demo user (prompted for if not set)
#   DARKROOM_USER      demo user's email           (default: darkroom-demo@example.com)
#   STACK              CloudFormation stack name   (default: darkroom)
#   AWS_REGION         region of the stack         (default: us-east-1)
#
# The demo user is created in the pool on the first run. If it already exists its
# password is left alone, so use the one you chose before.

set -euo pipefail

STACK=${STACK:-darkroom}
REGION=${AWS_REGION:-us-east-1}
DARKROOM_USER=${DARKROOM_USER:-darkroom-demo@example.com}
WAIT_SECONDS=120

usage() {
    sed -n '3,16p' "$0" | sed 's/^# \{0,1\}//'
}

step() { printf '\n==> %s\n' "$1"; }
ok() { printf '    ok: %s\n' "$1"; }
die() { printf 'error: %s\n' "$1" >&2; exit 1; }

IMAGE=""
FAILURE_CASES=1
for arg in "$@"; do
    case "$arg" in
        -h | --help) usage; exit 0 ;;
        --no-failure-cases) FAILURE_CASES=0 ;;
        -*) die "unknown option: $arg (see --help)" ;;
        *) [ -z "$IMAGE" ] || die "only one image is accepted"; IMAGE=$arg ;;
    esac
done

[ -n "$IMAGE" ] || { usage >&2; exit 1; }
[ -f "$IMAGE" ] || die "file not found: $IMAGE"
for tool in aws curl jq; do
    command -v "$tool" > /dev/null || die "$tool is required but not installed"
done

case "$(printf '%s' "${IMAGE##*.}" | tr '[:upper:]' '[:lower:]')" in
    jpg | jpeg) CONTENT_TYPE=image/jpeg ;;
    png) CONTENT_TYPE=image/png ;;
    *) die "only .jpg, .jpeg and .png files are accepted by the API" ;;
esac
FILENAME=$(basename "$IMAGE")
SIZE=$(wc -c < "$IMAGE" | tr -d ' ')

if [ -z "${DARKROOM_PASSWORD:-}" ]; then
    [ -t 0 ] || die "set DARKROOM_PASSWORD (there is no terminal to ask on)"
    read -r -s -p "Password for $DARKROOM_USER (Cognito policy: mixed case, number, symbol): " DARKROOM_PASSWORD
    echo
fi

stack_output() {
    aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK" \
        --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

# POST /upload with the given token and JSON body. Prints the HTTP status, then the body.
request_upload() {
    curl -s -w '\n%{http_code}' -X POST "$API/upload" \
        -H "Authorization: Bearer $1" -H "Content-Type: application/json" -d "$2" |
        awk '{ lines[NR] = $0 } END { print lines[NR]; for (i = 1; i < NR; i++) print lines[i] }'
}

# Prints "ok" or exits when the status of a call is not the expected one.
expect_status() {
    local label=$1 expected=$2 actual=$3
    [ "$actual" = "$expected" ] || die "$label: expected HTTP $expected, got $actual"
    ok "$label -> $actual"
}

step "Looking up the stack '$STACK' in $REGION"
API=$(stack_output ApiUrl)
CLIENT_ID=$(stack_output UserPoolClientId)
POOL_ID=$(stack_output UserPoolId)
PROCESSED=$(stack_output ProcessedBucketName)
TABLE=$(stack_output TableName)
for value in "$API" "$CLIENT_ID" "$POOL_ID" "$PROCESSED" "$TABLE"; do
    [ -n "$value" ] && [ "$value" != None ] || die "stack '$STACK' is missing an output; is it deployed?"
done
ok "$API"

step "Signing in as $DARKROOM_USER"
if aws cognito-idp admin-get-user --region "$REGION" --user-pool-id "$POOL_ID" \
    --username "$DARKROOM_USER" > /dev/null 2>&1; then
    ok "user exists, password left as it is"
else
    aws cognito-idp admin-create-user --region "$REGION" --user-pool-id "$POOL_ID" \
        --username "$DARKROOM_USER" --message-action SUPPRESS > /dev/null
    aws cognito-idp admin-set-user-password --region "$REGION" --user-pool-id "$POOL_ID" \
        --username "$DARKROOM_USER" --password "$DARKROOM_PASSWORD" --permanent
    ok "user created"
fi
TOKEN=$(aws cognito-idp initiate-auth --region "$REGION" --client-id "$CLIENT_ID" \
    --auth-flow USER_PASSWORD_AUTH \
    --auth-parameters "USERNAME=$DARKROOM_USER,PASSWORD=$DARKROOM_PASSWORD" \
    --query 'AuthenticationResult.IdToken' --output text) || die "sign-in failed (wrong password?)"
ok "got an ID token"

step "Asking for an upload URL ($FILENAME, $CONTENT_TYPE, $SIZE bytes)"
BODY=$(jq -n --arg f "$FILENAME" --arg t "$CONTENT_TYPE" --argjson s "$SIZE" \
    '{filename: $f, contentType: $t, size: $s}')
RESPONSE=$(request_upload "$TOKEN" "$BODY")
expect_status "POST /upload" 200 "$(echo "$RESPONSE" | head -n 1)"
UPLOAD_URL=$(echo "$RESPONSE" | tail -n +2 | jq -r .uploadUrl)
KEY=$(echo "$RESPONSE" | tail -n +2 | jq -r .key)
ok "key: $KEY"

step "Uploading to S3"
STATUS=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$UPLOAD_URL" \
    -H "Content-Type: $CONTENT_TYPE" --data-binary @"$IMAGE")
expect_status "PUT to S3" 200 "$STATUS"

step "Waiting for the analyzer and the resizer (up to ${WAIT_SECONDS}s)"
DEADLINE=$((SECONDS + WAIT_SECONDS))
ITEM='{}'
ANALYSIS=MISSING
RESIZE=MISSING
is_final() { [ "$1" = COMPLETE ] || [ "$1" = FAILED ]; }
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    # get-item prints nothing at all while the item does not exist yet.
    ITEM=$(aws dynamodb get-item --region "$REGION" --table-name "$TABLE" \
        --key "$(jq -n --arg k "$KEY" '{id: {S: $k}}')" --output json)
    ITEM=${ITEM:-'{}'}
    ANALYSIS=$(echo "$ITEM" | jq -r '.Item.analysis.M.status.S // "MISSING"')
    RESIZE=$(echo "$ITEM" | jq -r '.Item.resize.M.status.S // "MISSING"')
    printf '    analysis: %-9s resize: %s\n' "$ANALYSIS" "$RESIZE"
    if is_final "$ANALYSIS" && is_final "$RESIZE"; then
        break
    fi
    sleep 3
done
is_final "$ANALYSIS" && is_final "$RESIZE" ||
    die "not finished after ${WAIT_SECONDS}s (analysis: $ANALYSIS, resize: $RESIZE); check the queues and DLQs"

step "Result"
echo "$ITEM" | jq '{id: .Item.id.S, analysis: .Item.analysis, resize: .Item.resize}'
echo
echo "Copies in s3://$PROCESSED/:"
COPY_PREFIX=${KEY#uploads/}
COPY_PREFIX=${COPY_PREFIX%.*}_
aws s3 ls "s3://$PROCESSED/$COPY_PREFIX" --region "$REGION" | sed 's/^/    /' ||
    echo "    (none; the resizer recorded $RESIZE)"

if [ "$FAILURE_CASES" -eq 1 ]; then
    step "Checking that bad requests are rejected"

    STATUS=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$API/upload" \
        -H "Content-Type: application/json" -d "$BODY")
    expect_status "no token" 401 "$STATUS"

    BAD=$(jq -n '{filename: "doc.pdf", contentType: "application/pdf", size: 1000}')
    expect_status "disallowed type" 400 "$(request_upload "$TOKEN" "$BAD" | head -n 1)"

    BAD=$(jq -n '{filename: "photo.png", contentType: "image/jpeg", size: 1000}')
    expect_status "extension does not match the type" 400 "$(request_upload "$TOKEN" "$BAD" | head -n 1)"

    BAD=$(jq -n '{filename: "photo.jpg", contentType: "image/jpeg", size: 20000000}')
    expect_status "file over 10 MB" 400 "$(request_upload "$TOKEN" "$BAD" | head -n 1)"

    # A fresh URL whose signed content type is then contradicted. Nothing reaches the bucket.
    FRESH=$(request_upload "$TOKEN" "$BODY")
    FRESH_URL=$(echo "$FRESH" | tail -n +2 | jq -r .uploadUrl)
    WRONG_TYPE=image/png
    [ "$CONTENT_TYPE" = image/png ] && WRONG_TYPE=image/jpeg
    STATUS=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$FRESH_URL" \
        -H "Content-Type: $WRONG_TYPE" --data-binary @"$IMAGE")
    expect_status "PUT with a different content type" 403 "$STATUS"
fi

echo
echo "Done."
