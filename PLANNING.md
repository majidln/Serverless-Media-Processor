# Media Processor: V6 Execution Roadmap

Harden the V5 image pipeline and add image resizing as a second consumer of the SNS fan-out. V5 was built as a first SAM project; V6 fixes what it left behind and proves the fan-out design by adding a consumer that has nothing to do with Rekognition.

One codebase and one stack. The starting point is tagged `v5-baseline`. At the end of each step we run `git tag step-N-complete`, so the diff between two tags is the record of what changed. Work happens on the `v6-hardening-and-resize` branch.

This file is the single source for the plan: there is no guide per step. Each step below says what is wrong, what changes and how we know it's done.

Out of scope: frontend, Bedrock or other new AI features, Step Functions, Powertools, CI/CD, dev/prod environments. They belong to later versions.

## Status

| Step | What | Status | Tag | Cost at rest |
|---|---|---|---|---|
| 0 | Baseline and a working test setup | Done | `step-0-complete` | Free |
| 1 | Failures stop being silent | Done | `step-1-complete` | Free |
| 2 | Safe uploads and unique keys | Done | `step-2-complete` | Free |
| 3 | Queues between SNS and the consumers | Not started | `step-3-complete` | Free |
| 4 | Image resizer | Not started | `step-4-complete` | S3 storage only, cents |
| 5 | Docs and cleanup | Not started | `v6-complete` | Free |

---

## 1. The problem

### 1.1 Scenario

An authenticated user uploads an image through a presigned URL. The pipeline records it, tags it with Rekognition and stores the result. It's a learning project, but we treat it as if it had real users uploading from several accounts.

### 1.2 Problem statement

**V5 works on the happy path only. Failures are swallowed, two users uploading `photo.jpg` overwrite each other, anyone signed in can upload any file of any size, and there is no way to add a second processing step without touching the first.**

We want a pipeline that never loses an upload silently, keeps every user's images separate, bounds what can be uploaded, and lets new consumers (starting with a resizer) subscribe to uploads without changing the existing ones.

### 1.3 Success criteria

| ID | Criterion | Target | How we measure it |
|---|---|---|---|
| **SC-1** | No silent failures | Every processing error is retried, then lands in a DLQ with an alarm | Force a failure; DLQ alarm fires |
| **SC-2** | No overwrites | Two uploads of the same filename produce two items and two objects | Upload twice, check DynamoDB and S3 |
| **SC-3** | Bounded uploads | Wrong type, wrong extension or too-large uploads are rejected before a URL is signed | Unit tests plus a manual `400` |
| **SC-4** | Independent consumers | Analyzer and resizer fail and succeed independently; one never changes the other's data | Break one consumer; the other still completes |
| **SC-5** | Correct resizing | Thumbnail ≤ 256 px, medium ≤ 1024 px on the long edge, aspect ratio kept, small images not upscaled | Resizer unit tests; one real image |
| **SC-6** | No trigger loops | Resizer output can never trigger the dispatcher | Separate bucket plus a prefix filter; one upload makes exactly one dispatcher invocation |
| **SC-7** | Idempotent processing | A duplicate S3 event or redelivered message changes nothing | Unit tests with a repeated event |
| **SC-8** | Least privilege and no dead config | No unused policy; each function has only the access it uses | Template review |
| **SC-9** | Tests and a clean build | `npm test`, `sam validate --lint` and `sam build` pass | Run all three |

### 1.4 Where V5 stands

`POST /upload` (Cognito JWT) → the function signs a PUT URL for a client-chosen key → S3 `ObjectCreated` invokes the dispatcher → it writes the item and publishes to SNS → the analyzer (SNS subscriber) calls Rekognition and updates the item.

| # | Gap | Where | Violates | Fixed in |
|---|---|---|---|---|
| G1 | Errors are caught and only logged, so Lambda reports success: no retries, no DLQ, rows stuck at `INITIALIZED` | `image-dispatcher.ts`, `image-analyzer.ts` | SC-1 | 1, 3 |
| G2 | The dispatcher's `PutItem` can overwrite an `ANALYSIS_COMPLETE` row; one shared `status` field belongs to whoever writes last | `image-dispatcher.ts`, `image-analyzer.ts` | SC-4, SC-7 | 1 |
| G3 | The client chooses the S3 key (and the item `id`), unsanitized, so users overwrite each other | `get-upload-url.ts` | SC-2 | 2 |
| G4 | No limit on content type, extension or size; Rekognition only reads JPEG and PNG, so other files fail silently | `get-upload-url.ts` | SC-3 | 2 |
| G5 | The SNS topic policy allows `s3.amazonaws.com` to publish, but S3 never publishes to the topic; it's also unscoped | `template.yaml` | SC-8 | 3 |
| G6 | Hardcoded bucket and topic names block a second deploy and collide globally | `template.yaml` | SC-8 | 3 |
| G7 | No DLQ, no alarms, and Lambda logs are kept forever | `template.yaml` | SC-1 | 3 |
| G8 | Only one consumer exists; the fan-out design is unproven | architecture | SC-4, SC-5 | 4 |
| G9 | The only test is the SAM hello-world; it doesn't compile against the real handler | `tests/unit` | SC-9 | 0 |
| G10 | Stale dependencies (`esbuild ^0.14`), an unused `uuid`, `env.json` committed, README out of date | repo-wide | SC-9 | 0, 5 |

---

## 2. Prerequisites

- [x] AWS CLI, SAM CLI and an AWS account with a deployable identity.
- [x] Node.js 20+ locally; functions run on `nodejs20.x`, arm64.
- [ ] Docker, only if the resizer's `sharp` binary needs `sam build --use-container` (see §3).
- [ ] A Cognito test user to get a token for `/upload`.

---

## 3. Decisions

| | |
|---|---|
| **Region** | `us-east-1`, from `samconfig.toml` |
| **Tooling** | SAM for everything; raw `AWS::` resources where SAM has no shorthand (SNS subscriptions, queue policies, alarms) |
| **Runtime** | TypeScript on Node.js 20, arm64, bundled with esbuild by SAM |
| **Upload keys** | `uploads/{sub}/{uuid}.{ext}`: `sub` from `requestContext.authorizer.jwt.claims.sub`, `uuid` from `crypto.randomUUID()` (the `uuid` package is removed). The item `id` is the key |
| **Upload rules** | The client sends `filename`, `contentType` and `size`. Allowed: `image/jpeg` and `image/png` (the types Rekognition reads), max 10 MB. The server signs `ContentType` and `ContentLength`, so the PUT must match what was declared. Anything else gets `400` |
| **Fan-out** | Dispatcher → SNS → one SQS queue per consumer, each with a DLQ. A new consumer is a new queue, a subscription and a function, and no change to existing ones |
| **Queue settings** | Raw message delivery. Analyzer: batch 5, `MaximumConcurrency: 5`, timeout 30 s, visibility timeout 180 s (6× the timeout). Resizer: batch 1 (memory-heavy), timeout 60 s, visibility timeout 360 s. `maxReceiveCount: 3` on both. Both report partial batch failures (`ReportBatchItemFailures`) |
| **Idempotency** | Dispatcher: `PutItem` with `attribute_not_exists(id)`; a conditional failure means "already seen" and is not an error. Consumers: `UpdateItem` with `attribute_exists(id)` |
| **Resizer output** | A separate `ProcessedBucket`, never the upload bucket. The upload trigger is also filtered to the `uploads/` prefix. Variants: `thumb` (256 px) and `medium` (1024 px) WebP, keyed `{sub}/{uuid}_thumb.webp` and `{sub}/{uuid}_medium.webp`. Fit inside the box, keep aspect ratio, never upscale |
| **Resizer library** | `sharp` if it builds and runs on arm64 Lambda (spike first); fallback is `jimp` (pure JS, slower). See §6 |
| **Names** | No hardcoded bucket or topic names; CloudFormation generates them |
| **Logs** | 14-day retention on every function's log group; don't log whole events |
| **Rollback** | `disable_rollback` is turned off once the stack is stable (end of Step 3) |

Each consumer owns its own part of the item, so consumers never overwrite each other:

```json
{
  "id": "uploads/<sub>/<uuid>.jpg",
  "bucket": "...",
  "size": 123456,
  "createdAt": "...",
  "analysis": { "status": "COMPLETE", "labels": ["..."], "at": "..." },
  "resize":   { "status": "COMPLETE", "variants": ["<sub>/<uuid>_thumb.webp", "<sub>/<uuid>_medium.webp"], "at": "..." }
}
```

---

## 4. Steps

### Step 0: We can't tell if a change breaks anything
The only test is the SAM hello-world: it calls the dispatcher with an API Gateway event and expects `"hello world"`, so `npm test` fails at the compile step. Nothing guards the existing behavior, and `env.json` is committed with a real table name.

Tag the current commit `v5-baseline`. Replace the broken test with a working setup (`aws-sdk-client-mock`) and one real test per existing handler on the current behavior. Move `env.json` to `.gitignore` with an `env.example.json`. Update `esbuild` and mark the SDK clients external where SAM allows it.

**Exit:**
- [x] `v5-baseline` tag exists on the V5 commit. *(on `a35e1c3`, next to the older `v5.0` tag)*
- [x] `npm test` passes with a test for each of the three handlers. *(10 tests, 100% coverage; the tests that record swallowed errors and the `500` on bad JSON say so in a comment, and Steps 1 and 2 change them)*
- [x] `env.json` is ignored; `env.example.json` committed. *(`env.json` untracked but kept on disk; `env.example.json` committed)*
- [x] `sam validate --lint` and `sam build` pass. *(bundles are 1 to 1.5 KB with `@aws-sdk/*` external; the external SDK is not yet proven at runtime, which the first deploy in Step 3 does)*

*Renames (same step, so the later steps use the new names):*

| Was | Now | Why |
|---|---|---|
| `image-logger/` | `functions/` | It holds every Lambda, not just the first "logger" one |
| `app.ts`, `lambdaHandler` | `image-dispatcher.ts`, `handler` | The file is the dispatcher; every handler is now called `handler` |
| `get-url.ts` | `get-upload-url.ts` | Matches `GetUploadUrlFunction` |
| `ImageProcessorFunction` | `ImageDispatcherFunction` | Matches the role in the README and this plan |
| `MyCognitoAuthorizer` | `CognitoJwtAuthorizer` | Drops the template leftover and says what it is |
| package `image_logger` | `media-processor-functions` | Matches the folder and project |

*The function rename replaces that Lambda on the next deploy (new physical name and log group). Test files were renamed to match.*

*Notes: esbuild is `^0.28.2` and stays in `dependencies`, because SAM installs production dependencies only before bundling and then can't find it. In this sandbox jest needs `--watchman=false`.*

*Free.*

### Step 1: A failure looks exactly like a success
Both handlers catch every error and return normally, so Lambda reports success and nothing retries. A Rekognition failure leaves the row at `INITIALIZED` forever. The dispatcher's `PutItem` can also overwrite a finished item, and both functions write the same `status` field.

Rethrow errors (or collect failures and throw at the end). Make the dispatcher's write conditional and treat "already exists" as a no-op. Move to per-consumer fields (`analysis.status`) and make the analyzer's update conditional on the item existing. Drop whole-event logging.

**The point: a failure must be visible and retryable, never disguised as success.**

**Exit:**
- [x] A forced Rekognition error fails the invocation. *(unit test; the live check waits for the first deploy in Step 3)*
- [x] A repeated S3 event leaves the item unchanged. *(the write is conditional; a duplicate is ignored once the analysis has finished)*
- [x] The analyzer never creates an item that doesn't exist. *(`attribute_exists(id)`; a missing item fails the invocation)*
- [x] The stored item uses the `analysis` map from §3. *(`PENDING` from the dispatcher, then `COMPLETE` with labels or `FAILED` with an error name; the top-level `status` is gone)*

*Notes: two choices go slightly beyond the plan. When the conditional write fails, the dispatcher reads the item, and publishes again only if `analysis.status` is still `PENDING`. Without that, a retry after a failed publish would see "already exists" and the event would be lost. And an image Rekognition can never read (`InvalidImageFormatException`, `ImageTooLargeException`) is recorded as `analysis.status = FAILED` instead of being retried, since retrying can't help. Everything else, including a malformed message, fails the invocation. Until Step 3 adds the queue and DLQ, SNS retries a failed delivery a few times and then drops it.*

*Free.*

### Step 2: Anyone signed in can overwrite anyone's file, with anything
The client picks the S3 key, so `photo.jpg` from two users is one object and one item. Nothing checks type or size, and Rekognition rejects anything that isn't JPEG or PNG.

Generate the key on the server from the user's `sub` and a UUID. Validate `filename`, `contentType` and `size` before signing, and sign `ContentType` and `ContentLength`. Return `400` with a clear message when validation fails, instead of the generic `500`.

**Exit:**
- [x] Two uploads named `photo.jpg` produce two objects and two items, each under the caller's `sub` (SC-2). *(two calls give two different `uploads/{sub}/{uuid}.jpg` keys; the items follow because the item id is the key; the live check waits for the first deploy)*
- [x] A wrong type, a wrong extension or an oversized request gets `400`; a PUT that doesn't match the signed type or length is rejected by S3 (SC-3). *(`400` is unit-tested for 13 bad inputs; the signed URL lists `content-length;content-type;host`, checked in a unit test; S3's own rejection is not tried until the first deploy)*
- [x] The upload bucket's trigger only fires for the `uploads/` prefix. *(S3 `prefix` filter, present in the built template)*
- [x] Unit tests cover valid, invalid, missing-claim and malformed-body cases. *(41 tests in total, 100% coverage)*

*Notes: the presigner does not sign `content-type` unless it is passed in `signableHeaders`; by default only `content-length` and `host` are signed, so without that option S3 would accept any type. A missing `sub` claim returns `401`. The filename is only validated, never used in the key. The `/upload` request body now needs `filename`, `contentType` and `size`, which breaks clients of V5.*

*Free.*

### Step 3: One consumer, no queue, no safety net
The analyzer hangs directly off SNS, so a bad message is retried a few times by SNS and then dropped, with no DLQ and no alarm. A second consumer would have no isolation from the first. The template carries a dead topic policy, fixed bucket and topic names, and log groups that never expire.

Subscribe an SQS queue (with a DLQ) to the topic and move the analyzer onto it, reporting partial batch failures. Remove the unused topic policy and the hardcoded names. Add log retention and an alarm on each DLQ. Turn `disable_rollback` off.

**Exit:**
- [ ] Analyzer reads from its queue and handles a batch with one bad record: the others are processed once, the bad one is retried and then lands in the DLQ (SC-1).
- [ ] The DLQ alarm is forced into `ALARM` once and fires.
- [ ] `ImageEventsTopicPolicy` is gone; no `BucketName` or `TopicName` in the template (SC-8).
- [ ] A second stack deploys without name collisions, or a rename is proven by an in-place update after emptying the bucket.
- [ ] The full flow (upload → item → labels) works end to end after the change.

*Free. Redeploying replaces the bucket and the topic, so use `sam delete` and a fresh deploy, or empty the old bucket first (§6).*

### Step 4: The fan-out has one consumer
The design promises that new processing steps are independent of the old ones, but there's only one subscriber, so nobody has proven it.

Add a `ProcessedBucket`, a resizer queue with a DLQ and alarm, and `ImageResizerFunction` (`image-resizer.ts`; 1024 MB, 60 s, arm64). It reads the original from the upload bucket, writes the `thumb` and `medium` WebP variants to the processed bucket, and updates `resize` on the item. IAM is read on uploads, write on processed and update on the table, and nothing else.

Start with a spike: a Hello-world `sharp` function that builds with `sam build` and runs on Lambda arm64. If the native binary is a problem, use `sam build --use-container` or a layer; if that's a time sink, switch to `jimp` (§6).

**Exit:**
- [ ] Spike result recorded here (which approach, and why).
- [ ] One upload yields two variants in the processed bucket, within the size limits and with the aspect ratio kept; a small image is not upscaled (SC-5).
- [ ] One upload triggers exactly one dispatcher invocation, so the processed bucket can't loop (SC-6).
- [ ] With the resizer deliberately broken, `analysis` still completes; with the analyzer broken, `resize` still completes (SC-4).
- [ ] A corrupt image goes to the resizer DLQ and the alarm fires.
- [ ] Resizer unit tests with a small fixture image.

*S3 storage for the variants only. Cents.*

### Step 5: The docs describe V5
The README calls the dispatcher the wrong name in places, had a typo in the project tree, and says nothing about how to get a token and call `/upload`.

Update the README for V6: architecture, project tree, status values, a short "get a token and upload" walkthrough with `curl`, local testing for the new functions, and clean-up for both buckets. Update the diagram (a Mermaid version is fine; the `Media-Processor-V6` image is redrawn by hand). Tag `v6-complete`.

**Exit:**
- [ ] README matches the code and the template.
- [ ] The walkthrough works from a clean deploy, copy and paste.
- [ ] Diagram updated.
- [ ] Final `npm test`, `sam validate --lint` and `sam build` pass (SC-9).

*Free.*

---

## 5. Session discipline

Validate and test locally → deploy → verify the exit criteria by hand with a real image → `git tag step-N-complete` → tear down anything that bills at rest.

| Resource | Rule |
|---|---|
| Main stack | Leave running between steps; it's all pay-per-use |
| Buckets | Empty them before `sam delete` (uploads and processed) |
| Deploys | `sam deploy` creates billable resources, so it's run on purpose by the owner, not automatically |

---

## 6. Open questions, and what was decided

| Question | Recommendation | Decision |
|---|---|---|
| Resizer library (Step 4) | `sharp`, after a build spike on arm64; `jimp` if the native binary becomes a time sink | Open |
| SNS → SQS now, or SNS → Lambda direct with rethrown errors only (Step 3) | SNS → SQS: it's what makes the retry, DLQ and alarm fixes real, and the resizer needs its own queue anyway | Open |
| Fresh deploy or in-place update (Step 3) | `sam delete` and a fresh deploy: removing the names replaces the bucket and topic, and the data is disposable | Open |
| A read endpoint (`GET /images/{id}` with presigned GET URLs for the variants) | Not in V6. Without it, results are checked in the S3 console and DynamoDB; add it in V7 | Open |

*Costs are approximate `us-east-1` list prices. Confirm them before relying on them.*
