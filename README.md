# E-Files Shirts Order Backend

Python tooling and AWS Lambda code for inventory allocation, Printful image
creation, Stripe capture, and fulfillment submission.

The canonical Next.js integration contract is
[`../docs/BACKEND_INTEGRATION.md`](../docs/BACKEND_INTEGRATION.md).

## Production order flow

The Lambda is invoked asynchronously after the signature-verified Next.js
webhook route validates `checkout.session.completed` and its underlying
manual-capture PaymentIntent.

```text
1. Re-fetch and validate the manual-capture Stripe PaymentIntent
2. Atomically increment the global counter and claim an AVAILABLE inventory row
3. Record the canonical inventory_id in Stripe metadata
4. Render/upload 300-DPI front/back PNGs
5. Create or reuse a Printful draft by unique external_id
6. Capture Stripe
7. Confirm Printful
8. Mark the inventory row SOLD
```

There is no API Gateway or public Lambda URL. The browser never invokes this
Lambda and never queries DynamoDB directly.

## Product configuration

White Gildan 5000, quantity 1, fixed `$44.00` USD product subtotal plus fixed
`$4.95` Standard US shipping. Application tax calculation is disabled, so the
Checkout total is exactly `$48.95`:

| Size | Printful variant ID |
|---|---:|
| S | 11576 |
| M | 11577 |
| L | 11578 |
| XL | 11579 |

The front and back images are transparent 3600×4800 PNGs representing the
12″×16″ print area at 300 DPI. They are stored at:

```text
ORDERS/<file_id>/front.png
ORDERS/<file_id>/back.png
```

Printful receives short public URLs for only this generated-image prefix because
its API truncates Lambda-role S3 presigned URLs. Keep source PDFs outside
`ORDERS/` private, deny bucket listing, and allow public `s3:GetObject` only on:

```text
arn:aws:s3:::kz-pdf-files-bucket/ORDERS/*
```

The previous Lambda-role presigned URLs exceeded Printful's 1,000-character
source URL limit because they included an STS session token. Printful truncated
the URLs before the required expiry parameter, so its exact stored URLs returned
S3 `403 AccessDenied` and both files failed with zero size. Short unsigned URLs
avoid that limit.

These URLs contain no AWS credentials and grant no write or list access. They are
still public: anyone who learns or guesses a path can download the generated
image until it is deleted or the policy changes. Current file IDs are
predictable, and public requests can create bandwidth cost if scraped. This is
acceptable only while generated shirt artwork is non-sensitive. Use lifecycle
deletion, random object paths, or private CloudFront delivery if requirements
change. Never put source PDFs, customer data, or credentials under `ORDERS/`.

Bucket versioning is suspended. A lifecycle rule scoped exactly to `ORDERS/`
expires current generated-image versions 25 days after creation, making their
public URLs unavailable. It does not delete Printful orders, Printful's ingested
copies, DynamoDB records, Stripe payments, or private source PDFs outside that
prefix. Historical noncurrent S3 versions remain unless the rule also includes a
noncurrent-version expiration action.

## DynamoDB schema

### Inventory: `kz-pdf-files-db`

| Attribute | Type | Description |
|---|---|---|
| `ID` | Number | Partition key |
| `S3Key` | String | Source PDF key |
| `Status` | String | Current lifecycle state |
| `OrderID` | String | Sparse internal order ID; GSI partition key |
| `PaymentIntentID` | String | Stripe PaymentIntent ID |
| `ShirtSize` | String | S, M, L, or XL |
| `PrintfulOrderID` | Number | Printful order ID |
| `FrontS3Key` | String | Generated front PNG key |
| `BackS3Key` | String | Generated back PNG key |
| `StripePaymentStatus` | String | Stripe status after capture |
| `PrintfulStatus` | String | Printful status after confirm |
| `FileID` | String | Document ID exposed after success |
| `UpdatedAt` | String | ISO-8601 status timestamp |
| `ErrorMessage` | String | Truncated failure detail |

Lifecycle:

```text
AVAILABLE → PROCESSING → PRINTFUL_DRAFT_CREATED → PAYMENT_CAPTURED → SOLD
                  └─ PROCESSING_RETRY                         └─ REFUNDED_FAILED
                  └─ FAILED
```

Terminal-state conditional updates prevent duplicate invocations from
regressing `SOLD`, `FAILED`, or `REFUNDED_FAILED`.

Create a sparse GSI:

```text
Name:           OrderID-index
Partition key:  OrderID (String)
Projection:     INCLUDE Status, UpdatedAt, FileID, PrintfulOrderID,
                PrintfulStatus, ErrorMessage, ShirtSize
```

The Next.js status route queries this index. Do not scan the roughly 3-million-
row inventory table for browser polling.

### Counter: `kz-pdf-files-store-state`

This table contains exactly one row and no order records:

```json
{ "pk": "global_counter", "NextIdToSell": 5 }
```

`NextIdToSell=N` means claim inventory `ID=N`, then atomically store `N+1` for
the next order. `ReturnValues="UPDATED_OLD"` returns the claimed ID.

## Idempotency and duplicates

Stripe and AWS async delivery can invoke the Lambda multiple times.

- Stripe metadata `inventory_id` selects one canonical inventory row.
- Concurrent claims converge on that canonical ID; extra rows can be orphaned
  but no PDF is assigned to two completed orders.
- Stable Stripe idempotency keys protect metadata update, capture, cancel, and
  refund operations.
- Capture is skipped when the PaymentIntent is already `succeeded`.
- Printful is queried as `GET /orders/@<order_id>` before creation.
- A concurrent Printful create conflict is followed by another lookup and full
  external ID/variant validation.
- Printful confirmation is skipped when the order is already past `draft`.

## Fixed Checkout pricing

Stripe-hosted Checkout displays a `$44.00` product line and a separate `$4.95`
fixed shipping option, collects a US-only address, and creates a manual-capture
PaymentIntent for exactly `$48.95`. No Lambda shipping quote is needed.

The PaymentIntent must contain server-generated metadata for product (`4400`),
shipping (`495`), tax (`0`), and shipping method (`STANDARD`). Lambda requires
exact equality, USD currency, and a US address.

The product price and Gildan 5000 cost are both fixed, and US Standard shipping
is a flat `$4.95`, so the margin is deterministic at the configured price point.
Lambda no longer polls Printful for `costs.total` or enforces a minimum-margin
gate before capture; it captures immediately after the Printful draft is created
and validated.

## Image code versus the layer

Production image-generation functions are directly in
`lambda_order_processor.py`. `generate_printful_images.py` is only a local
runner that imports those production functions; the Lambda does not import or
deploy that runner.

The `print-images-layer` contains only PyMuPDF and Pillow. These packages include
Linux-native binaries and cannot be embedded into a single `.py` source file.
AWS supports either bundling them in the function zip or placing them in a
layer. The layer keeps code deployments small and can be rebuilt independently;
it does not contain image assets or business logic.

## Local verification

```bash
uv sync
uv run python -m unittest -v test_lambda_order_processor.py
```

Manual local image rendering:

```bash
LOCAL_PDF=/absolute/path/to/test.pdf SKIP_UPLOAD=1 \
  uv run python generate_printful_images.py
```

## Deployment

### 1. Build the dependency layer

Requirements: Docker and `zip`.

```bash
chmod +x build_lambda_layer.sh
./build_lambda_layer.sh
```

This creates `print-images-layer.zip` with Python 3.13 x86_64 Linux builds of
PyMuPDF and Pillow. Create an AWS Lambda layer from the zip. If direct upload is
too large, upload it to S3 and create the layer from that object.

### 2. Package application code

```bash
zip -j lambda-order-processor.zip lambda_order_processor.py
```

Only that Python file is needed in the application zip. Boto3 is supplied by
the Lambda Python runtime; PyMuPDF and Pillow come from the attached layer.

### 3. Create/configure the Lambda

```text
Function name:  efiles-order-processor
Runtime:        Python 3.13
Architecture:   x86_64
Handler:        lambda_order_processor.lambda_handler
Memory:         2048 MB
Timeout:        300 seconds
Code:           lambda-order-processor.zip
Layer:          print-images-layer
Public URL:     none
API Gateway:    none
```

Configure async invocation with two retries, a bounded event age, and an SQS
on-failure destination.

### 4. Lambda environment

| Variable | Required | Description |
|---|---|---|
| `AWS_S3_BUCKET_NAME` | yes | `kz-pdf-files-bucket` |
| `AWS_DYNAMO_DB_NAME` | yes | `kz-pdf-files-db` |
| `AWS_DYNAMO_STORE_DB_NAME` | yes | `kz-pdf-files-store-state` |
| `STRIPE_SECRET_KEY_SECRET_ARN` | one of | Production Stripe secret ARN |
| `STRIPE_SECRET_KEY` | one of | Plaintext only for local/dev |
| `PRINTFUL_TOKEN_SECRET_ARN` | one of | Production Printful secret ARN containing `PRINTFUL_SECRET_KEY` |
| `PRINTFUL_SECRET_KEY` | one of | Plaintext only for local/dev |
| `PRINTFUL_STORE_ID` | optional | Needed for account-level token |
| `ORDER_AMOUNT_CENTS` | optional | Fixed product subtotal; default `4400` |
| `SHIPPING_AMOUNT_CENTS` | optional | Fixed US shipping; default `495` |
| `SHIPPING_METHOD` | optional | Printful method; default `STANDARD` |
| `PRINTFUL_ASSET_BASE_URL` | optional | Public base URL for `ORDERS/*`; defaults to the bucket's S3 URL |

Before production, verify Printful's published single-T-shirt US Standard rate
is still `$4.95`; update both Checkout and Lambda configuration together if it
changes. Stripe automatic tax is disabled for MVP.

### 5. Lambda execution role

Attach `AWSLambdaBasicExecutionRole` for CloudWatch Logs and add:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::kz-pdf-files-bucket/*"
    },
    {
      "Effect": "Allow",
      "Action": ["dynamodb:GetItem", "dynamodb:UpdateItem"],
      "Resource": [
        "arn:aws:dynamodb:us-east-1:800618367364:table/kz-pdf-files-db",
        "arn:aws:dynamodb:us-east-1:800618367364:table/kz-pdf-files-store-state"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": [
        "<STRIPE_SECRET_KEY_SECRET_ARN>",
        "<PRINTFUL_TOKEN_SECRET_ARN>"
      ]
    }
  ]
}
```

Do not reuse the `s3-pdf-uploader` IAM user. Create a dedicated Lambda
execution role.

### 6. Next.js IAM role

The separate Next.js/Vercel backend role needs only:

- `lambda:InvokeFunction` on `efiles-order-processor`
- `dynamodb:Query` on `kz-pdf-files-db/index/OrderID-index`

Vercel OIDC is preferred over permanent AWS access keys. See
`../docs/BACKEND_INTEGRATION.md` for the route contract and policy.

## Orphan cleanup

Automatic cleanup is safe only before Printful draft creation/capture:

```bash
# Dry run
uv run python ../scripts/reclaim_stale_inventory.py --threshold-hours 24

# Apply
uv run python ../scripts/reclaim_stale_inventory.py --threshold-hours 24 --apply
```

The script only resets stale `PROCESSING` and `PROCESSING_RETRY` rows. Never
blindly reclaim `PRINTFUL_DRAFT_CREATED` or `PAYMENT_CAPTURED`; reconcile those
with Stripe and Printful first.

## Other data-pipeline tools

- `s3_upload.py`: bulk source PDF upload
- `prepare_dynamo_import.py`: prepare inventory CSV for DynamoDB import
- `combine_csv.py`: combine S3 inventory reports
- `scripts/test_printful.py`: manual Printful draft testing
