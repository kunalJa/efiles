# E-Files PDF Pipeline

A serverless pipeline for managing and processing millions of PDF files stored in S3, with DynamoDB inventory tracking and Stripe/Shopify webhook integration for order fulfillment.

> **Built with [uv](https://github.com/astral-sh/uv)** — the fast Python package manager

## Architecture Overview

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  Local E_Files  │────▶│   S3 Bucket  │◀────│  Lambda Order   │
│   (3.5M PDFs)   │     │              │     │   Processor     │
└─────────────────┘     └──────────────┘     └────────┬────────┘
        │                      │                      │
        ▼                      ▼                      ▼
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  S3 Inventory   │────▶│  DynamoDB    │◀────│ Stripe/Shopify  │
│    Report       │     │  Inventory   │     │    Webhook      │
└─────────────────┘     └──────────────┘     └─────────────────┘
```

## Components

### 1. S3 Upload (`s3_upload.py`)

Bulk upload PDFs from local storage to S3 with multithreading.

```bash
# Upload all volumes
uv run s3_upload.py

# Upload specific volume
uv run s3_upload.py --volume 9

# Dry run (list files without uploading)
uv run s3_upload.py --dry-run

# Upload only first page of each PDF
uv run s3_upload.py --first-page-only

# Analyze page distribution
uv run s3_upload.py --analyze
```

**Features:**
- Multithreaded uploads (configurable workers)
- First-page extraction using `pikepdf`
- S3 key format: `VOL00001/filename.pdf`

### 2. Inventory Preparation (`prepare_dynamo_import.py`)

Prepares S3 inventory CSV for DynamoDB bulk import.

```bash
uv run prepare_dynamo_import.py
```

**Features:**
- Filters to PDF files only
- Prioritizes 500 items from VOL00009/VOL00010 at the start
- Shuffles remaining items randomly
- Adds `ID` (sequential) and `Status` (`AVAILABLE`) columns
- Outputs CSV ready for DynamoDB S3 import

### 3. Lambda Order Processor (`lambda_order_processor.py`)

Serverless function triggered by Stripe/Shopify webhooks to process orders.

**Flow:**
1. Parse webhook event → extract OrderID
2. Atomic increment `NextIdToSell` counter
3. Claim item from inventory (status: `AVAILABLE` → `PROCESSING`)
4. Download PDF from S3
5. Apply transformation pipeline (rotate, watermark, etc.)
6. Upload transformed PDF to `ORDER/` prefix
7. Update status to `READY_PRINT`

**Environment Variables:**
| Variable | Description |
|----------|-------------|
| `AWS_S3_BUCKET_NAME` | S3 bucket for PDFs |
| `AWS_DYNAMO_DB_NAME` | Inventory table (e.g., `kz-pdf-files-db`) |
| `AWS_DYNAMO_STORE_DB_NAME` | State table (e.g., `kz-pdf-files-store-state`) |

**Supported Webhooks:**
- Stripe: `checkout.session.completed`, `payment_intent.succeeded`
- Shopify: Order creation webhook
- Direct: `{"order_id": "test_123"}`

### 4. Lambda Layer Builder (`build_lambda_layer.sh`)

Builds a Lambda Layer with `pikepdf` and dependencies for Amazon Linux.

```bash
chmod +x build_lambda_layer.sh
./build_lambda_layer.sh
```

**Output:** `pikepdf-layer.zip` — upload to AWS Lambda Layers.

### 5. CSV Combiner (`combine_csv.py`)

Combines multiple S3 inventory report CSVs into one.

```bash
uv run combine_csv.py
```

## DynamoDB Schema

### Inventory Table (`kz-pdf-files-db`)

| Attribute | Type | Description |
|-----------|------|-------------|
| `ID` | Number | Partition key (sequential) |
| `S3Key` | String | S3 object key |
| `Status` | String | `AVAILABLE`, `PROCESSING`, `READY_PRINT`, `FAILED` |
| `OrderID` | String | Injected on purchase (sparse) |
| `UpdatedAt` | String | ISO timestamp (sparse) |
| `ErrorMessage` | String | Error details if failed (sparse) |

### State Table (`kz-pdf-files-store-state`)

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | String | Partition key (`global_counter`) |
| `NextIdToSell` | Number | Atomic counter for next available item |

## Setup

### Prerequisites

- [uv](https://github.com/astral-sh/uv) (Python package manager)
- AWS CLI configured
- Docker (for Lambda layer building)

### Installation

```bash
# Clone the repo
git clone https://github.com/yourusername/efiles.git
cd efiles

# Install dependencies (uv reads pyproject.toml automatically)
uv sync
```

### Environment Variables

Create a `.env` file:

```env
E_FILES_BASE="/mnt/c/Users/kcube/Desktop/E_Files"
AWS_S3_BUCKET_NAME="your-bucket-name"
AWS_DYNAMO_DB_NAME="kz-pdf-files-db"
AWS_DYNAMO_STORE_DB_NAME="kz-pdf-files-store-state"
```

## Deployment

### Lambda Deployment

1. Build the layer:
   ```bash
   ./build_lambda_layer.sh
   ```

2. Upload `pikepdf-layer.zip` to AWS Lambda Layers

3. Create Lambda function with:
   - Runtime: Python 3.13
   - Handler: `lambda_order_processor.lambda_handler`
   - Attach the pikepdf layer
   - Set environment variables
   - IAM permissions: DynamoDB read/write, S3 read/write

4. Create API Gateway trigger for webhooks

### DynamoDB Import

1. Run `uv run prepare_dynamo_import.py` to generate CSV
2. Upload CSV to S3
3. DynamoDB Console → Imports from S3
4. Select CSV, enable "First row is header"
5. Set partition key: `ID` (Number)

## Transformation Pipeline

Add custom PDF transformations in `lambda_order_processor.py`:

```python
def my_custom_transform(pdf_buffer: BytesIO) -> BytesIO:
    """Your custom transformation."""
    # Process pdf_buffer
    return output_buffer

# Add to pipeline
DEFAULT_TRANSFORMATIONS = [
    rotate_90_degrees,
    my_custom_transform,  # Add here
]
```

## License

MIT
