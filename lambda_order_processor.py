"""
AWS Lambda: PDF Order Processor

Triggered by: Stripe/Shopify webhook via API Gateway

Flow:
1. Parse webhook event to extract OrderID
2. Atomically increment NextIdToSell counter in kz-pdf-files-store-state
3. Fetch PDF record from kz-pdf-files-db by ID, set status to PROCESSING + inject OrderID/UpdatedAt
4. Download PDF from S3
5. Apply transformation pipeline (extensible)
6. Upload transformed PDF to ORDER/ prefix
7. Update status to READY_PRINT

Environment Variables:
- AWS_S3_BUCKET_NAME: S3 bucket for PDFs
- AWS_DYNAMO_DB_NAME: DynamoDB table for PDF inventory (e.g., kz-pdf-files-db)
- AWS_DYNAMO_STORE_DB_NAME: DynamoDB table for global state (e.g., kz-pdf-files-store-state)

Webhook Payload Examples:
- Stripe: {"type": "checkout.session.completed", "data": {"object": {"id": "cs_live_xxx"}}}
- Shopify: {"id": 5678901234, ...}
- Direct/Test: {"order_id": "test_order_123"}
"""

import os
import json
import boto3
from io import BytesIO
from datetime import datetime, timezone
from typing import Callable, List, Optional
from boto3.dynamodb.conditions import Key

# ============================================================================
# TRANSFORMATION PIPELINE
# ============================================================================
# Add new transformation functions here. Each takes a BytesIO PDF buffer
# and returns a new BytesIO buffer with the transformed PDF.

def rotate_90_degrees(pdf_buffer: BytesIO) -> BytesIO:
    """Rotate all pages in PDF 90 degrees clockwise."""
    import pikepdf
    
    pdf_buffer.seek(0)
    with pikepdf.open(pdf_buffer) as pdf:
        for page in pdf.pages:
            page.Rotate = (page.get('/Rotate', 0) + 90) % 360
        
        output = BytesIO()
        pdf.save(output)
        output.seek(0)
        return output


def add_watermark_stamp(pdf_buffer: BytesIO) -> BytesIO:
    """Example: Add a simple annotation/stamp to PDF (placeholder for future)."""
    # For now, just pass through - implement actual watermarking as needed
    pdf_buffer.seek(0)
    return pdf_buffer


# Default pipeline - add/remove/reorder transformations here
DEFAULT_TRANSFORMATIONS: List[Callable[[BytesIO], BytesIO]] = [
    rotate_90_degrees,
    # add_watermark_stamp,  # Uncomment to enable
]


def apply_transformations(pdf_buffer: BytesIO, transformations: List[Callable] = None) -> BytesIO:
    """
    Apply a list of transformation functions to a PDF.
    
    Each transformation receives a BytesIO and returns a BytesIO.
    Transformations are applied in order (pipeline).
    """
    if transformations is None:
        transformations = DEFAULT_TRANSFORMATIONS
    
    current_buffer = pdf_buffer
    for transform_fn in transformations:
        current_buffer = transform_fn(current_buffer)
    
    return current_buffer


# ============================================================================
# DYNAMODB OPERATIONS
# ============================================================================

def atomic_increment_counter(dynamodb, table_name: str) -> int:
    """
    Atomically fetch and increment NextIdToSell from state table.
    
    Returns the ID that was claimed (before increment).
    """
    table = dynamodb.Table(table_name)
    
    response = table.update_item(
        Key={'pk': 'global_counter'},
        UpdateExpression='SET NextIdToSell = NextIdToSell + :inc',
        ExpressionAttributeValues={':inc': 1},
        ReturnValues='UPDATED_OLD'  # Get the value BEFORE increment
    )
    
    claimed_id = int(response['Attributes']['NextIdToSell'])
    print(f"Claimed ID: {claimed_id}")
    return claimed_id


def set_status_processing(dynamodb, table_name: str, item_id: int, order_id: str) -> dict:
    """
    Fetch item from inventory table and set status to PROCESSING.
    
    Injects OrderID and UpdatedAt (sparse columns) on-the-fly.
    Only succeeds if current status is AVAILABLE (prevents double-processing).
    Returns the item attributes.
    
    Raises:
        botocore.exceptions.ClientError: If status is not AVAILABLE (ConditionalCheckFailedException)
    """
    table = dynamodb.Table(table_name)
    now = datetime.now(timezone.utc).isoformat()
    
    response = table.update_item(
        Key={'ID': item_id},
        UpdateExpression='SET #status = :processing, OrderID = :oid, UpdatedAt = :time',
        ConditionExpression='#status = :available',
        ExpressionAttributeNames={'#status': 'Status'},
        ExpressionAttributeValues={
            ':processing': 'PROCESSING',
            ':available': 'AVAILABLE',
            ':oid': order_id,
            ':time': now
        },
        ReturnValues='ALL_NEW'
    )
    
    item = response['Attributes']
    print(f"Set ID {item_id} to PROCESSING. OrderID: {order_id}, S3Key: {item.get('S3Key')}")
    return item


def set_status_ready_print(dynamodb, table_name: str, item_id: int) -> None:
    """Update item status to READY_PRINT after successful processing."""
    table = dynamodb.Table(table_name)
    now = datetime.now(timezone.utc).isoformat()
    
    table.update_item(
        Key={'ID': item_id},
        UpdateExpression='SET #status = :ready, UpdatedAt = :time',
        ExpressionAttributeNames={'#status': 'Status'},
        ExpressionAttributeValues={
            ':ready': 'READY_PRINT',
            ':time': now
        }
    )
    
    print(f"Set ID {item_id} to READY_PRINT")


def set_status_failed(dynamodb, table_name: str, item_id: int, error_msg: str) -> None:
    """Mark item as FAILED if processing errors out."""
    table = dynamodb.Table(table_name)
    now = datetime.now(timezone.utc).isoformat()
    
    table.update_item(
        Key={'ID': item_id},
        UpdateExpression='SET #status = :failed, ErrorMessage = :err, UpdatedAt = :time',
        ExpressionAttributeNames={'#status': 'Status'},
        ExpressionAttributeValues={
            ':failed': 'FAILED',
            ':err': error_msg[:500],
            ':time': now
        }
    )
    
    print(f"Set ID {item_id} to FAILED: {error_msg}")


# ============================================================================
# S3 OPERATIONS
# ============================================================================

def download_pdf_from_s3(s3_client, bucket: str, s3_key: str) -> BytesIO:
    """Download PDF from S3 into memory."""
    print(f"Downloading s3://{bucket}/{s3_key}")
    
    buffer = BytesIO()
    s3_client.download_fileobj(bucket, s3_key, buffer)
    buffer.seek(0)
    return buffer


def upload_pdf_to_s3(s3_client, bucket: str, s3_key: str, pdf_buffer: BytesIO) -> None:
    """Upload transformed PDF to S3."""
    print(f"Uploading to s3://{bucket}/{s3_key}")
    
    pdf_buffer.seek(0)
    s3_client.upload_fileobj(pdf_buffer, bucket, s3_key)


def generate_output_key(original_s3_key: str) -> str:
    """
    Convert source key to output key.
    
    VOL00009/EFTA00505541.pdf -> ORDER/EFTA00505541.pdf
    """
    filename = original_s3_key.split('/')[-1]
    return f"ORDER/{filename}"


# ============================================================================
# WEBHOOK PARSING
# ============================================================================

def extract_order_id(event: dict) -> str:
    """
    Extract OrderID from webhook event.
    
    Supports:
    - Stripe: checkout.session.completed -> data.object.id
    - Shopify: order webhook -> id
    - Direct/Test: {"order_id": "xxx"}
    - API Gateway: body may be JSON string
    
    Returns the order ID string.
    """
    # API Gateway wraps body as JSON string
    if 'body' in event and isinstance(event['body'], str):
        try:
            event = json.loads(event['body'])
        except json.JSONDecodeError:
            pass
    
    # Stripe webhook: checkout.session.completed
    if event.get('type', '').startswith('checkout.session'):
        return event['data']['object']['id']
    
    # Stripe webhook: payment_intent.succeeded
    if event.get('type', '').startswith('payment_intent'):
        return event['data']['object']['id']
    
    # Shopify webhook (order object)
    if 'order_number' in event:
        return f"shopify_{event['id']}"
    
    # Direct invocation with order_id field
    if 'order_id' in event:
        return event['order_id']
    
    # Fallback: generate a timestamp-based ID for testing
    return f"direct_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"


# ============================================================================
# LAMBDA HANDLER
# ============================================================================

def lambda_handler(event, context):
    """
    Main Lambda entry point. Triggered by Stripe/Shopify webhook via API Gateway.
    
    Processes the next available PDF in the queue:
    1. Extracts OrderID from webhook payload
    2. Claims an ID atomically
    3. Downloads and transforms the PDF
    4. Uploads to ORDER/ prefix
    5. Updates status to READY_PRINT
    """
    # Load environment variables
    bucket_name = os.environ.get('AWS_S3_BUCKET_NAME')
    db_table = os.environ.get('AWS_DYNAMO_DB_NAME')
    store_table = os.environ.get('AWS_DYNAMO_STORE_DB_NAME')
    
    # Validate required env vars
    missing = []
    if not bucket_name: missing.append('AWS_S3_BUCKET_NAME')
    if not db_table: missing.append('AWS_DYNAMO_DB_NAME')
    if not store_table: missing.append('AWS_DYNAMO_STORE_DB_NAME')
    
    if missing:
        return {
            'statusCode': 500,
            'body': f"Missing environment variables: {', '.join(missing)}"
        }
    
    # Initialize AWS clients
    dynamodb = boto3.resource('dynamodb')
    s3_client = boto3.client('s3')
    
    claimed_id = None
    s3_key = None
    order_id = None
    
    try:
        # Step 1: Extract OrderID from webhook event
        order_id = extract_order_id(event)
        print(f"Processing order: {order_id}")
        
        # Step 2: Atomically claim the next ID
        claimed_id = atomic_increment_counter(dynamodb, store_table)
        
        # Step 3: Set status to PROCESSING and inject OrderID/UpdatedAt
        item = set_status_processing(dynamodb, db_table, claimed_id, order_id)
        s3_key = item.get('S3Key')
        
        if not s3_key:
            raise ValueError(f"No S3Key found for ID {claimed_id}")
        
        # Step 4: Download PDF from S3
        pdf_buffer = download_pdf_from_s3(s3_client, bucket_name, s3_key)
        
        # Step 5: Apply transformation pipeline
        transformed_pdf = apply_transformations(pdf_buffer)
        
        # Step 6: Upload transformed PDF to ORDER/ prefix
        output_key = generate_output_key(s3_key)
        upload_pdf_to_s3(s3_client, bucket_name, output_key, transformed_pdf)
        
        # Step 7: Update status to READY_PRINT
        set_status_ready_print(dynamodb, db_table, claimed_id)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f"Successfully claimed and locked {s3_key} for order {order_id}",
                'order_id': order_id,
                'item_id': claimed_id,
                'output_key': output_key
            })
        }
    
    except Exception as e:
        error_msg = str(e)
        print(f"CRITICAL ERROR: {error_msg}")
        
        # Mark as FAILED if we claimed an ID
        if claimed_id is not None:
            try:
                set_status_failed(dynamodb, db_table, claimed_id, error_msg)
            except Exception as fail_err:
                print(f"Failed to set FAILED status: {fail_err}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Failed to process order',
                'order_id': order_id,
                'item_id': claimed_id,
                'message': error_msg
            })
        }


# ============================================================================
# LOCAL TESTING
# ============================================================================

if __name__ == "__main__":
    # For local testing - requires AWS credentials and env vars
    from dotenv import load_dotenv
    load_dotenv()
    
    # Simulate a test webhook event
    test_event = {
        'order_id': 'test_local_001'
    }
    
    print("Running local test...")
    print(f"Event: {test_event}")
    result = lambda_handler(test_event, None)
    print(f"\nResult: {json.dumps(json.loads(result['body']), indent=2)}" if result['statusCode'] == 200 else f"\nError: {result}")
