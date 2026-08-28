"""
AWS Lambda: PDF Order Processor

Triggered asynchronously by the signature-verified Next.js Stripe webhook route.

Flow:
1. Validate the manual-capture Stripe PaymentIntent
2. Atomically claim one inventory item and record it in Stripe metadata
3. Generate 300-DPI front/back images and upload them to S3
4. Create an idempotent Printful draft order
5. Capture the Stripe authorization
6. Confirm the Printful order for fulfillment
7. Update DynamoDB to SOLD

Environment Variables:
- AWS_S3_BUCKET_NAME: S3 bucket for PDFs and generated images
- AWS_DYNAMO_DB_NAME: DynamoDB inventory table
- AWS_DYNAMO_STORE_DB_NAME: DynamoDB global-counter-only table
- STRIPE_SECRET_KEY or STRIPE_SECRET_KEY_SECRET_ARN
- PRINTFUL_SECRET_KEY or PRINTFUL_TOKEN_SECRET_ARN
- PRINTFUL_STORE_ID: optional for account-level Printful tokens
- ORDER_AMOUNT_CENTS: fixed product subtotal before shipping (default 4400)
- SHIPPING_AMOUNT_CENTS: fixed US Standard shipping (default 495)
- SHIPPING_METHOD: Printful shipping method (default STANDARD)
- PRINTFUL_ASSET_BASE_URL: public base URL for generated ORDERS images
- CONFIRM_PRINTFUL_ORDERS: set true to submit drafts for fulfillment (default false)

The production trigger should be a signature-verified Stripe
checkout.session.completed webhook validated and forwarded asynchronously by
the web application. Direct input requires order_id, payment_intent_id, size,
and quantity=1.
"""

import os
import re
import json
import time
import base64
import boto3
import urllib.error
import urllib.parse
import urllib.request
from io import BytesIO
from datetime import datetime, timezone
from typing import Optional
from botocore.exceptions import ClientError


PRINT_AREA_WIDTH = 3600
PRINT_AREA_HEIGHT = 4800
ORDERS_PREFIX = 'ORDERS'
MONOSPACE_FONT_CANDIDATES = [
    '/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf',
]


def file_id_from_key(s3_key: str) -> str:
    return os.path.splitext(s3_key.split('/')[-1])[0]


def render_front_image(pdf_buffer: BytesIO) -> BytesIO:
    import pymupdf
    from PIL import Image

    pdf_buffer.seek(0)
    document = pymupdf.open(stream=pdf_buffer.read(), filetype='pdf')
    try:
        if document.page_count == 0:
            raise PermanentOrderError('Source PDF has no pages')
        page = document[0]
        zoom = min(PRINT_AREA_WIDTH / page.rect.width, PRINT_AREA_HEIGHT / page.rect.height)
        pixmap = page.get_pixmap(matrix=pymupdf.Matrix(zoom, zoom), alpha=True)
        page_image = Image.open(BytesIO(pixmap.tobytes('png')))
        page_image.load()
    finally:
        document.close()

    canvas = Image.new('RGBA', (PRINT_AREA_WIDTH, PRINT_AREA_HEIGHT), (0, 0, 0, 0))
    offset = ((PRINT_AREA_WIDTH - page_image.width) // 2,
              (PRINT_AREA_HEIGHT - page_image.height) // 2)
    canvas.paste(page_image, offset)
    output = BytesIO()
    canvas.save(output, format='PNG')
    output.seek(0)
    return output


def load_monospace_font(size: int):
    from PIL import ImageFont

    for path in MONOSPACE_FONT_CANDIDATES:
        if os.path.exists(path):
            return ImageFont.truetype(path, size)
    return ImageFont.load_default(size=size)


def render_back_image(file_id: str) -> BytesIO:
    from PIL import Image, ImageDraw

    canvas = Image.new('RGBA', (PRINT_AREA_WIDTH, PRINT_AREA_HEIGHT), (0, 0, 0, 0))
    draw = ImageDraw.Draw(canvas)
    max_text_width = int(PRINT_AREA_WIDTH * 0.85)
    font_size = int(PRINT_AREA_WIDTH * 0.12)
    step = max(10, font_size // 20)
    font = load_monospace_font(font_size)
    while font_size > 20:
        bounding_box = draw.textbbox((0, 0), file_id, font=font)
        if bounding_box[2] - bounding_box[0] <= max_text_width:
            break
        font_size -= step
        font = load_monospace_font(font_size)

    bounding_box = draw.textbbox((0, 0), file_id, font=font)
    text_width = bounding_box[2] - bounding_box[0]
    text_height = bounding_box[3] - bounding_box[1]
    x = (PRINT_AREA_WIDTH - text_width) // 2 - bounding_box[0]
    y = int(PRINT_AREA_HEIGHT * 0.40) - text_height // 2 - bounding_box[1]
    draw.text((x, y), file_id, font=font, fill=(44, 44, 44, 255))

    output = BytesIO()
    canvas.save(output, format='PNG')
    output.seek(0)
    return output


def upload_print_file(s3_client, bucket: str, key: str,
                      png_buffer: BytesIO, asset_base_url: str) -> str:
    png_buffer.seek(0)
    s3_client.upload_fileobj(
        png_buffer, bucket, key, ExtraArgs={'ContentType': 'image/png'})
    encoded_key = urllib.parse.quote(key, safe='/')
    return f"{asset_base_url.rstrip('/')}/{encoded_key}"


def generate_printful_images(s3_client, bucket: str, pdf_s3_key: str,
                             asset_base_url: Optional[str] = None) -> dict:
    file_id = file_id_from_key(pdf_s3_key)
    pdf_buffer = download_pdf_from_s3(s3_client, bucket, pdf_s3_key)
    front_png = render_front_image(pdf_buffer)
    back_png = render_back_image(file_id)
    front_key = f'{ORDERS_PREFIX}/{file_id}/front.png'
    back_key = f'{ORDERS_PREFIX}/{file_id}/back.png'
    base_url = asset_base_url or f'https://{bucket}.s3.amazonaws.com'
    return {
        'file_id': file_id,
        'front_key': front_key,
        'back_key': back_key,
        'front_url': upload_print_file(
            s3_client, bucket, front_key, front_png, base_url),
        'back_url': upload_print_file(
            s3_client, bucket, back_key, back_png, base_url),
    }


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


def set_status_processing(dynamodb, table_name: str, item_id: int, order_id: str,
                          payment_intent_id: str, size: str) -> dict:
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
        UpdateExpression=('SET #status = :processing, OrderID = :oid, '
                          'PaymentIntentID = :payment, ShirtSize = :size, UpdatedAt = :time'),
        ConditionExpression='#status = :available',
        ExpressionAttributeNames={'#status': 'Status'},
        ExpressionAttributeValues={
            ':processing': 'PROCESSING',
            ':available': 'AVAILABLE',
            ':oid': order_id,
            ':payment': payment_intent_id,
            ':size': size,
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
# ORDER WORKFLOW
# ============================================================================

GILDAN_5000_WHITE_VARIANTS = {'S': 11576, 'M': 11577, 'L': 11578, 'XL': 11579}
SUPPORTED_DESTINATION_COUNTRIES = {'US'}
PRINTFUL_API_BASE = 'https://api.printful.com'
STRIPE_API_BASE = 'https://api.stripe.com/v1'
_SECRET_CACHE = {}


class PermanentOrderError(Exception):
    pass


class RetryableOrderError(Exception):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def printful_confirmation_enabled() -> bool:
    return os.environ.get('CONFIRM_PRINTFUL_ORDERS', 'false').strip().lower() == 'true'


def require_remaining_time(context, minimum_milliseconds: int, operation: str) -> None:
    if context and context.get_remaining_time_in_millis() < minimum_milliseconds:
        raise RetryableOrderError(f'Not enough Lambda time remaining for {operation}')


def parse_event(event: dict) -> dict:
    if 'body' in event and isinstance(event['body'], str):
        event = json.loads(event['body'])
    if event.get('type') == 'payment_intent.amount_capturable_updated':
        payment_intent = event['data']['object']
        metadata = payment_intent.get('metadata', {})
        return {
            'order_id': metadata.get('order_id') or metadata.get('orderId'),
            'payment_intent_id': payment_intent['id'],
            'size': metadata.get('size'),
            'quantity': int(metadata.get('quantity', '1')),
        }
    return {
        'order_id': event.get('order_id'),
        'payment_intent_id': event.get('payment_intent_id'),
        'size': event.get('size'),
        'quantity': int(event.get('quantity', 1)),
    }


def get_secret(env_name: str, secret_arn_env_name: str) -> str:
    value = os.environ.get(env_name)
    if value:
        return value
    secret_arn = os.environ.get(secret_arn_env_name)
    if not secret_arn:
        raise PermanentOrderError(f"Set {env_name} or {secret_arn_env_name}")
    if secret_arn not in _SECRET_CACHE:
        response = boto3.client('secretsmanager').get_secret_value(SecretId=secret_arn)
        value = response.get('SecretString')
        if not value:
            value = base64.b64decode(response['SecretBinary']).decode()
        try:
            secret_object = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            secret_object = None
        if isinstance(secret_object, dict):
            value = secret_object.get(env_name)
            if not value:
                raise PermanentOrderError(
                    f'Secret {secret_arn} does not contain {env_name}')
        _SECRET_CACHE[secret_arn] = value
    return _SECRET_CACHE[secret_arn]


def request_json(method: str, url: str, headers: dict, data=None, retries: int = 3) -> dict:
    body = None
    if data is not None:
        if headers.get('Content-Type') == 'application/x-www-form-urlencoded':
            body = urllib.parse.urlencode(data).encode()
        else:
            body = json.dumps(data).encode()
    for attempt in range(retries):
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as error:
            response_body = error.read().decode()
            try:
                details = json.loads(response_body)
            except json.JSONDecodeError:
                details = {'message': response_body or error.reason}
            if error.code == 429 or error.code >= 500:
                if attempt + 1 < retries:
                    time.sleep(2 ** attempt)
                    continue
                raise RetryableOrderError(f"HTTP {error.code} from {url}: {details}") from error
            raise PermanentOrderError(f"HTTP {error.code} from {url}: {details}") from error
        except urllib.error.URLError as error:
            if attempt + 1 < retries:
                time.sleep(2 ** attempt)
                continue
            raise RetryableOrderError(f"Request failed for {url}: {error}") from error


def stripe_request(method: str, path: str, secret_key: str, data=None, idempotency_key=None) -> dict:
    credentials = base64.b64encode(f'{secret_key}:'.encode()).decode()
    headers = {
        'Authorization': f'Basic {credentials}',
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': 'e-files-shirts-lambda/1.0',
    }
    if idempotency_key:
        headers['Idempotency-Key'] = idempotency_key
    return request_json(method, f'{STRIPE_API_BASE}{path}', headers, data)


def printful_request(method: str, path: str, token: str, data=None, allow_not_found=False) -> Optional[dict]:
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'e-files-shirts-lambda/1.0',
    }
    store_id = os.environ.get('PRINTFUL_STORE_ID')
    if store_id:
        headers['X-PF-Store-Id'] = store_id
    try:
        response = request_json(method, f'{PRINTFUL_API_BASE}{path}', headers, data)
    except PermanentOrderError as error:
        if allow_not_found and 'HTTP 404' in str(error):
            return None
        raise
    if response.get('code') != 200 or not isinstance(response.get('result'), dict):
        raise PermanentOrderError(f"Printful rejected {path}: {response}")
    return response['result']


def parse_nonnegative_cents(metadata: dict, field: str) -> int:
    try:
        value = int(metadata[field])
    except (KeyError, TypeError, ValueError) as error:
        raise PermanentOrderError(f'PaymentIntent metadata {field} is invalid') from error
    if value < 0:
        raise PermanentOrderError(f'PaymentIntent metadata {field} is invalid')
    return value


def payment_pricing(payment_intent: dict) -> dict:
    metadata = payment_intent.get('metadata', {})
    product_amount = parse_nonnegative_cents(metadata, 'product_amount_cents')
    shipping_amount = parse_nonnegative_cents(metadata, 'shipping_amount_cents')
    tax_amount = parse_nonnegative_cents(metadata, 'tax_amount_cents')
    shipping_method = metadata.get('shipping_method', '')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', shipping_method):
        raise PermanentOrderError('PaymentIntent shipping method is invalid')
    return {
        'product_amount_cents': product_amount,
        'shipping_amount_cents': shipping_amount,
        'tax_amount_cents': tax_amount,
        'shipping_method': shipping_method,
        'total_amount_cents': product_amount + shipping_amount + tax_amount,
    }


def validate_payment(payment_intent: dict, order_id: str, requested_size: Optional[str],
                     quantity: int, allow_succeeded: bool = False) -> str:
    metadata = payment_intent.get('metadata', {})
    payment_order_id = metadata.get('order_id') or metadata.get('orderId')
    size = (metadata.get('size') or requested_size or '').upper()
    configured_product_amount = int(os.environ.get('ORDER_AMOUNT_CENTS', '4400'))
    configured_shipping_amount = int(os.environ.get('SHIPPING_AMOUNT_CENTS', '495'))
    configured_shipping_method = os.environ.get('SHIPPING_METHOD', 'STANDARD')
    pricing = payment_pricing(payment_intent)
    if not order_id or not re.fullmatch(r'[A-Za-z0-9_-]{1,32}', order_id):
        raise PermanentOrderError('order_id must be 1-32 letters, numbers, underscores, or hyphens')
    if payment_order_id != order_id:
        raise PermanentOrderError('PaymentIntent order metadata does not match order_id')
    if requested_size and requested_size.upper() != size:
        raise PermanentOrderError('Requested size does not match PaymentIntent metadata')
    if size not in GILDAN_5000_WHITE_VARIANTS:
        supported = ', '.join(GILDAN_5000_WHITE_VARIANTS)
        raise PermanentOrderError(f'Only White Gildan 5000 sizes {supported} are supported')
    if quantity != 1 or metadata.get('quantity', '1') != '1':
        raise PermanentOrderError('MVP orders must have quantity 1')
    if payment_intent.get('capture_method') != 'manual':
        raise PermanentOrderError('PaymentIntent must use manual capture')
    payment_amount = int(payment_intent.get('amount', 0))
    if payment_intent.get('currency') != 'usd':
        raise PermanentOrderError('PaymentIntent currency must be USD')
    if pricing['product_amount_cents'] != configured_product_amount:
        raise PermanentOrderError('PaymentIntent product price is invalid')
    if pricing['shipping_amount_cents'] != configured_shipping_amount:
        raise PermanentOrderError('PaymentIntent shipping price is invalid')
    if pricing['shipping_method'] != configured_shipping_method:
        raise PermanentOrderError('PaymentIntent shipping method is invalid')
    if pricing['tax_amount_cents'] != 0:
        raise PermanentOrderError('PaymentIntent tax amount must be zero for the MVP')
    if payment_amount != pricing['total_amount_cents']:
        raise PermanentOrderError('PaymentIntent amount does not match its server-calculated breakdown')
    country_code = ((payment_intent.get('shipping') or {}).get('address') or {}).get('country', '').upper()
    if country_code not in SUPPORTED_DESTINATION_COUNTRIES:
        raise PermanentOrderError('Only US shipping addresses are supported')
    status = payment_intent.get('status')
    if status != 'requires_capture' and not (allow_succeeded and status == 'succeeded'):
        raise PermanentOrderError(f"PaymentIntent is not capturable: {status}")
    if status == 'requires_capture' and int(payment_intent.get('amount_capturable', 0)) < payment_amount:
        raise PermanentOrderError('PaymentIntent does not have the full amount available to capture')
    return size


def recipient_from_payment(payment_intent: dict) -> dict:
    shipping = payment_intent.get('shipping') or {}
    address = shipping.get('address') or {}
    recipient = {
        'name': shipping.get('name'),
        'address1': address.get('line1'),
        'city': address.get('city'),
        'state_code': address.get('state'),
        'country_code': address.get('country'),
        'zip': address.get('postal_code'),
        'email': payment_intent.get('receipt_email') or payment_intent.get('metadata', {}).get('customer_email'),
        'phone': shipping.get('phone'),
    }
    if address.get('line2'):
        recipient['address2'] = address['line2']
    required = ('name', 'address1', 'city', 'state_code', 'country_code', 'zip', 'email')
    missing = [field for field in required if not recipient.get(field)]
    if missing:
        raise PermanentOrderError(f"PaymentIntent shipping data is missing: {', '.join(missing)}")
    return {key: value for key, value in recipient.items() if value}


def claim_order(dynamodb, files_table: str, state_table: str, order_id: str,
                payment_intent_id: str, size: str, resume_item_id: Optional[int] = None) -> dict:
    if resume_item_id is not None:
        item = dynamodb.Table(files_table).get_item(
            Key={'ID': resume_item_id}, ConsistentRead=True).get('Item')
        if not item or item.get('OrderID') != order_id or item.get('PaymentIntentID') != payment_intent_id:
            raise PermanentOrderError('Resume inventory ID does not match this order')
        return item

    for _ in range(25):
        candidate = atomic_increment_counter(dynamodb, state_table)
        try:
            return set_status_processing(
                dynamodb, files_table, candidate, order_id, payment_intent_id, size)
        except ClientError as error:
            if error.response.get('Error', {}).get('Code') != 'ConditionalCheckFailedException':
                raise
            print(f"Inventory ID {candidate} was not AVAILABLE; requesting the next counter value")
    raise RetryableOrderError('Could not claim an AVAILABLE inventory item after 25 counter increments')


def update_workflow_status(dynamodb, files_table: str,
                           item_id: int, status: str, **attributes) -> bool:
    now = utc_now()
    names = {'#status': 'Status'}
    values = {
        ':status': status,
        ':now': now,
        ':sold': 'SOLD',
        ':failed': 'FAILED',
        ':refunded': 'REFUNDED_FAILED',
    }
    assignments = ['#status = :status', 'UpdatedAt = :now']
    for index, (name, value) in enumerate(attributes.items()):
        if value is None:
            continue
        name_key = f'#field{index}'
        value_key = f':value{index}'
        names[name_key] = name
        values[value_key] = value
        assignments.append(f'{name_key} = {value_key}')
    update_expression = f"SET {', '.join(assignments)}"
    try:
        dynamodb.Table(files_table).update_item(
            Key={'ID': item_id}, UpdateExpression=update_expression,
            ConditionExpression=(
                'attribute_not_exists(#status) OR '
                'NOT (#status IN (:sold, :failed, :refunded)) OR #status = :status'),
            ExpressionAttributeNames=names, ExpressionAttributeValues=values)
        return True
    except ClientError as error:
        if error.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
            return False
        raise


def validate_printful_order(order: dict, order_id: str, variant_id: int,
                            shipping_method: Optional[str] = None) -> dict:
    items = order.get('items') or []
    existing_variant = items[0].get('variant_id') if items else None
    if order.get('status') in ('failed', 'canceled'):
        raise PermanentOrderError(f"Existing Printful order is {order.get('status')}")
    if order.get('external_id') != order_id or int(existing_variant or 0) != variant_id:
        raise PermanentOrderError('Existing Printful order does not match this order and size')
    if shipping_method and order.get('shipping') != shipping_method:
        raise PermanentOrderError('Existing Printful order uses a different shipping method')
    return order


def get_or_create_printful_draft(token: str, order_id: str, recipient: dict, variant_id: int,
                                  front_url: str, back_url: str, pricing: dict) -> dict:
    shipping_method = pricing['shipping_method']
    external_path = urllib.parse.quote(f'@{order_id}', safe='@')
    existing = printful_request('GET', f'/orders/{external_path}', token, allow_not_found=True)
    if existing:
        return validate_printful_order(
            existing, order_id, variant_id, shipping_method)

    payload = {
        'external_id': order_id,
        'shipping': shipping_method,
        'recipient': recipient,
        'items': [{
            'variant_id': variant_id,
            'quantity': 1,
            'name': 'Mystery File T-Shirt',
            'retail_price': f"{pricing['product_amount_cents'] / 100:.2f}",
            'files': [
                {'type': 'front', 'url': front_url},
                {'type': 'back', 'url': back_url},
            ],
        }],
        'retail_costs': {
            'currency': 'USD',
            'subtotal': f"{pricing['product_amount_cents'] / 100:.2f}",
            'shipping': f"{pricing['shipping_amount_cents'] / 100:.2f}",
            'tax': f"{pricing['tax_amount_cents'] / 100:.2f}",
        },
    }
    try:
        return validate_printful_order(
            printful_request('POST', '/orders', token, payload),
            order_id, variant_id, shipping_method)
    except PermanentOrderError as create_error:
        existing = printful_request(
            'GET', f'/orders/{external_path}', token, allow_not_found=True)
        if existing:
            return validate_printful_order(
                existing, order_id, variant_id, shipping_method)
        raise create_error


def attach_inventory_id_to_payment(secret_key: str, payment_intent_id: str,
                                   order_id: str, item_id: int) -> int:
    try:
        payment_intent = stripe_request(
            'POST', f'/payment_intents/{payment_intent_id}', secret_key,
            {'metadata[inventory_id]': str(item_id)}, f'{order_id}-assign-inventory')
    except (RetryableOrderError, PermanentOrderError):
        payment_intent = stripe_request(
            'GET', f'/payment_intents/{payment_intent_id}', secret_key)

    assigned_id = payment_intent.get('metadata', {}).get('inventory_id')
    if not assigned_id:
        raise RetryableOrderError('Stripe has not recorded an inventory assignment yet')
    try:
        return int(assigned_id)
    except (TypeError, ValueError) as error:
        raise PermanentOrderError('Stripe inventory assignment is invalid') from error


def capture_payment(secret_key: str, payment_intent: dict, order_id: str) -> dict:
    if payment_intent.get('status') == 'succeeded':
        return payment_intent
    return stripe_request('POST', f"/payment_intents/{payment_intent['id']}/capture",
                          secret_key, {}, f'{order_id}-capture')


def cancel_payment(secret_key: str, payment_intent: dict, order_id: str) -> None:
    if payment_intent.get('status') not in ('succeeded', 'canceled'):
        stripe_request('POST', f"/payment_intents/{payment_intent['id']}/cancel",
                       secret_key, {}, f'{order_id}-cancel')


def refund_payment(secret_key: str, payment_intent_id: str, order_id: str) -> None:
    stripe_request('POST', '/refunds', secret_key,
                   {'payment_intent': payment_intent_id}, f'{order_id}-refund')


def confirm_printful_order(token: str, printful_order_id: int, order_id: str) -> dict:
    order = printful_request('GET', f'/orders/{printful_order_id}', token)
    if order.get('external_id') != order_id:
        raise PermanentOrderError('Printful order external_id does not match this order')
    if order.get('status') == 'draft':
        return printful_request('POST', f'/orders/{printful_order_id}/confirm', token, {})
    if order.get('status') in ('failed', 'canceled'):
        raise PermanentOrderError(f"Printful order is {order.get('status')}")
    return order


# ============================================================================
# LAMBDA HANDLER
# ============================================================================

def lambda_handler(event, context):
    bucket_name = os.environ.get('AWS_S3_BUCKET_NAME')
    files_table = os.environ.get('AWS_DYNAMO_DB_NAME')
    state_table = os.environ.get('AWS_DYNAMO_STORE_DB_NAME')
    missing = [name for name, value in (
        ('AWS_S3_BUCKET_NAME', bucket_name), ('AWS_DYNAMO_DB_NAME', files_table),
        ('AWS_DYNAMO_STORE_DB_NAME', state_table)) if not value]
    if missing:
        raise PermanentOrderError(f"Missing environment variables: {', '.join(missing)}")

    order_id = None
    try:
        payload = parse_event(event)
        order_id = payload.get('order_id')
        payment_intent_id = payload.get('payment_intent_id')
        if not order_id or not payment_intent_id:
            raise PermanentOrderError('order_id and payment_intent_id are required')

        dynamodb = boto3.resource('dynamodb')
        s3_client = boto3.client('s3')
        stripe_secret = get_secret('STRIPE_SECRET_KEY', 'STRIPE_SECRET_KEY_SECRET_ARN')
        printful_token = get_secret('PRINTFUL_SECRET_KEY', 'PRINTFUL_TOKEN_SECRET_ARN')
        payment_intent = stripe_request('GET', f'/payment_intents/{payment_intent_id}', stripe_secret)
        inventory_id = payment_intent.get('metadata', {}).get('inventory_id')
        resume_item_id = int(inventory_id) if inventory_id else None
        size = validate_payment(
            payment_intent, order_id, payload.get('size'), payload['quantity'],
            allow_succeeded=resume_item_id is not None)
        recipient = recipient_from_payment(payment_intent)
    except PermanentOrderError as error:
        print(f"Order validation failure for {order_id or 'unknown'}: {error}")
        return {'statusCode': 422, 'body': json.dumps({'status': 'FAILED', 'message': str(error)})}
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"Malformed order event for {order_id or 'unknown'}: {error}")
        return {'statusCode': 400, 'body': json.dumps({'status': 'FAILED', 'message': str(error)})}

    item = None
    try:
        item = claim_order(
            dynamodb, files_table, state_table, order_id, payment_intent_id,
            size, resume_item_id)
        item_id = int(item['ID'])
        if resume_item_id is None:
            assigned_item_id = attach_inventory_id_to_payment(
                stripe_secret, payment_intent_id, order_id, item_id)
            if assigned_item_id != item_id:
                item = claim_order(
                    dynamodb, files_table, state_table, order_id, payment_intent_id,
                    size, assigned_item_id)
                item_id = assigned_item_id
        if item.get('Status') == 'SOLD':
            return {'statusCode': 200, 'body': json.dumps({
                'order_id': order_id, 'item_id': item_id,
                'file_id': item.get('FileID'),
                'printful_order_id': int(item['PrintfulOrderID']), 'status': 'SOLD'})}
        if item.get('Status') in ('FAILED', 'REFUNDED_FAILED'):
            raise PermanentOrderError(f"Order is already {item.get('Status')}")

        pricing = payment_pricing(payment_intent)
        printful_order_id = item.get('PrintfulOrderID')
        if not printful_order_id:
            image_result = generate_printful_images(
                s3_client, bucket_name, item['S3Key'],
                os.environ.get('PRINTFUL_ASSET_BASE_URL'))
            printful_order = get_or_create_printful_draft(
                printful_token, order_id, recipient, GILDAN_5000_WHITE_VARIANTS[size],
                image_result['front_url'], image_result['back_url'], pricing)
            printful_order_id = int(printful_order['id'])
            update_workflow_status(
                dynamodb, files_table, item_id,
                'PRINTFUL_DRAFT_CREATED', PrintfulOrderID=printful_order_id,
                FrontS3Key=image_result['front_key'], BackS3Key=image_result['back_key'])

        require_remaining_time(context, 30000, 'payment capture and Printful confirmation')
        printful_order = validate_printful_order(
            printful_request('GET', f'/orders/{int(printful_order_id)}', printful_token),
            order_id, GILDAN_5000_WHITE_VARIANTS[size], pricing['shipping_method'])
        payment_intent = stripe_request(
            'GET', f'/payment_intents/{payment_intent_id}', stripe_secret)
        validate_payment(payment_intent, order_id, size, 1, allow_succeeded=True)
        captured = capture_payment(stripe_secret, payment_intent, order_id)
        update_workflow_status(
            dynamodb, files_table, item_id,
            'PAYMENT_CAPTURED', PrintfulOrderID=printful_order_id)
        file_id = os.path.splitext(os.path.basename(item['S3Key']))[0]
        if not printful_confirmation_enabled():
            print(f'Printful confirmation disabled; leaving order {printful_order_id} as a draft')
            update_workflow_status(
                dynamodb, files_table, item_id, 'DRAFT_ONLY',
                PrintfulOrderID=int(printful_order_id), StripePaymentStatus=captured['status'],
                PrintfulStatus=printful_order['status'], FileID=file_id)
            return {'statusCode': 200, 'body': json.dumps({
                'order_id': order_id, 'item_id': item_id, 'file_id': file_id,
                'printful_order_id': int(printful_order_id), 'status': 'DRAFT_ONLY'})}

        require_remaining_time(context, 10000, 'Printful confirmation')
        try:
            confirmed = confirm_printful_order(
                printful_token, int(printful_order_id), order_id)
        except PermanentOrderError:
            refund_payment(stripe_secret, payment_intent_id, order_id)
            update_workflow_status(
                dynamodb, files_table, item_id,
                'REFUNDED_FAILED', PrintfulOrderID=printful_order_id,
                ErrorMessage='Printful confirmation failed after payment capture')
            raise

        update_workflow_status(
            dynamodb, files_table, item_id, 'SOLD',
            PrintfulOrderID=int(printful_order_id), StripePaymentStatus=captured['status'],
            PrintfulStatus=confirmed['status'], FileID=file_id)
        return {'statusCode': 200, 'body': json.dumps({
            'order_id': order_id, 'item_id': item_id, 'file_id': file_id,
            'printful_order_id': int(printful_order_id), 'status': 'SOLD'})}

    except RetryableOrderError as error:
        if item:
            update_workflow_status(
                dynamodb, files_table, int(item['ID']),
                'PROCESSING_RETRY', ErrorMessage=str(error)[:500])
        raise
    except PermanentOrderError as error:
        if item and item.get('Status') not in ('FAILED', 'REFUNDED_FAILED'):
            latest_payment = stripe_request(
                'GET', f'/payment_intents/{payment_intent_id}', stripe_secret)
            if latest_payment.get('status') != 'succeeded':
                cancel_payment(stripe_secret, latest_payment, order_id)
                update_workflow_status(
                    dynamodb, files_table, int(item['ID']),
                    'FAILED', ErrorMessage=str(error)[:500])
        print(f"Permanent order failure for {order_id}: {error}")
        return {'statusCode': 422, 'body': json.dumps({
            'order_id': order_id, 'status': 'FAILED', 'message': str(error)})}
    except Exception as error:
        if item:
            update_workflow_status(
                dynamodb, files_table, int(item['ID']),
                'PROCESSING_RETRY', ErrorMessage=str(error)[:500])
        raise


# ============================================================================
# LOCAL TESTING
# ============================================================================

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env.development'))
    test_event = {
        'order_id': os.environ.get('TEST_ORDER_ID'),
        'payment_intent_id': os.environ.get('TEST_PAYMENT_INTENT_ID'),
        'size': os.environ.get('TEST_SIZE', 'M'),
        'quantity': 1,
    }
    print("Running local test...")
    print(f"Event: {test_event}")
    result = lambda_handler(test_event, None)
    print(json.dumps(json.loads(result['body']), indent=2))
