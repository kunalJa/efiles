import json
import unittest
import urllib.error
from io import BytesIO
from unittest.mock import DEFAULT, MagicMock, patch

import pymupdf
from botocore.exceptions import ClientError
from PIL import Image

import lambda_order_processor as processor


class OrderValidationTests(unittest.TestCase):
    def setUp(self):
        self.payment_intent = {
            'id': 'pi_test',
            'livemode': False,
            'amount': 4895,
            'amount_capturable': 4895,
            'currency': 'usd',
            'capture_method': 'manual',
            'status': 'requires_capture',
            'metadata': {
                'order_id': 'order_test',
                'size': 'M',
                'quantity': '1',
                'customer_email': 'customer@example.com',
                'product_amount_cents': '4400',
                'shipping_amount_cents': '495',
                'tax_amount_cents': '0',
                'shipping_method': 'STANDARD',
            },
            'shipping': {
                'name': 'Test Customer',
                'phone': '5555555555',
                'address': {
                    'line1': '123 Main St',
                    'line2': 'Apt 4',
                    'city': 'Burbank',
                    'state': 'CA',
                    'country': 'US',
                    'postal_code': '91502',
                },
            },
        }

    def test_parses_amount_capturable_webhook(self):
        payload = processor.parse_event({
            'type': 'payment_intent.amount_capturable_updated',
            'data': {'object': self.payment_intent},
        })
        self.assertEqual(payload, {
            'order_id': 'order_test',
            'payment_intent_id': 'pi_test',
            'size': 'M',
            'quantity': 1,
        })

    def test_validates_manual_capture_payment(self):
        self.assertEqual(
            processor.validate_payment(self.payment_intent, 'order_test', 'M', 1), 'M')

    @patch.dict('os.environ', {'CONFIRM_PRINTFUL_ORDERS': 'true'})
    def test_test_payment_cannot_submit_real_fulfillment(self):
        with self.assertRaises(processor.PermanentOrderError):
            processor.validate_payment(self.payment_intent, 'order_test', 'M', 1)

    @patch.dict('os.environ', {'CONFIRM_PRINTFUL_ORDERS': 'false'})
    def test_live_payment_cannot_be_captured_for_a_draft_only_order(self):
        self.payment_intent['livemode'] = True
        with self.assertRaises(processor.PermanentOrderError):
            processor.validate_payment(self.payment_intent, 'order_test', 'M', 1)

    @patch.dict('os.environ', {'CONFIRM_PRINTFUL_ORDERS': 'true'})
    def test_live_payment_can_submit_real_fulfillment(self):
        self.payment_intent['livemode'] = True
        self.assertEqual(processor.validate_payment(self.payment_intent, 'order_test', 'M', 1), 'M')

    def test_supports_large_and_extra_large(self):
        for size in ('L', 'XL'):
            self.payment_intent['metadata']['size'] = size
            self.assertEqual(
                processor.validate_payment(self.payment_intent, 'order_test', size, 1), size)

    def test_rejects_partial_authorization(self):
        self.payment_intent['amount_capturable'] = 4874
        with self.assertRaises(processor.PermanentOrderError):
            processor.validate_payment(self.payment_intent, 'order_test', 'M', 1)

    def test_rejects_modified_fixed_shipping(self):
        self.payment_intent['amount'] = 5000
        self.payment_intent['amount_capturable'] = 5000
        self.payment_intent['metadata']['shipping_amount_cents'] = '600'
        with self.assertRaises(processor.PermanentOrderError):
            processor.validate_payment(self.payment_intent, 'order_test', 'M', 1)

    def test_rejects_amount_that_does_not_match_breakdown(self):
        self.payment_intent['amount'] = 5000
        self.payment_intent['amount_capturable'] = 5000
        with self.assertRaises(processor.PermanentOrderError):
            processor.validate_payment(self.payment_intent, 'order_test', 'M', 1)

    def test_rejects_quantity_above_one(self):
        with self.assertRaises(processor.PermanentOrderError):
            processor.validate_payment(self.payment_intent, 'order_test', 'M', 2)

    def test_rejects_non_us_shipping_address(self):
        self.payment_intent['shipping']['address']['country'] = 'CA'
        with self.assertRaises(processor.PermanentOrderError):
            processor.validate_payment(self.payment_intent, 'order_test', 'M', 1)

    def test_rejects_tax_added_to_mvp_payment(self):
        self.payment_intent['metadata']['tax_amount_cents'] = '100'
        self.payment_intent['amount'] = 4975
        self.payment_intent['amount_capturable'] = 4975
        with self.assertRaises(processor.PermanentOrderError):
            processor.validate_payment(self.payment_intent, 'order_test', 'M', 1)

    def test_rejects_captured_payment_without_existing_order(self):
        self.payment_intent['status'] = 'succeeded'
        with self.assertRaises(processor.PermanentOrderError):
            processor.validate_payment(self.payment_intent, 'order_test', 'M', 1)
        self.assertEqual(
            processor.validate_payment(
                self.payment_intent, 'order_test', 'M', 1, allow_succeeded=True),
            'M')

    def test_builds_printful_recipient_from_stripe_shipping(self):
        recipient = processor.recipient_from_payment(self.payment_intent)
        self.assertEqual(recipient['address1'], '123 Main St')
        self.assertEqual(recipient['address2'], 'Apt 4')
        self.assertEqual(recipient['email'], 'customer@example.com')


class LambdaHandlerTests(unittest.TestCase):
    @patch.dict('os.environ', {
        'AWS_S3_BUCKET_NAME': 'bucket',
        'AWS_DYNAMO_DB_NAME': 'files',
        'AWS_DYNAMO_STORE_DB_NAME': 'state',
    }, clear=True)
    @patch('builtins.print')
    def test_logs_pre_claim_validation_failure(self, print_mock):
        with self.assertRaises(processor.PermanentOrderError):
            processor.lambda_handler({}, None)

        print_mock.assert_called_once_with(
            'Order validation failure for unknown: order_id and payment_intent_id are required')

    @patch.dict('os.environ', {
        'AWS_S3_BUCKET_NAME': 'bucket',
        'AWS_DYNAMO_DB_NAME': 'files',
        'AWS_DYNAMO_STORE_DB_NAME': 'state',
    }, clear=True)
    def test_draft_only_sends_confirmation_without_confirming_printful(self):
        with patch.multiple('lambda_order_processor', **{
            name: DEFAULT for name in (
                'boto3', 'get_secret', 'stripe_request', 'validate_payment',
                'recipient_from_payment', 'claim_order', 'payment_pricing',
                'printful_request', 'validate_printful_order', 'capture_payment',
                'update_workflow_status', 'send_order_confirmation',
                'printful_confirmation_enabled', 'confirm_printful_order')
        }) as mocks:
            mocks['stripe_request'].return_value = {'metadata': {'inventory_id': '42'}}
            mocks['validate_payment'].return_value = 'M'
            mocks['recipient_from_payment'].return_value = {'email': 'customer@example.com'}
            mocks['claim_order'].return_value = {
                'ID': 42, 'Status': 'DRAFT_ONLY', 'PrintfulOrderID': 123,
                'S3Key': 'VOL00001/EFTA00000042.pdf'}
            mocks['payment_pricing'].return_value = {'total_amount_cents': 4895, 'shipping_method': 'STANDARD'}
            mocks['printful_request'].return_value = {'status': 'draft'}
            mocks['validate_printful_order'].return_value = {'status': 'draft'}
            mocks['capture_payment'].return_value = {'status': 'succeeded'}
            mocks['printful_confirmation_enabled'].return_value = False

            result = processor.lambda_handler({
                'order_id': 'order_test', 'payment_intent_id': 'pi_test', 'size': 'M'}, None)

            self.assertEqual(json.loads(result['body'])['status'], 'DRAFT_ONLY')
            mocks['send_order_confirmation'].assert_called_once_with(
                mocks['boto3'].resource.return_value, 'files', 42, 'order_test',
                {'email': 'customer@example.com'}, 'M',
                {'total_amount_cents': 4895, 'shipping_method': 'STANDARD'})
            mocks['confirm_printful_order'].assert_not_called()

    @patch.dict('os.environ', {
        'AWS_S3_BUCKET_NAME': 'bucket',
        'AWS_DYNAMO_DB_NAME': 'files',
        'AWS_DYNAMO_STORE_DB_NAME': 'state',
    }, clear=True)
    def test_captured_order_that_failed_before_retry_is_refunded_and_raises(self):
        with patch.multiple('lambda_order_processor', **{
            name: DEFAULT for name in (
                'boto3', 'get_secret', 'stripe_request', 'validate_payment',
                'recipient_from_payment', 'claim_order', 'payment_pricing',
                'printful_request', 'update_workflow_status', 'refund_payment',
                'capture_payment', 'send_order_confirmation')
        }) as mocks:
            mocks['stripe_request'].return_value = {'status': 'succeeded', 'metadata': {'inventory_id': '42'}}
            mocks['validate_payment'].return_value = 'M'
            mocks['claim_order'].return_value = {
                'ID': 42, 'Status': 'PROCESSING_RETRY', 'PrintfulOrderID': 123,
                'S3Key': 'VOL00001/EFTA00000042.pdf'}
            mocks['payment_pricing'].return_value = {'shipping_method': 'STANDARD'}
            mocks['printful_request'].return_value = {
                'id': 123, 'external_id': 'order_test', 'status': 'failed',
                'items': [{'variant_id': 11577}], 'shipping': 'STANDARD'}

            with self.assertRaises(processor.PermanentOrderError):
                processor.lambda_handler({
                    'order_id': 'order_test', 'payment_intent_id': 'pi_test', 'size': 'M'}, None)

            mocks['refund_payment'].assert_called_once()
            self.assertEqual(mocks['update_workflow_status'].call_args.args[3], 'REFUNDED_FAILED')
            mocks['capture_payment'].assert_not_called()
            mocks['send_order_confirmation'].assert_not_called()


class CompletedOrderEmailTests(unittest.TestCase):
    def setUp(self):
        env = patch.dict('os.environ', {
            'AWS_S3_BUCKET_NAME': 'bucket', 'AWS_DYNAMO_DB_NAME': 'files',
            'AWS_DYNAMO_STORE_DB_NAME': 'state'}, clear=True)
        env.start()
        self.addCleanup(env.stop)
        dependencies = patch.multiple('lambda_order_processor', **{
            name: DEFAULT for name in (
                'boto3', 'get_secret', 'stripe_request', 'validate_payment',
                'recipient_from_payment', 'claim_order', 'payment_pricing',
                'generate_printful_images', 'get_or_create_printful_draft',
                'printful_request', 'validate_printful_order', 'capture_payment',
                'update_workflow_status', 'send_order_confirmation',
                'printful_confirmation_enabled', 'confirm_printful_order',
                'refund_payment', 'cancel_payment')})
        self.mocks = dependencies.start()
        self.addCleanup(dependencies.stop)
        self.mocks['stripe_request'].return_value = {
            'status': 'succeeded', 'metadata': {'inventory_id': '42'}}
        self.mocks['validate_payment'].return_value = 'M'
        self.mocks['recipient_from_payment'].return_value = {'email': 'customer@example.com'}
        self.item = {'ID': 42, 'Status': 'DRAFT_ONLY', 'PrintfulOrderID': 123,
                     'S3Key': 'VOL00001/EFTA00000042.pdf', 'FileID': 'EFTA00000042'}
        self.mocks['claim_order'].return_value = self.item
        self.mocks['payment_pricing'].return_value = {
            'total_amount_cents': 4895, 'shipping_method': 'STANDARD'}
        self.mocks['printful_request'].return_value = {'status': 'draft'}
        self.mocks['capture_payment'].return_value = {'status': 'succeeded'}
        self.mocks['confirm_printful_order'].return_value = {'status': 'pending'}
        self.mocks['printful_confirmation_enabled'].return_value = False

    def invoke(self):
        return processor.lambda_handler({
            'order_id': 'order_test', 'payment_intent_id': 'pi_test', 'size': 'M'}, None)

    def test_completed_order_retry_only_attempts_email(self):
        for status in ('SOLD', 'DRAFT_ONLY'):
            with self.subTest(status=status):
                self.item['Status'] = status
                result = self.invoke()
                self.assertEqual(json.loads(result['body'])['status'], status)
                self.mocks['send_order_confirmation'].assert_called()
                for name in ('generate_printful_images', 'get_or_create_printful_draft',
                             'printful_request', 'capture_payment', 'confirm_printful_order',
                             'update_workflow_status', 'refund_payment', 'cancel_payment'):
                    self.mocks[name].assert_not_called()

    def test_email_failure_after_completion_raises_without_changing_order(self):
        for status in ('SOLD', 'DRAFT_ONLY'):
            for error in (RuntimeError('sender failed'),
                          processor.RetryableOrderError('active email claim'),
                          processor.PermanentOrderError('email configuration'),
                          ClientError({'Error': {'Code': 'MessageRejected'}}, 'SendEmail')):
                with self.subTest(status=status, error=type(error).__name__):
                    self.item['Status'] = status
                    self.mocks['send_order_confirmation'].side_effect = error
                    with self.assertRaises(processor.ConfirmationEmailError) as raised:
                        self.invoke()
                    self.assertIs(raised.exception.__cause__, error)
                    for name in ('update_workflow_status', 'capture_payment',
                                 'confirm_printful_order', 'refund_payment', 'cancel_payment'):
                        self.mocks[name].assert_not_called()

    def test_first_completion_remains_saved_when_email_fails(self):
        for confirm, expected_status in ((False, 'DRAFT_ONLY'), (True, 'SOLD')):
            with self.subTest(status=expected_status):
                self.item['Status'] = 'PRINTFUL_DRAFT_CREATED'
                self.mocks['printful_confirmation_enabled'].return_value = confirm
                self.mocks['send_order_confirmation'].side_effect = RuntimeError('email failed')
                update = self.mocks['update_workflow_status']
                update.reset_mock()
                with self.assertRaises(processor.ConfirmationEmailError):
                    self.invoke()
                self.assertEqual([call.args[3] for call in update.call_args_list],
                                 ['PAYMENT_CAPTURED', expected_status])
                self.mocks['refund_payment'].assert_not_called()
                self.mocks['cancel_payment'].assert_not_called()


class SecretLoadingTests(unittest.TestCase):
    def tearDown(self):
        processor._SECRET_CACHE.clear()

    @patch.dict('os.environ', {'PRINTFUL_TOKEN_SECRET_ARN': 'arn:printful'}, clear=True)
    @patch('lambda_order_processor.boto3.client')
    def test_loads_printful_key_from_json_secret(self, boto_client):
        boto_client.return_value.get_secret_value.return_value = {
            'SecretString': '{"PRINTFUL_SECRET_KEY": "secret-value"}',
        }

        value = processor.get_secret(
            'PRINTFUL_SECRET_KEY', 'PRINTFUL_TOKEN_SECRET_ARN')

        self.assertEqual(value, 'secret-value')


class DynamoAllocationTests(unittest.TestCase):
    def test_counter_returns_previous_value_and_stores_next_value(self):
        table = MagicMock()
        table.update_item.return_value = {'Attributes': {'NextIdToSell': 5}}
        dynamodb = MagicMock()
        dynamodb.Table.return_value = table

        claimed_id = processor.atomic_increment_counter(dynamodb, 'state')

        self.assertEqual(claimed_id, 5)
        self.assertEqual(table.update_item.call_args.kwargs['ReturnValues'], 'UPDATED_OLD')

    @patch('lambda_order_processor.set_status_processing')
    @patch('lambda_order_processor.atomic_increment_counter')
    def test_unavailable_item_requests_next_atomic_counter_value(self, increment, set_processing):
        increment.side_effect = [5, 6]
        unavailable = ClientError(
            {'Error': {'Code': 'ConditionalCheckFailedException', 'Message': 'not available'}},
            'UpdateItem')
        item = {'ID': 6, 'Status': 'PROCESSING', 'S3Key': 'VOL00001/EFTA00000006.pdf'}
        set_processing.side_effect = [unavailable, item]

        returned_item = processor.claim_order(
            MagicMock(), 'files', 'state', 'order_test', 'pi_test', 'M')

        self.assertEqual(returned_item, item)
        self.assertEqual(increment.call_count, 2)
        self.assertEqual(set_processing.call_args.args[2], 6)

    def test_resume_item_must_match_order_and_payment(self):
        files_table = MagicMock()
        files_table.get_item.return_value = {'Item': {
            'ID': 42,
            'OrderID': 'order_test',
            'PaymentIntentID': 'pi_test',
            'S3Key': 'VOL00001/EFTA00000042.pdf',
        }}
        dynamodb = MagicMock()
        dynamodb.Table.return_value = files_table

        item = processor.claim_order(
            dynamodb, 'files', 'state', 'order_test', 'pi_test', 'M', resume_item_id=42)

        self.assertEqual(item['ID'], 42)
        files_table.get_item.assert_called_once_with(Key={'ID': 42}, ConsistentRead=True)


class WorkflowStatusTests(unittest.TestCase):
    def test_terminal_status_cannot_be_regressed(self):
        conditional_error = ClientError(
            {'Error': {'Code': 'ConditionalCheckFailedException'}}, 'UpdateItem')
        table = MagicMock()
        table.update_item.side_effect = conditional_error
        dynamodb = MagicMock()
        dynamodb.Table.return_value = table

        updated = processor.update_workflow_status(
            dynamodb, 'files', 42, 'PAYMENT_CAPTURED')

        self.assertFalse(updated)
        condition = table.update_item.call_args.kwargs['ConditionExpression']
        self.assertIn(':sold', condition)
        self.assertIn(':draft', condition)
        self.assertEqual(table.update_item.call_args.kwargs['ExpressionAttributeValues'][':draft'], 'DRAFT_ONLY')
        self.assertIn(':refunded', condition)


class PrintfulIdempotencyTests(unittest.TestCase):
    def setUp(self):
        self.pricing = {
            'product_amount_cents': 4400,
            'shipping_amount_cents': 495,
            'tax_amount_cents': 0,
            'shipping_method': 'STANDARD',
        }

    @patch('lambda_order_processor.printful_request')
    def test_reuses_order_by_external_id(self, request):
        request.return_value = {
            'id': 123,
            'external_id': 'order_test',
            'status': 'draft',
            'shipping': 'STANDARD',
            'items': [{'variant_id': 11577}],
        }
        order = processor.get_or_create_printful_draft(
            'token', 'order_test', {'name': 'Test'}, 11577, 'front', 'back', self.pricing)
        self.assertEqual(order['id'], 123)
        self.assertEqual(request.call_count, 1)
        self.assertEqual(request.call_args.args[1], '/orders/@order_test')

    @patch('lambda_order_processor.printful_request')
    def test_creates_quantity_one_order_when_external_id_is_missing(self, request):
        request.side_effect = [None, {
            'id': 123,
            'external_id': 'order_test',
            'status': 'draft',
            'shipping': 'STANDARD',
            'items': [{'variant_id': 11577}],
        }]
        processor.get_or_create_printful_draft(
            'token', 'order_test', {'name': 'Test'}, 11577, 'front', 'back', self.pricing)
        payload = request.call_args.args[3]
        self.assertEqual(payload['external_id'], 'order_test')
        self.assertEqual(payload['items'][0]['quantity'], 1)
        self.assertEqual(payload['items'][0]['variant_id'], 11577)
        self.assertEqual(payload['items'][0]['retail_price'], '44.00')
        self.assertEqual(payload['shipping'], 'STANDARD')
        self.assertEqual(payload['retail_costs']['shipping'], '4.95')
        self.assertEqual(payload['retail_costs']['tax'], '0.00')

    @patch('lambda_order_processor.printful_request')
    def test_create_race_reuses_order_created_by_other_invocation(self, request):
        existing = {
            'id': 123,
            'external_id': 'order_test',
            'status': 'draft',
            'shipping': 'STANDARD',
            'items': [{'variant_id': 11577}],
        }
        request.side_effect = [
            None,
            processor.PermanentOrderError('external_id already exists'),
            existing,
        ]

        order = processor.get_or_create_printful_draft(
            'token', 'order_test', {'name': 'Test'}, 11577, 'front', 'back', self.pricing)

        self.assertEqual(order['id'], 123)
        self.assertEqual(request.call_count, 3)

    @patch('lambda_order_processor.stripe_request')
    def test_inventory_assignment_race_returns_canonical_item(self, request):
        request.side_effect = [
            processor.PermanentOrderError('idempotency key parameter mismatch'),
            {'metadata': {'inventory_id': '42'}},
        ]

        assigned_id = processor.attach_inventory_id_to_payment(
            'secret', 'pi_test', 'order_test', 43)

        self.assertEqual(assigned_id, 42)

    @patch('lambda_order_processor.printful_request')
    def test_confirmation_race_reuses_order_confirmed_by_other_invocation(self, request):
        draft = {'id': 123, 'external_id': 'order_test', 'status': 'draft'}
        confirmed = {**draft, 'status': 'pending'}
        request.side_effect = [
            draft, processor.PermanentOrderError('HTTP 400: order already confirmed'), confirmed]

        result = processor.confirm_printful_order('token', 123, 'order_test')

        self.assertEqual(result, confirmed)
        self.assertEqual(request.call_args.args[:2], ('GET', '/orders/123'))

    @patch('lambda_order_processor.printful_request')
    def test_confirmation_rejects_failed_success_response(self, request):
        draft = {'id': 123, 'external_id': 'order_test', 'status': 'draft'}
        request.side_effect = [draft, {**draft, 'status': 'failed'}]

        with self.assertRaises(processor.PermanentOrderError):
            processor.confirm_printful_order('token', 123, 'order_test')

    @patch('lambda_order_processor.printful_request')
    def test_confirmation_does_not_call_a_remaining_draft_sold(self, request):
        draft = {'id': 123, 'external_id': 'order_test', 'status': 'draft'}
        request.side_effect = [draft, draft]

        with self.assertRaises(processor.RetryableOrderError):
            processor.confirm_printful_order('token', 123, 'order_test')

    @patch('lambda_order_processor.printful_request')
    def test_confirmation_preserves_a_real_permanent_rejection(self, request):
        draft = {'id': 123, 'external_id': 'order_test', 'status': 'draft'}
        request.side_effect = [draft, processor.PermanentOrderError('HTTP 400: invalid print file'), draft]

        with self.assertRaises(processor.PermanentOrderError):
            processor.confirm_printful_order('token', 123, 'order_test')

    @patch('lambda_order_processor.printful_request')
    def test_confirmation_does_not_refund_when_reconciliation_is_unavailable(self, request):
        draft = {'id': 123, 'external_id': 'order_test', 'status': 'draft'}
        request.side_effect = [draft, processor.PermanentOrderError('HTTP 400'),
                               processor.PermanentOrderError('HTTP 403')]

        with self.assertRaises(processor.RetryableOrderError):
            processor.confirm_printful_order('token', 123, 'order_test')

    @patch('lambda_order_processor.stripe_request')
    def test_capture_is_not_repeated_after_success(self, request):
        payment_intent = {'id': 'pi_test', 'status': 'succeeded'}
        self.assertIs(processor.capture_payment('secret', payment_intent, 'order_test'), payment_intent)
        request.assert_not_called()


class RequestRetryTests(unittest.TestCase):
    @patch('lambda_order_processor.time.sleep')
    @patch('lambda_order_processor.urllib.request.urlopen')
    def test_stripe_idempotency_conflict_is_retryable(self, urlopen, sleep):
        url = f'{processor.STRIPE_API_BASE}/payment_intents/pi_test/capture'
        urlopen.side_effect = urllib.error.HTTPError(
            url, 409, 'Conflict', {}, BytesIO(b'{"error":{"code":"idempotency_key_in_use"}}'))

        with self.assertRaises(processor.RetryableOrderError):
            processor.request_json('POST', url, {}, {}, retries=1)


class ResendEmailTests(unittest.TestCase):
    def setUp(self):
        client_patch = patch('lambda_order_processor.boto3.client')
        client_patch.start()
        self.addCleanup(client_patch.stop)
        self.table = MagicMock()
        self.dynamodb = MagicMock()
        self.dynamodb.Table.return_value = self.table
        self.pricing = {'product_amount_cents': 4400, 'shipping_amount_cents': 495,
                        'total_amount_cents': 4895}

    def tearDown(self):
        processor._SECRET_CACHE.clear()

    def send(self):
        processor.send_order_confirmation(self.dynamodb, 'files', 42, 'order_test',
                                          {'email': 'customer@example.com'}, 'M', self.pricing)

    @patch.dict('os.environ', {'RESEND_SECRET_KEY_SECRET_ARN': 'efiles/resend/production'}, clear=True)
    @patch('lambda_order_processor.request_json')
    @patch('lambda_order_processor.boto3.client')
    def test_default_sender_uses_resend_and_named_json_secret(self, client, request):
        client.return_value.get_secret_value.return_value = {
            'SecretString': '{"RESEND_SECRET_KEY": "test-resend-key"}'}
        request.return_value = {'id': 'email_test'}

        self.send()

        client.assert_called_once_with('secretsmanager')
        client.return_value.get_secret_value.assert_called_once_with(SecretId='efiles/resend/production')
        method, url, headers, message = request.call_args.args
        self.assertEqual((method, url), ('POST', 'https://api.resend.com/emails'))
        self.assertEqual(headers['Authorization'], 'Bearer test-resend-key')
        self.assertEqual(headers['Idempotency-Key'], 'order-confirmation/order_test')
        self.assertEqual(message['from'], 'noreply@mysteryfile.store')
        self.assertEqual(message['to'], ['customer@example.com'])
        self.assertEqual(message['reply_to'], ['support@mysteryfile.store'])
        self.assertEqual(message['subject'], 'Your Mystery File order order_test')
        self.assertIn('Total charged: $48.95', message['text'])
        self.assertIn('https://mysteryfile.store/orders/order_test', message['text'])
        self.assertIn('ConfirmationEmailSentAt', self.table.update_item.call_args.kwargs['UpdateExpression'])
        client.return_value.send_email.assert_not_called()

    @patch.dict('os.environ', {'RESEND_SECRET_KEY': 'test-key',
                              'RESEND_FROM_EMAIL': 'orders@mysteryfile.store'}, clear=True)
    @patch('lambda_order_processor.request_json', return_value={'id': 'email_test'})
    @patch('lambda_order_processor.boto3.client')
    def test_resend_sender_override_and_local_key(self, client, request):
        self.send()
        self.assertEqual(request.call_args.args[3]['from'], 'orders@mysteryfile.store')
        client.assert_not_called()

    @patch.dict('os.environ', {'RESEND_SECRET_KEY': 'test-key'}, clear=True)
    @patch('lambda_order_processor.request_json')
    def test_resend_failure_releases_claim_and_does_not_raise_order_validation_error(self, request):
        request.side_effect = processor.PermanentOrderError('HTTP 403: private-provider-detail')
        with self.assertRaisesRegex(RuntimeError, 'Resend confirmation request failed') as raised:
            self.send()
        self.assertNotIn('private-provider-detail', str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        self.assertEqual(self.table.update_item.call_args.kwargs['UpdateExpression'],
                         'REMOVE ConfirmationEmailClaimedAt')

    @patch.dict('os.environ', {'RESEND_SECRET_KEY': 'test-key'}, clear=True)
    @patch('lambda_order_processor.request_json')
    def test_resend_response_requires_a_message_id(self, request):
        for response in ({}, {'id': ''}, {'id': 123}, {'error': 'rejected'}, None):
            with self.subTest(response=response):
                request.return_value = response
                with self.assertRaisesRegex(RuntimeError, 'Resend did not accept'):
                    self.send()
                self.assertEqual(self.table.update_item.call_args.kwargs['UpdateExpression'],
                                 'REMOVE ConfirmationEmailClaimedAt')

    @patch.dict('os.environ', {'RESEND_SECRET_KEY_SECRET_ARN': 'arn:resend'}, clear=True)
    @patch('lambda_order_processor.request_json', return_value={'id': 'email_test'})
    @patch('lambda_order_processor.boto3.client')
    def test_resend_secret_arn_is_supported(self, client, request):
        client.return_value.get_secret_value.return_value = {
            'SecretString': '{"RESEND_SECRET_KEY": "test-key"}'}
        self.send()
        client.return_value.get_secret_value.assert_called_once_with(SecretId='arn:resend')

    @patch.dict('os.environ', {'RESEND_SECRET_KEY_SECRET_ARN': 'efiles/resend/production'}, clear=True)
    @patch('lambda_order_processor.request_json')
    @patch('lambda_order_processor.boto3.client')
    def test_missing_resend_key_releases_claim_and_raises(self, client, request):
        client.return_value.get_secret_value.return_value = {'SecretString': '{}'}
        with self.assertRaisesRegex(RuntimeError, 'Unable to load Resend sending credentials'):
            self.send()
        request.assert_not_called()
        self.assertEqual(self.table.update_item.call_args.kwargs['UpdateExpression'],
                         'REMOVE ConfirmationEmailClaimedAt')


    @patch.dict('os.environ', {}, clear=True)
    @patch('lambda_order_processor.request_json')
    @patch('lambda_order_processor.boto3.client')
    def test_resend_requires_explicit_secret_configuration(self, client, request):
        with self.assertRaisesRegex(RuntimeError, 'Unable to load Resend sending credentials'):
            self.send()
        client.assert_not_called()
        request.assert_not_called()
        self.assertEqual(self.table.update_item.call_args.kwargs['UpdateExpression'],
                         'REMOVE ConfirmationEmailClaimedAt')

    @patch.dict('os.environ', {'RESEND_SECRET_KEY': 'test-key'}, clear=True)
    @patch('lambda_order_processor.time.sleep')
    @patch('lambda_order_processor.urllib.request.urlopen')
    def test_transient_resend_errors_retry_with_the_same_idempotency_key(self, urlopen, sleep):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b'{"id":"email_test"}'
        urlopen.side_effect = [
            urllib.error.HTTPError('https://api.resend.com/emails', 429, 'Rate limited', {}, BytesIO(b'{}')),
            urllib.error.HTTPError('https://api.resend.com/emails', 503, 'Unavailable', {}, BytesIO(b'{}')),
            response,
        ]
        self.send()
        self.assertEqual(urlopen.call_count, 3)
        for call in urlopen.call_args_list:
            self.assertEqual(call.args[0].get_header('Idempotency-key'), 'order-confirmation/order_test')
            self.assertEqual(call.kwargs['timeout'], 20)
        self.assertIn('ConfirmationEmailSentAt', self.table.update_item.call_args.kwargs['UpdateExpression'])

    @patch.dict('os.environ', {'RESEND_SECRET_KEY': 'test-key'}, clear=True)
    @patch('lambda_order_processor.urllib.request.urlopen')
    def test_http_rejection_keeps_status_code_without_provider_details(self, urlopen):
        urlopen.side_effect = urllib.error.HTTPError(
            'https://api.resend.com/emails', 403, 'Forbidden', {},
            BytesIO(b'{"message":"private-provider-detail"}'))
        with self.assertRaisesRegex(RuntimeError, 'HTTP 403') as raised:
            self.send()
        self.assertNotIn('private-provider-detail', str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)
        urlopen.assert_called_once()


class EmailTests(unittest.TestCase):
    @patch.dict('os.environ', {'ORDER_EMAIL_PROVIDER': 'ses'}, clear=True)
    @patch('lambda_order_processor.boto3.client')
    def test_confirmation_sends_order_link_and_records_delivery(self, client):
        table = MagicMock()
        dynamodb = MagicMock()
        dynamodb.Table.return_value = table
        pricing = {'product_amount_cents': 4400, 'shipping_amount_cents': 495,
                   'total_amount_cents': 4895}
        processor.send_order_confirmation(dynamodb, 'files', 42, 'order_test',
                                          {'email': 'customer@example.com'}, 'M', pricing)
        message = client.return_value.send_email.call_args.kwargs
        self.assertEqual(message['Source'], 'noreply@mysteryfile.store')
        self.assertEqual(message['ReplyToAddresses'], ['support@mysteryfile.store'])
        self.assertEqual(message['Destination']['ToAddresses'], ['customer@example.com'])
        self.assertEqual(message['Message']['Subject']['Data'], 'Your Mystery File order order_test')
        self.assertEqual(message['Message']['Body']['Text']['Data'],
                         'Thank you for your order!\n\n'
                         'Order reference: order_test\n'
                         'Mystery File T-Shirt, size M, quantity 1\n'
                         'Product: $44.00\n'
                         'Shipping: $4.95\n'
                         'Total charged: $48.95\n\n'
                         'Your order has been submitted for fulfillment. '
                         'Check the latest status and tracking here: https://mysteryfile.store/orders/order_test\n\n'
                         'Questions? Write to support@mysteryfile.store.\n')
        self.assertEqual(table.update_item.call_count, 2)
        claim = table.update_item.call_args_list[0].kwargs
        self.assertIn('attribute_not_exists(ConfirmationEmailSentAt)', claim['ConditionExpression'])
        self.assertIn('#status IN (:sold, :draft)', claim['ConditionExpression'])
        self.assertEqual(claim['ExpressionAttributeValues'][':draft'], 'DRAFT_ONLY')

    @patch.dict('os.environ', {'ORDER_EMAIL_PROVIDER': 'ses'}, clear=True)
    @patch('lambda_order_processor.boto3.client')
    def test_ses_failure_releases_the_claim_and_raises(self, client):
        client.return_value.send_email.side_effect = ClientError(
            {'Error': {'Code': 'MessageRejected'}}, 'SendEmail')
        table = MagicMock()
        dynamodb = MagicMock()
        dynamodb.Table.return_value = table
        pricing = {'product_amount_cents': 4400, 'shipping_amount_cents': 495,
                   'total_amount_cents': 4895}

        with self.assertRaises(ClientError):
            processor.send_order_confirmation(dynamodb, 'files', 42, 'order_test',
                                              {'email': 'customer@example.com'}, 'M', pricing)

        self.assertEqual(table.update_item.call_args.kwargs['UpdateExpression'], 'REMOVE ConfirmationEmailClaimedAt')
        self.assertEqual(table.update_item.call_count, 2)

    @patch.dict('os.environ', {}, clear=True)
    @patch('lambda_order_processor.boto3.client')
    def test_unsent_active_email_claim_raises_instead_of_silently_succeeding(self, client):
        table = MagicMock()
        table.update_item.side_effect = ClientError(
            {'Error': {'Code': 'ConditionalCheckFailedException'}}, 'UpdateItem')
        table.get_item.return_value = {'Item': {'Status': 'SOLD', 'ConfirmationEmailClaimedAt': processor.utc_now()}}
        dynamodb = MagicMock()
        dynamodb.Table.return_value = table

        with self.assertRaises(processor.RetryableOrderError):
            processor.send_order_confirmation(dynamodb, 'files', 42, 'order_test',
                                              {'email': 'customer@example.com'}, 'M', {})
        client.assert_not_called()

    @patch.dict('os.environ', {'SES_FROM_EMAIL': 'noreply@mysteryfile.store',
                                'ORDER_SITE_URL': 'https://mysteryfile.store',
                                'SUPPORT_EMAIL': 'support@mysteryfile.store'}, clear=True)
    @patch('lambda_order_processor.boto3.client')
    def test_confirmation_is_not_resent_when_claim_fails(self, client):
        table = MagicMock()
        table.get_item.return_value = {'Item': {'ConfirmationEmailSentAt': processor.utc_now()}}
        table.update_item.side_effect = ClientError(
            {'Error': {'Code': 'ConditionalCheckFailedException'}}, 'UpdateItem')
        dynamodb = MagicMock()
        dynamodb.Table.return_value = table
        processor.send_order_confirmation(dynamodb, 'files', 42, 'order_test',
                                          {'email': 'customer@example.com'}, 'M', {})
        client.assert_not_called()


class ImageGenerationTests(unittest.TestCase):
    def test_upload_returns_short_public_asset_url(self):
        s3_client = MagicMock()

        url = processor.upload_print_file(
            s3_client, 'bucket', 'ORDERS/file id/front.png', BytesIO(b'png'),
            'https://bucket.s3.amazonaws.com/')

        self.assertEqual(
            url, 'https://bucket.s3.amazonaws.com/ORDERS/file%20id/front.png')
        s3_client.upload_fileobj.assert_called_once()
        s3_client.generate_presigned_url.assert_not_called()

    def test_front_and_back_are_300_dpi_pixel_dimensions(self):
        document = pymupdf.open()
        page = document.new_page(width=612, height=792)
        page.insert_text((72, 72), 'E-Files test document')
        pdf = BytesIO(document.tobytes())
        document.close()

        front = Image.open(processor.render_front_image(pdf))
        back = Image.open(processor.render_back_image('EFTA00000001'))
        self.assertEqual(front.size, (3600, 4800))
        self.assertEqual(back.size, (3600, 4800))


if __name__ == '__main__':
    unittest.main()
