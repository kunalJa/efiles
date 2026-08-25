import unittest
from io import BytesIO
from unittest.mock import MagicMock, patch

import pymupdf
from botocore.exceptions import ClientError
from PIL import Image

import lambda_order_processor as processor


class OrderValidationTests(unittest.TestCase):
    def setUp(self):
        self.payment_intent = {
            'id': 'pi_test',
            'amount': 4875,
            'amount_capturable': 4875,
            'currency': 'usd',
            'capture_method': 'manual',
            'status': 'requires_capture',
            'metadata': {
                'order_id': 'order_test',
                'size': 'M',
                'quantity': '1',
                'customer_email': 'customer@example.com',
                'product_amount_cents': '4400',
                'shipping_amount_cents': '475',
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


class SecretLoadingTests(unittest.TestCase):
    def tearDown(self):
        processor._SECRET_CACHE.clear()

    @patch.dict('os.environ', {'TEST_SECRET_ARN': 'arn:test'}, clear=True)
    @patch('lambda_order_processor.boto3.client')
    def test_loads_key_from_json_secret(self, boto_client):
        boto_client.return_value.get_secret_value.return_value = {
            'SecretString': '{"TEST_SECRET": "secret-value"}',
        }

        value = processor.get_secret('TEST_SECRET', 'TEST_SECRET_ARN')

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
        self.assertIn(':refunded', condition)


class PrintfulIdempotencyTests(unittest.TestCase):
    def setUp(self):
        self.pricing = {
            'product_amount_cents': 4400,
            'shipping_amount_cents': 475,
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
        self.assertEqual(payload['retail_costs']['shipping'], '4.75')
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

    @patch('lambda_order_processor.stripe_request')
    def test_capture_is_not_repeated_after_success(self, request):
        payment_intent = {'id': 'pi_test', 'status': 'succeeded'}
        self.assertIs(processor.capture_payment('secret', payment_intent, 'order_test'), payment_intent)
        request.assert_not_called()


class ImageGenerationTests(unittest.TestCase):
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
