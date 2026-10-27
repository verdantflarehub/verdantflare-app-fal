"""Cost evidence must be real, request-bound and separate from reservations."""
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
import unittest
from unittest.mock import patch

import httpx
from starlette.testclient import TestClient

from fal_gateway.billing import BillingError, estimate, fetch_bill, H3, SD2, VALID_UNTIL
from fal_gateway.calls import Calls
from fal_gateway.server import create_apps
from fal_gateway.store import Conflict, Store
import test_calls
from test_gateway import MANAGE, EXECUTE, KEY, PAYLOAD


NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)


def bill_event(**updates):
    return {'request_id': 'original-request', 'endpoint_id': H3,
            'timestamp': '2026-10-26T10:00:00+00:00',
            'cost_total': '0.123456789', 'cost_estimate_nano_usd': 123456789, **updates}


def page(events, cursor=None, more=False):
    return {'billing_events': events, 'next_cursor': cursor, 'has_more': more}


class BillingEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.call = {'id': 'call', 'model_id': H3, 'provider_request_id': 'original-request',
                     'accepted': 1, 'state': 'completed', 'created_at': '2026-10-26T09:00:00+00:00'}
        self.requests = []

    def read(self, responses):
        def handle(request):
            self.requests.append(request)
            response = responses.pop(0)
            return response if isinstance(response, httpx.Response) else httpx.Response(200, json=response)
        with httpx.Client(transport=httpx.MockTransport(handle)) as client:
            return fetch_bill(client, KEY, self.call, at=NOW)

    def test_complete_pagination_and_nano_usd_precision(self):
        result = self.read([page([], 'next', True), page([bill_event()])])
        self.assertEqual(result['amount'], '0.123456789')
        self.assertEqual(result['source'], 'fal_billing_events')
        self.assertEqual(len(self.requests), 2)
        self.assertTrue(all(r.method == 'GET' and r.url.host == 'api.fal.ai' for r in self.requests))
        self.assertEqual(self.requests[1].url.params['cursor'], 'next')
        self.assertEqual(self.requests[0].url.params['request_id'], 'original-request')

    def test_missing_duplicate_or_foreign_bills_never_become_zero(self):
        cases = [([page([])], 'billing_record_not_found'),
                 ([page([bill_event(), bill_event()])], 'billing_record_ambiguous'),
                 ([page([bill_event(endpoint_id=SD2)])], 'billing_identity_mismatch'),
                 ([page([bill_event(request_id='another')])], 'billing_identity_mismatch'),
                 ([page([bill_event(currency='EUR')])], 'billing_currency_mismatch'),
                 ([page([bill_event(cost_total='0.1')])], 'billing_amount_mismatch'),
                 ([page([bill_event(cost_total='NaN')])], 'billing_response_invalid'),
                 ([page([bill_event(cost_estimate_nano_usd=True)])], 'billing_response_invalid'),
                 ([page([], 'same', True), page([], 'same', True)], 'billing_pagination_invalid'),
                 ([page([], 'unread', False)], 'billing_pagination_invalid')]
        for responses, code in cases:
            with self.subTest(code=code), self.assertRaisesRegex(BillingError, code):
                self.read(responses)

    def test_authorization_and_rate_errors_remain_specific(self):
        for status, code in [(403, 'billing_access_denied'), (401, 'billing_access_denied'),
                             (429, 'billing_rate_limited'), (503, 'billing_unavailable')]:
            with self.subTest(status=status), self.assertRaisesRegex(BillingError, code):
                self.read([httpx.Response(status, json={'error': 'secret upstream content'})])

    def test_zero_requires_explicit_bill_and_uncertain_tasks_are_not_queried(self):
        self.assertEqual(self.read([page([bill_event(cost_total=0, cost_estimate_nano_usd=0)])])['amount'], '0')
        self.call['accepted'] = None
        with self.assertRaisesRegex(BillingError, 'billing_request_unconfirmed'):
            self.read([])
        self.call['accepted'], self.call['state'] = 1, 'in_progress'
        with self.assertRaisesRegex(BillingError, 'billing_task_not_terminal'):
            self.read([])


class SettlementTest(unittest.TestCase):
    def setUp(self):
        self.f = test_calls.CallsTest()
        self.f.setUp()
        self.addCleanup(self.f.tearDown)
        self.original_id = self.f.submit().json()['call_id']
        with self.f.store.connect() as db:
            db.execute("UPDATE channel_calls SET state='completed',reservation_unknown=1,reservation_amount='0',"
                       "created_at='2026-10-26T09:00:00+00:00' WHERE id=?", (self.original_id,))
        self.original = self.f.gateway.calls.get('org', self.original_id)
        self.management = TestClient(create_apps(self.f.gateway, MANAGE, EXECUTE)[0])
        self.addCleanup(self.management.close)
        self.headers = {'Authorization': 'Bearer ' + MANAGE, 'X-Organization-Id': 'org', 'X-User-Id': 'admin'}
        self.bill = {'amount': '0.123456789', 'source': 'fal_billing_events', 'evidence_digest': 'a' * 64,
                     'evidence_json': json.dumps(bill_event()), 'event_count': 1, 'observed_at': NOW.isoformat()}

    def refresh(self, revision=None, headers=None, **extra):
        return self.management.post('/v1/calls/' + self.original_id + '/settlement/refresh',
            headers=headers or self.headers, json={'expected_revision': revision or self.original['revision'], **extra})

    def test_verified_bill_unlocks_original_budget_without_rewriting_call(self):
        self.assertEqual(self.f.submit('blocked').json()['error'], 'historical_cost_unknown')
        with patch('fal_gateway.server.fetch_bill', return_value=self.bill) as fetch:
            result = self.refresh()
            self.assertEqual(result.status_code, 200)
            self.assertEqual(fetch.call_args.args[1], KEY)
        after = self.f.gateway.calls.get('org', self.original_id)
        for key in ('provider_request_id', 'model_id', 'accepted', 'state', 'reservation_unknown', 'reservation_amount', 'digest'):
            self.assertEqual(after[key], self.original[key])
        self.assertEqual(after['settlement_amount'], '0.123456789')
        self.assertEqual(self.f.submit('allowed', budget_limit='9.8').status_code, 200)
        self.assertEqual(self.f.submit('over', budget_limit='1').json()['error'], 'budget_exceeded')
        with self.f.store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM channel_call_settlements').fetchone()[0], 1)

    def test_no_bill_or_permission_does_not_change_any_accounting(self):
        for code, status in [('billing_access_denied', 403), ('billing_record_not_found', 409)]:
            with patch('fal_gateway.server.fetch_bill', side_effect=BillingError(code, status)):
                response = self.refresh()
                self.assertEqual(response.status_code, status)
                self.assertEqual(response.json()['error'], code)
                self.assertEqual(self.f.gateway.calls.get('org', self.original_id), self.original)

    def test_permission_revision_and_payload_fail_before_remote_read(self):
        with patch('fal_gateway.server.fetch_bill') as fetch:
            self.assertEqual(self.refresh(headers=self.f.headers).status_code, 401)
            self.assertEqual(self.refresh(headers={**self.headers, 'X-Organization-Id': 'other'}).status_code, 404)
            self.assertEqual(self.refresh(revision=self.original['revision'] + 1).status_code, 409)
            self.assertEqual(self.refresh(amount='0').status_code, 400)
            fetch.assert_not_called()

    def test_same_evidence_is_idempotent_and_different_evidence_cannot_overwrite(self):
        first = self.f.gateway.calls.settle_bill(self.original, 'admin', self.bill)
        second = self.f.gateway.calls.settle_bill(self.original, 'admin', self.bill)
        self.assertEqual(first, second)
        with self.assertRaisesRegex(Conflict, 'settlement_conflict'):
            self.f.gateway.calls.settle_bill(self.original, 'admin', {**self.bill, 'amount': '0'})
        reopened = Calls(Store(str(self.f.store.path), self.f.key))
        self.assertEqual(reopened.get('org', self.original_id)['settlement_amount'], '0.123456789')

    def test_concurrent_status_revision_blocks_commit(self):
        with self.f.store.connect() as db:
            db.execute('UPDATE channel_calls SET revision=revision+1 WHERE id=?', (self.original_id,))
        with self.assertRaisesRegex(Conflict, 'revision_conflict'):
            self.f.gateway.calls.settle_bill(self.original, 'admin', self.bill)
        self.assertIsNone(self.f.gateway.calls.get('org', self.original_id)['settlement_amount'])


class EstimateTest(unittest.TestCase):
    def test_h3_duration_and_extra_images_are_estimates_not_reservations(self):
        payload = {**PAYLOAD, 'duration': 10, 'reference_image_urls': ['data:image/png;base64,AAAA'] * 7}
        quote = estimate(H3, payload, at=NOW)
        self.assertEqual(quote['estimate']['amount'], '0.66')
        self.assertEqual(quote['estimate']['source'], 'official_price_table')
        with self.assertRaisesRegex(BillingError, 'pricing_review_required'):
            estimate(H3, payload, at=VALID_UNTIL)

    def test_sd2_video_references_change_rate_but_images_do_not_invent_tokens(self):
        payload = {'resolution': '720p', 'duration': 4, 'image_urls': ['image']}
        quote = estimate(SD2, payload, at=NOW)
        self.assertIsNone(quote['estimate']['amount'])
        self.assertEqual(quote['rates']['video_tokens_1000'], '0.014')
        self.assertEqual(estimate(SD2, {**payload, 'video_urls': ['video']}, at=NOW)['rates']['video_tokens_1000'], '0.0086016')
        self.assertEqual(quote['reason_code'], 'provider_usage_required')

    def test_cost_never_calls_supplier_and_low_ceiling_never_submits(self):
        f = test_calls.CallsTest()
        f.setUp()
        try:
            response = f.client.post('/v1/cost', headers=f.headers, json={'model_id': H3, 'payload': PAYLOAD})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()['estimate']['amount'], '0.25')
            self.assertEqual(f.sent, [])
            self.assertEqual(f.submit(budget_limit='0.24').json()['error'], 'budget_below_estimate')
            self.assertEqual(f.sent, [])
            first = f.submit().json()
            with patch('fal_gateway.calls.estimate', side_effect=BillingError('pricing_review_required')):
                self.assertEqual(f.submit().json(), first)
            self.assertEqual(len(f.sent), 1)
        finally:
            f.tearDown()


if __name__ == '__main__':
    unittest.main()
