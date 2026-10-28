"""Verified original inputs may reserve estimated costs, never invent settlement."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import unittest
from unittest.mock import patch

from starlette.testclient import TestClient

from fal_gateway.billing import H3, SD2
from fal_gateway.calls import Calls
from fal_gateway.server import create_apps
from fal_gateway.store import Conflict
import test_calls
from test_gateway import MANAGE, EXECUTE, PAYLOAD


class BusinessEstimateTest(unittest.TestCase):
    def setUp(self):
        self.f = test_calls.CallsTest()
        self.f.setUp()
        self.addCleanup(self.f.tearDown)
        self.identity = self.f.submit().json()['call_id']
        with self.f.store.connect() as db:
            db.execute("UPDATE channel_calls SET state='completed',reservation_unknown=1,reservation_amount='0' WHERE id=?",
                       (self.identity,))
        self.original = self.f.gateway.calls.get('org', self.identity)
        self.management = TestClient(create_apps(self.f.gateway, MANAGE, EXECUTE)[0])
        self.addCleanup(self.management.close)
        self.headers = {'Authorization': 'Bearer ' + MANAGE, 'X-Organization-Id': 'org', 'X-User-Id': 'admin'}

    def request(self, revision=None, payload=None, headers=None, **extra):
        return self.management.post('/v1/calls/' + self.identity + '/estimate',
            headers=headers or self.headers, json={'expected_revision': revision or self.original['revision'],
                                                  'payload': payload or PAYLOAD, **extra})

    def current(self):
        return self.f.gateway.calls.get('org', self.identity)

    def snapshot(self):
        with self.f.store.connect() as db:
            return list(db.iterdump())

    def test_verified_estimate_restores_admission_without_changing_original_accounting(self):
        self.assertEqual(self.f.submit('before').json()['error'], 'historical_cost_unknown')
        sent = list(self.f.sent)
        response = self.request()
        self.assertEqual(response.status_code, 200, response.text)
        value = response.json()
        self.assertEqual(value['business_estimate']['amount'], '0.25')
        self.assertEqual(value['business_estimate']['source'], 'official_price_table_estimate')
        self.assertEqual(value['budget_occupancy'], {'amount': '0.25', 'unit': 'USD', 'source': 'business_estimate'})
        self.assertEqual(value['settlement'], {'amount': None, 'unit': 'USD', 'source': 'unknown'})
        self.assertEqual(value['reservation']['source'], 'unknown')
        self.assertEqual(self.f.sent, sent)
        after = self.current()
        for key in ('provider_request_id', 'credential_version', 'account_id', 'quota_subject_id', 'proxy_json',
                    'digest', 'model_id', 'accepted', 'state', 'reservation_amount', 'reservation_unknown',
                    'settlement_amount', 'settlement_source', 'budget_amount', 'billing_json'):
            self.assertEqual(after[key], self.original[key], key)
        result = self.f.gateway.calls.preflight('org', H3, PAYLOAD, None, '9.75')
        self.assertTrue(result['can_submit'])
        self.assertEqual(self.f.gateway.calls.preflight('org', H3, PAYLOAD, None, '9.76')['blocked_reason'], 'budget_exceeded')
        listing = self.f.gateway.calls.list('org')
        self.assertEqual(listing['items'][0]['business_estimate'], value['business_estimate'])
        self.assertEqual(listing['summary']['settlement']['unknown_count'], 1)
        self.assertEqual(listing['summary']['budget_occupancy']['estimated_count'], 1)
        self.assertEqual(listing['summary']['budget_occupancy']['amount'], '0.25')
        reopened = Calls(self.f.store)
        self.assertEqual(reopened.project(reopened.get('org', self.identity))['business_estimate'], value['business_estimate'])

    def test_legacy_payload_digest_is_required_and_media_not_retained(self):
        canonical = json.dumps(PAYLOAD, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        digest = hashlib.sha256((H3 + '\n' + canonical).encode()).hexdigest()
        with self.f.store.connect() as db:
            db.execute("UPDATE channel_calls SET caller_id='legacy-video-mcp',digest=?,billing_json=NULL WHERE id=?", (digest, self.identity))
        self.assertEqual(self.request(payload={**PAYLOAD, 'duration': 6}).json()['error'], 'business_estimate_input_mismatch')
        response = self.request()
        self.assertEqual(response.status_code, 200)
        with self.f.store.connect() as db:
            evidence = dict(db.execute('SELECT * FROM channel_call_estimates WHERE call_id=?', (self.identity,)).fetchone())
        self.assertEqual(evidence['original_digest'], digest)
        self.assertNotIn('data:image', evidence['evidence_json'])
        self.assertNotIn(PAYLOAD['prompt'], evidence['evidence_json'])
        self.assertEqual(json.loads(evidence['evidence_json'])['reference_image_count'], 1)

    def test_scope_revision_state_and_amount_injection_are_rejected_without_writes(self):
        before = self.snapshot()
        self.assertEqual(self.request(headers=self.f.headers).status_code, 401)
        self.assertEqual(self.request(headers={**self.headers, 'X-Organization-Id': 'other'}).status_code, 404)
        self.assertEqual(self.request(revision=999).json()['error'], 'revision_conflict')
        self.assertEqual(self.request(amount='0').status_code, 400)
        self.assertEqual(self.request(payload={**PAYLOAD, 'duration': 6}).json()['error'], 'business_estimate_input_mismatch')
        self.assertEqual(self.snapshot(), before)
        for state, accepted in [('acceptance_unknown', None), ('queued', 1), ('failed', 1), ('rejected', 0)]:
            with self.subTest(state=state):
                with self.f.store.connect() as db:
                    db.execute('UPDATE channel_calls SET state=?,accepted=? WHERE id=?', (state, accepted, self.identity))
                response = self.request()
                self.assertEqual(response.json()['error'], 'business_estimate_requires_completed_call')
                self.assertEqual(self.f.gateway.calls.preflight('org', H3, PAYLOAD, None, '1')['blocked_reason'], 'historical_cost_unknown')

    def test_repeat_is_idempotent_and_conflicting_evidence_cannot_overwrite(self):
        first = self.request().json()
        again = self.request(revision=first['revision']).json()
        self.assertEqual(first, again)
        with self.f.store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM channel_call_estimates').fetchone()[0], 1)
        self.assertEqual(self.request().json()['error'], 'revision_conflict')
        with patch('fal_gateway.calls.business_estimate', side_effect=AssertionError('must not reprice an existing estimate')):
            self.assertEqual(self.request(revision=first['revision']).json(), first)
        with self.f.store.connect() as db:
            db.execute("UPDATE channel_call_estimates SET original_digest='conflicting' WHERE call_id=?", (self.identity,))
        self.assertEqual(self.request(revision=first['revision']).json()['error'], 'business_estimate_conflict')
        self.assertEqual(self.current()['business_estimate_amount'], '0.25')

    def test_actual_settlement_replaces_occupancy_once_and_keeps_estimate(self):
        self.request()
        current = self.current()
        bill = {'amount': '0.30', 'source': 'fal_billing_events', 'evidence_digest': 'b' * 64,
                'evidence_json': '{}', 'event_count': 1, 'observed_at': '2026-10-28T00:00:00+00:00'}
        value = self.f.gateway.calls.settle_bill(current, 'admin', bill)
        self.assertEqual(value['budget_occupancy'], {'amount': '0.30', 'unit': 'USD', 'source': 'fal_billing_events'})
        self.assertEqual(value['business_estimate']['amount'], '0.25')
        self.assertEqual(self.f.gateway.calls.settle_bill(current, 'admin', bill), value)
        self.assertTrue(self.f.gateway.calls.preflight('org', H3, PAYLOAD, None, '9.70')['can_submit'])
        self.assertEqual(self.request(revision=value['revision']).json()['error'], 'business_estimate_not_required')
        self.assertEqual(self.f.gateway.calls.list('org')['summary']['budget_occupancy']['estimated_count'], 0)

    def test_known_reservation_is_not_reduced_to_smaller_estimate(self):
        with self.f.store.connect() as db:
            db.execute("UPDATE channel_calls SET reservation_unknown=0,reservation_amount='6' WHERE id=?", (self.identity,))
        value = self.request().json()
        self.assertEqual(value['budget_occupancy'], {'amount': '6', 'unit': 'USD', 'source': 'budget_limit'})
        self.assertEqual(self.f.gateway.calls.preflight('org', H3, PAYLOAD, None, '4.01')['blocked_reason'], 'budget_exceeded')

    def test_estimate_above_known_reservation_is_used_and_other_unknown_calls_still_block(self):
        with self.f.store.connect() as db:
            db.execute("UPDATE channel_calls SET reservation_unknown=0,reservation_amount='0.10' WHERE id=?", (self.identity,))
        self.assertEqual(self.request().json()['budget_occupancy']['amount'], '0.25')
        second = self.f.submit('second', budget_limit='1').json()['call_id']
        with self.f.store.connect() as db:
            db.execute("UPDATE channel_calls SET reservation_unknown=1,reservation_amount='0' WHERE id=?", (second,))
        self.assertEqual(self.f.gateway.calls.preflight('org', H3, PAYLOAD, None, '1')['blocked_reason'], 'historical_cost_unknown')

    def test_concurrent_budget_reservations_cannot_exceed_remaining_occupancy(self):
        self.request()
        def reserve(index):
            try:
                self.f.gateway.calls.reserve('org', 'video-mcp', f'parallel-{index}', f'task-{index}', H3, PAYLOAD, None, '6')
                return 'allowed'
            except Conflict as error:
                return str(error)
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(reserve, (1, 2)))
        self.assertCountEqual(results, ['allowed', 'budget_exceeded'])

    def test_only_supported_fixed_price_profiles_and_bounded_requests(self):
        with self.f.store.connect() as db:
            db.execute('UPDATE channel_calls SET model_id=? WHERE id=?', (SD2, self.identity))
        payload = {'prompt': 'test', 'duration': 5, 'resolution': '720p', 'aspect_ratio': '16:9',
                   'generate_audio': True, 'image_urls': ['data:image/png;base64,AAAA']}
        with self.f.store.connect() as db:
            canonical = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
            digest = hashlib.sha256((SD2 + '\n' + canonical).encode()).hexdigest()
            db.execute("UPDATE channel_calls SET caller_id='legacy-video-mcp',digest=? WHERE id=?", (digest, self.identity))
        self.assertEqual(self.request(payload=payload).json()['error'], 'business_estimate_not_supported')
        response = self.request(payload={**PAYLOAD, 'prompt': 'a' * (1024 * 1024)})
        self.assertEqual(response.json()['error'], 'request_too_large')
        self.assertEqual(response.status_code, 413)

    def test_rounding_and_price_review_expiry(self):
        from fal_gateway.billing import business_estimate, BillingError
        with patch('fal_gateway.billing.estimate', return_value={'estimate': {'amount': '0.251'}, 'input_digest': 'd',
                'rates': {}, 'source_url': 'https://fal.ai/models/' + H3, 'price_version': 'test',
                'reviewed_at': '2026-10-28', 'quoted_at': '2026-10-28'}):
            self.assertEqual(business_estimate(H3, PAYLOAD)['amount'], '0.26')
        with patch('fal_gateway.billing.estimate', side_effect=BillingError('pricing_review_required')):
            self.assertEqual(self.request().json()['error'], 'pricing_review_required')


if __name__ == '__main__':
    unittest.main()
