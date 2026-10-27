"""Read-only admission must agree with actual reservation gates."""
import json
import unittest

from fal_gateway.server import MODEL
import test_calls
from test_gateway import PAYLOAD


class VideoPreflightTest(unittest.TestCase):
    def setUp(self):
        self.fixture = test_calls.CallsTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def query(self, **updates):
        return self.fixture.client.post('/v1/video-preflight', headers=self.fixture.headers,
            json={'model_id': MODEL, 'payload': PAYLOAD, 'budget_limit': '1', **updates})

    def snapshot(self):
        with self.fixture.store.connect() as db:
            return list(db.iterdump())

    def test_quote_and_admission_create_no_call_or_reservation(self):
        before = self.snapshot()
        result = self.query().json()
        self.assertTrue(result['can_submit'])
        self.assertEqual(result['account_id'], self.fixture.account['account_id'])
        self.assertEqual(result['pricing']['estimate']['amount'], '0.25')
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.fixture.sent, [])

    def test_historical_unknown_blocks_read_and_create_without_rewriting(self):
        submitted = self.fixture.submit().json()
        with self.fixture.store.connect() as db:
            db.execute('UPDATE channel_calls SET reservation_unknown=1,settlement_amount=NULL WHERE id=?', (submitted['call_id'],))
        before, sent = self.snapshot(), list(self.fixture.sent)
        result = self.query().json()
        self.assertFalse(result['can_submit'])
        self.assertEqual(result['blocked_reason'], 'historical_cost_unknown')
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.fixture.sent, sent)
        response = self.fixture.submit('new-after-unknown')
        self.assertEqual(response.json()['error'], 'historical_cost_unknown')
        self.assertEqual(self.snapshot(), before)

    def test_shared_budget_and_daily_gate_match_reserve(self):
        self.fixture.submit()
        result = self.query(budget_limit='6').json()
        self.assertEqual(result['blocked_reason'], 'budget_exceeded')
        with self.fixture.store.connect() as db:
            row = db.execute('SELECT * FROM channel_accounts WHERE organization_id=?', ('org',)).fetchone()
            registration = json.loads(row['registration_json'])
            registration['daily_limit'] = 1
            db.execute('UPDATE channel_accounts SET registration_json=? WHERE id=?', (json.dumps(registration), row['id']))
        self.assertEqual(self.query().json()['blocked_reason'], 'daily_limit_reached')

    def test_missing_or_low_budget_is_not_admitted(self):
        self.assertEqual(self.query(budget_limit=None).json()['blocked_reason'], 'cost_unknown')
        self.assertEqual(self.query(budget_limit='0.01').json()['blocked_reason'], 'budget_below_estimate')
        self.assertEqual(self.fixture.sent, [])

    def test_authentication_and_payload_checks_precede_admission(self):
        for headers in ({}, {**self.fixture.headers, 'X-Caller-Id': 'other'}):
            result = self.fixture.client.post('/v1/video-preflight', headers=headers,
                json={'model_id': MODEL, 'payload': PAYLOAD, 'budget_limit': '1'})
            self.assertIn(result.status_code, {401, 403})
        self.assertEqual(self.query(payload={**PAYLOAD, 'arbitrary_url': 'https://invalid.example'}).status_code, 400)
        self.assertEqual(self.fixture.sent, [])

    def test_draining_channel_is_not_admitted_but_quote_is_available(self):
        self.fixture.gateway.accepting = False
        result = self.query().json()
        self.assertFalse(result['can_submit'])
        self.assertEqual(result['blocked_reason'], 'channel_unavailable')
        self.assertEqual(result['pricing']['estimate']['amount'], '0.25')
