"""Manual status refresh must never resubmit, cross organizations or settle money."""
import unittest
import asyncio
import time
import httpx
from unittest.mock import patch
from starlette.testclient import TestClient
import test_calls
from test_gateway import MANAGE, EXECUTE, KEY
from fal_gateway.server import create_apps, APIError


class CallRefreshTest(unittest.TestCase):
    def setUp(self):
        self.fixture = test_calls.CallsTest()
        self.fixture.setUp()
        self.management = TestClient(create_apps(self.fixture.gateway, MANAGE, EXECUTE)[0])
        self.headers = {'Authorization': 'Bearer ' + MANAGE, 'X-Organization-Id': 'org', 'X-User-Id': 'admin'}

    def tearDown(self):
        self.management.close()
        self.fixture.tearDown()

    def refresh(self, ids, headers=None):
        return self.management.post('/v1/calls/refresh', headers=headers or self.headers, json={'call_ids': ids})

    def test_terminal_refresh_keeps_original_key_and_unknown_finances(self):
        f = self.fixture
        call = f.submit().json()
        account = f.accounts.checked('org', 'admin', f.account['account_id'], f.account['revision'], 'active', 'rotated-key-for-test')
        f.accounts.patch('org', 'admin', account['account_id'], account['revision'], {}, archive=True)
        f.sent.clear()
        response = self.refresh([call['call_id']])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['refreshed'], 1)
        self.assertEqual(f.sent, [('GET', 'Key ' + KEY)])
        record = f.gateway.calls.get('org', call['call_id'])
        self.assertEqual(record['state'], 'completed')
        self.assertEqual(record['budget_amount'], '0.25')
        self.assertIsNone(record['settlement_amount'])
        self.assertEqual(self.refresh([call['call_id']]).json()['unchanged'], 1)
        self.assertEqual(len(f.sent), 1)

    def test_unavailable_preserves_status_and_unknown_acceptance_never_submits(self):
        f = self.fixture
        call = f.submit().json()
        f.sent.clear()
        with patch.object(f.gateway, '_remote', side_effect=APIError('provider_unavailable', 503)):
            self.assertEqual(self.refresh([call['call_id']]).json()['unavailable'], 1)
        self.assertEqual(f.gateway.calls.get('org', call['call_id'])['state'], 'queued')
        f.mode = 'timeout'
        uncertain = f.submit('unknown', budget_limit='3').json()
        f.sent.clear()
        self.assertEqual(self.refresh([uncertain['call_id']]).json()['unchanged'], 1)
        self.assertEqual(f.sent, [])

    def test_authorizes_entire_batch_before_any_supplier_request(self):
        f = self.fixture
        call = f.submit().json()
        f.sent.clear()
        foreign = {**self.headers, 'X-Organization-Id': 'other'}
        self.assertEqual(self.refresh([call['call_id']], foreign).status_code, 404)
        self.assertEqual(self.refresh([call['call_id'], '00000000-0000-4000-8000-000000000001']).status_code, 404)
        self.assertEqual(self.refresh([call['call_id']], f.headers).status_code, 401)
        for ids in [[], ['bad'], [call['call_id']] * 2, [call['call_id']] * 17]:
            self.assertEqual(self.refresh(ids).status_code, 400)
        self.assertEqual(f.sent, [])

    def test_same_batch_is_coalesced(self):
        f = self.fixture
        call = f.submit().json()
        observed = []
        def remote(item, key, result, **options):
            observed.append(item['id'])
            time.sleep(.1)
            return {'id': item['id'], 'state': 'completed'}
        async def run():
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=self.management.app), base_url='http://test') as client:
                return await asyncio.gather(*(client.post('/v1/calls/refresh', headers=self.headers,
                    json={'call_ids': [call['call_id']]}) for _ in range(6)))
        with patch.object(f.gateway, '_remote', side_effect=remote):
            responses = asyncio.run(run())
        self.assertTrue(all(r.status_code == 200 for r in responses))
        self.assertEqual(observed, [call['call_id']])
