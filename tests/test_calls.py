from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest

from cryptography.fernet import Fernet
import httpx
from starlette.testclient import TestClient

from fal_gateway.accounts import Accounts
from fal_gateway.calls import Calls, amount
from fal_gateway.server import Gateway, MODEL, create_apps
from fal_gateway.store import Conflict, Store
from test_gateway import PAYLOAD, MANAGE, EXECUTE, KEY


class CallsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.key = Fernet.generate_key().decode()
        self.store = Store(str(Path(self.temp.name) / 'state.sqlite3'), self.key)
        self.accounts = Accounts(self.store)
        self.sent = []
        self.mode = 'success'

        def upstream(request):
            self.sent.append((request.method, request.headers.get('authorization')))
            if request.method == 'POST':
                if self.mode == 'timeout':
                    raise httpx.ReadTimeout('mock interruption', request=request)
                if self.mode == 'reject':
                    return httpx.Response(429)
                return httpx.Response(200, json={'request_id': 'original-request'})
            return httpx.Response(200, json={'status': 'COMPLETED'})

        self.gateway = Gateway(self.store, httpx.Client(transport=httpx.MockTransport(upstream)))
        _, execution = create_apps(self.gateway, MANAGE, EXECUTE)
        self.client = TestClient(execution)
        self.headers = {'Authorization': 'Bearer ' + EXECUTE, 'X-Organization-Id': 'org',
                        'X-Caller-Id': 'video-mcp'}
        self.account = self.create_account('one', default=True)

    def tearDown(self):
        self.client.close()
        self.gateway.client.close()
        self.temp.cleanup()

    def create_account(self, operation, default=False):
        account = self.accounts.create('org', 'admin', operation, {
            'api_url': 'https://queue.fal.run', 'api_key': KEY, 'proxy': {'mode': 'direct'},
            'default': default, 'quota_subject_id': 'shared', 'registration': {'budget_limit': '10'}})
        return self.accounts.checked('org', 'admin', account['account_id'], account['revision'], 'active')

    def submit(self, operation='op', **extra):
        return self.client.post('/v1/calls', headers=self.headers, json={
            'idempotency_key': operation, 'task_id': 'task', 'model_id': MODEL,
            'capability': 'video', 'payload': PAYLOAD, 'budget_limit': '6', **extra})

    def test_pinned_credentials_survive_rotation_and_archival(self):
        first = self.submit()
        self.assertEqual(first.status_code, 200)
        self.assertTrue(first.json()['accepted'])
        self.assertIsNone(first.json()['budget']['amount'])
        rotated = self.accounts.checked('org', 'admin', self.account['account_id'],
            self.account['revision'], 'active', 'new-key-with-enough-length')
        self.accounts.patch('org', 'admin', rotated['account_id'], rotated['revision'], {}, archive=True)
        self.assertEqual(self.submit().json(), first.json())
        self.assertEqual(self.submit('new').json()['error'], 'account_required')
        call_id = first.json()['call_id']
        result = self.client.get('/v1/calls/' + call_id, headers=self.headers)
        self.assertEqual(result.json()['state'], 'completed')
        self.assertIsNone(result.json()['settlement']['amount'])
        self.assertEqual(self.sent[-1], ('GET', 'Key ' + KEY))
        self.assertEqual(len([x for x in self.sent if x[0] == 'POST']), 1)
        for headers in ({**self.headers, 'X-Organization-Id': 'other'},
                        {**self.headers, 'X-Caller-Id': 'other'}):
            self.assertEqual(self.client.get('/v1/calls/' + call_id, headers=headers).status_code, 404)

    def test_disabled_default_never_falls_back(self):
        self.create_account('two')
        self.accounts.patch('org', 'admin', self.account['account_id'], self.account['revision'], {'status': 'disabled'})
        self.assertEqual(self.submit().json()['error'], 'account_disabled')
        self.assertEqual(self.sent, [])

    def test_unknown_survives_restart_and_blocks_shared_budget(self):
        self.mode = 'timeout'
        first = self.submit().json()
        self.assertIsNone(first['accepted'])
        self.assertEqual(first['state'], 'acceptance_unknown')
        second = self.create_account('two', default=True)
        self.assertEqual(self.submit().json(), first)
        self.assertEqual(self.submit('new', account_id=second['account_id']).json()['error'], 'budget_exceeded')
        reopened = Calls(Store(str(self.store.path), self.key))
        self.assertEqual(reopened.by_operation('org', 'video-mcp', 'op'), first)
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.submit(budget_limit='5').json()['error'], 'idempotency_conflict')

    def test_rejected_releases_reservation_and_missing_cost_refuses(self):
        self.assertEqual(self.submit(budget_limit=None).json()['error'], 'cost_unknown')
        self.assertEqual(self.sent, [])
        self.mode = 'reject'
        rejected = self.submit().json()
        self.assertFalse(rejected['accepted'])
        self.mode = 'success'
        self.assertTrue(self.submit('new').json()['accepted'])

    def test_concurrent_keys_cannot_overspend_shared_budget(self):
        second = self.create_account('two')

        def reserve(args):
            operation, account = args
            try:
                self.gateway.calls.reserve('org', 'video-mcp', operation, 'task', MODEL,
                                           PAYLOAD, account, '6')
                return 'reserved'
            except Conflict as error:
                return str(error)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(reserve, [('one', self.account['account_id']), ('two', second['account_id'])]))
        self.assertCountEqual(results, ['reserved', 'budget_exceeded'])

    def test_log_summary_is_filtered_and_unknown_is_not_zero(self):
        self.submit()
        logs = self.gateway.calls.list('org')
        self.assertEqual(logs['total'], 1)
        self.assertEqual(logs['summary']['task_count'], 1)
        for field in ('budget', 'settlement'):
            self.assertIsNone(logs['summary'][field]['amount'])
            self.assertEqual(logs['summary'][field]['unknown_count'], 1)
        self.assertEqual(self.gateway.calls.list('org', state='failed')['total'], 0)
        self.assertEqual(self.gateway.calls.list('other')['items'], [])
        self.assertEqual(self.gateway.calls.list('org', search='missing')['items'], [])
        # Pagination changes rows, not the current-filter totals.
        self.assertEqual(self.gateway.calls.list('org', offset=1)['summary'], logs['summary'])
        self.assertNotIn('credential_version', logs['items'][0])

    def test_amounts_and_manual_observation_validation(self):
        self.assertEqual(amount('0e999999999'), '0')
        self.assertEqual(Accounts.registration({'balance': '0e999999999'})['balance'], '0')
        self.assertIn('observed_at', Accounts.registration({'balance': '12.50'}))
        for value in ('NaN', 'Infinity', '-1', '1e99', '0.000000001'):
            with self.assertRaises(ValueError):
                amount(value)
        for value in ('yesterday', '2026-10-16T12:00:00'):
            with self.assertRaises(ValueError):
                Accounts.registration({'balance': '1', 'observed_at': value})

    def test_management_logs_require_management_identity(self):
        management, _ = create_apps(self.gateway, MANAGE, EXECUTE)
        self.submit()
        with TestClient(management) as client:
            headers = {'Authorization': 'Bearer ' + MANAGE, 'X-Organization-Id': 'org', 'X-User-Id': 'admin'}
            response = client.get('/v1/calls', headers=headers)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()['total'], 1)
            queried = client.post('/v1/calls/search', headers=headers, json={'search': 'task', 'limit': 50})
            self.assertEqual(queried.json()['summary'], response.json()['summary'])
            self.assertEqual(client.get('/v1/calls', headers=self.headers).status_code, 401)
            self.assertEqual(client.get('/v1/calls?limit=0', headers=headers).status_code, 400)

    def test_log_filters_and_archived_account_names(self):
        item = self.submit().json()
        self.accounts.patch('org', 'admin', self.account['account_id'], self.account['revision'], {}, archive=True)
        with self.store.connect() as db:
            db.execute('UPDATE channel_calls SET created_at=? WHERE id=?',
                       ('2026-10-16T16:00:00+00:00', item['call_id']))
        query = dict(account=self.account['account_id'], media='video', search='fal.ai',
                     created_from='2026-10-17T00:00:00+08:00', created_before='2026-10-18T00:00:00+08:00')
        rows = self.gateway.calls.list('org', **query)
        self.assertEqual(rows['total'], 1)
        self.assertEqual(rows['items'][0]['account_status'], 'archived')
        self.assertEqual(rows['items'][0]['account_name'], 'fal.ai 账号')
        self.assertEqual(rows['accounts'][0]['status'], 'archived')
        self.assertEqual(self.gateway.calls.list('org', **{**query, 'media': 'image'})['total'], 0)
        self.assertEqual(self.gateway.calls.list('org', created_before='2026-10-17T00:00:00+08:00')['total'], 0)
        self.assertEqual(self.gateway.calls.list('other')['accounts'], [])
        for bounds in ({'created_from': 'bad'}, {'created_from': '2026-10-16T00:00:00'},
                       {'created_from': '2026-10-18T00:00:00Z', 'created_before': '2026-10-17T00:00:00Z'}):
            with self.assertRaises(ValueError):
                self.gateway.calls.list('org', **bounds)

    def test_terminal_observation_is_idempotent(self):
        item = self.submit().json()
        self.gateway.calls.observed(item['call_id'], 'completed')
        first = self.gateway.calls.get('org', item['call_id'])
        self.gateway.calls.observed(item['call_id'], 'in_progress')
        self.gateway.calls.observed(item['call_id'], 'completed')
        after = self.gateway.calls.get('org', item['call_id'])
        self.assertEqual(first, after)
        projected = self.gateway.calls.project(after)
        self.assertIsNotNone(projected['duration_seconds'])
        self.assertEqual(projected['duration_source'], 'local_elapsed')
        self.assertIsNone(projected['settlement']['amount'])


if __name__ == '__main__':
    unittest.main()
