from pathlib import Path
import tempfile
import unittest

from cryptography.fernet import Fernet
import httpx
from starlette.testclient import TestClient

from fal_gateway.activity import activity, ActivityUnavailable
from fal_gateway.accounts import Accounts
from fal_gateway.migration import migrate_legacy
from fal_gateway.server import Gateway, create_apps
from fal_gateway.store import Store
from test_gateway import KEY, PAYLOAD, MANAGE, EXECUTE


class ActivityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(str(Path(self.temp.name) / 'state.sqlite3'), Fernet.generate_key().decode())
        self.requests = []
        self.remote_state = 'COMPLETED'
        self.hook = None

        def upstream(request):
            self.requests.append((request.method, request.headers['authorization']))
            if self.hook:
                self.hook()
            if self.remote_state == 'timeout':
                raise httpx.ReadTimeout('test-only')
            return httpx.Response(200, json={'status': self.remote_state})

        self.gateway = Gateway(self.store, httpx.Client(transport=httpx.MockTransport(upstream)))

    def tearDown(self):
        self.gateway.client.close()
        self.temp.cleanup()

    def legacy(self, org='org', operation='original', confirmed=True):
        if not self.store.overview(org)['version']:
            version = self.store.candidate(org, KEY, 0)
            rev = self.store.activate(org, version, 0)['revision']
            self.store.set_limits(org, True, 100, rev)
        item, _ = self.store.reserve_submission(org, operation, 'minimax/h3/reference-to-video', PAYLOAD, operation + '-' + org)
        if confirmed:
            self.store.confirm_submission(item['id'], 'remote-' + operation)
        return item

    def dump(self):
        with self.store.connect() as db:
            return '\n'.join(db.iterdump())

    def test_stale_queue_checks_original_credential_without_writes(self):
        self.legacy()
        before = self.dump()
        self.assertEqual(activity(self.gateway), {'schema_version': 1, 'scope': 'installation',
                                                 'submitted_count': 1, 'active_count': 0})
        self.assertEqual(self.requests, [('GET', 'Key ' + KEY)])
        self.assertEqual(before, self.dump())

    def test_all_organizations_and_unknown_are_included(self):
        self.legacy()
        self.legacy('other', confirmed=False)
        self.assertEqual(activity(self.gateway)['active_count'], 1)
        self.assertEqual(activity(self.gateway)['submitted_count'], 2)
        self.assertTrue(all(method == 'GET' for method, _ in self.requests))

    def test_migrated_task_is_counted_once_and_keeps_old_key_after_rotation(self):
        self.legacy()
        migrate_legacy(self.store)
        accounts = Accounts(self.store)
        account = accounts.list('org')['items'][0]
        accounts.checked('org', 'admin', account['account_id'], account['revision'], 'active', 'new-test-key-long-enough')
        before = self.dump()
        self.assertEqual(activity(self.gateway)['submitted_count'], 1)
        self.assertEqual(self.requests[-1], ('GET', 'Key ' + KEY))
        self.assertEqual(before, self.dump())

    def test_new_calls_use_saved_key_and_do_not_update_log(self):
        account = Accounts(self.store).create('org', 'admin', 'create', {
            'api_url': 'https://queue.fal.run', 'api_key': KEY, 'proxy': {'mode': 'direct'}})
        account = Accounts(self.store).checked('org', 'admin', account['account_id'], account['revision'], 'active')
        item, _ = self.gateway.calls.reserve('org', 'video', 'operation', 'task',
            'minimax/h3/reference-to-video', PAYLOAD, account['account_id'], '1')
        self.gateway.calls.accepted(item['id'], 'new-request')
        before = self.dump()
        self.assertEqual(activity(self.gateway)['active_count'], 0)
        self.assertEqual(self.requests, [('GET', 'Key ' + KEY)])
        self.assertEqual(before, self.dump())

    def test_upstream_running_busy_failure_or_invalid_unavailable(self):
        self.legacy()
        for state in ['IN_QUEUE', 'IN_PROGRESS']:
            self.remote_state = state
            self.assertEqual(activity(self.gateway)['active_count'], 1)
        for state in ['timeout', 'invalid']:
            self.remote_state = state
            with self.assertRaises(ActivityUnavailable):
                activity(self.gateway)

    def test_concurrent_submission_and_bounds_do_not_prove_idle(self):
        self.legacy()
        self.hook = lambda: self.legacy('other', confirmed=False)
        with self.assertRaises(ActivityUnavailable):
            activity(self.gateway)
        self.hook = None
        for kwargs in [{'max_pending': 1}, {'budget_seconds': 0}]:
            with self.assertRaises(ActivityUnavailable):
                activity(self.gateway, **kwargs)

    def test_management_auth_and_failure_response(self):
        self.legacy()
        management, execution = create_apps(self.gateway, MANAGE, EXECUTE)
        with TestClient(management) as client:
            self.assertEqual(client.get('/v1/activity').status_code, 401)
            headers = {'Authorization': 'Bearer ' + MANAGE, 'X-User-Id': 'admin', 'X-Organization-Id': 'org'}
            self.assertEqual(client.get('/v1/activity', headers=headers).json()['scope'], 'installation')
            self.remote_state = 'timeout'
            response = client.get('/v1/activity', headers=headers)
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json(), {'error': 'activity_unavailable', 'code': 'ACTIVITY_UNAVAILABLE'})
        with TestClient(execution) as client:
            self.assertEqual(client.get('/v1/activity').status_code, 404)


if __name__ == '__main__':
    unittest.main()
