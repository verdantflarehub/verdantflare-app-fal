"""Persistence, isolation, revision and credential privacy of account management."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
from starlette.testclient import TestClient

from fal_gateway.accounts import Accounts
from fal_gateway.server import Gateway, create_apps
from fal_gateway.store import Store


class AccountTest(unittest.TestCase):
    def test_errors_survive_studio_code_only_contract(self):
        result = self.client.post('/v1/accounts', headers=self.headers, json={})
        self.assertEqual(result.status_code, 400)
        self.assertRegex(result.json()['code'], r'^[A-Z][A-Z0-9_]{0,63}$')

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'state.db'
        self.key = Fernet.generate_key().decode()
        self.store = Store(str(self.path), self.key)
        self.gateway = Gateway(self.store)
        self.client = TestClient(create_apps(self.gateway, 'm' * 40, 'e' * 40)[0])
        self.headers = {'Authorization': 'Bearer ' + 'm' * 40,
                        'X-Organization-Id': 'org-a', 'X-User-Id': 'operator'}
        self.payload = {'name': 'Account A', 'api_url': 'https://queue.fal.run',
                        'api_key': 'synthetic-secret-for-testing', 'proxy': {'mode': 'direct'},
                        'registration': {'balance': '0.00'}, 'default': True}

    def tearDown(self):
        self.client.close()
        self.gateway.client.close()
        self.temp.cleanup()

    def create(self, operation='op-a', **changes):
        return self.client.post('/v1/accounts', headers={**self.headers, 'Idempotency-Key': operation},
                                json={**self.payload, **changes})

    def test_encryption_idempotency_and_reopen(self):
        first = self.create()
        self.assertEqual(first.status_code, 201, first.text)
        self.assertNotIn(self.payload['api_key'], first.text)
        self.assertNotIn(self.payload['api_key'].encode(), self.path.read_bytes())
        account = first.json()['account_id']
        self.assertEqual(self.create().json()['account_id'], account)
        self.assertEqual(self.create(name='different').status_code, 409)
        reopened = Accounts(Store(str(self.path), self.key))
        self.assertEqual(reopened.get('org-a', account)['registration']['balance'], '0.00')
        self.assertEqual(reopened.list('org-a')['total'], 1)

    def test_sdk_body_idempotency_and_read_filters(self):
        value = {**self.payload, 'operation_id': 'sdk-create'}
        first = self.client.post('/v1/accounts', headers=self.headers, json=value)
        self.assertEqual(first.status_code, 201)
        again = self.client.post('/v1/accounts', headers=self.headers, json=value)
        self.assertEqual(again.json(), first.json())
        filtered = self.client.post('/v1/accounts/search', headers=self.headers,
                                    json={'search': 'Account', 'status': 'active', 'offset': 0, 'limit': 50})
        self.assertEqual(filtered.status_code, 200)
        self.assertEqual(filtered.json()['total'], 1)
        mismatch = self.client.post('/v1/accounts', headers={**self.headers, 'Idempotency-Key': 'other'}, json=value)
        self.assertEqual(mismatch.status_code, 400)
        for malformed in ({'limit': None}, {'offset': True}, {'search': {}}, {'unknown': 'x'}):
            self.assertEqual(self.client.post('/v1/accounts/search', headers=self.headers, json=malformed).status_code, 400)

    def test_org_isolation_revision_and_archive_keeps_credentials(self):
        account = self.create().json()['account_id']
        url = '/v1/accounts/' + account
        other = {**self.headers, 'X-Organization-Id': 'org-b'}
        self.assertEqual(self.client.get(url, headers=other).status_code, 404)
        self.assertEqual(self.client.get('/v1/accounts', headers=other).json()['total'], 0)
        updated = self.client.patch(url, headers=self.headers,
            json={'expected_revision': 1, 'status': 'disabled'})
        self.assertEqual(updated.status_code, 200, updated.text)
        self.assertTrue(updated.json()['default'])
        self.assertEqual(updated.json()['status'], 'disabled')
        self.assertEqual(self.client.patch(url, headers=self.headers,
            json={'expected_revision': 1, 'status': 'active'}).status_code, 409)
        deleted = self.client.request('DELETE', url, headers=self.headers, json={'expected_revision': 2})
        self.assertEqual(deleted.status_code, 200, deleted.text)
        self.assertFalse(deleted.json()['default'])
        self.assertEqual(self.client.get('/v1/accounts', headers=self.headers).json()['total'], 0)
        self.assertEqual(self.client.get(url, headers=self.headers).json()['status'], 'archived')
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM channel_account_credentials').fetchone()[0], 1)

    def test_default_transfer_revises_previous_account(self):
        first = self.create().json()
        second = self.create('op-b', name='Account B').json()
        self.assertTrue(second['default'])
        old = self.client.get('/v1/accounts/' + first['account_id'], headers=self.headers).json()
        self.assertFalse(old['default'])
        self.assertEqual(old['revision'], 2)

    def test_invalid_inputs_and_execution_token_rejected(self):
        self.assertEqual(self.create(api_url='https://untrusted.invalid').status_code, 400)
        self.assertEqual(self.create(proxy={'mode': 'proxy', 'proxy_id': 'unresolved'}).status_code, 400)
        self.assertEqual(self.create(registration={'balance': 'NaN'}).status_code, 400)
        self.assertEqual(self.create(registration={'balance': '-1'}).status_code, 400)
        self.assertEqual(self.create(registration={'balance': '1e9999999'}).status_code, 400)
        self.assertEqual(self.client.get('/v1/accounts?limit=100000', headers=self.headers).status_code, 400)
        headers = {**self.headers, 'Authorization': 'Bearer ' + 'e' * 40}
        self.assertEqual(self.client.get('/v1/accounts', headers=headers).status_code, 401)
        self.assertEqual(self.client.get('/v1/accounts', headers={}).status_code, 401)

    def test_refresh_and_rotation_preserve_balance_and_previous_credentials(self):
        created = self.create().json()
        url = '/v1/accounts/' + created['account_id']
        with patch.object(self.gateway, 'check', return_value='active'):
            refreshed = self.client.post(url + '/refresh', headers=self.headers)
        self.assertEqual(refreshed.status_code, 200, refreshed.text)
        self.assertEqual(refreshed.json()['balance_status'], 'manual_only')
        self.assertEqual(refreshed.json()['account']['registration']['balance'], '0.00')
        revision = refreshed.json()['account']['revision']
        with patch.object(self.gateway, 'check', return_value='invalid'):
            failed = self.client.put(url + '/credential', headers=self.headers,
                json={'expected_revision': revision, 'api_key': 'synthetic-rejected-secret'})
        self.assertEqual(failed.status_code, 422)
        self.assertEqual(failed.json()['account']['authorization_state'], 'active')
        self.assertEqual(Accounts(self.store).credential('org-a', created['account_id'])[1], self.payload['api_key'])
        with patch.object(self.gateway, 'check', return_value='active'):
            rotated = self.client.put(url + '/credential', headers=self.headers,
                json={'expected_revision': revision, 'api_key': 'synthetic-accepted-secret'})
        self.assertEqual(rotated.status_code, 200, rotated.text)
        self.assertNotIn('synthetic-accepted-secret', rotated.text)
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM channel_account_credentials').fetchone()[0], 2)

    def test_concurrent_change_during_refresh_cannot_overwrite_account(self):
        created = self.create().json()
        account = created['account_id']

        def check(_, **_connection):
            Accounts(self.store).patch('org-a', 'operator', account, 1, {'status': 'disabled'})
            return 'active'

        with patch.object(self.gateway, 'check', side_effect=check):
            result = self.client.post('/v1/accounts/' + account + '/refresh', headers=self.headers)
        self.assertEqual(result.status_code, 409)
        self.assertEqual(Accounts(self.store).get('org-a', account)['status'], 'disabled')


if __name__ == '__main__':
    unittest.main()
