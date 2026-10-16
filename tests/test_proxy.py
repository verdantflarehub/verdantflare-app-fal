import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
import httpx
from starlette.testclient import TestClient

from fal_gateway.accounts import Accounts
from fal_gateway.proxy import NetworkProxies, ProxyUnavailable
from fal_gateway.server import Gateway, APIError, create_apps
from fal_gateway.store import Store
from test_gateway import KEY, PAYLOAD, MANAGE, EXECUTE

ORG = '0199b501-0000-7000-8000-000000000001'
PID = '0199b501-0000-7000-8000-000000000002'
REF = {'mode': 'proxy', 'proxy_id': PID, 'revision': 4}


class NetworkProxyTest(unittest.TestCase):
    def test_resolution_is_bound_and_does_not_follow_redirects(self):
        sent = []
        response = {'schema_version': 1, 'proxy_id': PID, 'revision': 4, 'protocol': 'socks5h',
                    'host': 'proxy.example.invalid', 'port': 1080, 'username': 'fixture@user', 'password': 'a/b:c'}
        def upstream(request):
            sent.append(request)
            return httpx.Response(200, json=response)
        with httpx.Client(transport=httpx.MockTransport(upstream), follow_redirects=False) as client:
            network = NetworkProxies('http://network.example.invalid', 't' * 40, client=client)
            self.assertEqual(network.resolve_url(ORG, REF), 'socks5h://fixture%40user:a%2Fb%3Ac@proxy.example.invalid:1080')
            self.assertEqual(sent[0].headers['x-organization-id'], ORG)
            self.assertEqual(json.loads(sent[0].content), {'schema_version': 1, 'proxy_id': PID, 'revision': 4})
            response['revision'] = 5
            with self.assertRaises(ProxyUnavailable):
                network.resolve_url(ORG, REF)
            response['revision'] = 4
            response['host'] = 'proxy.example.invalid@attacker.invalid'
            with self.assertRaises(ProxyUnavailable):
                network.resolve_url(ORG, REF)
        redirects = []
        def redirect(request):
            redirects.append(str(request.url))
            return httpx.Response(307, headers={'location': 'http://attacker.invalid/'})
        with httpx.Client(transport=httpx.MockTransport(redirect), follow_redirects=False) as client:
            network = NetworkProxies('http://network.example.invalid', 't' * 40, client=client)
            with self.assertRaises(ProxyUnavailable):
                network.resolve_url(ORG, REF)
        self.assertEqual(len(redirects), 1)

    def test_list_rejects_secret_fields_and_duplicate_json(self):
        for content in [json.dumps({'schema_version': 1, 'items': [{'proxy_id': PID, 'name': 'Proxy',
                'protocol': 'http', 'status': 'active', 'revision': 4, 'expires_at': None, 'password': 'synthetic'}]}),
                '{"schema_version":1,"schema_version":1,"items":[]}']:
            with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, content=content))) as client:
                network = NetworkProxies('http://network.example.invalid', 't' * 40, client=client)
                with self.assertRaises(ProxyUnavailable):
                    network.list(ORG)


class FakeNetwork:
    def __init__(self):
        self.seen = []
        self.fail = False

    def resolve_url(self, org, value):
        self.seen.append((org, dict(value)))
        if self.fail:
            raise ProxyUnavailable()
        return 'http://proxy.example.invalid:1080'

    def list(self, org):
        return {'schema_version': 1, 'items': [{'proxy_id': PID, 'name': 'Proxy', 'protocol': 'http',
            'revision': 4, 'status': 'active', 'expires_at': None}]}

    def close(self):
        pass


class ProxyExecutionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(str(Path(self.temp.name) / 'state.sqlite3'), Fernet.generate_key().decode())
        self.network = FakeNetwork()
        self.direct = []
        def direct(request):
            self.direct.append(request.method)
            raise AssertionError('proxy call fell back to direct')
        self.gateway = Gateway(self.store, httpx.Client(transport=httpx.MockTransport(direct)), network=self.network)
        self.accounts = Accounts(self.store)
        self.account = self.accounts.create(ORG, 'admin', 'account', {'api_url': 'https://queue.fal.run',
            'api_key': KEY, 'proxy': REF})
        self.account = self.accounts.checked(ORG, 'admin', self.account['account_id'], self.account['revision'], 'active')
        self.real_client = httpx.Client
        self.sent = []
        self.timeout = False

    def tearDown(self):
        self.gateway.close()
        self.temp.cleanup()

    def proxy_client(self, **kwargs):
        self.assertFalse(kwargs['trust_env'])
        self.assertFalse(kwargs['follow_redirects'])
        self.assertEqual(kwargs['proxy'], 'http://proxy.example.invalid:1080')
        def upstream(request):
            self.sent.append(request.method)
            if self.timeout:
                raise httpx.ReadTimeout('synthetic')
            return httpx.Response(200, json={'request_id': 'remote'} if request.method == 'POST' else {'status': 'COMPLETED'})
        return self.real_client(transport=httpx.MockTransport(upstream))

    def submit(self, operation='task'):
        return self.gateway.submit_call(ORG, 'video', operation, operation, PAYLOAD, self.account['account_id'], '1')

    def test_original_proxy_survives_account_change_and_task_replay(self):
        with patch('fal_gateway.server.httpx.Client', side_effect=self.proxy_client):
            item = self.submit()
            self.accounts.patch(ORG, 'admin', self.account['account_id'], self.account['revision'], {'proxy': {'mode': 'direct'}})
            self.gateway.remote_call(ORG, 'video', item['call_id'])
            self.submit()
        self.assertEqual(self.sent, ['POST', 'GET'])
        self.assertEqual(self.network.seen, [(ORG, REF), (ORG, REF)])
        self.assertEqual(self.direct, [])
        current = self.accounts.get(ORG, self.account['account_id'])
        self.assertEqual(current['authorization_state'], 'pending_verification')

    def test_resolution_failure_is_pre_send_and_unknown_send_never_retries(self):
        self.network.fail = True
        with self.assertRaises(APIError):
            self.submit('blocked')
        blocked = self.gateway.calls.by_operation(ORG, 'video', 'blocked')
        self.assertFalse(blocked['accepted'])
        self.assertEqual(self.direct, [])
        self.network.fail = False
        self.timeout = True
        with patch('fal_gateway.server.httpx.Client', side_effect=self.proxy_client):
            unknown = self.submit('unknown')
            self.assertIsNone(unknown['accepted'])
            self.submit('unknown')
        self.assertEqual(self.sent, ['POST'])

    def test_management_lists_no_credentials_and_create_replay_survives_revocation(self):
        headers = {'Authorization': 'Bearer ' + MANAGE, 'X-Organization-Id': ORG, 'X-User-Id': 'admin'}
        payload = {'operation_id': 'new-account', 'api_url': 'https://queue.fal.run', 'api_key': KEY, 'proxy': REF}
        with TestClient(create_apps(self.gateway, MANAGE, EXECUTE)[0]) as client:
            self.assertEqual(client.get('/v1/proxies', headers=headers).status_code, 200)
            first = client.post('/v1/accounts', headers=headers, json=payload)
            self.assertEqual(first.status_code, 201)
            self.network.fail = True
            replay = client.post('/v1/accounts', headers=headers, json=payload)
            self.assertEqual(replay.status_code, 201)
            self.assertEqual(first.json()['account_id'], replay.json()['account_id'])
            rejected = client.post('/v1/accounts', headers=headers, json={**payload, 'operation_id': 'blocked'})
            self.assertEqual(rejected.status_code, 503)
            self.assertEqual(self.accounts.list(ORG)['total'], 2)


if __name__ == '__main__':
    unittest.main()
