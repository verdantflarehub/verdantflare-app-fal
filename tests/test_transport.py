from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cryptography.fernet import Fernet
import httpx

from fal_gateway.accounts import Accounts
from fal_gateway.activity import activity
from fal_gateway.server import Gateway
from fal_gateway.store import Store
from test_gateway import KEY, PAYLOAD


class TransportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(str(Path(self.temp.name) / 'state.sqlite3'), Fernet.generate_key().decode())

    def tearDown(self):
        self.temp.cleanup()

    def test_direct_ignores_invalid_environment_proxy_and_legacy_does_not(self):
        with patch.dict('os.environ', {'HTTPS_PROXY': 'unsupported://proxy.invalid:9',
                                      'https_proxy': 'unsupported://proxy.invalid:9'}):
            gateway = Gateway(self.store)
            try:
                with self.assertRaises(ValueError):
                    _ = gateway.legacy_client
            finally:
                gateway.close()

    def test_old_recovery_and_new_calls_use_different_transports(self):
        used = []
        def transport(label):
            def respond(request):
                used.append((label, request.method, request.url.path))
                return httpx.Response(200, json={'request_id': 'remote', 'status': 'COMPLETED', 'items': []})
            return httpx.Client(transport=httpx.MockTransport(respond))
        gateway = Gateway(self.store, transport('direct'))
        gateway._legacy_client = transport('legacy')
        try:
            version = self.store.candidate('org', KEY, 0)
            revision = self.store.activate('org', version, 0)['revision']
            self.store.set_limits('org', True, 10, revision)
            old = gateway.submit('org', 'old', PAYLOAD)
            gateway.remote('org', old['id'])
            gateway.check(KEY, legacy=True)
            self.assertEqual({label for label, _, _ in used}, {'legacy'})
            used.clear()
            accounts = Accounts(self.store)
            a = accounts.create('org', 'admin', 'create', {'api_url': 'https://queue.fal.run',
                'api_key': KEY, 'proxy': {'mode': 'direct'}})
            accounts.checked('org', 'admin', a['account_id'], a['revision'], 'active')
            gateway.check(KEY)
            new = gateway.submit_call('org', 'video', 'new', 'task', PAYLOAD, a['account_id'], '1')
            gateway.remote_call('org', 'video', new['call_id'])
            self.assertEqual({label for label, _, _ in used}, {'direct'})
            used.clear()
            self.assertEqual(activity(gateway)['active_count'], 0)
            # The completed new call needs no polling; old stale queued must use
            # its historical transport and must never be submitted again.
            self.assertEqual([(label, method) for label, method, _ in used], [('legacy', 'GET')])
        finally:
            gateway.close()


if __name__ == '__main__':
    unittest.main()
