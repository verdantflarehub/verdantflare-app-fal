from pathlib import Path
import json
import tempfile
import unittest
from unittest.mock import patch
from contextlib import contextmanager

from cryptography.fernet import Fernet
import httpx

from fal_gateway.accounts import Accounts
from fal_gateway.calls import Calls
from fal_gateway.migration import migrate_legacy, resolve_legacy_proxies
from fal_gateway.server import Gateway, MODEL
from fal_gateway.store import Store, Conflict
from test_gateway import KEY, PAYLOAD


class MigrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.key = Fernet.generate_key().decode()
        self.store = Store(str(Path(self.temp.name) / 'state.sqlite3'), self.key)
        self.sent = []
        def upstream(request):
            self.sent.append((request.method, request.headers['authorization']))
            return httpx.Response(200, json={'request_id': 'legacy-remote'} if request.method == 'POST'
                                  else {'status': 'COMPLETED'})
        self.gateway = Gateway(self.store, httpx.Client(transport=httpx.MockTransport(upstream)))
        version = self.store.candidate('org', KEY, 0)
        revision = self.store.activate('org', version, 0)['revision']
        self.store.set_limits('org', True, 1, revision)

    def tearDown(self):
        self.gateway.client.close()
        self.temp.cleanup()

    def imported(self):
        return Accounts(self.store).list('org')['items'][0]

    def test_migration_preserves_identity_limits_and_replay(self):
        old = self.gateway.submit('org', 'original', PAYLOAD)
        migrate_legacy(self.store)
        first = self.imported()
        self.assertEqual(first['registration']['daily_limit'], 1)
        self.assertTrue(first['default'])
        self.assertEqual(self.gateway.calls.list('org')['items'][0]['call_id'], old['id'])
        self.assertEqual(self.gateway.submit('org', 'original', PAYLOAD), old)
        with self.assertRaisesRegex(Conflict, 'legacy_call_requires_recovery'):
            self.gateway.calls.reserve('org', 'video', 'original', 'task', MODEL, PAYLOAD, None, '1')
        with self.assertRaisesRegex(Conflict, 'channel_accounts_required'):
            self.gateway.submit('org', 'new-legacy', PAYLOAD)
        with self.assertRaisesRegex(Conflict, 'daily_limit_reached'):
            self.gateway.calls.reserve('org', 'video', 'new', 'task', MODEL, PAYLOAD, None, '1')
        with self.assertRaisesRegex(Conflict, 'channel_accounts_required'):
            self.store.candidate('org', 'another-test-key-long-enough', 2)
        migrate_legacy(self.store)
        self.assertEqual(self.imported(), first)
        reopened = Store(str(self.store.path), self.key)
        Calls(reopened)
        migrate_legacy(reopened)
        self.assertEqual(Accounts(reopened).list('org')['items'][0], first)
        self.assertEqual(len([method for method, _ in self.sent if method == 'POST']), 1)

    def test_original_version_recovers_after_rotation_and_archive(self):
        old = self.gateway.submit('org', 'original', PAYLOAD)
        revision = self.store.overview('org')['revision']
        candidate = self.store.candidate('org', 'pending-key-long-enough', revision)
        migrate_legacy(self.store)
        accounts = Accounts(self.store)
        first = self.imported()
        rotated = accounts.checked('org', 'admin', first['account_id'], first['revision'],
                                   'active', 'new-active-key-long-enough')
        with self.store.connect() as db:
            version = db.execute('SELECT credential_version FROM channel_accounts WHERE id=?',
                                 (first['account_id'],)).fetchone()[0]
        self.assertGreater(version, candidate)
        accounts.patch('org', 'admin', rotated['account_id'], rotated['revision'], {}, archive=True)
        self.assertEqual(self.gateway.remote('org', old['id'])['state'], 'completed')
        self.assertEqual(self.sent[-1], ('GET', 'Key ' + KEY))
        entry = self.gateway.calls.list('org')['items'][0]
        self.assertEqual(entry['state'], 'completed')
        self.assertEqual(entry['account_status'], 'archived')
        self.assertIsNone(entry['settlement']['amount'])

    def test_missing_recovery_key_rolls_back_entire_cutover(self):
        self.gateway.submit('org', 'original', PAYLOAD)
        with self.store.connect() as db:
            db.execute('UPDATE submissions SET credential_version=99')
        with self.assertRaisesRegex(ValueError, 'legacy_recovery_credential_missing'):
            migrate_legacy(self.store)
        self.assertEqual(Accounts(self.store).list('org')['total'], 0)
        with self.store.connect() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM channel_metadata WHERE name='accounts_cutover'").fetchone())
            self.assertEqual(db.execute('SELECT COUNT(*) FROM channel_calls').fetchone()[0], 0)

    def test_existing_default_not_replaced_and_revoked_import_stays_disabled(self):
        accounts = Accounts(self.store)
        existing = accounts.create('org', 'admin', 'new-account', {'api_url': 'https://queue.fal.run',
            'api_key': KEY, 'proxy': {'mode': 'direct'}, 'default': True})
        revision = self.store.overview('org')['revision']
        self.store.revoke('org', revision)
        migrate_legacy(self.store)
        items = accounts.list('org')['items']
        self.assertEqual(len(items), 2)
        self.assertEqual(next(a for a in items if a['default'])['account_id'], existing['account_id'])
        imported = next(a for a in items if a['account_id'] != existing['account_id'])
        self.assertEqual(imported['status'], 'disabled')
        self.assertEqual(imported['authorization_state'], 'invalid')

    def test_historical_unknown_cost_is_not_zero_budget(self):
        self.gateway.submit('org', 'original', PAYLOAD)
        migrate_legacy(self.store)
        account = self.imported()
        Accounts(self.store).patch('org', 'admin', account['account_id'], account['revision'],
            {'registration': {'daily_limit': 10, 'budget_limit': '100'}})
        with self.assertRaisesRegex(Conflict, 'historical_cost_unknown'):
            self.gateway.calls.reserve('org', 'video', 'new', 'task', MODEL, PAYLOAD, None, '1')

    def test_existing_proxy_blocks_cutover_without_mutating_original_data(self):
        old = self.gateway.submit('org', 'original', PAYLOAD)
        with self.store.connect() as db:
            before = '\n'.join(db.iterdump())
        with self.assertRaisesRegex(ValueError, '^legacy_proxy_mapping_required$'):
            migrate_legacy(self.store, legacy_proxy_configured=True)
        with self.store.connect() as db:
            self.assertEqual(before, '\n'.join(db.iterdump()))
        self.assertEqual(self.gateway.submit('org', 'original', PAYLOAD), old)
        self.assertEqual(self.gateway.remote('org', old['id'])['state'], 'completed')

    def test_explicit_proxy_mapping_preserves_original_calls_and_rejects_different_exit(self):
        reference = {'mode': 'proxy', 'proxy_id': '0199b501-0000-7000-8000-000000000002', 'revision': 4}
        class Resolver:
            url = 'socks5h://proxy.example.invalid:1081'
            def resolve_url(self, organization, value):
                if organization != 'org' or value != reference:
                    raise ValueError('unexpected mapping')
                return self.url
        resolver = Resolver()
        old = self.gateway.submit('org', 'original', PAYLOAD)
        for url in ['http://other.example.invalid:1081', 'http://proxy.example.invalid:1082',
                    'http://user:secret@proxy.example.invalid:1081']:
            with self.assertRaisesRegex(ValueError, '^legacy_proxy_mapping_unverified$'):
                resolve_legacy_proxies(self.store, resolver, url, reference)
        mappings = resolve_legacy_proxies(self.store, resolver, 'http://proxy.example.invalid:1081', reference)
        migrate_legacy(self.store, legacy_proxy_configured=True, legacy_proxies=mappings)
        self.assertEqual(self.imported()['proxy'], reference)
        self.assertEqual(json.loads(self.gateway.calls.get('org', old['id'])['proxy_json']), reference)
        @contextmanager
        def mapped_transport(organization, value, *, legacy=False):
            self.assertEqual(organization, 'org')
            self.assertEqual(value, reference)
            self.assertFalse(legacy)
            yield self.gateway.client
        with patch.object(self.gateway, 'transport', mapped_transport):
            self.assertEqual(self.gateway.remote('org', old['id'])['state'], 'completed')

    def legacy_writes_after_failed_upgrade(self):
        # Original 0.1.0 ignores the newer cutover marker. Simulate its SQL writes.
        with self.store.connect() as db:
            db.execute("INSERT INTO credentials SELECT organization_id,version+1,ciphertext,state,checked_at,created_at FROM credentials WHERE version=1")
            db.execute('UPDATE organizations SET active_version=2,revision=revision+1,daily_limit=7')
            db.execute("INSERT INTO submissions SELECT 'after-rollback',organization_id,'after-rollback',digest,2,model_id,'remote-after-rollback','queued',created_at FROM submissions LIMIT 1")

    def test_failed_upgrade_retry_imports_old_writes_and_preserves_account_id(self):
        old = self.gateway.submit('org', 'original', PAYLOAD)
        migrate_legacy(self.store)
        original_account = self.imported()['account_id']
        self.legacy_writes_after_failed_upgrade()
        migrate_legacy(self.store)
        self.assertEqual(self.imported()['account_id'], original_account)
        self.assertEqual(self.imported()['registration']['daily_limit'], 7)
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM channel_account_credentials').fetchone()[0], 2)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM channel_calls').fetchone()[0], 2)
            self.assertEqual(db.execute('SELECT credential_version FROM channel_accounts').fetchone()[0], 2)
        self.assertEqual(self.gateway.calls.get('org', old['id'])['provider_request_id'], 'legacy-remote')
        migrate_legacy(self.store)
        self.assertEqual(self.gateway.calls.list('org')['total'], 2)
        self.assertEqual(len([method for method, _ in self.sent if method == 'POST']), 1)

    def test_retry_preserves_both_sides_if_target_was_edited(self):
        self.gateway.submit('org', 'original', PAYLOAD)
        migrate_legacy(self.store)
        account = self.imported()
        Accounts(self.store).patch('org', 'admin', account['account_id'], account['revision'], {'name': 'User edit'})
        self.legacy_writes_after_failed_upgrade()
        with self.store.connect() as db:
            before = '\n'.join(db.iterdump())
        for operation in (lambda: migrate_legacy(self.store),
                          lambda: resolve_legacy_proxies(self.store, None, '', None)):
            with self.assertRaisesRegex(ValueError, '^legacy_migration_reconciliation_required$'):
                operation()
        with self.store.connect() as db:
            self.assertEqual(before, '\n'.join(db.iterdump()))

    def test_retry_proxy_failure_rolls_back_projection_cleanup(self):
        self.gateway.submit('org', 'original', PAYLOAD)
        migrate_legacy(self.store)
        self.legacy_writes_after_failed_upgrade()
        with self.store.connect() as db:
            before = '\n'.join(db.iterdump())
        with self.assertRaisesRegex(ValueError, '^legacy_proxy_mapping_required$'):
            migrate_legacy(self.store, legacy_proxy_configured=True)
        with self.store.connect() as db:
            self.assertEqual(before, '\n'.join(db.iterdump()))

    def test_missing_checkpoint_never_assumes_successful_import(self):
        migrate_legacy(self.store)
        with self.store.connect() as db:
            db.execute("DELETE FROM channel_metadata WHERE name='legacy_source_digest'")
        with self.assertRaisesRegex(ValueError, '^legacy_migration_checkpoint_missing$'):
            migrate_legacy(self.store)


if __name__ == '__main__':
    unittest.main()
