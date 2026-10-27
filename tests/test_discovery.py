import base64
import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
import httpx

from fal_gateway.discovery import Config, DiscoveryError, Etcd, Publisher, Superseded, encode
from fal_gateway.store import Store
from fal_gateway.discovery_lifecycle import PublicationLifetime, configured_publisher


class Registry:
    def __init__(self):
        self.requests = []
        self.snapshot = {'kvs': []}
        self.revision = 20
        self.lease = 100
        self.reject = None
        self.conflict = False
        self.expired = False
        self.closed = False

    def request(self, path, body):
        self.requests.append((path, body))
        if path == self.reject:
            raise DiscoveryError('synthetic_denial')
        if path == 'kv/range':
            return self.snapshot
        if path == 'lease/grant':
            self.lease += 1
            return {'ID': str(self.lease), 'TTL': '300'}
        if path == 'lease/keepalive':
            return {'result': {'ID': body['ID'], 'TTL': '0' if self.expired else '300'}}
        if path == 'lease/revoke':
            return {}
        if path == 'kv/txn':
            self.revision += 1
            return {'succeeded': not self.conflict, 'header': {'revision': str(self.revision)}}
        raise AssertionError('unexpected request')

    def close(self):
        self.closed = True


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / 'state.db', Fernet.generate_key().decode())
        self.config = Config('http://etcd.verdantflare-station.svc.cluster.local:2379',
                             'station-test', 'synthetic-user', 'synthetic-password')
        self.registry = Registry()
        self.publisher = self.make()

    def make(self):
        return Publisher(self.config, self.store, '0.1.4', [{
            'capability_id': 'image.generate', 'adapter_version': '1',
            'execution_state': 'adapter_connected_unverified'}], etcd=self.registry)

    def tearDown(self):
        self.publisher.close()
        self.temp.cleanup()

    def test_atomic_registration_heartbeat_and_minimal_metadata(self):
        self.publisher.publish()
        transaction = self.registry.requests[-1][1]
        self.assertEqual(transaction['compare'][0]['mod_revision'], '0')
        writes = [x['request_put'] for x in transaction['success']]
        self.assertEqual(len(writes), 2)
        self.assertEqual({x['lease'] for x in writes}, {'101'})
        values = [json.loads(base64.b64decode(x['value'])) for x in writes]
        self.assertEqual(set(values[1]), {'state', 'reason_code', 'revision', 'observed_at'})
        self.assertNotIn('synthetic-password', json.dumps(values))
        self.assertEqual(values[0]['capabilities'][0]['execution_state'], 'adapter_connected_unverified')
        self.publisher.publish()
        self.assertEqual(self.registry.requests[-2][0], 'lease/keepalive')
        self.assertEqual(self.registry.requests[-1][1]['compare'][0]['mod_revision'], '21')

    def test_durable_state_precedes_publication_and_survives_restart(self):
        self.publisher.set_state('disabled', 'operator_disabled')
        self.registry.reject = 'kv/txn'
        with self.assertRaises(DiscoveryError):
            self.publisher.publish()
        replacement = self.make()
        self.assertGreater(replacement.generation, self.publisher.generation)
        self.assertEqual(replacement.state()['state'], 'disabled')
        self.assertEqual(replacement.state()['revision'], 2)
        with self.assertRaises(Superseded):
            self.publisher.set_state('enabled', 'ready')
        with self.assertRaises(Superseded):
            self.publisher.publish()
        replacement.close()

    def test_failed_write_revokes_own_lease_then_republishes(self):
        self.registry.reject = 'kv/txn'
        with self.assertRaises(DiscoveryError):
            self.publisher.publish()
        self.assertEqual(self.registry.requests[-1], ('lease/revoke', {'ID': '101'}))
        self.assertIsNone(self.publisher.lease)
        self.registry.reject = None
        self.publisher.publish()
        self.assertEqual(self.publisher.lease, '102')

    def test_expired_lease_cannot_claim_online_and_next_tick_reacquires(self):
        self.publisher.publish()
        self.registry.expired = True
        count = sum(path == 'kv/txn' for path, _ in self.registry.requests)
        with self.assertRaises(DiscoveryError):
            self.publisher.publish()
        self.assertEqual(sum(path == 'kv/txn' for path, _ in self.registry.requests), count)
        self.registry.expired = False
        self.publisher.publish()
        self.assertEqual(self.publisher.lease, '102')

    def test_conflicting_writer_permanently_fences_old_object(self):
        self.publisher.publish()
        self.registry.conflict = True
        with self.assertRaises(DiscoveryError):
            self.publisher.publish()
        self.registry.conflict = False
        count = len(self.registry.requests)
        with self.assertRaises(Superseded):
            self.publisher.publish()
        self.assertEqual(len(self.registry.requests), count)

    def test_newer_remote_generation_prevents_recovery_with_stale_database(self):
        self.registry.snapshot = {'kvs': [{'key': encode(self.config.prefix + 'instances/newer'),
            'value': encode(json.dumps({'generation': 100}))}]}
        with self.assertRaises(DiscoveryError):
            self.publisher.publish()
        self.assertTrue(self.publisher.fenced)
        self.assertFalse(any(path == 'lease/grant' for path, _ in self.registry.requests))

    def test_partial_snapshot_cannot_be_used_for_takeover(self):
        self.registry.snapshot = {'more': True, 'kvs': []}
        with self.assertRaises(DiscoveryError):
            self.publisher.publish()
        self.assertFalse(any(path == 'lease/grant' for path, _ in self.registry.requests))

    def test_close_never_deletes_channel_keys_directly(self):
        self.publisher.publish()
        self.publisher.close()
        self.assertEqual(self.registry.requests[-1], ('lease/revoke', {'ID': '101'}))
        self.assertTrue(self.registry.closed)
        self.assertFalse(any(path == 'kv/deleterange' for path, _ in self.registry.requests))

    def test_shutdown_restart_preserves_disabled_and_degraded_configuration(self):
        for state in ('disabled', 'degraded', 'unavailable'):
            self.publisher.set_state(state, 'operator_choice')
            self.publisher.drain()
            self.assertEqual(self.publisher.state()['state'], 'draining')
            self.publisher = self.make()
            self.publisher.resume()
            self.assertEqual(self.publisher.state()['state'], state)
            self.assertEqual(self.publisher.state()['reason_code'], 'operator_choice')

    def test_operator_draining_is_not_automatically_enabled(self):
        self.publisher.set_state('draining', 'operator_choice')
        self.publisher.drain()
        self.publisher = self.make()
        self.publisher.resume()
        self.assertEqual(self.publisher.state()['state'], 'draining')
        self.assertEqual(self.publisher.state()['reason_code'], 'operator_choice')

    def test_opt_in_requires_complete_scope_and_authentication(self):
        self.assertIsNone(configured_publisher({}, self.store, '0.1.4'))
        for values in ({'CHANNEL_DISCOVERY_SCOPE_ID': 'scope'},
                       {'CHANNEL_DISCOVERY_ETCD_URL': self.config.endpoint},
                       {'CHANNEL_DISCOVERY_SCOPE_ID': 'scope', 'CHANNEL_DISCOVERY_ETCD_URL': self.config.endpoint}):
            with self.assertRaises(ValueError):
                configured_publisher(values, self.store, '0.1.4')

    def test_execution_url_uses_installed_service_configuration(self):
        values = {'CHANNEL_DISCOVERY_SCOPE_ID': 'installation-scope',
                  'CHANNEL_DISCOVERY_ETCD_URL': self.config.endpoint,
                  'ETCD_USERNAME': 'synthetic', 'ETCD_PASSWORD': 'synthetic',
                  'CHANNEL_DISCOVERY_EXECUTION_URL': 'http://fal-custom.workspace.svc.cluster.local:8001'}
        with patch('fal_gateway.discovery_lifecycle.Publisher') as publisher:
            configured_publisher(values, self.store, '0.2.16')
            config = publisher.call_args.args[0]
            self.assertEqual(config.execution_url, values['CHANNEL_DISCOVERY_EXECUTION_URL'])
            self.assertEqual(config.scope, 'installation-scope')
            values.pop('CHANNEL_DISCOVERY_EXECUTION_URL')
            configured_publisher(values, self.store, '0.2.16')
            self.assertEqual(publisher.call_args.args[0].execution_url, 'http://fal-channel.verdantflare-app.svc.cluster.local:8001')
        for url in ('https://public.invalid:8001', 'http://fal.workspace.svc.cluster.local:8001/path',
                    'http://user:secret@fal.workspace.svc.cluster.local:8001', 'http://fal.workspace.svc.cluster.local:8001?token=secret'):
            with self.subTest(url=url), self.assertRaises(ValueError):
                Config(self.config.endpoint, 'scope', 'synthetic', 'synthetic', execution_url=url)


class LifetimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_listeners_must_be_ready_then_drain_blocks_submissions_before_revoke(self):
        class FakePublisher:
            fenced = False
            def __init__(self):
                self.published = []
                self.current = 'enabled'
                self.closed = False
            def state(self):
                return {'state': self.current}
            def resume(self):
                pass
            def publish(self):
                self.published.append(self.current)
            def drain(self):
                self.current = 'draining'
            def close(self):
                self.closed = True
        publisher = FakePublisher()
        servers = [SimpleNamespace(started=False), SimpleNamespace(started=False)]
        accepting = []
        lifetime = PublicationLifetime(publisher, servers, accepting.append)
        task = asyncio.create_task(lifetime.run(interval=.01))
        try:
            servers[0].started = True
            await asyncio.sleep(.02)
            self.assertFalse(publisher.published)
            self.assertEqual(accepting, [False])
            servers[1].started = True
            async with asyncio.timeout(2):
                while True not in accepting:
                    await asyncio.sleep(.01)
            lifetime.begin_draining()
            self.assertFalse(accepting[-1])
            async with asyncio.timeout(2):
                while 'draining' not in publisher.published:
                    await asyncio.sleep(.01)
            self.assertFalse(publisher.closed)
        finally:
            await lifetime.finish(task)
        self.assertTrue(publisher.closed)
        self.assertFalse(accepting[-1])

    async def test_immediate_finish_advertises_draining_before_lease_cleanup(self):
        class FakePublisher:
            current = 'enabled'
            fenced = False
            events = []
            def resume(self):
                pass
            def publish(self):
                self.events.append(self.current)
            def state(self):
                return {'state': self.current}
            def drain(self):
                self.current = 'draining'
            def close(self):
                self.events.append('closed')
        publisher = FakePublisher()
        accepting = asyncio.Event()
        lifetime = PublicationLifetime(publisher, [SimpleNamespace(started=True)],
            lambda value: accepting.set() if value else None)
        task = asyncio.create_task(lifetime.run(interval=60))
        await asyncio.wait_for(accepting.wait(), 2)
        await lifetime.finish(task)
        self.assertEqual(publisher.events, ['enabled', 'draining', 'closed'])

    async def test_finish_cancellation_waits_for_registry_cleanup(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def registry_work():
            entered.set()
            await release.wait()
        lifetime = PublicationLifetime(object(), [], lambda value: None)
        work = asyncio.create_task(registry_work())
        await entered.wait()
        finish = asyncio.create_task(lifetime.finish(work))
        await asyncio.sleep(0)
        finish.cancel()
        await asyncio.sleep(0)
        finish.cancel()
        await asyncio.sleep(0)
        self.assertFalse(work.done())
        self.assertFalse(finish.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await finish
        self.assertFalse(work.cancelled())

    async def test_shutdown_before_startup_never_advertises(self):
        class FakePublisher:
            def __init__(self):
                self.closed = False
            def close(self):
                self.closed = True
        publisher = FakePublisher()
        lifetime = PublicationLifetime(publisher, [SimpleNamespace(started=False)], lambda value: None)
        task = asyncio.create_task(lifetime.run())
        await lifetime.finish(task)
        self.assertTrue(publisher.closed)


class TransportTests(unittest.TestCase):
    def test_bounded_reauthentication_and_no_anonymous_fallback(self):
        requests = []
        def respond(request):
            requests.append(request)
            if request.url.path.endswith('authenticate'):
                return httpx.Response(200, json={'token': 'synthetic-token'})
            return httpx.Response(401, json={'code': 16, 'error': 'sensitive upstream detail'})
        config = Config('http://etcd.example.svc.cluster.local:2379', 'scope', 'user', 'secret')
        registry = Etcd(config, transport=httpx.MockTransport(respond))
        with self.assertRaisesRegex(DiscoveryError, '^registry_request_rejected$'):
            registry.request('kv/range', {'key': encode('key')})
        self.assertEqual(len(requests), 4)
        for request in requests:
            if request.url.path.endswith('range'):
                self.assertEqual(request.headers['Authorization'], 'synthetic-token')
        registry.close()
        self.assertIsNone(registry._token)

    def test_permission_denial_and_redirect_do_not_reauthenticate(self):
        for status, data in [(403, {'code': 7}), (302, {})]:
            requests = []
            def respond(request):
                requests.append(request)
                if request.url.path.endswith('authenticate'):
                    return httpx.Response(200, json={'token': 'token'})
                return httpx.Response(status, json=data, headers={'Location': 'https://example.invalid'})
            registry = Etcd(Config('http://etcd.example.svc.cluster.local:2379', 'scope', 'user', 'secret'),
                            transport=httpx.MockTransport(respond))
            with self.assertRaises(DiscoveryError):
                registry.request('kv/range', {})
            self.assertEqual(len(requests), 2)
            registry.close()

    def test_external_endpoint_or_embedded_credentials_are_rejected(self):
        for endpoint in ('https://example.com', 'http://user:secret@etcd.example.svc.cluster.local:2379',
                         'http://etcd.example.svc.cluster.local:2379/other',
                         'http://etcd.example.svc.cluster.local:bad'):
            with self.assertRaisesRegex(ValueError, '^invalid_discovery_configuration$'):
                Config(endpoint, 'scope', 'user', 'secret')
        config = Config('http://etcd.example.svc.cluster.local:2379', 'scope', 'private-user', 'private-secret')
        self.assertNotIn('private', repr(config))


class ServerOwnerTest(unittest.IsolatedAsyncioTestCase):
    async def test_listener_failure_drains_peer_without_cancelling_its_worker(self):
        from fal_gateway.discovery_lifecycle import serve_owned_servers
        released, draining = asyncio.Event(), asyncio.Event()
        class Peer:
            started = True
            should_exit = False
            async def serve(self):
                await released.wait()
        class Failed:
            started = False
            should_exit = False
            async def serve(self):
                raise RuntimeError('synthetic_failure')
        peer = Peer()
        task = asyncio.create_task(serve_owned_servers([peer, Failed()], draining.set))
        await draining.wait()
        self.assertTrue(peer.should_exit)
        self.assertFalse(task.done())
        released.set()
        with self.assertRaisesRegex(RuntimeError, 'channel_listener_failed'):
            await task

    async def test_owner_cancellation_waits_for_original_worker_and_then_propagates(self):
        from fal_gateway.discovery_lifecycle import serve_owned_servers
        started, released, draining = asyncio.Event(), asyncio.Event(), asyncio.Event()
        class Peer:
            started = True
            should_exit = False
            async def serve(self):
                started.set()
                await released.wait()
        peer = Peer()
        task = asyncio.create_task(serve_owned_servers([peer], draining.set))
        await started.wait()
        task.cancel()
        await draining.wait()
        task.cancel()
        await asyncio.sleep(0)
        self.assertTrue(peer.should_exit)
        self.assertFalse(task.done())
        released.set()
        with self.assertRaises(asyncio.CancelledError):
            await task


if __name__ == '__main__':
    unittest.main()
