from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from cryptography.fernet import Fernet
from starlette.testclient import TestClient
import yaml

from fal_gateway.capabilities import load_metadata
from fal_gateway.server import Gateway, create_apps
from fal_gateway.store import Store


TOKEN = 'e' * 40


class CapabilitiesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'channel.yaml'
        self.data = {'schema_version': 1, 'channel_id': 'fal', 'capabilities': [{
            'capability_id': 'video.h3max.camera_controls', 'type': 'video',
            'model_id': 'minimax/h3-max', 'endpoint_id': 'minimax/h3-max/camera-controls',
            'display_name': 'Camera Controls', 'operation': 'video.generate.camera_controls',
            'parameter_schema': {}, 'execution_state': 'catalog_only',
            'protocol': 'queue', 'output_resolutions': ['480P', '768P', '1080P'],
            'source_url': 'https://fal.ai/models/minimax/h3-max/camera-controls/api'}]}
        self.write()

    def tearDown(self):
        self.temp.cleanup()

    def write(self):
        self.path.write_text(yaml.safe_dump(self.data, allow_unicode=True), encoding='utf-8')

    def test_read_only_directory_requires_video_service_identity(self):
        store = Store(str(Path(self.temp.name) / 'state.sqlite3'), Fernet.generate_key().decode())
        gateway = Gateway(store)
        with mock.patch.dict('os.environ', {'CHANNEL_METADATA_FILE': str(self.path)}):
            management, execution = create_apps(gateway, 'm' * 40, TOKEN)
        with TestClient(management) as admin, TestClient(execution) as runner:
            headers = {'Authorization': 'Bearer ' + TOKEN, 'X-Caller-Id': 'video-mcp'}
            result = runner.get('/v1/capabilities', headers=headers)
            self.assertEqual(result.status_code, 200)
            self.assertEqual(result.json()['capabilities'][0]['endpoint_id'],
                             'minimax/h3-max/camera-controls')
            self.assertEqual(result.json()['capabilities'][0]['execution_state'], 'catalog_only')
            self.assertEqual(runner.get('/v1/capabilities', headers={**headers,
                             'X-Caller-Id': 'another-service'}).status_code, 403)
            self.assertEqual(runner.get('/v1/capabilities', headers={**headers,
                             'Authorization': 'Bearer ' + 'x' * 40}).status_code, 401)
            self.assertEqual(admin.get('/v1/capabilities', headers=headers).status_code, 404)
            submit = runner.post('/v1/calls', headers={**headers, 'X-Organization-Id': 'org-one'},
                json={'capability': 'video', 'model_id': 'minimax/h3-max/camera-controls',
                      'idempotency_key': 'test-one', 'task_id': 'task-one', 'payload': {},
                      'budget_limit': '1'})
            self.assertEqual(submit.status_code, 400)
            self.assertEqual(submit.json()['error'], 'capability_not_supported')
        gateway.close()

    def test_declared_catalog_cannot_claim_execution(self):
        self.data['capabilities'][0]['execution_state'] = 'adapter_connected_unverified'
        self.write()
        with self.assertRaisesRegex(ValueError, 'execution_claim'):
            load_metadata(self.path)

    def test_duplicate_key_and_wrong_source_fail_closed(self):
        self.path.write_text('schema_version: 1\nschema_version: 1\n', encoding='utf-8')
        with self.assertRaises(ValueError):
            load_metadata(self.path)
        self.data['capabilities'][0]['source_url'] = 'https://example.com/attacker'
        self.write()
        with self.assertRaisesRegex(ValueError, 'source'):
            load_metadata(self.path)
