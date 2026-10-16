from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from cryptography.fernet import Fernet
import httpx
from starlette.testclient import TestClient

from fal_gateway.server import Gateway, create_apps
from fal_gateway.store import Store


MANAGE = "m" * 40
EXECUTE = "e" * 40
KEY = "test-key-with-enough-length"
PAYLOAD = {"prompt": "a safe test", "duration": 5, "resolution": "480P",
           "enable_safety_checker": True, "sync_mode": False, "prompt_expansion_mode": "disabled",
           "aspect_ratio": "9:16", "reference_image_urls": ["data:image/png;base64,AAAA"]}


class GatewayTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.encryption_key = Fernet.generate_key().decode()
        self.calls = []

        def fake(request):
            self.calls.append((request.method, str(request.url)))
            if request.url.path.endswith("/by-endpoint"):
                return httpx.Response(200, json={"items": [], "has_more": False, "next_cursor": None})
            if request.method == "POST":
                return httpx.Response(200, json={"request_id": "remote-123"})
            if request.url.path.endswith("/status"):
                return httpx.Response(200, json={"status": "COMPLETED"})
            return httpx.Response(200, json={"video": {"url": "https://files.fal.media/example.mp4"}})

        self.store = Store(str(Path(self.temp.name) / "state.sqlite3"), self.encryption_key)
        self.gateway = Gateway(self.store, httpx.Client(transport=httpx.MockTransport(fake)))
        management, execution = create_apps(self.gateway, MANAGE, EXECUTE)
        self.management = TestClient(management)
        self.execution = TestClient(execution)
        self.admin = {"Authorization": "Bearer " + MANAGE, "X-Organization-Id": "org-one",
                      "X-User-Id": "admin-one"}
        self.runner = {"Authorization": "Bearer " + EXECUTE, "X-Organization-Id": "org-one"}

    def tearDown(self):
        self.management.close()
        self.execution.close()
        self.temp.cleanup()

    def authorize(self):
        response = self.management.put("/v1/authorization", headers=self.admin,
                                       json={"api_key": KEY, "expected_revision": 0})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "active")
        return response.json()["revision"]

    def test_admin_key_is_encrypted_and_never_returned(self):
        self.assertEqual(self.management.get("/v1/overview", headers=self.admin).json()["status"], "unconfigured")
        self.assertEqual(self.management.get("/v1/overview", headers={"Authorization": "Bearer " + MANAGE,
                        "X-Organization-Id": "org-one"}).status_code, 403)
        self.assertEqual(self.management.get("/v1/overview", headers={"Authorization": "Bearer " + EXECUTE,
                        "X-Organization-Id": "org-one", "X-User-Id": "admin-one"}).status_code, 401)
        self.authorize()
        self.assertNotIn(KEY, json.dumps(self.management.get("/v1/authorization", headers=self.admin).json()))
        self.assertNotIn(KEY.encode(), (Path(self.temp.name) / "state.sqlite3").read_bytes())
        other = {**self.admin, "X-Organization-Id": "org-two"}
        self.assertEqual(self.management.get("/v1/authorization", headers=other).json()["status"], "unconfigured")

    def test_bad_rotation_retains_active_version(self):
        revision = self.authorize()
        self.gateway.check = lambda key, *, legacy=False: "invalid"
        response = self.management.put("/v1/authorization", headers=self.admin,
                                       json={"api_key": "different-valid-shape", "expected_revision": revision})
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["active"]["status"], "active")
        self.assertEqual(response.json()["active"]["version"], 1)

    def test_restart_recovers_credential_and_recheck_failure_pauses(self):
        self.authorize()
        reopened = Store(str(Path(self.temp.name) / "state.sqlite3"), self.encryption_key)
        self.assertEqual(reopened.overview("org-one")["status"], "active")
        self.assertEqual(reopened.decrypt("org-one", 1), KEY)
        self.gateway.check = lambda key, *, legacy=False: "invalid"
        result = self.management.post("/v1/authorization/check", headers=self.admin, json={})
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json()["status"], "invalid")
        self.assertFalse(result.json()["enabled"])

    def test_submission_idempotency_and_organization_isolation(self):
        revision = self.authorize()
        enabled = self.management.put("/v1/limits", headers=self.admin,
                                      json={"enabled": True, "daily_limit": 1, "expected_revision": revision})
        self.assertEqual(enabled.status_code, 200)
        submission = {"operation_id": "op-1", "model_id": "minimax/h3/reference-to-video", "payload": PAYLOAD}
        first = self.execution.post("/v1/submissions", headers=self.runner, json=submission)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.json()["state"], "queued")
        again = self.execution.post("/v1/submissions", headers=self.runner, json=submission)
        self.assertEqual(again.json(), first.json())
        self.assertEqual(len([call for call in self.calls if call[0] == "POST"]), 1)
        submission["operation_id"] = "op-2"
        self.assertEqual(self.execution.post("/v1/submissions", headers=self.runner, json=submission).status_code, 409)
        url = "/v1/submissions/" + first.json()["id"]
        other = {**self.runner, "X-Organization-Id": "org-two"}
        self.assertEqual(self.execution.get(url, headers=other).status_code, 404)
        self.assertEqual(self.execution.get(url, headers=self.runner).json()["state"], "completed")
        self.assertEqual(self.execution.get(url + "/result", headers=self.runner).json()["state"], "completed")

    def test_revocation_stops_new_submissions(self):
        revision = self.authorize()
        revision = self.management.put("/v1/limits", headers=self.admin,
            json={"enabled": True, "daily_limit": 1, "expected_revision": revision}).json()["revision"]
        self.management.request("DELETE", "/v1/authorization", headers=self.admin,
                                json={"expected_revision": revision})
        result = self.execution.post("/v1/submissions", headers=self.runner,
            json={"operation_id": "after-revoke", "model_id": "minimax/h3/reference-to-video", "payload": PAYLOAD})
        self.assertEqual(result.status_code, 409)
        self.assertFalse(any(call[0] == "POST" for call in self.calls))


if __name__ == "__main__":
    unittest.main()
