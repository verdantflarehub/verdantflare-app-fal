"""Validate the runtime dashboard artifact consumed by Studio's integrity checks."""
from pathlib import Path
import tempfile
import unittest

from fal_gateway import __version__
from fal_gateway.dashboard import build, DashboardAssets


class DashboardTest(unittest.TestCase):
    def test_runtime_package_is_deterministic_and_rejects_modified_assets(self):
        source = Path(__file__).resolve().parents[1] / 'dashboard'
        with tempfile.TemporaryDirectory() as directory:
            first, second = Path(directory) / 'first', Path(directory) / 'second'
            declaration = build(source, first, __version__)
            self.assertEqual(build(source, second, __version__), declaration)
            package = DashboardAssets(first)
            html, content_type = package.asset('index.html')
            self.assertIn('text/html', content_type)
            self.assertNotIn(b'./dashboard.mjs', html)
            javascript = next(name for name in package.files if name.endswith('.mjs'))
            data, _ = package.asset(javascript)
            self.assertIn(b'sdk.app.request', data)
            (first / javascript).write_bytes(data + b'\n// changed\n')
            with self.assertRaisesRegex(ValueError, 'DASHBOARD_ASSET_MISMATCH'):
                DashboardAssets(first)


if __name__ == '__main__':
    unittest.main()
