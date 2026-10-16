import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from contextlib import closing

from cryptography.fernet import Fernet
from fal_gateway.calls import Calls
from fal_gateway.migration import backup_legacy, fingerprint, LEGACY_TABLES, migrate_legacy
from fal_gateway.store import Store


class LegacyBackupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(str(Path(self.temp.name) / 'state.sqlite3'), Fernet.generate_key().decode())
        Calls(self.store)
        self.store.candidate('test-organization', 'test-only-key-value', 0)

    def test_backup_preserves_original_rows_and_is_immutable(self):
        first = backup_legacy(self.store)
        original_bytes = first.read_bytes()
        self.assertEqual(backup_legacy(self.store), first)
        self.assertEqual(first.read_bytes(), original_bytes)
        with self.store.connect() as db, closing(sqlite3.connect(first)) as copy:
            self.assertEqual(fingerprint(db, self.store, LEGACY_TABLES), fingerprint(copy, self.store, LEGACY_TABLES))
        with self.store.connect() as db:
            db.execute('UPDATE organizations SET daily_limit=2,revision=revision+1')
        second = backup_legacy(self.store)
        self.assertNotEqual(first, second)
        self.assertEqual(first.read_bytes(), original_bytes)
        migrate_legacy(self.store)
        self.assertIsNone(backup_legacy(self.store))

    def test_corrupted_existing_backup_is_rejected_without_overwrite(self):
        path = backup_legacy(self.store)
        with closing(sqlite3.connect(path)) as db:
            db.execute('UPDATE organizations SET daily_limit=5')
            db.commit()
        corrupted = path.read_bytes()
        with self.assertRaisesRegex(ValueError, 'legacy_backup_mismatch'):
            backup_legacy(self.store)
        self.assertEqual(path.read_bytes(), corrupted)

    def test_failed_atomic_publication_leaves_source_and_no_partial_backup(self):
        with self.store.connect() as db:
            before = fingerprint(db, self.store, LEGACY_TABLES)
        with patch('fal_gateway.migration.os.link', side_effect=OSError('test-only failure')):
            with self.assertRaises(OSError):
                backup_legacy(self.store)
        self.assertEqual(list((self.store.path.parent / 'backups').iterdir()), [])
        with self.store.connect() as db:
            self.assertEqual(fingerprint(db, self.store, LEGACY_TABLES), before)
            self.assertIsNone(db.execute("SELECT 1 FROM channel_metadata WHERE name='accounts_cutover'").fetchone())

    def test_retry_after_old_version_change_takes_new_snapshot(self):
        first = backup_legacy(self.store)
        migrate_legacy(self.store)
        with self.store.connect() as db:
            db.execute('UPDATE organizations SET daily_limit=3,revision=revision+1')
        second = backup_legacy(self.store)
        self.assertNotEqual(first, second)
        migrate_legacy(self.store)
        self.assertIsNone(backup_legacy(self.store))


if __name__ == '__main__':
    unittest.main()
