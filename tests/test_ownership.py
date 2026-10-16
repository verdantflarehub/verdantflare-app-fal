from pathlib import Path
import tempfile
import unittest

from fal_gateway.ownership import execution_owner


class OwnershipTest(unittest.TestCase):
    def test_second_writer_refused_and_exception_releases_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'state.sqlite3'
            with self.assertRaisesRegex(ValueError, 'stop'):
                with execution_owner(database):
                    with self.assertRaisesRegex(RuntimeError, 'channel_execution_owner_exists'):
                        with execution_owner(database):
                            self.fail('second writer acquired state volume')
                    raise ValueError('stop')
            with execution_owner(database):
                self.assertTrue(Path(str(database) + '.owner.lock').exists())


if __name__ == '__main__':
    unittest.main()
