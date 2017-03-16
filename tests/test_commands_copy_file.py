from pathlib import Path
import os

from test_runner import AbstractCommandTest


class CopyFileTest(AbstractCommandTest):
    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import CopyFileCommand
        import tempfile

        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmppath = Path(self.tmpdir.name)

        self.cmd = CopyFileCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )

    def tearDown(self):
        super().tearDown()
        self.tmpdir.cleanup()

    def test_validate_settings(self):
        self.assertIn('src', self.cmd.validate({'src': 12, 'dest': '/valid/path'}))
        self.assertIn('src', self.cmd.validate({'src': '', 'dest': '/valid/path'}))
        self.assertIn('dest', self.cmd.validate({'src': '/valid/path', 'dest': 12}))
        self.assertIn('dest', self.cmd.validate({'src': '/valid/path', 'dest': ''}))
        self.assertTrue(self.cmd.validate({}))
        self.assertFalse(self.cmd.validate({'src': '/some/path', 'dest': '/some/path'}))

    def test_nonexistant_source_and_dest(self):
        src = self.tmppath / 'nonexisting'
        dest = self.tmppath / 'dest'
        task = self.cmd.run({'src': str(src), 'dest': str(dest)})
        ok = self.loop.run_until_complete(task)

        self.assertFalse(ok)
        self.assertFalse(src.exists())
        self.assertFalse(dest.exists())

    def test_existing_source__nonexisting_dest(self):
        src = self.tmppath / 'existing'
        src.touch()
        dest = self.tmppath / 'dest'
        task = self.cmd.run({'src': str(src), 'dest': str(dest)})
        ok = self.loop.run_until_complete(task)

        self.assertTrue(ok)
        self.assertTrue(src.exists())
        self.assertTrue(dest.exists())

    def test_nonexisting_source__existing_dest(self):
        src = self.tmppath / 'non-existing'

        dest = self.tmppath / 'dest'
        with open(str(dest), 'w') as outfile:
            outfile.write('dest')

        task = self.cmd.run({'src': str(src), 'dest': str(dest)})
        ok = self.loop.run_until_complete(task)

        self.assertFalse(ok)
        self.assertFalse(src.exists())
        self.assertTrue(dest.exists())

        with open(str(dest), 'r') as infile:
            self.assertEqual('dest', infile.read())

    def test_existing_source_and_dest(self):
        src = self.tmppath / 'existing'
        with open(str(src), 'w') as outfile:
            outfile.write('src')

        dest = self.tmppath / 'dest'
        with open(str(dest), 'w') as outfile:
            outfile.write('dest')

        task = self.cmd.run({'src': str(src), 'dest': str(dest)})
        ok = self.loop.run_until_complete(task)

        self.assertTrue(ok)
        self.assertTrue(src.exists())
        self.assertTrue(dest.exists())

        with open(str(src), 'r') as infile:
            self.assertEqual('src', infile.read())

        with open(str(dest), 'r') as infile:
            self.assertEqual('src', infile.read())

    def test_dest_in_nonexisting_subdir(self):
        src = self.tmppath / 'existing'
        with open(str(src), 'w') as outfile:
            outfile.write('src')

        dest = self.tmppath / 'nonexisting' / 'subdir' / 'dest'
        task = self.cmd.run({'src': str(src), 'dest': str(dest)})
        ok = self.loop.run_until_complete(task)

        self.assertTrue(ok)
        self.assertTrue(src.exists())
        self.assertTrue(dest.exists())

        with open(str(dest), 'r') as infile:
            self.assertEqual('src', infile.read())
