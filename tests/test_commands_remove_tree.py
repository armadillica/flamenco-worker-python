from pathlib import Path
import os

from .test_runner import AbstractCommandTest


class RemoveTreeTest(AbstractCommandTest):
    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import RemoveTreeCommand
        import tempfile

        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmppath = Path(self.tmpdir.name)

        self.cmd = RemoveTreeCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )

    def tearDown(self):
        super().tearDown()
        self.tmpdir.cleanup()

    def test_validate_settings(self):
        self.assertIn('path', self.cmd.validate({'path': 12}))
        self.assertIn('path', self.cmd.validate({'path': ''}))
        self.assertIn('path', self.cmd.validate({}))
        self.assertFalse(self.cmd.validate({'path': '/some/path'}))

    def test_nonexistant_source(self):
        path = self.tmppath / 'nonexisting'
        task = self.cmd.run({'path': str(path)})
        ok = self.loop.run_until_complete(task)

        self.assertTrue(ok)
        self.assertFalse(path.exists())

    def test_source_file(self):
        path = self.tmppath / 'existing'
        path.touch()
        task = self.cmd.run({'path': str(path)})
        ok = self.loop.run_until_complete(task)

        self.assertTrue(ok)
        self.assertFalse(path.exists())

    def test_soure_dir_with_files(self):
        path = self.tmppath / 'dir'
        path.mkdir()
        (path / 'a.file').touch()
        (path / 'b.file').touch()
        (path / 'c.file').touch()

        task = self.cmd.run({'path': str(path)})
        ok = self.loop.run_until_complete(task)

        self.assertTrue(ok)
        self.assertFalse(path.exists())

    def test_soure_dir_with_files_and_dirs(self):
        path = self.tmppath / 'dir'
        path.mkdir()
        (path / 'subdir-a' / 'subsub-1').mkdir(parents=True)
        (path / 'subdir-a' / 'subsub-2').mkdir()
        (path / 'subdir-a' / 'subsub-3').mkdir()
        (path / 'subdir-b' / 'subsub-1').mkdir(parents=True)
        (path / 'subdir-c' / 'subsub-1').mkdir(parents=True)
        (path / 'subdir-c' / 'subsub-2').mkdir()
        (path / 'a.file').touch()
        (path / 'b.file').touch()
        (path / 'c.file').touch()

        (path / 'subdir-a' / 'subsub-1' / 'a.file').touch()
        (path / 'subdir-a' / 'subsub-1' / 'b.file').touch()
        (path / 'subdir-a' / 'subsub-2' / 'a.file').touch()
        (path / 'subdir-a' / 'subsub-2' / 'b.file').touch()
        (path / 'subdir-a' / 'subsub-3' / 'a.file').touch()
        (path / 'subdir-a' / 'subsub-3' / 'b.file').touch()
        (path / 'subdir-b' / 'subsub-1' / 'a.file').touch()
        (path / 'subdir-b' / 'subsub-1' / 'b.file').touch()
        (path / 'subdir-c' / 'subsub-1' / 'a.file').touch()
        (path / 'subdir-c' / 'subsub-1' / 'b.file').touch()
        (path / 'subdir-c' / 'subsub-2' / 'a.file').touch()
        (path / 'subdir-c' / 'subsub-2' / 'b.file').touch()

        task = self.cmd.run({'path': str(path)})
        ok = self.loop.run_until_complete(task)

        self.assertTrue(ok)
        self.assertFalse(path.exists())
