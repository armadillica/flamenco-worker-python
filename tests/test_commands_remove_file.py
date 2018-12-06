from pathlib import Path

from tests.test_runner import AbstractCommandTest


class RemoveFileTest(AbstractCommandTest):
    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import RemoveFileCommand
        import tempfile

        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmppath = Path(self.tmpdir.name)

        self.cmd = RemoveFileCommand(
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

        self.assertFalse(ok)
        self.assertTrue(path.exists())
