from pathlib import Path
import os

from tests.test_runner import AbstractCommandTest


class CreatePythonFileTest(AbstractCommandTest):
    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import CreatePythonFile
        import tempfile

        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmppath = Path(self.tmpdir.name)

        self.cmd = CreatePythonFile(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )

    def tearDown(self):
        super().tearDown()
        self.tmpdir.cleanup()

    def test_validate_settings(self):
        self.assertIn('filepath', self.cmd.validate({'filepath': 12, 'contents': '# comment'}))
        self.assertIn('filepath', self.cmd.validate({'filepath': '', 'contents': '# comment'}))
        self.assertIn('filepath', self.cmd.validate({'filepath': '/nonpy/path', 'contents': '#'}))
        self.assertIn('filepath', self.cmd.validate({'contents': '#'}))

        self.assertIn('content', self.cmd.validate({'filepath': '/valid/path.py', 'contents': 12}))
        self.assertIn('content', self.cmd.validate({'filepath': '/valid/path.py'}))

        self.assertTrue(self.cmd.validate({}))
        self.assertFalse(self.cmd.validate({'filepath': '/valid/path.py', 'contents': ''}))
        self.assertFalse(self.cmd.validate({'filepath': '/valid/path.py', 'contents': '#'}))
        self.assertFalse(self.cmd.validate({'filepath': '/valid/path.py', 'contents': '##\na=b\n'}))

    def test_nonexistant_path(self):
        filepath = self.tmppath / 'nonexisting-dir' / 'somefile.py'
        task = self.cmd.run({'filepath': str(filepath), 'contents': 'aapje'})
        ok = self.loop.run_until_complete(task)

        self.assertTrue(ok)
        self.assertTrue(filepath.exists())
        self.assertEqual('aapje', filepath.read_text())

    def test_existing_path(self):
        filepath = self.tmppath / 'existing.py'
        filepath.write_text('old content')

        task = self.cmd.run({'filepath': str(filepath), 'contents': 'öpje'})
        ok = self.loop.run_until_complete(task)

        self.assertTrue(ok)
        self.assertTrue(filepath.exists())
        self.assertEqual('öpje', filepath.read_text(encoding='utf8'))
