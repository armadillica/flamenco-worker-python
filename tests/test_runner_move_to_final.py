import os
from pathlib import Path

from test_runner import AbstractCommandTest


class MoveToFinalTest(AbstractCommandTest):
    def setUp(self):
        super().setUp()

        from flamenco_worker.runner import MoveToFinalCommand
        import tempfile

        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmppath = Path(self.tmpdir.name)

        self.cmd = MoveToFinalCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )
        self.cmd.__attrs_post_init__()

    def tearDown(self):
        super().tearDown()
        del self.tmpdir

    def test_nonexistant_source(self):
        src = self.tmppath / 'nonexistant-dir'
        dest = self.tmppath / 'dest'

        task = self.cmd.run({'src': str(src), 'dest': str(dest)})
        ok = self.loop.run_until_complete(task)

        # Should be fine.
        self.assertTrue(ok)
        self.assertFalse(src.exists())
        self.assertFalse(dest.exists())

    def test_existing_source_and_dest(self):
        src = self.tmppath / 'existing-dir'
        src.mkdir()
        (src / 'src-contents').touch()

        # Make sure that the destination already exists, with some contents.
        dest = self.tmppath / 'dest'
        dest.mkdir()
        (dest / 'dest-contents').touch()
        (dest / 'dest-subdir').mkdir()
        (dest / 'dest-subdir' / 'sub-contents').touch()

        os.utime(str(dest), (1330712280.01, 1330712292.02))  # fixed (atime, mtime) for testing

        # Run the command.
        task = self.cmd.run({'src': str(src), 'dest': str(dest)})
        ok = self.loop.run_until_complete(task)
        self.assertTrue(ok)

        renamed_dest = dest.with_name('dest-2012-03-02_191812')

        # old 'dest' contents should exist at 'renamed_dest'
        self.assertTrue(renamed_dest.exists())
        self.assertTrue((renamed_dest / 'dest-contents').exists())
        self.assertTrue((renamed_dest / 'dest-subdir').exists())
        self.assertTrue((renamed_dest / 'dest-subdir' / 'sub-contents').exists())

        # old 'src' contents should exist at 'dest'
        self.assertTrue(dest.exists())
        self.assertTrue((dest / 'src-contents').exists())

        # old 'src' should no longer exist.
        self.assertFalse(src.exists())

    def test_nonexistant_dest(self):
        src = self.tmppath / 'existing-dir'
        src.mkdir()
        (src / 'src-contents').touch()

        dest = self.tmppath / 'dest'
        self.assertFalse(dest.exists())

        task = self.cmd.run({'src': str(src), 'dest': str(dest)})
        ok = self.loop.run_until_complete(task)
        self.assertTrue(ok)

        # 'dest-{timestamp}' shouldn't exist.
        self.assertFalse(list(self.tmppath.glob('dest-*')))

        # old 'src' contents should exist at 'dest'
        self.assertTrue(dest.exists())
        self.assertTrue((dest / 'src-contents').exists())

        # old 'src' should no longer exist.
        self.assertFalse(src.exists())
