import logging
import shutil
import typing
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile

from tests.test_runner import AbstractCommandTest

log = logging.getLogger(__name__)
frame_dir = Path(__file__).with_name('test_frames')


class MoveWithCounterTest(AbstractCommandTest):
    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import MoveWithCounterCommand

        self.cmd = MoveWithCounterCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )

        self._tempdir = tempfile.TemporaryDirectory()
        self.temppath = Path(self._tempdir.name)

        self.srcpath = (self.temppath / 'somefile.mkv')
        self.srcpath.touch()

        (self.temppath / '2018_06_12_001-spring.mkv').touch()
        (self.temppath / '2018_06_12_004-spring.mkv').touch()

    def tearDown(self):
        self._tempdir.cleanup()
        super().tearDown()

    def test_numbers_with_holes(self):
        settings = {
            'src': str(self.srcpath),
            'dest': str(self.temppath / '2018_06_12-spring.mkv'),
        }
        task = self.cmd.execute(settings)
        self.loop.run_until_complete(task)

        self.assertFalse(self.srcpath.exists())
        self.assertTrue((self.temppath / '2018_06_12_005-spring.mkv').exists())

    def test_no_regexp_match(self):
        settings = {
            'src': str(self.srcpath),
            'dest': str(self.temppath / 'jemoeder.mkv'),
        }
        task = self.cmd.execute(settings)
        self.loop.run_until_complete(task)

        self.assertFalse(self.srcpath.exists())
        self.assertTrue((self.temppath / 'jemoeder_001.mkv').exists())
