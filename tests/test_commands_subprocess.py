import os
from pathlib import Path
import random
import tempfile

import psutil

from .test_runner import AbstractCommandTest


class PIDFileTest(AbstractCommandTest):
    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import ExecCommand

        self.cmd = ExecCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )

    def test_alive(self):
        with tempfile.NamedTemporaryFile(suffix='.pid') as tmpfile:
            pidfile = Path(tmpfile.name)
            my_pid = os.getpid()
            pidfile.write_text(str(my_pid))

            self.cmd.worker.trunner.subprocess_pid_file = pidfile

            msg = self.cmd.validate({'cmd': 'echo'})
            self.assertIn(str(pidfile), msg)
            self.assertIn(str(psutil.Process(my_pid)), msg)

    def test_alive_newlines(self):
        with tempfile.NamedTemporaryFile(suffix='.pid') as tmpfile:
            pidfile = Path(tmpfile.name)
            my_pid = os.getpid()
            pidfile.write_text('\n%s\n' % my_pid)

            self.cmd.worker.trunner.subprocess_pid_file = pidfile

            msg = self.cmd.validate({'cmd': 'echo'})
            self.assertIn(str(pidfile), msg)
            self.assertIn(str(psutil.Process(my_pid)), msg)

    def test_dead(self):
        # Find a PID that doesn't exist.
        for _ in range(1000):
            pid = random.randint(1, 2**16)
            try:
                psutil.Process(pid)
            except psutil.NoSuchProcess:
                break
        else:
            self.fail('Unable to find unused PID')

        with tempfile.TemporaryDirectory(suffix='.pid') as tmpname:
            tmpdir = Path(tmpname)
            pidfile = tmpdir / 'stale.pid'
            pidfile.write_text(str(pid))

            self.cmd.worker.trunner.subprocess_pid_file = pidfile

            msg = self.cmd.validate({'cmd': 'echo'})
            self.assertFalse(msg)
            self.assertFalse(pidfile.exists(), 'Stale PID file should have been deleted')

    def test_nonexistant(self):
        with tempfile.TemporaryDirectory(suffix='.pid') as tmpname:
            tmpdir = Path(tmpname)
            pidfile = tmpdir / 'stale.pid'

            self.cmd.worker.trunner.subprocess_pid_file = pidfile

            msg = self.cmd.validate({'cmd': 'echo'})
            self.assertFalse(msg)

    def test_empty(self):
        with tempfile.TemporaryDirectory(suffix='.pid') as tmpname:
            tmpdir = Path(tmpname)
            pidfile = tmpdir / 'empty.pid'
            pidfile.write_bytes(b'')

            self.cmd.worker.trunner.subprocess_pid_file = pidfile

            msg = self.cmd.validate({'cmd': 'echo'})
            self.assertFalse(msg)

    def test_not_configured(self):
        self.cmd.worker.trunner.subprocess_pid_file = None

        msg = self.cmd.validate({'cmd': 'echo'})
        self.assertFalse(msg)
