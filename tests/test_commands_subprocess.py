import asyncio
import os
from pathlib import Path
import random
import shlex
import sys
import tempfile
import time

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
            pidfile = tmpdir / 'nonexistant.pid'

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
            self.assertTrue(msg, "Empty PID file should be treated as 'alive'")
            self.assertTrue(pidfile.exists(), 'Empty PID file should not have been deleted')

    def test_not_configured(self):
        self.cmd.worker.trunner.subprocess_pid_file = None

        msg = self.cmd.validate({'cmd': 'echo'})
        self.assertFalse(msg)

    def test_race_open_exclusive(self):
        # When there is a race condition such that the exclusive open() of the
        # subprocess PID file fails, the new subprocess should be killed.

        # Use shlex to quote strings like this, so we're sure it's done well.
        args = [sys.executable, '-c', 'import time; time.sleep(1)']
        cmd = ' '.join(shlex.quote(s) for s in args)

        with tempfile.TemporaryDirectory() as tmpdir:
            pidfile = Path(tmpdir) / 'race.pid'
            my_pid = os.getpid()

            # Set up the race condition: at validation time the PID file doesn't exist yet,
            # but at execute time it does.
            self.cmd.worker.trunner.subprocess_pid_file = pidfile
            msg = self.cmd.validate({'cmd': cmd})
            self.assertIsNone(msg)

            # Mock an already-running process by writing our own PID.
            pidfile.write_text(str(my_pid))

            start_time = time.time()
            with self.assertRaises(FileExistsError):
                self.loop.run_until_complete(asyncio.wait_for(
                    self.cmd.execute({'cmd': cmd}),
                    1.3  # no more than 300 ms longer than the actual sleep
                ))
            duration = time.time() - start_time

            # This shouldn't take anywhere near the entire sleep time, as that would
            # mean the command was executed while there was already another one running.
            self.assertLess(duration, 0.8,
                            "Checking the PID file and killing the process should be fast")

            pid = self.cmd.proc.pid
            with self.assertRaises(psutil.NoSuchProcess):
                process = psutil.Process(pid)
                self.fail(f'Process {process} is still running')
