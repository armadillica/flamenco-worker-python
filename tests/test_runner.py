import asyncio
import datetime
import logging
import shlex
import sys
from pathlib import Path
from unittest.mock import Mock, call

from tests.abstract_worker_test import AbstractWorkerTest

executable = Path(sys.executable).as_posix()


class AbstractCommandTest(AbstractWorkerTest):
    def setUp(self):
        from tests.mock_responses import CoroMock
        from flamenco_worker.worker import FlamencoWorker
        from flamenco_worker.runner import TaskRunner
        from flamenco_worker.cli import construct_asyncio_loop

        self.loop = construct_asyncio_loop()
        self.fworker = Mock(spec=FlamencoWorker)
        self.fworker.trunner = Mock(spec=TaskRunner)
        self.fworker.trunner.subprocess_pid_file = None
        self.fworker.register_log = CoroMock()
        self.fworker.register_task_update = CoroMock()

        logging.getLogger('flamenco_worker.commands').setLevel(logging.DEBUG)

    def tearDown(self):
        # This is required for subprocesses, otherwise unregistering signal handlers goes wrong.
        self.loop.close()


class SleepCommandTest(AbstractCommandTest):
    def test_sleep(self):
        import time
        from flamenco_worker.commands import SleepCommand

        cmd = SleepCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )

        time_before = time.time()
        ok = self.loop.run_until_complete(asyncio.wait_for(
            cmd.run({'time_in_seconds': 0.5}),
            0.6  # the 'sleep' should be over in not more than 0.1 seconds extra
        ))
        duration = time.time() - time_before
        self.assertGreaterEqual(duration, 0.5)
        self.assertTrue(ok)


class ExecCommandTest(AbstractCommandTest):
    def construct(self):
        from flamenco_worker.commands import ExecCommand
        cmd = ExecCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )
        return cmd

    def test_bad_settings(self):
        cmd = self.construct()

        settings = {'cmd': [1, 2, 3]}

        ok = self.loop.run_until_complete(asyncio.wait_for(
            cmd.run(settings),
            0.6
        ))

        self.assertFalse(ok)
        self.fworker.register_task_update.assert_called_once_with(
            task_status='failed',
            activity='exec.(task_id=12345, command_idx=0): Invalid settings: "cmd" must be a string'
        )

    def test_exec_python(self):
        cmd = self.construct()

        # Use shlex to quote strings like this, so we're sure it's done well.
        args = [executable, '-c', r'print("hello, this is two lines\nYes, really.")']
        settings = {
            'cmd': ' '.join(shlex.quote(s) for s in args)
        }

        ok = self.loop.run_until_complete(asyncio.wait_for(
            cmd.run(settings),
            0.6
        ))
        self.assertTrue(ok)
        pid = cmd.proc.pid

        # Check that both lines have been reported.
        total_time = cmd.timing['total']
        self.fworker.register_log.assert_has_calls([
            call('exec: Starting'),
            call(f'exec: Executing {settings["cmd"]}'),
            call('pid=%d > hello, this is two lines' % pid),
            call('pid=%d > Yes, really.' % pid),  # note the logged line doesn't end in a newline
            call(f'exec: Subprocess {settings["cmd"]}: Process pid={pid} exited with status code 0'),
            call(f'exec: command timing information: {{"total": {total_time}}}'),
            call('exec: Finished'),
        ])

        self.fworker.register_task_update.assert_called_with(
            activity='finished exec',
            command_progress_percentage=100,
            current_command_idx=0
        )

    def test_exec_invalid_utf(self):
        cmd = self.construct()

        # Use shlex to quote strings like this, so we're sure it's done well.
        # Writes an invalid sequence of continuation bytes.
        args = [executable, '-c', r'import sys; sys.stdout.buffer.write(bytes((0x80, 0x80, 0x80)))']
        settings = {
            'cmd': ' '.join(shlex.quote(s) for s in args)
        }

        ok = self.loop.run_until_complete(asyncio.wait_for(
            cmd.run(settings),
            0.6
        ))
        self.assertFalse(ok)

        # Check that the error has been reported.
        pid = cmd.proc.pid
        decode_err = f"exec.(task_id=12345, command_idx=0): Error executing: Command pid={pid} " \
                     f"produced non-UTF8 output, aborting: 'utf-8' codec can't decode byte 0x80 "\
                     f"in position 0: invalid start byte"
        self.fworker.register_log.assert_has_calls([
            call('exec: Starting'),
            call(f'exec: Executing {executable} '
                 f'-c \'import sys; sys.stdout.buffer.write(bytes((0x80, 0x80, 0x80)))\''),
            call(f'exec: TERMinating subprocess pid={pid}'),
            call(decode_err),
        ])

        # The update should NOT contain a new task status -- that is left to the Worker.
        self.fworker.register_task_update.assert_called_with(activity=decode_err)

    def test_exec_python_fails(self):
        cmd = self.construct()

        # Use shlex to quote strings like this, so we're sure it's done well.
        args = [executable, '-c', r'raise SystemExit("FAIL")']
        settings = {
            'cmd': ' '.join(shlex.quote(s) for s in args)
        }

        ok = self.loop.run_until_complete(asyncio.wait_for(
            cmd.run(settings),
            0.6
        ))
        self.assertFalse(ok)
        pid = cmd.proc.pid

        # Check that the execution error has been reported.
        total_time = cmd.timing['total']
        self.fworker.register_log.assert_has_calls([
            call('exec: Starting'),
            call(f'exec: Executing {settings["cmd"]}'),
            call(f'pid={pid} > FAIL'),  # note the logged line doesn't end in a newline
            call(f'exec: Subprocess {settings["cmd"]}: Process pid={pid} exited with status code 1'),
            call(f'exec.(task_id=12345, command_idx=0): Error executing: '
                 f'Command {settings["cmd"]} (pid={pid}) failed with status 1'),
            call(f'exec: command timing information: {{"total": {total_time}}}'),
        ])

        # The update should NOT contain a new task status -- that is left to the Worker.
        self.fworker.register_task_update.assert_called_with(
            activity='exec.(task_id=12345, command_idx=0): Error executing: '
                     'Command %s (pid=%d) failed with status 1' % (settings['cmd'], pid),
        )
