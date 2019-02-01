from pathlib import Path
import subprocess
import tempfile
from unittest import mock

from unittest.mock import patch

from tests.test_runner import AbstractCommandTest


class BlenderRenderProgressiveTest(AbstractCommandTest):
    thisfile = Path(__file__).as_posix()

    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import EXRSequenceToJPEGCommand

        self.cmd = EXRSequenceToJPEGCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )

    def test_exr_glob(self):
        from tests.mock_responses import CoroMock

        filepath = Path(__file__).parent.as_posix()
        settings = {
            # Point blender_cmd to this file so that we're sure it exists.
            'blender_cmd': f'{self.thisfile!r} --with --cli="args for CLI"',
            'filepath': filepath,
            'exr_glob': '/some/path/to/files-*.exr',
            'output_pattern': 'preview-######',
        }

        cse = CoroMock(...)
        cse.coro.return_value.wait = CoroMock(return_value=0)
        cse.coro.return_value.pid = 47
        with patch('asyncio.create_subprocess_exec', new=cse) as mock_cse:
            self.loop.run_until_complete(self.cmd.run(settings))

            mock_cse.assert_called_once_with(
                self.thisfile,
                '--with',
                '--cli=args for CLI',
                '--enable-autoexec',
                '-noaudio',
                '--background',
                filepath,
                '--python-exit-code', '32',
                '--python', str(self.cmd.pyscript),
                '--',
                '--exr-glob', '/some/path/to/files-*.exr',
                '--output-pattern', 'preview-######',
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

    def test_exr_directory(self):
        from tests.mock_responses import CoroMock

        filepath = Path(__file__).parent.as_posix()
        settings = {
            # Point blender_cmd to this file so that we're sure it exists.
            'blender_cmd': f'{self.thisfile!r} --with --cli="args for CLI"',
            'filepath': filepath,
            'exr_directory': '/some/path/to/exr',
            'output_pattern': 'preview-######',
        }

        cse = CoroMock(...)
        cse.coro.return_value.wait = CoroMock(return_value=0)
        cse.coro.return_value.pid = 47
        with patch('asyncio.create_subprocess_exec', new=cse) as mock_cse:
            self.loop.run_until_complete(self.cmd.run(settings))

            mock_cse.assert_called_once_with(
                self.thisfile,
                '--with',
                '--cli=args for CLI',
                '--enable-autoexec',
                '-noaudio',
                '--background',
                filepath,
                '--python-exit-code', '32',
                '--python', str(self.cmd.pyscript),
                '--',
                '--exr-glob', '/some/path/to/exr/*.exr',
                '--output-pattern', 'preview-######',
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
