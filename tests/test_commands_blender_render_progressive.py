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

        from flamenco_worker.commands import BlenderRenderProgressiveCommand

        self.cmd = BlenderRenderProgressiveCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )

    def test_cli_args(self):
        """Test that CLI arguments in the blender_cmd setting are handled properly."""
        from tests.mock_responses import CoroMock

        filepath = Path(__file__).parent.as_posix()
        settings = {
            # Point blender_cmd to this file so that we're sure it exists.
            'blender_cmd': f'{self.thisfile!r} --with --cli="args for CLI"',
            'chunk_size': 100,
            'frames': '1..2',
            'format': 'EXR',
            'filepath': filepath,
            'render_output': '/some/path/there.exr',
            'cycles_num_chunks': 400,
            'cycles_chunk_start': 223,
            'cycles_chunk_end': 311,
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
                '--render-output', '/some/path/there.exr',
                '--render-format', 'EXR',
                '--render-frame', '1..2',
                '--',
                '--cycles-resumable-num-chunks', '400',
                '--cycles-resumable-start-chunk', '223',
                '--cycles-resumable-end-chunk', '311',
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
