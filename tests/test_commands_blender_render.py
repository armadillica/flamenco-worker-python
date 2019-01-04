from pathlib import Path
import subprocess
import tempfile
from unittest import mock

from unittest.mock import patch

from tests.test_runner import AbstractCommandTest


class BlenderRenderTest(AbstractCommandTest):
    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import BlenderRenderCommand

        self.cmd = BlenderRenderCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )

    def test_re_time(self):
        line = '| Time:00:04.17 |'
        m = self.cmd.re_time.search(line)

        self.assertEqual(m.groupdict(), {
            'hours': None,
            'minutes': '00',
            'seconds': '04',
            'hunds': '17',
        })

    def test_is_sync_line(self):
        # Cycles
        line = 'Fra:116 Mem:2348.62M (0.00M, Peak 2562.33M) | Time:02:31.54 | Mem:0.00M, ' \
               'Peak:0.00M | 02_005_A.lighting, R-final ' \
               '| Synchronizing object | GEO-frost_particle.007'
        self.assertTrue(self.cmd._is_sync_line(line))

        # Non-cycles (render engine set to Cycles without Cycles support in Blender).
        line = 'Fra:1 Mem:67.05M (0.00M, Peak 98.78M) | Time:00:00.17 | Syncing Suzanne.003'
        self.assertTrue(self.cmd._is_sync_line(line))

    def test_parse_render_line(self):
        line = 'Fra:10 Mem:17.52M (0.00M, Peak 33.47M) | Time:00:04.17 | Remaining:00:00.87 | ' \
               'Mem:1.42M, Peak:1.42M | Scene, RenderLayer | Path Tracing Tile 110/135'
        self.assertEqual(
            self.cmd.parse_render_line(line),
            {'fra': 10,
             'mem': '17.52M',
             'peakmem': '33.47M',
             'time_sec': 4.17,
             'remaining_sec': 0.87,
             'status': 'Path Tracing Tile 110/135',
             }
        )

        line = 'Fra:003 Mem:17.52G (0.00M, Peak 33G) | Time:03:00:04.17 | Remaining:44:00:00.87 | ' \
               'Mem:1.42M, Peak:1.42M | Séance, RenderLëør | Computing cosmic flöw 110/13005'
        self.assertEqual(
            self.cmd.parse_render_line(line),
            {'fra': 3,
             'mem': '17.52G',
             'peakmem': '33G',
             'time_sec': 3 * 3600 + 4.17,
             'remaining_sec': 44 * 3600 + 0.87,
             'status': 'Computing cosmic flöw 110/13005',
             }
        )

    def test_missing_files(self):
        """Missing files should not abort the render."""

        line = 'Warning: Unable to open je moeder'
        self.cmd.proc = mock.Mock()
        self.cmd.proc.pid = 47
        self.loop.run_until_complete(self.cmd.process_line(line))
        self.fworker.register_task_update.assert_called_once_with(activity=line)

        self.fworker.register_task_update.reset_mock()
        line = "Warning: Path 'je moeder' not found"
        self.loop.run_until_complete(self.cmd.process_line(line))
        self.fworker.register_task_update.assert_called_once_with(activity=line)

    def test_cli_args(self):
        """Test that CLI arguments in the blender_cmd setting are handled properly."""
        from tests.mock_responses import CoroMock

        filepath = str(Path(__file__).parent)
        settings = {
            # Point blender_cmd to this file so that we're sure it exists.
            'blender_cmd': '%s --with --cli="args for CLI"' % __file__,
            'chunk_size': 100,
            'frames': '1..2',
            'format': 'JPEG',
            'filepath': filepath,
        }

        cse = CoroMock(...)
        cse.coro.return_value.wait = CoroMock(return_value=0)
        cse.coro.return_value.pid = 47
        with patch('asyncio.create_subprocess_exec', new=cse) as mock_cse:
            self.loop.run_until_complete(self.cmd.run(settings))

            mock_cse.assert_called_once_with(
                __file__,
                '--with',
                '--cli=args for CLI',
                '--enable-autoexec',
                '-noaudio',
                '--background',
                filepath,
                '--render-format', 'JPEG',
                '--render-frame', '1..2',
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

    def test_python_expr(self):
        from tests.mock_responses import CoroMock

        filepath = str(Path(__file__).parent)
        settings = {
            # Point blender_cmd to this file so that we're sure it exists.
            'blender_cmd': '%s --with --cli="args for CLI"' % __file__,
            'python_expr': 'print("yay in \'quotes\'")',
            'chunk_size': 100,
            'frames': '1..2',
            'format': 'JPEG',
            'filepath': filepath,
        }

        cse = CoroMock(...)
        cse.coro.return_value.wait = CoroMock(return_value=0)
        cse.coro.return_value.pid = 47
        with patch('asyncio.create_subprocess_exec', new=cse) as mock_cse:
            self.loop.run_until_complete(self.cmd.run(settings))

            mock_cse.assert_called_once_with(
                __file__,
                '--with',
                '--cli=args for CLI',
                '--enable-autoexec',
                '-noaudio',
                '--background',
                filepath,
                '--python-expr', 'print("yay in \'quotes\'")',
                '--render-format', 'JPEG',
                '--render-frame', '1..2',
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

    def test_cli_args_override_file(self):
        """Test that an override file next to the blend file is recognised."""
        from tests.mock_responses import CoroMock

        with tempfile.TemporaryDirectory() as tempdir:
            temppath = Path(tempdir)

            blendpath = temppath / 'thefile.blend'
            blendpath.touch()
            override = temppath / 'thefile-overrides.py'
            override.touch()

            settings = {
                # Point blender_cmd to this file so that we're sure it exists.
                'blender_cmd': __file__,
                'chunk_size': 100,
                'frames': '1..2',
                'format': 'JPEG',
                'filepath': str(blendpath),
            }

            cse = CoroMock(...)
            cse.coro.return_value.wait = CoroMock(return_value=0)
            cse.coro.return_value.pid = 47
            with patch('asyncio.create_subprocess_exec', new=cse) as mock_cse:
                self.loop.run_until_complete(self.cmd.run(settings))

                mock_cse.assert_called_once_with(
                    __file__,
                    '--enable-autoexec',
                    '-noaudio',
                    '--background',
                    str(blendpath),
                    '--python', str(override),
                    '--render-format', 'JPEG',
                    '--render-frame', '1..2',
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
