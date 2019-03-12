import logging
import shutil
import typing
from pathlib import Path
import platform
import shlex
import subprocess
import sys
import tempfile
from unittest import mock

from tests.test_runner import AbstractCommandTest

log = logging.getLogger(__name__)


class CreateVideoTest(AbstractCommandTest):
    settings: typing.Dict[str, typing.Any] = {
        'ffmpeg_cmd': f'{Path(sys.executable).absolute().as_posix()!r} -hide_banner',
        'input_files': '/tmp/*.png',
        'output_file': '/tmp/merged.mkv',
        'fps': 24,
    }

    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import CreateVideoCommand

        self.cmd = CreateVideoCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )
        self.settings = self.settings.copy()

    def test_validate(self):
        self.assertIn('not found on $PATH', self.cmd.validate({'ffmpeg_cmd': '/does/not/exist'}))
        self.assertIsNone(self.cmd.validate(self.settings))

    def test_validate_without_ffmpeg(self):
        settings = self.settings.copy()
        del settings['ffmpeg_cmd']

        self.assertIsNone(self.cmd.validate(settings))
        self.assertEqual(['ffmpeg'], settings['ffmpeg_cmd'],
                         'The default setting should be stored in the dict after validation')

    def test_build_ffmpeg_cmd_windows(self):
        self.cmd.validate(self.settings)

        with mock.patch('platform.system') as mock_system:
            mock_system.return_value = 'Windows'
            cliargs = self.cmd._build_ffmpeg_command(self.settings)

        self.assertEqual([
            Path(sys.executable).absolute().as_posix(), '-hide_banner',
            '-r', '24',
            '-f', 'concat',
            '-i', Path(self.settings['input_files']).absolute().with_name('ffmpeg-input.txt').as_posix(),
            '-c:v', 'h264',
            '-crf', '23',
            '-g', '18',
            '-vf', 'pad=ceil(iw/2)*2:ceil(ih/2)*2',
            '-pix_fmt', 'yuv420p',
            '-y',
            '-bf', '0',
            '/tmp/merged.mkv',
        ], cliargs)

    def test_build_ffmpeg_cmd_linux(self):
        self.cmd.validate(self.settings)

        with mock.patch('platform.system') as mock_system:
            mock_system.return_value = 'Linux'
            cliargs = self.cmd._build_ffmpeg_command(self.settings)

        self.assertEqual([
            Path(sys.executable).absolute().as_posix(), '-hide_banner',
            '-r', '24',
            '-pattern_type', 'glob',
            '-i', '/tmp/*.png',
            '-c:v', 'h264',
            '-crf', '23',
            '-g', '18',
            '-vf', 'pad=ceil(iw/2)*2:ceil(ih/2)*2',
            '-pix_fmt', 'yuv420p',
            '-y',
            '-bf', '0',
            '/tmp/merged.mkv',
        ], cliargs)

    def test_run_ffmpeg(self):
        with tempfile.TemporaryDirectory() as tempdir:
            outfile = Path(tempdir) / 'merged.mkv'
            frame_dir = Path(__file__).with_name('test_frames')
            settings: typing.Dict[str, typing.Any] = {
                **self.settings,
                'ffmpeg_cmd': 'ffmpeg',  # use the real FFmpeg for this test.
                'input_files': f'{frame_dir.as_posix()}/*.png',
                'output_file': outfile.as_posix(),
            }

            self.loop.run_until_complete(self.cmd.run(settings))
            self.assertTrue(outfile.exists())

            ffprobe_cmd = [shutil.which('ffprobe'), '-v', 'error',
                           '-show_entries', 'format=duration',
                           '-of', 'default=noprint_wrappers=1:nokey=1',
                           outfile.as_posix()]
            log.debug('Running %s', ' '.join(shlex.quote(arg) for arg in ffprobe_cmd))
            probe_out = subprocess.check_output(ffprobe_cmd)
            probed_duration = float(probe_out)
            fps: int = settings['fps']
            expect_duration = len(list(frame_dir.glob('*.png'))) / fps
            self.assertAlmostEqual(expect_duration, probed_duration, places=3)
