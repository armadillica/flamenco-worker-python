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


class BlenderRenderTest(AbstractCommandTest):
    settings: typing.Dict[str, typing.Any] = {
        'ffmpeg': sys.executable,
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

    def test_validate(self):
        self.assertIn('not found on $PATH', self.cmd.validate({'ffmpeg': '/does/not/exist'}))
        self.assertIsNone(self.cmd.validate(self.settings))

    def test_validate_without_ffmpeg(self):
        settings = self.settings.copy()
        del settings['ffmpeg']

        self.assertIsNone(self.cmd.validate(settings))
        self.assertEqual('ffmpeg', settings['ffmpeg'],
                         'The default setting should be stored in the dict after validation')

    def test_build_ffmpeg_cmd(self):
        self.assertEqual([
            sys.executable,
            '-pattern_type', 'glob',
            '-i', '/tmp/*.png',
            '-c:v', 'h264',
            '-crf', '17',
            '-g', '1',
            '-r', '24',
            '-bf', '0',
            '/tmp/merged.mkv',
        ], self.cmd._build_ffmpeg_command(self.settings))

    def test_run_ffmpeg(self):
        with tempfile.TemporaryDirectory() as tempdir:
            outfile = Path(tempdir) / 'merged.mkv'
            frame_dir = Path(__file__).with_name('test_frames')
            settings: typing.Dict[str, typing.Any] = {
                **self.settings,
                'ffmpeg': 'ffmpeg',  # use the real FFmpeg for this test.
                'input_files': f'{frame_dir}/*.png',
                'output_file': str(outfile),
            }

            self.loop.run_until_complete(self.cmd.run(settings))
            self.assertTrue(outfile.exists())

            ffprobe_cmd = [shutil.which('ffprobe'), '-v', 'error',
                           '-show_entries', 'format=duration',
                           '-of', 'default=noprint_wrappers=1:nokey=1',
                           str(outfile)]
            log.debug('Running %s', ' '.join(shlex.quote(arg) for arg in ffprobe_cmd))
            probe_out = subprocess.check_output(ffprobe_cmd)
            probed_duration = float(probe_out)
            expect_duration = len(list(frame_dir.glob('*.png'))) / settings['fps']
            self.assertAlmostEqual(expect_duration, probed_duration, places=3)
