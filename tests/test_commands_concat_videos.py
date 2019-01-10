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


class ConcatVideosTest(AbstractCommandTest):
    settings: typing.Dict[str, typing.Any] = {
        'ffmpeg_cmd': f'{Path(sys.executable).absolute().as_posix()!r} -hide_banner',
        'input_files': str(frame_dir / 'chunk-*.mkv'),
        'output_file': '/tmp/merged.mkv',
    }

    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import ConcatenateVideosCommand

        self.cmd = ConcatenateVideosCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )
        self.settings = self.settings.copy()

    def test_build_ffmpeg_cmd(self):
        self.cmd.validate(self.settings)
        cliargs = self.cmd._build_ffmpeg_command(self.settings)

        self.assertEqual([
            Path(sys.executable).as_posix(), '-hide_banner',
            '-f', 'concat',
            '-i', (frame_dir / 'ffmpeg-input.txt').as_posix(),
            '-c', 'copy',
            '-y',
            '/tmp/merged.mkv',
        ], cliargs)

    def test_run_ffmpeg(self):
        with tempfile.TemporaryDirectory() as tempdir:
            outfile = Path(tempdir) / 'merged.mkv'
            settings: typing.Dict[str, typing.Any] = {
                **self.settings,
                'ffmpeg_cmd': 'ffmpeg',  # use the real FFmpeg for this test.
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

            # The combined videos are 7 frames @ 24 frames per second.
            self.assertAlmostEqual(0.291, probed_duration, places=3)
