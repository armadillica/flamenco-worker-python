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


class EncodeAudioTest(AbstractCommandTest):
    settings: typing.Dict[str, typing.Any] = {
        'ffmpeg_cmd': f'"{sys.executable}" -hide_banner',
        'input_file': f'{frame_dir}/audio.flac',
        'codec': 'aac',
        'bitrate': '192k',
        'output_file': f'{frame_dir}/audio.aac',
    }

    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import EncodeAudioCommand

        self.cmd = EncodeAudioCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )
        self.settings = self.__class__.settings.copy()

    def test_build_ffmpeg_cmd(self):
        self.cmd.validate(self.settings)
        cliargs = self.cmd._build_ffmpeg_command(self.settings)

        self.assertEqual([
            sys.executable, '-hide_banner',
            '-i', str(frame_dir / 'audio.flac'),
            '-c:a', 'aac',
            '-b:a', '192k',
            '-y',
            str(frame_dir / 'audio.aac'),
        ], cliargs)
