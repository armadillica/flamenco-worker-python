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


class MuxAudioTest(AbstractCommandTest):
    settings: typing.Dict[str, typing.Any] = {
        'ffmpeg_cmd': f'"{sys.executable}" -hide_banner',
        'audio_file': str(frame_dir / 'audio.mkv'),
        'video_file': str(frame_dir / 'video.mkv'),
        'output_file': str(frame_dir / 'muxed.mkv'),
    }

    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import MuxAudioCommand

        self.cmd = MuxAudioCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )
        self.settings = self.settings.copy()

    def test_build_ffmpeg_cmd(self):
        self.cmd.validate(self.settings)
        cliargs = self.cmd._build_ffmpeg_command(self.settings)

        self.assertEqual([
            sys.executable, '-hide_banner',
            '-i', str(frame_dir / 'audio.mkv'),
            '-i', str(frame_dir / 'video.mkv'),
            '-c', 'copy',
            '-y',
            str(frame_dir / 'muxed.mkv'),
        ], cliargs)
