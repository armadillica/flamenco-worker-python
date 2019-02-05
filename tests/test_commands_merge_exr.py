from pathlib import Path

from tests.test_runner import AbstractCommandTest


class MergeProgressiveRendersCommandTest(AbstractCommandTest):
    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import MergeProgressiveRendersCommand
        import tempfile

        self.tmpdir = tempfile.TemporaryDirectory()
        self.mypath = Path(__file__).parent

        self.cmd = MergeProgressiveRendersCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )

    def tearDown(self):
        super().tearDown()
        self.tmpdir.cleanup()

    def test_happy_flow(self):
        output = Path(self.tmpdir.name) / 'merged.exr'

        settings = {
            'blender_cmd': self.find_blender_cmd(),
            'input1': str(self.mypath / 'Corn field-1k.exr'),
            'input2': str(self.mypath / 'Deventer-1k.exr'),
            'weight1': 20,
            'weight2': 100,
            'output': str(output)
        }

        task = self.cmd.run(settings)
        ok = self.loop.run_until_complete(task)
        self.assertTrue(ok)

        # Assuming that if the files exist, the merge was ok.
        self.assertTrue(output.exists())
        self.assertTrue(output.is_file())


class MergeProgressiveRenderSequenceCommandTest(AbstractCommandTest):
    def setUp(self):
        super().setUp()

        from flamenco_worker.commands import MergeProgressiveRenderSequenceCommand
        import tempfile

        self.tmpdir = tempfile.TemporaryDirectory()
        self.mypath = Path(__file__).parent

        self.cmd = MergeProgressiveRenderSequenceCommand(
            worker=self.fworker,
            task_id='12345',
            command_idx=0,
        )

    def tearDown(self):
        super().tearDown()
        self.tmpdir.cleanup()

    def test_happy_flow(self):
        output = Path(self.tmpdir.name) / 'merged-samples-######.exr'

        settings = {
            'blender_cmd': self.find_blender_cmd(),
            'input1': str(self.mypath / 'Corn field-1k.exr'),
            'input2': str(self.mypath / 'Deventer-1k.exr'),
            'weight1': 20,
            'weight2': 100,
            'output': str(output),
            'frame_start': 3,
            'frame_end': 5,
        }

        task = self.cmd.run(settings)
        ok = self.loop.run_until_complete(task)
        self.assertTrue(ok)

        # Assuming that if the files exist, the merge was ok.
        for framenr in range(3, 6):
            framefile = output.with_name(f'merged-samples-{framenr:06}.exr')
            self.assertTrue(framefile.exists(), f'cannot find {framefile}')
            self.assertTrue(framefile.is_file(), f'{framefile} is not a file')
