import contextlib
import tempfile
from pathlib import Path
from unittest import mock

from test_worker import AbstractFWorkerTest


# Mock merge_with_home_config() so that it doesn't overwrite actual config.
@mock.patch('flamenco_worker.config.merge_with_home_config', new=lambda *args: None)
@mock.patch('socket.gethostname', new=lambda: 'ws-unittest')
class PretaskWriteCheckTest(AbstractFWorkerTest):
    def test_not_writable_dir(self):
        with self.write_check() as tdir:
            unwritable_dir = tdir / 'unwritable'
            unwritable_dir.mkdir(0o555)
            self.worker.pretask_check_params.pre_task_check_write = (unwritable_dir, )

    def test_not_writable_file(self):
        with self.write_check() as tdir:
            unwritable_dir = tdir / 'unwritable'
            unwritable_dir.mkdir(0o555)
            unwritable_file = unwritable_dir / 'testfile.txt'
            self.worker.pretask_check_params.pre_task_check_write = (unwritable_file, )

    def test_write_file_exists(self):
        def post_run():
            self.assertTrue(existing.exists(), '%s should not have been deleted' % existing)

        with self.write_check(post_run) as tdir:
            existing = tdir / 'unwritable-testfile.txt'
            existing.write_bytes(b'x')
            existing.chmod(0o444)  # only readable
            self.worker.pretask_check_params.pre_task_check_write = (existing, )

    def test_happy_remove_file(self):
        from mock_responses import EmptyResponse, CoroMock

        self.manager.post = CoroMock(return_value=EmptyResponse())

        with tempfile.TemporaryDirectory() as tdir_name:
            tdir = Path(tdir_name)
            testfile = tdir / 'writable-testfile.txt'
            self.worker.pretask_check_params.pre_task_check_write = (testfile, )

            self.worker.schedule_fetch_task()
            self.asyncio_loop.run_until_complete(self.worker.single_iteration_task)

            self.assertFalse(testfile.exists(), '%s should have been deleted' % testfile)

        self.manager.post.assert_called_once_with('/task', loop=mock.ANY)
        self.assertIsNone(self.worker.sleeping_task)

    def test_happy_not_remove_file(self):
        from mock_responses import EmptyResponse, CoroMock

        self.manager.post = CoroMock(return_value=EmptyResponse())

        with tempfile.TemporaryDirectory() as tdir_name:
            tdir = Path(tdir_name)
            testfile = tdir / 'writable-testfile.txt'
            # The file exists before, so it shouldn't be deleted afterwards.
            with testfile.open('wb') as outfile:
                outfile.write(b'x')
            self.worker.pretask_check_params.pre_task_check_write = (testfile, )

            self.worker.schedule_fetch_task()
            self.asyncio_loop.run_until_complete(self.worker.single_iteration_task)

            self.assertTrue(testfile.exists(), '%s should not have been deleted' % testfile)

        self.manager.post.assert_called_once_with('/task', loop=mock.ANY)
        self.assertIsNone(self.worker.sleeping_task)

    @contextlib.contextmanager
    def write_check(self, post_run=None):
        from mock_responses import EmptyResponse, CoroMock

        self.manager.post = CoroMock(return_value=EmptyResponse())

        with tempfile.TemporaryDirectory() as tdir_name:
            tdir = Path(tdir_name)

            yield tdir

            self.worker.schedule_fetch_task()
            self.asyncio_loop.run_until_complete(self.worker.single_iteration_task)

            if post_run is not None:
                post_run()

        self.manager.post.assert_called_once_with('/ack-status-change/error', loop=mock.ANY)
        self.assertFalse(self.worker.sleeping_task.done())


# Mock merge_with_home_config() so that it doesn't overread actual config.
@mock.patch('flamenco_worker.config.merge_with_home_config', new=lambda *args: None)
@mock.patch('socket.gethostname', new=lambda: 'ws-unittest')
class PretaskReadCheckTest(AbstractFWorkerTest):
    def test_not_readable_dir(self):
        def cleanup():
            unreadable_dir.chmod(0o755)

        with self.read_check(cleanup) as tdir:
            unreadable_dir = tdir / 'unreadable'
            unreadable_dir.mkdir(0o000)
            self.worker.pretask_check_params.pre_task_check_read = (unreadable_dir, )

    def test_read_file_exists(self):
        def post_run():
            self.assertTrue(existing.exists(), '%s should not have been deleted' % existing)

        with self.read_check(post_run) as tdir:
            existing = tdir / 'unreadable-testfile.txt'
            existing.write_bytes(b'x')
            existing.chmod(0o222)  # only writable
            self.worker.pretask_check_params.pre_task_check_read = (existing, )

    def test_read_file_not_exists(self):
        with self.read_check() as tdir:
            nonexistant = tdir / 'nonexistant-testfile.txt'
            self.worker.pretask_check_params.pre_task_check_read = (nonexistant, )

    @contextlib.contextmanager
    def read_check(self, post_run=None):
        from mock_responses import EmptyResponse, CoroMock

        self.manager.post = CoroMock(return_value=EmptyResponse())

        with tempfile.TemporaryDirectory() as tdir_name:
            tdir = Path(tdir_name)

            yield tdir

            self.worker.schedule_fetch_task()
            self.asyncio_loop.run_until_complete(self.worker.single_iteration_task)

            if post_run is not None:
                post_run()

        self.manager.post.assert_called_once_with('/ack-status-change/error', loop=mock.ANY)
        self.assertFalse(self.worker.sleeping_task.done())
