"""Unit test for our CoroMock implementation."""

import asyncio
import unittest


class CoroMockTest(unittest.TestCase):
    def setUp(self):
        from flamenco_worker.cli import construct_asyncio_loop
        self.loop = construct_asyncio_loop()

    def test_setting_return_value(self):
        from tests.mock_responses import CoroMock

        cm = CoroMock()
        cm.coro.return_value = '123'

        result = self.loop.run_until_complete(cm(3, 4))

        cm.assert_called_once_with(3, 4)
        self.assertEqual('123', result)

    def test_setting_side_effect(self):
        from tests.mock_responses import CoroMock

        cm = CoroMock()
        cm.coro.side_effect = ['123', '456', IOError('oops')]

        self.assertEqual('123', self.loop.run_until_complete(cm(3, 4)))
        self.assertEqual('456', self.loop.run_until_complete(cm(3, 4)))

        with self.assertRaises(IOError):
            self.loop.run_until_complete(cm(3, 4))

        # A generator is not allowed to raise StopIteration by itself,
        # so the StopIteration caused by side_effect being exhausted
        # results in a RuntimeError.
        with self.assertRaises(RuntimeError):
            self.loop.run_until_complete(cm(3, 4))
