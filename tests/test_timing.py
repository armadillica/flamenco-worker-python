from collections import OrderedDict
import json
import unittest
from unittest import mock

from flamenco_worker import timing, json_encoder


class TimingTest(unittest.TestCase):
    @mock.patch('time.monotonic')
    def test_record_duration(self, mock_monotonic):
        epoch = 260690.552905407
        mock_monotonic.side_effect = [epoch, epoch + 3.125, epoch + 10, epoch + 11.5]

        t = timing.Timing()
        with t.record_duration('testing'):
            pass
        with t.record_duration('global warming'):
            pass

        self.assertEqual(OrderedDict([
            ('testing', 3.125),
            ('global warming', 1.5),
        ]), t.events)

    @mock.patch('time.monotonic')
    def test_record_duration_exception(self, mock_monotonic):
        epoch = 260690.552905407
        mock_monotonic.side_effect = [epoch, epoch + 3.125]

        t = timing.Timing()

        with self.assertRaises(ValueError):
            with t.record_duration('testing'):
                raise ValueError('just testing here')

        self.assertEqual(OrderedDict([
            ('testing', 3.125),
        ]), t.events)

    @mock.patch('time.monotonic')
    def test_checkpoints(self, mock_monotonic):
        epoch = 260690.552905407
        mock_monotonic.side_effect = [epoch, epoch + 3.125, epoch + 4.75, epoch + 5, epoch + 5.5]

        t = timing.Timing()
        t.checkpoint('starting')
        t.checkpoint('finishing')
        t.checkpoint('')
        t.checkpoint('oh and another thing')
        t.checkpoint('')

        self.assertEqual(OrderedDict([
            ('starting', 3.125),
            ('finishing', 1.625),
            ('oh and another thing', 0.5),
        ]), t.events)

    def test_add_empty(self):
        t1 = timing.Timing()
        t2 = timing.Timing()
        tsum = t1 + t2

        self.assertIsNot(t1, tsum)
        self.assertIsNot(t2, tsum)
        self.assertEqual(OrderedDict(), tsum.events)

    def test_add(self):
        t1 = timing.Timing(OrderedDict([
            ('starting', 3.125),
            ('finishing', 1.625),
        ]))
        t2 = timing.Timing(OrderedDict([
            ('starting', 3),
            ('oh and another thing', 0.5),
        ]))
        t1_events_copy = t1.events.copy()
        t2_events_copy = t2.events.copy()

        tsum = t1 + t2

        self.assertIsNot(t1, tsum)
        self.assertIsNot(t2, tsum)
        self.assertIsNot(t1.events, tsum.events)
        self.assertIsNot(t2.events, tsum.events)
        self.assertEqual(t1.events, t1_events_copy)
        self.assertEqual(t2.events, t2_events_copy)

        self.assertEqual(OrderedDict([
            ('starting', 6.125),
            ('finishing', 1.625),
            ('oh and another thing', 0.5),
        ]), tsum.events)

    def test_iadd(self):
        t1 = timing.Timing(OrderedDict([
            ('starting', 3.125),
            ('finishing', 1.625),
        ]))
        t2 = timing.Timing(OrderedDict([
            ('starting', 3),
            ('oh and another thing', 0.5),
        ]))
        t1_events_before = t1.events
        t2_events_copy = t2.events.copy()
        t1 += t2

        self.assertIsNot(t1.events, t2.events)
        self.assertIs(t1.events, t1_events_before)
        self.assertEqual(t2.events, t2_events_copy)

        self.assertEqual(OrderedDict([
            ('starting', 6.125),
            ('finishing', 1.625),
            ('oh and another thing', 0.5),
        ]), t1.events)

    def test_add_bad_type(self):
        with self.assertRaises(TypeError):
            timing.Timing() + 3
        with self.assertRaises(TypeError):
            3 + timing.Timing()

    def test_clear(self):
        t1 = timing.Timing(OrderedDict([
            ('starting', 3.125),
            ('finishing', 1.625),
        ]))
        t1_events_before = t1.events
        t1.clear()
        self.assertIs(t1_events_before, t1.events)
        self.assertEqual(OrderedDict(), t1.events)

    def test_json_encoding(self):
        t1 = timing.Timing(OrderedDict([
            ('starting', 3.125),
            ('rendering', 41.625),
            ('finishing', 1.625),
        ]))

        as_json = json.dumps(t1, cls=json_encoder.JSONEncoder)
        from_json = json.loads(as_json)

        # For now we loose ordering of the timing info.
        self.assertEqual({"starting": 3.125, "rendering": 41.625, "finishing": 1.625},
                         from_json)
