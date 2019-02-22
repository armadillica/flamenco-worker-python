"""For keeping track of timing information."""

import collections
import contextlib
import dataclasses
import datetime
import time
import typing


@dataclasses.dataclass
class Timing:
    """Registers intervals by name and duration (in seconds)."""

    events: typing.Dict[str, float] = dataclasses.field(
        default_factory=collections.OrderedDict)

    _last_name = ''
    _last_checkpoint = 0.0

    def clear(self) -> None:
        self.events.clear()

    @contextlib.contextmanager
    def record_duration(self, name: str):
        """Records the duration of the context under the given name."""

        start_time = time.monotonic()
        try:
            yield
        finally:
            duration = time.monotonic() - start_time
            assert name not in self.events, \
                f'{name} not expected in {self.events}'
            self.events[name] = duration

    def checkpoint(self, name: str):
        """Registers a checkpoint for the interval with the given name.

        This checkpoint indicates when the interval starts. It is assumed to
        end when the next call to checkpoint() comes in.

        Always end the last interval with a call to checkpoint('').
        """
        now = time.monotonic()

        if self._last_name:
            duration = now - self._last_checkpoint
            assert self._last_name not in self.events, \
                f'{self._last_name} not expected in {self.events}'
            self.events[self._last_name] = duration

        self._last_name = name
        self._last_checkpoint = now

    @property
    def last_name(self) -> str:
        return self._last_name

    def __add__(self, other: 'Timing') -> 'Timing':
        """Returns the merger of 'self' and 'other'.

        Durations of intervals that appear in both Timing instances are
        added together.
        """
        if not isinstance(other, Timing):
            return NotImplemented

        result = Timing(events=self.events.copy())
        result += other
        return result

    def __iadd__(self, other: 'Timing') -> 'Timing':
        """Merges 'other' into 'self'.

        Durations of intervals that appear in both Timing instances are
        added together.
        """
        if not isinstance(other, Timing):
            return NotImplemented

        for name, duration in other.events.items():
            self_duration = self.events.get(name, 0.0)
            self.events[name] = self_duration + duration

        return self

    def __getitem__(self, item: str) -> float:
        return self.events[item]

    def __setitem__(self, key: str, value: float):
        assert isinstance(key, str)
        assert isinstance(value, float)
        self.events[key] = value

    def to_json_compat(self):
        """Returns the timing info as JSON-compatible value.

        For now this dicards the order of the events.
        """
        return dict(self.events)
