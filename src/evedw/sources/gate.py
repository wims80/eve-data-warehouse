"""One request in flight, and request starts spaced, for every client sharing a gate
(design §11a, §11b). A source keeps one gate per process for all its clients."""

import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager


class RequestGate:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_start = 0.0

    @contextmanager
    def turn(
        self, spacing: float, clock: Callable[[], float], sleep: Callable[[float], None]
    ) -> Generator[None]:
        """Hold the gate for one request, starting at least ``spacing`` seconds after the
        previous start."""
        with self._lock:
            now = clock()
            start = max(now, self._next_start)
            if start > now:
                sleep(start - now)
            self._next_start = start + spacing
            yield
