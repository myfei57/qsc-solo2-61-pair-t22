"""Readers/writer gate that makes a snapshot a consistent point in time.

Online mutating requests take the gate as a *writer*; simulation snapshot
capture takes it as a *reader*. Snapshot capture therefore waits for in-flight
online writes to finish and blocks new ones while the whole live state is read.
GET endpoints never acquire the gate, so read-only console traffic is not
serialized. Multiple simulations may snapshot concurrently.
"""

from __future__ import annotations

import threading
from types import TracebackType


class WriteGate:
    def __init__(self) -> None:
        self._cond = threading.Condition(threading.RLock())
        self._writers = 0
        self._readers = 0

    def write(self) -> "_WriteScope":
        return _WriteScope(self)

    def read(self) -> "_ReadScope":
        return _ReadScope(self)

    def _acquire_write(self) -> None:
        with self._cond:
            while self._writers or self._readers:
                self._cond.wait()
            self._writers += 1

    def _release_write(self) -> None:
        with self._cond:
            self._writers -= 1
            self._cond.notify_all()

    def _acquire_read(self) -> None:
        with self._cond:
            while self._writers:
                self._cond.wait()
            self._readers += 1

    def _release_read(self) -> None:
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()


class _WriteScope:
    def __init__(self, gate: WriteGate) -> None:
        self._gate = gate

    def __enter__(self) -> WriteGate:
        self._gate._acquire_write()
        return self._gate

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._gate._release_write()


class _ReadScope:
    def __init__(self, gate: WriteGate) -> None:
        self._gate = gate

    def __enter__(self) -> WriteGate:
        self._gate._acquire_read()
        return self._gate

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._gate._release_read()
