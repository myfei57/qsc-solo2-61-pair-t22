"""Build an isolated runtime clone for one simulation round.

The clone is a whole-state copy: in-memory components are rebuilt through the
snapshot/restore path and the file-backed store is recreated in a scratch
directory from the snapshot records. The clone reads and writes only its own
scratch store, so a round can persist approvals and execution marks without
ever touching the live store.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from typing import Any

from tunnelnet.store import Store

from .clock import DeterministicClock
from .ids import DeterministicSource
from .snapshot import capture_snapshot, restore_snapshot


class RuntimeClone:
    """A cloned runtime plus the scratch resources backing it."""

    def __init__(
        self,
        runtime: Any,
        snapshot: dict[str, Any],
        workdir: str,
        source: DeterministicSource,
        clock: DeterministicClock,
    ) -> None:
        self.runtime = runtime
        self.snapshot = snapshot
        self.workdir = workdir
        self.source = source
        self.clock = clock

    def close(self) -> None:
        shutil.rmtree(self.workdir, ignore_errors=True)


def build_clone(
    snapshot: dict[str, Any],
    seed: int,
    scratch_root: str,
) -> RuntimeClone:
    """Restore ``snapshot`` into a fresh isolated runtime under scratch storage."""

    workdir = tempfile.mkdtemp(prefix="sim-", dir=scratch_root)
    try:
        source = DeterministicSource(seed)
        clock = DeterministicClock()
        store = Store.open(os.path.join(workdir, "store"))
        runtime = restore_snapshot(snapshot, store, source, clock.now)
    except BaseException:
        shutil.rmtree(workdir, ignore_errors=True)
        raise

    return RuntimeClone(runtime, snapshot, workdir, source, clock)


def clone_runtime(live: Any, seed: int, scratch_root: str) -> RuntimeClone:
    """Capture ``live`` and return an equivalent, fully isolated runtime."""

    return build_clone(capture_snapshot(live), seed, scratch_root)
