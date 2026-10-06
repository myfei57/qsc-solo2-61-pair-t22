"""Isolated forks of the live runtime for offline simulation.

A fork is a *full copy* of every control component taken at one instant. We
chose a whole-state copy over copying only the components a run happens to
touch:

* memory here is small (a handful of in-memory registries plus a JSON store),
  so copying everything costs little, while a copy-on-read/copy-on-write
  scheme needs a proxy per component and silently aliases anything the proxy
  misses;
* a full copy is taken at one barrier, so the snapshot is internally
  consistent -- every component reflects the same online instant and no two
  components disagree about cabin/sensor identities;
* the forked runtime owns an ephemeral, private store directory, so whatever
  the simulation persists never touches the online store on disk.

Locks are deliberately *not* copied: every copy gets brand new locks, because
an RLock copied with an owner would be unusable by the fork's thread.
"""

from __future__ import annotations

import copy
import os
import shutil
import tempfile
import threading

from tunnelnet.store import Store

from ..runtime import Runtime

_LOCK_TYPES = (type(threading.RLock()), type(threading.Lock()))


def _fresh_lock(lock: object, memo: dict[int, object]) -> object:
    new = type(lock)()
    memo[id(lock)] = new
    return new


def _install_lock_copiers() -> None:
    for lock_type in _LOCK_TYPES:
        copy._deepcopy_dispatch[lock_type] = _fresh_lock  # type: ignore[attr-defined]


_install_lock_copiers()


def fork_runtime(rt: Runtime, work_root: str | None = None) -> Runtime:
    """Return an isolated runtime copied from ``rt`` at one instant.

    The caller is responsible for holding the snapshot barrier (see
    :func:`take_fork`) so online mutations cannot interleave with the copy.
    """

    sandbox = tempfile.mkdtemp(prefix="sim-sandbox-", dir=work_root)
    # The forked store's backing files are copied into the sandbox before any
    # simulation write, giving durable state the same isolation as memory.
    shutil.copytree(rt.store.root, os.path.join(sandbox, "store"), dirs_exist_ok=True)

    forked = copy.deepcopy(rt)
    forked.store = Store.open(os.path.join(sandbox, "store"))
    _rebind_stores(forked)
    forked.sandbox_root = sandbox  # type: ignore[attr-defined]
    return forked


def _rebind_stores(forked: Runtime) -> None:
    """Point components that captured the live Store back at the sandbox store."""

    private = forked.store
    forked.access._store = private  # noqa: SLF001 - rewiring the copied graph
    forked.drain._guard._store = private  # noqa: SLF001
    forked.fire._dampers = private  # noqa: SLF001


def take_fork(rt: Runtime, work_root: str | None = None) -> Runtime:
    """Take a barrier-protected, internally consistent fork of the runtime."""

    with rt.snapshot_barrier:
        return fork_runtime(rt, work_root)


def discard_fork(forked: Runtime) -> None:
    """Remove the sandbox directory of a finished fork."""

    sandbox = getattr(forked, "sandbox_root", None)
    if sandbox and os.path.isdir(sandbox):
        shutil.rmtree(sandbox, ignore_errors=True)
