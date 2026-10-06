"""Isolated, deterministic offline simulation ("推演").

A simulation round never touches the live :class:`~tunnelnet.console.runtime.Runtime`.
Every round clones the full online state once, runs against the clone and
persists an immutable record (input snapshot, tick journal and report) that
snapshots, journal entries and exports all reference through one round id.
"""

from .clock import DeterministicClock
from .clone import clone_runtime
from .engine import (
    DEFAULT_TICKS,
    MAX_TICKS,
    SimulationEngine,
    Tick,
)
from .gate import WriteGate
from .ids import DeterministicSource
from .journal import RoundStatus, RoundStore
from .manager import SimulationManager, round_id_for
from .snapshot import capture_snapshot, restore_snapshot

__all__ = [
    "DEFAULT_TICKS",
    "MAX_TICKS",
    "DeterministicClock",
    "DeterministicSource",
    "RoundStatus",
    "RoundStore",
    "SimulationEngine",
    "SimulationManager",
    "Tick",
    "WriteGate",
    "capture_snapshot",
    "clone_runtime",
    "restore_snapshot",
    "round_id_for",
]
