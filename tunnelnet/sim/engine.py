"""Deterministic simulation tick execution.

A tick walks the same control chain an online cycle uses, but every value that
would otherwise be random or wall-clock driven comes from the round's injected
:class:`~tunnelnet.sim.ids.DeterministicSource` and
:class:`~tunnelnet.sim.clock.DeterministicClock`. The engine operates solely on
an isolated clone; it never imports or touches the live runtime.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from tunnelnet.console.audit_log import record_event
from tunnelnet.ns import control_chain

DEFAULT_TICKS = 5
MAX_TICKS = 100

# Bounds of the deterministic per-tick variation. They are part of the engine
# contract: changing them changes round outputs, so they live as constants.
WATT_LOW = 1.0
WATT_HIGH = 10.0
SAMPLE_JITTER = 5.0


@dataclass(frozen=True)
class Tick:
    """One simulated control step and the counters observed after it."""

    index: int
    stage: str
    at: str
    environment_average: float
    drain_high: int
    patrol_missed: int
    quota_overage: float
    fire_alarms: int
    audit_records: int

    def as_dict(self) -> dict[str, object]:
        return {
            "index": self.index,
            "stage": self.stage,
            "at": self.at,
            "environment_average": self.environment_average,
            "drain_high": self.drain_high,
            "patrol_missed": self.patrol_missed,
            "quota_overage": self.quota_overage,
            "fire_alarms": self.fire_alarms,
            "audit_records": self.audit_records,
        }


@dataclass(frozen=True)
class SimulationReport:
    """The full tick sequence plus closing counters."""

    ticks: list[Tick] = field(default_factory=list)
    final_audit_count: int = 0
    last_stage: str = ""

    def as_dict(self) -> dict[str, object]:
        return {
            "ticks": [tick.as_dict() for tick in self.ticks],
            "final_audit_count": self.final_audit_count,
            "last_stage": self.last_stage,
        }


class SimulationEngine:
    """Runs ticks against one isolated runtime clone."""

    def __init__(self, clone_runtime: Any, source: Any, clock: Any) -> None:
        self.rt = clone_runtime
        self.source = source
        self.clock = clock

    def run_tick(self, index: int, cabin_id: str, sensor_id: str) -> Tick:
        """Execute tick ``index`` deterministically and return its projection."""

        chain = control_chain()
        stage = chain.stages[index % chain.count()].value
        at = self.clock.now()

        # Access stage: a deterministic approval id, persisted to the scratch store.
        self.rt.access.approve(cabin_id, "simulation")

        # Environment stage: a deterministic sample with seeded jitter.
        base = float(index + 1)
        jitter = self.source.uniform(-SAMPLE_JITTER, SAMPLE_JITTER)
        self.rt.env.sample(sensor_id, base + jitter, at)

        # Ventilation stage.
        self.rt.ventilation.start()

        # Fire stage: a stable, tick-scoped event key, so de-duplication is
        # deterministic across replay but distinct per tick.
        self.rt.fire.raise_alarm(f"sim:{cabin_id}:{index}")

        # Patrol and energy stages.
        self.rt.patrol.check_in(cabin_id, at)
        watts = self.source.uniform(WATT_LOW, WATT_HIGH)
        self.rt.quota.add_usage(cabin_id, watts)

        # Drain stage plus the audit tail of the chain.
        self.rt.drain.start_drain(cabin_id)
        self.rt.drain.record_cycle(cabin_id, at)
        record_event(self.rt, "simulation_tick", str(index), self.source.new_id, self.clock.now)

        self.clock.advance()
        return Tick(
            index=index,
            stage=stage,
            at=at,
            environment_average=self.rt.env.average(sensor_id),
            drain_high=self.rt.drain.high_count(),
            patrol_missed=len(self.rt.patrol.missed()),
            quota_overage=self.rt.quota.overage(cabin_id),
            fire_alarms=self.rt.fire.alarm_count(),
            audit_records=self.rt.audit.count(),
        )

    def run(self, ticks: int, cabin_id: str, sensor_id: str, start: int = 0) -> list[Tick]:
        """Run ``ticks`` steps beginning at logical tick index ``start``."""

        return [
            self.run_tick(index, cabin_id, sensor_id)
            for index in range(start, start + ticks)
        ]

    def report(self, ticks: list[Tick]) -> SimulationReport:
        last = control_chain().last()
        return SimulationReport(
            ticks=ticks,
            final_audit_count=self.rt.audit.count(),
            last_stage="" if last is None else last.value,
        )


def normalize_ticks(requested: int) -> int:
    """Clamp a requested tick count into the supported range."""

    if requested <= 0:
        return DEFAULT_TICKS
    return min(requested, MAX_TICKS)
