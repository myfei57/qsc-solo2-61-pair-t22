"""Whole-runtime state capture and restore.

The snapshot is one canonical, JSON-safe document covering every in-memory
component plus the file-backed store records. It is the single source shared
by three needs:

* cloning the online runtime into an isolated simulation runtime;
* persisting the exact starting state of a round for replay;
* fingerprinting that state so a replay can prove it started identically.

A whole-state copy is used deliberately (rather than copy-on-touch): the
runtime components share references (drain→cabins/pump, fire→vent/suppression/
dedupe, patrol→cabins, env→filter/partitions), and a partial copy could share
a mutable edge with the live runtime or restore an internally inconsistent
graph.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

from tunnelnet.access import AccessService
from tunnelnet.audit import AuditRegistry, MarkSet, Record
from tunnelnet.cabin import (
    Cabin,
    PartitionTable,
    PowerBook,
    Registry as CabinRegistry,
    SuppressionLog,
)
from tunnelnet.cable import Cable, CableRegistry, MovingAverage
from tunnelnet.drain import DrainCycle, DrainService, Pit, PumpUnit
from tunnelnet.env import Environment
from tunnelnet.fire import Detector, FireService
from tunnelnet.ns import Registry as NamespaceRegistry
from tunnelnet.patrol import CheckIn, PatrolService
from tunnelnet.quota import QuotaService
from tunnelnet.store import Store, export_all
from tunnelnet.vent import VentService

from .clock import DeterministicClock
from .ids import DeterministicSource

SNAPSHOT_VERSION = 1


def capture_snapshot(rt: Any) -> dict[str, Any]:
    """Read every component into one canonical JSON-safe document.

    The caller must hold the runtime write gate so no component can mutate
    while the document is assembled.
    """

    access_requests = [request.payload() for request in rt.access.requests()]
    return {
        "version": SNAPSHOT_VERSION,
        "namespaces": [item.as_dict() for item in rt.namespaces.list()],
        "cabins": [dataclasses.asdict(cabin) for cabin in _cabin_objects(rt.cabins)],
        "partitions": {
            "mapping": rt.partitions.mapping(),
            "revision": rt.partitions.revision(),
        },
        "filter": {"size": rt.filter.size(), "window": rt.filter.window()},
        "environment": {
            "threshold": rt.env.threshold(),
            "sensor_partition": dict(rt.env._sensor_partition),
            "readings": [
                [reading.sensor_id, reading.value, reading.at]
                for sensor_id in rt.env.sensors()
                for reading in rt.env.history(sensor_id)
            ],
            "alarms": [[alarm.sensor_id, alarm.value] for alarm in rt.env.alarms()],
        },
        "access_requests": access_requests,
        "store": export_all(rt.store),
        "pump_runs": rt.pump.runs(),
        "drain": {
            "pits": [dataclasses.asdict(pit) for pit in _pit_objects(rt.drain)],
            "cycles": [dataclasses.asdict(cycle) for cycle in rt.drain.cycles()],
            "threshold": rt.drain.threshold(),
            "state": rt.drain.state_name(),
            "runs": rt.drain.run_count(),
            "in_flight": rt.drain.pending_commands(),
        },
        "ventilation_running": rt.ventilation.running(),
        "suppression": [entry.as_dict() for entry in rt.suppression.entries()],
        "dedupe_marks": sorted(rt.dedupe._marks),
        "fire": {
            "zones": rt.fire.zones(),
            "detectors": [dataclasses.asdict(item) for item in rt.fire.detectors()],
            "alarms": rt.fire.alarms(),
        },
        "patrol": {
            "route": [[point.cabin_id, point.order] for point in rt.patrol.route()],
            "checks": [dataclasses.asdict(check) for check in rt.patrol.check_ins()],
            "due": dict(rt.patrol._due),
        },
        "power_book": dict(rt.power_book._baselines),
        "quota_usage": dict(rt.quota._usage),
        "cables": {
            "items": [dataclasses.asdict(cable) for cable in _cable_objects(rt.cables)],
            "history": {key: rt.cables.history(key) for key in sorted(rt.cables._items)},
        },
        "audit": [record.as_dict() for record in rt.audit.records()],
    }


def restore_snapshot(
    snapshot: dict[str, Any],
    store: Store,
    source: DeterministicSource | None = None,
    clock: Callable[[], str] | None = None,
) -> Any:
    """Build a fresh, fully isolated runtime from a captured snapshot.

    All shared references are rewired inside the new graph; nothing mutable is
    shared with the runtime the snapshot came from. ``source`` and ``clock``
    pin identity and time generation for deterministic simulation rounds.
    """

    from tunnelnet.console.runtime import Runtime

    source = source or DeterministicSource(0)
    clock = clock or (lambda: DeterministicClock().now())

    _load_store(store, snapshot["store"])

    rt = Runtime.__new__(Runtime)
    rt.store = store
    rt.namespaces = NamespaceRegistry()
    for item in snapshot["namespaces"]:
        rt.namespaces.add(item["id"], item["name"])

    rt.cabins = CabinRegistry()
    for payload in snapshot["cabins"]:
        rt.cabins.upsert(Cabin(**payload))

    rt.partitions = PartitionTable()
    for partition_id, cabin_id in snapshot["partitions"]["mapping"].items():
        rt.partitions.repartition(partition_id, cabin_id)
    _set_revision(rt.partitions, snapshot["partitions"]["revision"])

    filter_payload = snapshot["filter"]
    rt.filter = MovingAverage(filter_payload["size"])
    for value in filter_payload["window"]:
        rt.filter.filter(value)

    rt.power_book = PowerBook()
    for cabin_id, watts in snapshot["power_book"].items():
        rt.power_book.set_baseline(cabin_id, watts)

    rt.env = Environment(rt.filter, rt.partitions, snapshot["environment"]["threshold"])
    env_payload = snapshot["environment"]
    rt.env._sensor_partition = dict(env_payload["sensor_partition"])
    rt.env._readings = {}
    for sensor_id, value, at in env_payload["readings"]:
        # Install readings directly: sample() would re-run the shared filter and
        # fabricate alarms, neither of which belongs to a faithful restore.
        rt.env._readings.setdefault(sensor_id, []).append(
            _reading(sensor_id, value, at)
        )
    rt.env._alarms = [
        _alarm(sensor_id, value) for sensor_id, value in env_payload["alarms"]
    ]

    rt.access = AccessService(store, rt.cabins, source.new_id, clock)
    rt.access._requests = [
        _access_request(item) for item in snapshot["access_requests"]
    ]

    rt.pump = PumpUnit()
    for _ in range(int(snapshot["pump_runs"])):
        rt.pump.start()

    rt.drain = DrainService(rt.cabins, rt.pump, store, clock)
    drain = snapshot["drain"]
    for payload in drain["pits"]:
        rt.drain.add_pit(Pit(**payload))
    rt.drain._cycles = [DrainCycle(**payload) for payload in drain["cycles"]]
    rt.drain._threshold = float(drain["threshold"])
    rt.drain._state = drain["state"]
    rt.drain._runs = int(drain["runs"])
    for command_key in drain["in_flight"]:
        rt.drain.mark_in_flight(command_key)

    rt.ventilation = VentService()
    if not snapshot["ventilation_running"]:
        rt.ventilation.stop()

    rt.suppression = SuppressionLog()
    for entry in snapshot["suppression"]:
        if entry.get("active", True):
            rt.suppression.activate(entry["zone"])

    rt.dedupe = MarkSet()
    for key in snapshot["dedupe_marks"]:
        rt.dedupe.record(key)

    fire = snapshot["fire"]
    rt.fire = FireService(store, rt.ventilation, rt.suppression, rt.dedupe, tuple(fire["zones"]))
    for payload in fire["detectors"]:
        rt.fire.add_detector(Detector(**payload))
    rt.fire._alarms = list(fire["alarms"])

    rt.patrol = PatrolService(rt.cabins)
    patrol = snapshot["patrol"]
    rt.patrol._route = [_checkpoint(item) for item in patrol["route"]]
    rt.patrol._checks = [CheckIn(**item) for item in patrol["checks"]]
    rt.patrol._due = dict(patrol["due"])

    rt.quota = QuotaService(rt.cabins, rt.power_book)
    rt.quota._usage = {key: float(value) for key, value in snapshot["quota_usage"].items()}

    rt.audit = AuditRegistry()
    for payload in snapshot["audit"]:
        rt.audit.append(Record(**payload))

    rt.cables = CableRegistry()
    cables = snapshot["cables"]
    for payload in cables["items"]:
        rt.cables.add(Cable(**payload))
    for cable_id, values in cables["history"].items():
        rt.cables._history[cable_id] = [float(value) for value in values]

    return rt


def _load_store(store: Store, records: dict[str, dict[str, dict[str, Any]]]) -> None:
    for kind, documents in records.items():
        for record_id, payload in documents.items():
            store.save(kind, record_id, payload)


def _cabin_objects(registry: CabinRegistry) -> list[Cabin]:
    with registry._lock:
        return [registry._items[key] for key in sorted(registry._items)]


def _pit_objects(drain: DrainService) -> list[Pit]:
    with drain._lock:
        return [drain._pits[key] for key in sorted(drain._pits)]


def _cable_objects(registry: CableRegistry):
    with registry._lock:
        return [registry._items[key] for key in sorted(registry._items)]


def _set_revision(table: PartitionTable, revision: int) -> None:
    with table._lock:
        table._revision = int(revision)


def _alarm(payload_pair: list[Any]):
    from tunnelnet.env import Alarm

    return Alarm(sensor_id=payload_pair[0], value=float(payload_pair[1]))


def _reading(sensor_id: str, value: float, at: str):
    from tunnelnet.env import Reading

    return Reading(sensor_id=sensor_id, value=float(value), at=at)


def _access_request(payload: dict[str, str]):
    from tunnelnet.access import AccessRequest

    return AccessRequest(
        id=payload["id"],
        cabin_id=payload["cabin_id"],
        requester=payload["requester"],
        status=payload.get("status", "pending"),
    )


def _checkpoint(payload: list[Any]):
    from tunnelnet.cabin import Checkpoint

    return Checkpoint(cabin_id=payload[0], order=int(payload[1]))
