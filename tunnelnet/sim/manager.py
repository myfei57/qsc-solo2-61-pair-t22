"""Orchestrates isolated, replayable simulation rounds.

Flow of one request (:meth:`SimulationManager.run`):

1. The online input is normalized and serialized.
2. While holding the runtime write gate, the whole live state is snapshotted
   and fingerprinted. Online mutations are blocked for this instant, so the
   round binds to one consistent point-in-time state and has no read/write race
   with online writes.
3. The round id is ``H(snapshot fingerprint + canonical input)[:16]``. The same
   live state and the same input therefore always address the same round,
   making identical submissions idempotent and making history content-addressable.
4. Execution happens on a whole-state clone (separate components and a scratch
   store directory); the live runtime is never touched again.
5. Each tick is journaled before the next runs; an interrupted round resumes
   from the journal instead of repeating ticks. Completed rounds are sealed.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from typing import Any

from .clone import build_clone
from .engine import DEFAULT_TICKS, SimulationEngine, normalize_ticks
from .journal import RoundStatus, RoundStore
from .snapshot import capture_snapshot

ENGINE_VERSION = 1


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fingerprint_snapshot(snapshot: dict[str, Any]) -> str:
    payload = canonical_json(snapshot)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def round_id_for(snapshot_fingerprint: str, payload: dict[str, Any]) -> str:
    basis = canonical_json({"snapshot": snapshot_fingerprint, "input": payload})
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()[:16]


class SimulationManager:
    """Coordinates the live runtime, immutable round records and clones."""

    def __init__(
        self,
        live_runtime: Any,
        rounds_root: str,
        write_gate: Any,
        scratch_root: str | None = None,
    ) -> None:
        self._live = live_runtime
        self._rounds = RoundStore(rounds_root)
        self._gate = write_gate
        self._scratch = scratch_root or os.path.join(rounds_root, "scratch")
        os.makedirs(self._scratch, exist_ok=True)
        # Serializes the decide-then-create step so two concurrent requests for
        # the same content-addressed round id cannot both try to create it.
        self._registry_lock = threading.RLock()

    @property
    def rounds(self) -> RoundStore:
        return self._rounds

    def list_rounds(self) -> list[dict[str, Any]]:
        summaries = []
        for round_id in self._rounds.round_ids():
            handle = self._rounds.require(round_id)
            manifest = handle.read_manifest()
            summaries.append(
                {
                    "round_id": round_id,
                    "status": manifest.get("status"),
                    "input": manifest.get("input"),
                    "snapshot_digest": manifest.get("snapshot_digest"),
                    "ticks_done": len(handle.read_journal()),
                }
            )
        return summaries

    def get_round(self, round_id: str) -> dict[str, Any]:
        handle = self._rounds.require(round_id)
        manifest = handle.read_manifest()
        journal = handle.read_journal()
        result: dict[str, Any] = {
            "manifest": manifest,
            "journal": journal,
        }
        if os.path.exists(handle.report_path):
            result["report"] = handle.read_report()
        return result

    def export_round(self, round_id: str) -> dict[str, Any]:
        """The single bundle snapshot, journal and exports all reference."""

        handle = self._rounds.require(round_id)
        bundle = {
            "round_id": round_id,
            "manifest": handle.read_manifest(),
            "snapshot": handle.read_snapshot(),
            "journal": handle.read_journal(),
        }
        if os.path.exists(handle.report_path):
            bundle["report"] = handle.read_report()
        return bundle

    def run(self, request: Any) -> dict[str, Any]:
        """Run or resume one round; identical submissions are idempotent."""

        payload = self._normalize_request(request)

        # Point-in-time, race-free capture: block online mutations while the
        # whole live state is read; concurrent simulation snapshots may proceed.
        # No simulation work happens under the gate.
        with self._gate.read():
            snapshot = capture_snapshot(self._live)
        digest = fingerprint_snapshot(snapshot)
        round_id = round_id_for(digest, payload)
        seed = int(digest[:16], 16)

        # Decide-then-create under one lock so concurrent identical submissions
        # converge on the single round record instead of racing its creation.
        # The winner executes; concurrent duplicates wait for it to complete and
        # then return the sealed result.
        with self._registry_lock:
            handle = self._rounds.get(round_id)
            if handle is None:
                manifest = {
                    "round_id": round_id,
                    "engine_version": ENGINE_VERSION,
                    "status": RoundStatus.RUNNING.value,
                    "input": payload,
                    "seed": seed,
                    "snapshot_digest": digest,
                }
                handle = self._rounds.create(round_id, manifest, snapshot)
            else:
                manifest = handle.read_manifest()
                if manifest.get("status") == RoundStatus.COMPLETED.value:
                    return {
                        "round_id": round_id,
                        "replayed": True,
                        "report": handle.read_report(),
                        "journal": handle.read_journal(),
                    }
            return self._execute(handle, manifest, snapshot, seed, payload)

    def replay(self, round_id: str) -> dict[str, Any]:
        """Re-run a recorded round from its stored snapshot and verify equality."""

        handle = self._rounds.require(round_id)
        manifest = handle.read_manifest()
        snapshot = handle.read_snapshot()
        payload = dict(manifest["input"])

        # Replay starts exclusively from the recorded snapshot and seed; the
        # live runtime is never consulted.
        clone = build_clone(snapshot, int(manifest["seed"]), self._scratch)
        try:
            ticks = self._drive(clone, payload, persist=False, start_index=0)
            report = SimulationEngine(clone.runtime, clone.source, clone.clock).report(ticks)
        finally:
            clone.close()

        report_dict = report.as_dict()
        recorded = handle.read_journal()
        match = [tick["tick"] for tick in recorded] == report_dict["ticks"]
        recorded_report = (
            handle.read_report() if os.path.exists(handle.report_path) else None
        )
        if recorded_report is not None:
            match = match and canonical_json(recorded_report) == canonical_json(report_dict)
        return {
            "round_id": round_id,
            "replay_matches": match,
            "report": report_dict,
        }

    def resume(self, round_id: str) -> dict[str, Any]:
        """Recover an interrupted or failed round from its journal."""

        with self._registry_lock:
            handle = self._rounds.require(round_id)
            manifest = handle.read_manifest()
            if manifest.get("status") == RoundStatus.COMPLETED.value:
                return {
                    "round_id": round_id,
                    "replayed": True,
                    "report": handle.read_report(),
                    "journal": handle.read_journal(),
                }
            snapshot = handle.read_snapshot()
            return self._execute(
                handle, manifest, snapshot, int(manifest["seed"]), dict(manifest["input"])
            )

    def _execute(
        self,
        handle: Any,
        manifest: dict[str, Any],
        snapshot: dict[str, Any],
        seed: int,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        journal = handle.read_journal()
        done = len(journal)
        total = int(payload["ticks"])

        clone = build_clone(snapshot, seed, self._scratch)
        try:
            # Recovery: re-run the already journaled ticks against the recorded
            # snapshot to bring the deterministic RNG, virtual clock and state
            # to the exact continuation point, then resume from there.
            engine = SimulationEngine(clone.runtime, clone.source, clone.clock)
            if done:
                engine.run(done, payload["cabin_id"], payload["sensor_id"])
            if done < total:
                self._drive(clone, payload, persist=True, start_index=done, handle=handle)
            entries = handle.read_journal()
            ticks = [entry["tick"] for entry in entries]
            report = engine.report([_tick_from(entry) for entry in entries]).as_dict()
            report["ticks"] = ticks
            self._rounds.finish(handle, RoundStatus.COMPLETED, report)
        except Exception as exc:  # noqa: BLE001 - journal the failure, keep round recoverable
            self._rounds.finish(handle, RoundStatus.FAILED, error=str(exc))
            raise
        finally:
            clone.close()

        return {
            "round_id": handle.round_id,
            "replayed": False,
            "resumed_ticks": done,
            "report": report,
            "journal": entries,
        }

    def _drive(
        self,
        clone: Any,
        payload: dict[str, Any],
        persist: bool,
        start_index: int,
        handle: Any = None,
    ) -> list[Any]:
        engine = SimulationEngine(clone.runtime, clone.source, clone.clock)
        produced = []
        total = int(payload["ticks"])
        for index in range(start_index, total):
            tick = engine.run_tick(index, payload["cabin_id"], payload["sensor_id"])
            produced.append(tick)
            if persist and handle is not None:
                # Durable record before proceeding: a crash after this line is
                # resumable at the next tick; before it, the tick is retried.
                self._rounds.append_tick(
                    handle, {"tick": tick.as_dict(), "sequence": index}
                )
        return produced

    def _normalize_request(self, request: Any) -> dict[str, Any]:
        ticks = request.int_field("ticks", DEFAULT_TICKS)
        ticks = normalize_ticks(ticks)
        return {
            "ticks": ticks,
            "cabin_id": request.str_field("cabin_id", "cabin-1"),
            "sensor_id": request.str_field("sensor_id", "sensor-1"),
            "engine_version": ENGINE_VERSION,
        }


def _tick_from(entry: dict[str, Any]):
    from .engine import Tick

    payload = entry["tick"]
    return Tick(
        index=int(payload["index"]),
        stage=str(payload["stage"]),
        at=str(payload["at"]),
        environment_average=float(payload["environment_average"]),
        drain_high=int(payload["drain_high"]),
        patrol_missed=int(payload["patrol_missed"]),
        quota_overage=float(payload["quota_overage"]),
        fire_alarms=int(payload["fire_alarms"]),
        audit_records=int(payload["audit_records"]),
    )
