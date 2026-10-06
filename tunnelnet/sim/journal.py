"""Append-only, immutable persistence of simulation rounds.

Each round lives in its own directory and is written through staged files
(``*.tmp`` + atomic replace):

``round.json``     manifest, status, input, seed, snapshot fingerprint
``snapshot.json``  the exact starting-state snapshot (shared reference)
``journal.jsonl``  one durable entry per executed tick (recovery log)
``report.json``    the final report, written once on completion

Historical rounds are never rewritten: completed manifests and reports are
opened read-only and ``finish`` refuses to overwrite them. A crashed round is
recovered by replaying the journal before continuing, so partial progress is
resumed rather than duplicated.
"""

from __future__ import annotations

import enum
import json
import os
from typing import Any

from tunnelnet.store.record import encode

MANIFEST = "round.json"
SNAPSHOT = "snapshot.json"
JOURNAL = "journal.jsonl"
REPORT = "report.json"


class RoundStatus(str, enum.Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


def atomic_write(path: str, payload: str) -> None:
    temporary = f"{path}.tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def write_json(path: str, value: Any) -> None:
    atomic_write(path, encode(value))


def read_json(path: str) -> Any:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


class RoundHandle:
    """Files backing one round."""

    def __init__(self, root: str, round_id: str) -> None:
        self.root = root
        self.round_id = round_id
        self.dir = os.path.join(root, round_id)

    @property
    def manifest_path(self) -> str:
        return os.path.join(self.dir, MANIFEST)

    @property
    def snapshot_path(self) -> str:
        return os.path.join(self.dir, SNAPSHOT)

    @property
    def journal_path(self) -> str:
        return os.path.join(self.dir, JOURNAL)

    @property
    def report_path(self) -> str:
        return os.path.join(self.dir, REPORT)

    def exists(self) -> bool:
        return os.path.isdir(self.dir)

    def read_manifest(self) -> dict[str, Any]:
        return read_json(self.manifest_path)

    def read_snapshot(self) -> dict[str, Any]:
        return read_json(self.snapshot_path)

    def read_report(self) -> dict[str, Any]:
        return read_json(self.report_path)

    def read_journal(self) -> list[dict[str, Any]]:
        try:
            with open(self.journal_path, encoding="utf-8") as handle:
                return [json.loads(line) for line in handle if line.strip()]
        except FileNotFoundError:
            return []


class RoundStore:
    """A directory of immutable rounds, keyed by deterministic round id."""

    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)

    def round_ids(self) -> list[str]:
        return sorted(
            name
            for name in os.listdir(self.root)
            if os.path.exists(os.path.join(self.root, name, MANIFEST))
        )

    def get(self, round_id: str) -> RoundHandle | None:
        handle = RoundHandle(self.root, round_id)
        return handle if handle.exists() else None

    def require(self, round_id: str) -> RoundHandle:
        handle = self.get(round_id)
        if handle is None:
            raise KeyError(f"simulation round {round_id} not found")
        return handle

    def create(
        self,
        round_id: str,
        manifest: dict[str, Any],
        snapshot: dict[str, Any],
    ) -> RoundHandle:
        """Create a round directory and durably seed manifest plus snapshot.

        Fails if the id already exists: a deterministic id collision means the
        caller should replay the existing round, not overwrite history.
        """

        handle = RoundHandle(self.root, round_id)
        if handle.exists():
            raise FileExistsError(f"simulation round {round_id} already exists")
        os.makedirs(handle.dir)
        write_json(handle.snapshot_path, snapshot)
        write_json(handle.manifest_path, manifest)
        return handle

    def append_tick(self, handle: RoundHandle, tick: dict[str, Any]) -> None:
        """Durably append one tick before any later step depends on it."""

        line = json.dumps(tick, ensure_ascii=False, sort_keys=True)
        with open(handle.journal_path, "a", encoding="utf-8") as journal:
            journal.write(line + "\n")
            journal.flush()
            os.fsync(journal.fileno())

    def finish(
        self,
        handle: RoundHandle,
        status: RoundStatus,
        report: dict[str, Any] | None = None,
        error: str = "",
    ) -> None:
        """Transition the manifest; a completed round can never be rewritten."""

        manifest = handle.read_manifest()
        if manifest.get("status") == RoundStatus.COMPLETED.value:
            raise FileExistsError(f"round {handle.round_id} is sealed and immutable")
        if report is not None and not os.path.exists(handle.report_path):
            write_json(handle.report_path, report)
        manifest["status"] = status.value
        if error:
            manifest["error"] = error
        write_json(handle.manifest_path, manifest)
