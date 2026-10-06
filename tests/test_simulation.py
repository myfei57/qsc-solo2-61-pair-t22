"""Tests for isolated, deterministic, replayable simulation rounds."""

from __future__ import annotations

import json
import tempfile
import unittest

from tunnelnet.console.server import Server
from tunnelnet.sim import engine as engine_module
from tunnelnet.store import Store


class SimulationCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.server = Server(Store.open(f"{self._tmp.name}/data"), web_dir="web")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def call(self, method: str, path: str, payload=None, query=None):
        response = self.server.dispatch(method, path, payload, query)
        body = json.loads(response.body.decode("utf-8"))
        return response.status, body

    def test_simulation_does_not_mutate_live_runtime(self) -> None:
        _, quota_before = self.call("GET", "/api/quota/usage")
        _, audit_before = self.call("GET", "/api/audit")

        status, report = self.call("POST", "/api/simulate", {"ticks": 4})
        self.assertEqual(status, 200)
        self.assertEqual(len(report["ticks"]), 4)
        self.assertTrue(report["round_id"])

        _, quota_after = self.call("GET", "/api/quota/usage")
        _, audit_after = self.call("GET", "/api/audit")
        self.assertEqual(quota_after["usage"], quota_before["usage"])
        self.assertEqual(audit_after["count"], audit_before["count"])
        # Persistent side effects land in the scratch store, never online.
        self.assertEqual(self.server.runtime.store.kinds(), [])

    def test_identical_input_is_idempotent_and_returns_same_round(self) -> None:
        _, first = self.call("POST", "/api/simulate", {"ticks": 3})
        _, second = self.call("POST", "/api/simulate", {"ticks": 3})
        self.assertEqual(first["round_id"], second["round_id"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["ticks"], second["ticks"])

    def test_different_input_addresses_a_different_round(self) -> None:
        _, three = self.call("POST", "/api/simulate", {"ticks": 3})
        _, four = self.call("POST", "/api/simulate", {"ticks": 4})
        self.assertNotEqual(three["round_id"], four["round_id"])

    def test_replay_from_stored_snapshot_reproduces_result(self) -> None:
        _, report = self.call("POST", "/api/simulate", {"ticks": 5})
        status, replay = self.call(
            "GET", "/api/simulate/replay", query={"round_id": [report["round_id"]]}
        )
        self.assertEqual(status, 200)
        self.assertTrue(replay["replay_matches"])
        self.assertEqual(replay["report"]["ticks"], report["ticks"])

    def test_fresh_identical_server_produces_identical_round(self) -> None:
        _, first = self.call("POST", "/api/simulate", {"ticks": 3})
        other = Server(Store.open(f"{self._tmp.name}/data2"), web_dir="web")
        response = other.dispatch("POST", "/api/simulate", {"ticks": 3})
        second = json.loads(response.body.decode("utf-8"))
        self.assertEqual(second["round_id"], first["round_id"])
        self.assertEqual(second["ticks"], first["ticks"])

    def test_export_snapshot_journal_and_report_share_one_round(self) -> None:
        _, report = self.call("POST", "/api/simulate", {"ticks": 3})
        round_id = report["round_id"]
        _, bundle = self.call("GET", "/api/simulate/export", query={"round_id": [round_id]})
        self.assertEqual(bundle["round_id"], round_id)
        self.assertEqual(bundle["manifest"]["round_id"], round_id)
        self.assertEqual(len(bundle["journal"]), 3)
        self.assertEqual(
            bundle["manifest"]["snapshot_digest"],
            _digest(bundle["snapshot"]),
        )
        self.assertEqual([line["tick"] for line in bundle["journal"]], report["ticks"])
        self.assertEqual(bundle["report"]["ticks"], report["ticks"])
        # The bundled snapshot is the exact starting state (seed values).
        self.assertEqual(
            bundle["snapshot"]["quota_usage"], {"cabin-1": 1200.0, "cabin-2": 900.0}
        )

    def test_round_listing_and_detail(self) -> None:
        _, report = self.call("POST", "/api/simulate", {"ticks": 2})
        _, listing = self.call("GET", "/api/simulate/rounds")
        self.assertEqual(len(listing["rounds"]), 1)
        entry = listing["rounds"][0]
        self.assertEqual(entry["status"], "completed")
        self.assertEqual(entry["ticks_done"], 2)
        _, detail = self.call(
            "GET", "/api/simulate/round", query={"round_id": [report["round_id"]]}
        )
        self.assertEqual(len(detail["journal"]), 2)
        self.assertIn("report", detail)

    def test_interrupted_round_resumes_without_duplicating_ticks(self) -> None:
        original = engine_module.SimulationEngine.run_tick

        def failing(self, index, cabin_id, sensor_id):  # type: ignore[no-untyped-def]
            if index == 2:
                raise RuntimeError("boom")
            return original(self, index, cabin_id, sensor_id)

        engine_module.SimulationEngine.run_tick = failing
        try:
            status, body = self.call("POST", "/api/simulate", {"ticks": 5})
        finally:
            engine_module.SimulationEngine.run_tick = original
        self.assertEqual(status, 500)
        self.assertIn("boom", body["error"])

        _, listing = self.call("GET", "/api/simulate/rounds")
        failed = next(row for row in listing["rounds"] if row["status"] == "failed")
        self.assertEqual(failed["ticks_done"], 2)
        round_id = failed["round_id"]

        status, resumed = self.call("POST", "/api/simulate/resume", {"round_id": round_id})
        self.assertEqual(status, 200)
        self.assertEqual(resumed["resumed_ticks"], 2)
        self.assertEqual(len(resumed["ticks"]), 5)
        self.assertEqual([tick["index"] for tick in resumed["ticks"]], [0, 1, 2, 3, 4])

        # The recovered round must be bit-for-bit identical to a healthy run.
        healthy_server = Server(Store.open(f"{self._tmp.name}/healthy"), web_dir="web")
        healthy = json.loads(
            healthy_server.dispatch("POST", "/api/simulate", {"ticks": 5}).body.decode("utf-8")
        )
        self.assertEqual(healthy["round_id"], round_id)
        self.assertEqual(healthy["ticks"], resumed["ticks"])

        _, replay = self.call("GET", "/api/simulate/replay", query={"round_id": [round_id]})
        self.assertTrue(replay["replay_matches"])

        # Resuming a sealed round is idempotent and cannot rewrite history.
        _, again = self.call("POST", "/api/simulate/resume", {"round_id": round_id})
        self.assertTrue(again["replayed"])
        self.assertEqual(again["ticks"], resumed["ticks"])

    def test_unknown_round_is_a_404(self) -> None:
        for path in ("/api/simulate/round", "/api/simulate/replay", "/api/simulate/export"):
            status, body = self.call("GET", path, query={"round_id": ["doesnotexist"]})
            self.assertEqual(status, 404)
            self.assertIn("not found", body["error"])
        status, body = self.call("POST", "/api/simulate/resume", {"round_id": "doesnotexist"})
        self.assertEqual(status, 404)
        self.assertIn("not found", body["error"])


def _digest(snapshot: dict) -> str:
    from tunnelnet.sim.manager import fingerprint_snapshot

    return fingerprint_snapshot(snapshot)


if __name__ == "__main__":
    unittest.main()
