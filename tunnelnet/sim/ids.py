"""Deterministic identity and randomness for one simulation round.

Nothing inside a round may call :mod:`uuid`, :mod:`random` (the global
generator) or the wall clock. All randomness comes from a private
:class:`random.Random` seeded from the round, and generated identifiers are
derived from that generator. Replaying a round with the same seed therefore
reproduces every id and every random draw in the same order.
"""

from __future__ import annotations

import random


class DeterministicSource:
    """Seeded RNG plus monotonic, hex id generation scoped to one round."""

    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)
        self._counter = 0

    @property
    def rng(self) -> random.Random:
        return self._rng

    def uniform(self, low: float, high: float) -> float:
        return self._rng.uniform(low, high)

    def new_id(self) -> str:
        """Return the next deterministic 32-char hex identifier."""

        self._counter += 1
        # 16 random bytes reproduces the width of uuid4().hex, drawn from the
        # private generator so global random state is never touched.
        return self._rng.getrandbits(128).to_bytes(16, "big").hex()
