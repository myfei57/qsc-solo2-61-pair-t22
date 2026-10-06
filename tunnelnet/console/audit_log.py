"""Audit entries written by console actions."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING

from tunnelnet.audit import Record
from tunnelnet.drain import utc_now

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from .runtime import Runtime


def _new_record_id() -> str:
    return uuid.uuid4().hex


def record_event(
    rt: "Runtime",
    event: str,
    ref: str = "",
    new_id: Callable[[], str] | None = None,
    clock: Callable[[], str] | None = None,
) -> list[Record]:
    """Append an audit event and, when a reference is given, its ``_ref`` twin."""

    identify = new_id or _new_record_id
    stamp = (clock or utc_now)()
    entries = [Record(id=identify(), event=event, at=stamp)]
    if ref:
        entries.append(Record(id=identify(), event=f"{event}_ref", at=stamp))
    for entry in entries:
        rt.audit.append(entry)
    return entries
