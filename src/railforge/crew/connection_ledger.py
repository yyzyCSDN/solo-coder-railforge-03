"""State plane for cross-sector crew connections.

Every resource a connection can touch (crew duty row, relief point,
qualification rule set, lodging window) is a versioned row: its integer
``version`` is the "代次" (generation) the caller read earlier.

A commit is a single compare-and-swap across **all** touched resources.
If any observed generation is stale the whole order is rejected before any
value changes, so a failed order can never leave a half-written roster or a
relief booking without a lodging booking.
"""
from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from typing import Mapping

from railforge.crew.connection import (
    CrewConnectionOrder,
    required_keys,
    evaluate,
    BasisIncomplete,
    GenerationExpired,
    CrewUnknown,
)


@dataclass(frozen=True)
class ResourceView:
    key: str
    value: object
    version: int


@dataclass(frozen=True)
class ConnectionCommit:
    order_id: str
    plan: dict
    changes: dict[str, object]
    from_versions: dict[str, int]
    to_versions: dict[str, int]


class ConnectionLedger:
    def __init__(self):
        self._rows: dict[str, list] = {}  # key -> [value, version]
        self._lock = RLock()

    # ---- basic state -----------------------------------------------------
    def register(self, key: str, value: object) -> int:
        with self._lock:
            row = self._rows.get(key)
            if row is None:
                self._rows[key] = [value, 0]
                return 0
            row[0] = value
            row[1] += 1
            return row[1]

    def read(self, key: str) -> ResourceView:
        with self._lock:
            row = self._rows.get(key)
            if row is None:
                raise KeyError(key)
            return ResourceView(key, row[0], row[1])

    def exists(self, key: str) -> bool:
        with self._lock:
            return key in self._rows

    def versions(self, keys) -> dict[str, int]:
        with self._lock:
            return {k: self._rows[k][1] for k in keys if k in self._rows}

    def snapshot(self) -> dict[str, ResourceView]:
        with self._lock:
            return {k: ResourceView(k, row[0], row[1]) for k, row in self._rows.items()}

    def dump(self) -> dict[str, dict]:
        """Normalized (JSON-like) snapshot used to prove replay equality."""
        from railforge.crew.connection_workflow import _safe
        with self._lock:
            return {k: {"version": row[1], "value": _safe(row[0])}
                    for k, row in sorted(self._rows.items())}

    # ---- atomic commit ---------------------------------------------------
    def commit(self, order: CrewConnectionOrder) -> ConnectionCommit:
        keys = required_keys(order)
        with self._lock:
            missing_basis = [k for k in keys if k not in order.basis]
            if missing_basis:
                raise BasisIncomplete(
                    "order does not carry a generation for every touched resource",
                    {"order_id": order.order_id, "missing": sorted(missing_basis)},
                )
            missing_state = [k for k in keys if k not in self._rows]
            if missing_state:
                from railforge.crew.connection import crew_key
                if crew_key(order.crew_id) in missing_state:
                    raise CrewUnknown("crew is not registered",
                                      {"crew_id": order.crew_id})
                raise BasisIncomplete(
                    "referenced resource is not registered",
                    {"order_id": order.order_id, "missing": sorted(missing_state)},
                )
            stale = {k: (order.basis[k], self._rows[k][1])
                     for k in keys if order.basis[k] != self._rows[k][1]}
            if stale:
                raise GenerationExpired(
                    "resource generation expired; the whole order is rejected",
                    {"order_id": order.order_id,
                     "stale": {k: {"observed": v[0], "current": v[1]}
                               for k, v in sorted(stale.items())}},
                )

            crew_value = self._rows[f"crew:{order.crew_id}"][0]
            relief_value = self._rows[f"relief:{order.relief.point_id}"][0]
            qual_value = self._rows[f"qualification:{order.qualification_scope}"][0]
            lodging_states = {}
            for w in order.lodging:
                lodging_states[w.lodging_id] = self._rows[f"lodging:{w.lodging_id}"][0]

            plan, changes = evaluate(
                order,
                crew_segments=list(crew_value["segments"]),
                relief_state={"capacity": relief_value["capacity"],
                              "bookings": list(relief_value["bookings"])},
                qualification_rules=list(qual_value["rules"]),
                lodging_states={k: {"window": v["window"],
                                    "bookings": list(v["bookings"])}
                                for k, v in lodging_states.items()},
            )

            from_versions = {k: self._rows[k][1] for k in changes}
            for key, value in changes.items():
                self._rows[key][0] = value
                self._rows[key][1] += 1
            to_versions = {k: self._rows[k][1] for k in changes}
            return ConnectionCommit(order.order_id, plan, changes, from_versions, to_versions)

    # ---- replay ----------------------------------------------------------
    def replay_effect(self, order_id: str, changes: Mapping[str, object],
                      from_versions: Mapping[str, int], to_versions: Mapping[str, int]) -> None:
        """Re-apply a committed effect from an event, verifying every jump."""
        with self._lock:
            for key, value in changes.items():
                actual = self._rows[key][1] if key in self._rows else 0
                if actual != from_versions[key]:
                    raise GenerationExpired(
                        "replay hit an unexpected generation",
                        {"order_id": order_id, "key": key,
                         "expected": from_versions[key], "actual": actual},
                    )
                self._rows[key] = [value, to_versions[key]]
