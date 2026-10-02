from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from hashlib import sha256
from json import dumps
from threading import RLock
from typing import Iterable, Mapping


class StaleEpoch(RuntimeError):
    """Any pinned resource epoch has moved; the whole commit is rejected."""

    def __init__(self, stale):
        self.stale = tuple(stale)
        detail = ", ".join(f"{rid} expected={exp} actual={act}" for rid, exp, act in self.stale)
        super().__init__(f"stale resource epoch(s): {detail}")


class CapacityExceeded(RuntimeError):
    """A hold would exceed resource capacity; the whole commit is rejected."""


@dataclass(frozen=True)
class Hold:
    resource_id: str
    order_id: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class Resource:
    resource_id: str
    capacity: int
    epoch: int = 0
    holds: tuple[Hold, ...] = ()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class RelayStore:
    """Epoch-versioned relay resources.

    commit() validates every pinned epoch and every capacity check under one
    lock, then applies all holds at once. Any failure writes nothing, so a
    rejected order never leaves half a schedule behind.
    """

    def __init__(self):
        self._resources: dict[str, Resource] = {}
        self._lock = RLock()

    def register(self, resource_id: str, capacity: int) -> int:
        if capacity < 1:
            raise ValueError("capacity")
        with self._lock:
            current = self._resources.get(resource_id)
            if current is None:
                self._resources[resource_id] = Resource(resource_id, capacity)
                return 0
            if current.capacity != capacity:
                raise ValueError("resource capacity changed")
            return current.epoch

    def registered(self, resource_id: str) -> bool:
        with self._lock:
            return resource_id in self._resources

    def epochs(self, resource_ids: Iterable[str] | None = None) -> dict[str, int]:
        with self._lock:
            ids = list(self._resources) if resource_ids is None else list(resource_ids)
            missing = [rid for rid in ids if rid not in self._resources]
            if missing:
                raise KeyError(f"unknown resource(s): {missing}")
            return {rid: self._resources[rid].epoch for rid in ids}

    def holds(self, resource_id: str | None = None) -> tuple[Hold, ...]:
        with self._lock:
            out = []
            for r in self._resources.values():
                if resource_id is None or r.resource_id == resource_id:
                    out.extend(r.holds)
            return tuple(sorted(out, key=lambda h: (h.resource_id, _utc(h.start), h.order_id)))

    def commit(self, expected: Mapping[str, int], holds: Iterable[Hold]) -> dict[str, int]:
        holds = tuple(holds)
        with self._lock:
            stale = []
            for rid, pinned in expected.items():
                current = self._resources.get(rid)
                if current is None or current.epoch != pinned:
                    stale.append((rid, pinned, None if current is None else current.epoch))
            if stale:
                raise StaleEpoch(stale)
            for hold in holds:
                if _utc(hold.end) <= _utc(hold.start):
                    raise ValueError("hold interval")
            touched: dict[str, list[Hold]] = {}
            for hold in holds:
                resource = self._resources.get(hold.resource_id)
                if resource is None:
                    raise KeyError(f"unknown resource {hold.resource_id}")
                accepted = touched.setdefault(hold.resource_id, [])
                overlapping = sum(
                    1 for h in (*resource.holds, *accepted)
                    if max(_utc(h.start), _utc(hold.start)) < min(_utc(h.end), _utc(hold.end))
                )
                if overlapping >= resource.capacity:
                    raise CapacityExceeded(f"resource {hold.resource_id} capacity exceeded")
                accepted.append(hold)
            for rid, accepted in touched.items():
                resource = self._resources[rid]
                self._resources[rid] = replace(resource, epoch=resource.epoch + 1,
                                               holds=resource.holds + tuple(accepted))
            return {rid: self._resources[rid].epoch for rid in touched}

    def snapshot(self) -> dict[str, Resource]:
        with self._lock:
            return dict(self._resources)

    def digest(self) -> str:
        with self._lock:
            canonical = {
                rid: {
                    "capacity": r.capacity,
                    "epoch": r.epoch,
                    "holds": sorted(
                        [h.order_id, _utc(h.start).isoformat(), _utc(h.end).isoformat()]
                        for h in r.holds
                    ),
                }
                for rid, r in sorted(self._resources.items())
            }
        return sha256(dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
