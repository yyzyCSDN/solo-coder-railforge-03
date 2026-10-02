from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Mapping

from railforge.crew.duty import DutySegment, totals as duty_totals

POINT = "point"
CREW = "crew"
LODGING = "lodging"


def resource_id(kind: str, ident: str) -> str:
    return f"{kind}:{ident}"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class DutyLimits:
    max_duty_hours: float
    max_drive_hours: float
    max_span_hours: float


@dataclass(frozen=True)
class CrewMember:
    crew_id: str
    qualifications: frozenset[str]


@dataclass(frozen=True)
class RelayPoint:
    point_id: str
    capacity: int


@dataclass(frozen=True)
class LodgingWindow:
    facility_id: str
    point_id: str
    open_from: datetime
    open_to: datetime
    beds: int


@dataclass(frozen=True)
class PointEta:
    point_id: str
    handover_at: datetime
    drive_hours_to: float


@dataclass(frozen=True)
class RelayOrder:
    order_id: str
    train: str
    inbound_crew_id: str
    relief_crew: CrewMember
    required_quals: frozenset[str]
    duty_rows: tuple[DutySegment, ...]
    rest_hours: float
    handover_minutes: int
    report_lead_minutes: int
    onward_drive_hours: float


@dataclass(frozen=True)
class HoldView:
    resource_id: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class PlannedHold:
    resource_id: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class RelayPlan:
    order_id: str
    train: str
    point_id: str
    facility_id: str | None
    handover_at: datetime
    rest_from: datetime | None
    rest_to: datetime | None
    duty_hours: float
    drive_hours: float
    span_hours: float
    holds: tuple[PlannedHold, ...]
    epochs: tuple[tuple[str, int], ...]


def _overlaps(usage: Iterable[HoldView], resource: str, start: datetime, end: datetime) -> int:
    return sum(
        1 for u in usage
        if u.resource_id == resource and max(_utc(u.start), start) < min(_utc(u.end), end)
    )


def plan(order: RelayOrder, points: list[RelayPoint], etas: list[PointEta],
         facilities: list[LodgingWindow], limits: DutyLimits, usage: Iterable[HoldView],
         epochs: Mapping[str, int]) -> RelayPlan:
    """Jointly select a relay point, relief-crew interval and lodging window.

    Duty legality, driving hours, point capacity, crew qualification and
    availability, and the lodging window must all hold for the same
    candidate; the returned plan is pinned to the supplied resource epochs.
    """
    if order.rest_hours < 0:
        raise ValueError("rest hours")
    if order.handover_minutes <= 0:
        raise ValueError("handover minutes")
    if order.report_lead_minutes < 0:
        raise ValueError("report lead")
    if order.onward_drive_hours <= 0:
        raise ValueError("onward drive")
    duty_totals(order.duty_rows)  # validates the already-worked duty rows
    if not order.required_quals.issubset(order.relief_crew.qualifications):
        raise ValueError("relief crew not qualified")
    point_by_id = {}
    for p in points:
        if p.point_id in point_by_id:
            raise ValueError("duplicate relay point")
        if p.capacity < 1:
            raise ValueError("point capacity")
        point_by_id[p.point_id] = p
    eta_by_point: dict[str, PointEta] = {}
    for e in etas:
        if e.point_id not in point_by_id:
            raise ValueError("eta for unknown point")
        if e.point_id in eta_by_point:
            raise ValueError("duplicate eta")
        if e.drive_hours_to < 0:
            raise ValueError("drive hours")
        eta_by_point[e.point_id] = e
    if not eta_by_point:
        raise ValueError("no relay point eta")
    usage = tuple(usage)
    last_end = max((_utc(r.end) for r in order.duty_rows), default=None)
    crew_res = resource_id(CREW, order.relief_crew.crew_id)
    candidates = []
    for point_id in sorted(eta_by_point):
        eta = eta_by_point[point_id]
        point = point_by_id[point_id]
        handover_at = _utc(eta.handover_at)
        rows = list(order.duty_rows)
        if eta.drive_hours_to > 0:
            seg_start = handover_at - timedelta(hours=eta.drive_hours_to)
            if last_end is not None and seg_start < last_end:
                continue
            rows.append(DutySegment(seg_start, handover_at, True))
        total, drive, span = duty_totals(rows)
        if (total > limits.max_duty_hours or drive > limits.max_drive_hours
                or span > limits.max_span_hours):
            continue
        point_end = handover_at + timedelta(minutes=order.handover_minutes)
        if _overlaps(usage, resource_id(POINT, point_id), handover_at, point_end) >= point.capacity:
            continue
        crew_start = handover_at - timedelta(minutes=order.report_lead_minutes)
        crew_end = handover_at + timedelta(hours=order.onward_drive_hours)
        if _overlaps(usage, crew_res, crew_start, crew_end) >= 1:
            continue
        facility_id = None
        rest_from = rest_to = None
        if order.rest_hours > 0:
            rest_from = handover_at
            rest_to = handover_at + timedelta(hours=order.rest_hours)
            fits = [
                f for f in facilities
                if f.point_id == point_id and f.beds > 0
                and _utc(f.open_from) <= rest_from and rest_to <= _utc(f.open_to)
                and _overlaps(usage, resource_id(LODGING, f.facility_id), rest_from, rest_to) < f.beds
            ]
            if not fits:
                continue
            facility_id = min(fits, key=lambda f: (_utc(f.open_from), f.facility_id)).facility_id
        candidates.append((handover_at, point_id, facility_id, total, drive, span,
                           rest_from, rest_to, crew_start, crew_end, point_end))
    if not candidates:
        raise ValueError("no feasible relay plan")
    (handover_at, point_id, facility_id, total, drive, span,
     rest_from, rest_to, crew_start, crew_end, point_end) = min(candidates, key=lambda c: (c[0], c[1]))
    holds = [
        PlannedHold(resource_id(POINT, point_id), handover_at, point_end),
        PlannedHold(crew_res, crew_start, crew_end),
    ]
    if facility_id is not None:
        holds.append(PlannedHold(resource_id(LODGING, facility_id), rest_from, rest_to))
    return RelayPlan(
        order_id=order.order_id, train=order.train, point_id=point_id, facility_id=facility_id,
        handover_at=handover_at, rest_from=rest_from, rest_to=rest_to,
        duty_hours=total, drive_hours=drive, span_hours=span,
        holds=tuple(holds), epochs=tuple(sorted(epochs.items())),
    )
