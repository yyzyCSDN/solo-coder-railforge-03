"""Cross-sector crew connection (接续) with lodging guarantee.

A single connection order joins four constrained resources:

1. 值乘合法性 / 驾驶时长 -- the crew duty row must stay inside duty, driving and
   span limits once the cross-sector work is appended to the existing duty;
2. 接续点容量 -- the relief point must have a free slot for the connection;
3. 资格 -- the qualification rule effective at the relief time must grant the
   required scope/value;
4. 住宿窗口 -- a concrete lodging window with a free bed must be selected.

This module is deliberately pure: it only evaluates an order against a
consistent resource view and returns the resulting state changes.  Version
("代次") enforcement and atomic publication live in ``connection_ledger`` /
``connection_workflow`` so that a stale generation on *any* resource rejects
the whole order and never leaves a half-written roster.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Mapping

from railforge.core.time import utc
from railforge.crew.duty import DutySegment, legal as duty_legal, totals as duty_totals
from railforge.crew.qualification import active as active_qualification


class ConnectionRejected(RuntimeError):
    """Whole-order rejection; no resource may be mutated."""

    code = "rejected"

    def __init__(self, message: str, details: Mapping[str, object] | None = None):
        super().__init__(message)
        self.details: dict[str, object] = dict(details or {})


class BasisIncomplete(ConnectionRejected):
    code = "basis_incomplete"


class GenerationExpired(ConnectionRejected):
    code = "generation_expired"


class CrewUnknown(ConnectionRejected):
    code = "crew_unknown"


class DutyNotLegal(ConnectionRejected):
    code = "duty_not_legal"


class ReliefCapacityExceeded(ConnectionRejected):
    code = "relief_capacity_exceeded"


class QualificationDenied(ConnectionRejected):
    code = "qualification_denied"


class LodgingUnavailable(ConnectionRejected):
    code = "lodging_unavailable"


@dataclass(frozen=True)
class DutyLimits:
    max_duty_hours: float
    max_drive_hours: float
    max_span_hours: float


@dataclass(frozen=True)
class SectorWork:
    """One sector the crew works as part of the cross-sector relay."""

    sector: str
    start: datetime
    end: datetime
    driving: bool = True


@dataclass(frozen=True)
class ReliefUse:
    """Occupancy of a relief/connection point between sectors."""

    point_id: str
    arrival: datetime
    departure: datetime


@dataclass(frozen=True)
class LodgingWindow:
    """A bookable lodging window at a relief point."""

    lodging_id: str
    point_id: str
    start: datetime
    end: datetime
    beds: int
    cost: float = 0.0
    risk: float = 0.0


@dataclass(frozen=True)
class CrewConnectionOrder:
    order_id: str
    crew_id: str
    work: tuple[SectorWork, ...]
    relief: ReliefUse
    qualification_scope: str
    qualification_value: str
    limits: DutyLimits
    lodging: tuple[LodgingWindow, ...] = ()
    rest_required: bool = True
    rest_minutes: int = 480  # mandatory rest at the lodging, in minutes
    # Observed resource generations; every touched resource must appear here.
    basis: dict[str, int] = field(default_factory=dict)


def crew_key(crew_id: str) -> str:
    return f"crew:{crew_id}"


def relief_key(point_id: str) -> str:
    return f"relief:{point_id}"


def qualification_key(scope: str) -> str:
    return f"qualification:{scope}"


def lodging_key(lodging_id: str) -> str:
    return f"lodging:{lodging_id}"


def required_keys(order: CrewConnectionOrder) -> list[str]:
    """Every resource generation this order relies on."""
    keys = [
        crew_key(order.crew_id),
        relief_key(order.relief.point_id),
        qualification_key(order.qualification_scope),
    ]
    keys.extend(lodging_key(w.lodging_id) for w in order.lodging)
    return keys


def _overlaps(bookings, start: datetime, end: datetime) -> int:
    s, e = utc(start), utc(end)
    return sum(1 for a, b, _ in bookings if max(utc(a), s) < min(utc(b), e))


def _check_duty(order, existing_segments):
    rows = list(existing_segments)
    rows.extend(DutySegment(utc(w.start), utc(w.end), w.driving) for w in order.work)
    try:
        total, drive, span = duty_totals(rows)
        ok = duty_legal(rows, order.limits.max_duty_hours,
                        order.limits.max_drive_hours, order.limits.max_span_hours)
    except ValueError as exc:
        raise DutyNotLegal(f"duty row invalid: {exc}", {"crew_id": order.crew_id})
    if not ok:
        raise DutyNotLegal(
            "cross-sector work exceeds duty/drive/span limits",
            {"crew_id": order.crew_id, "total_hours": total,
             "drive_hours": drive, "span_hours": span,
             "limits": {"duty": order.limits.max_duty_hours,
                        "drive": order.limits.max_drive_hours,
                        "span": order.limits.max_span_hours}},
        )
    return rows, {"total_hours": total, "drive_hours": drive, "span_hours": span}


def _check_relief(order, relief_state):
    arrival, departure = utc(order.relief.arrival), utc(order.relief.departure)
    if departure <= arrival:
        raise ReliefCapacityExceeded(
            "relief interval invalid", {"point_id": order.relief.point_id})
    bookings = relief_state["bookings"]
    capacity = int(relief_state["capacity"])
    if _overlaps(bookings, arrival, departure) >= capacity:
        raise ReliefCapacityExceeded(
            "relief point has no free slot for the connection",
            {"point_id": order.relief.point_id, "capacity": capacity,
             "arrival": arrival.isoformat(), "departure": departure.isoformat()},
        )
    new_bookings = [*bookings, (arrival, departure, order.order_id)]
    return {"capacity": capacity, "bookings": new_bookings}


def _check_qualification(order, rules):
    rule = active_qualification(rules, order.qualification_scope, order.relief.arrival)
    if rule is None or rule.value != order.qualification_value:
        raise QualificationDenied(
            "crew lacks the effective qualification for this connection",
            {"crew_id": order.crew_id, "scope": order.qualification_scope,
             "required": order.qualification_value,
             "active_rule": None if rule is None else rule.rule_id},
        )
    return {"scope": rule.scope, "rule_id": rule.rule_id, "value": rule.value}


def _check_lodging(order, lodging_states):
    if not order.rest_required:
        return None, None
    if order.rest_minutes <= 0:
        raise LodgingUnavailable("rest duration must be positive", {"order_id": order.order_id})
    duration = timedelta(minutes=order.rest_minutes)
    departure = utc(order.relief.departure)
    candidates = []
    for w in sorted(order.lodging, key=lambda x: x.lodging_id):
        if w.point_id != order.relief.point_id:
            continue
        state = lodging_states.get(w.lodging_id)
        if state is None:
            continue
        win = state["window"]
        rest_start = max(utc(win.start), departure)
        rest_end = rest_start + duration
        if rest_end > utc(win.end):
            continue
        if _overlaps(state["bookings"], rest_start, rest_end) >= int(win.beds):
            continue
        candidates.append((win.cost, win.risk, utc(win.start), win.lodging_id,
                           rest_start, rest_end, state))
    if not candidates:
        raise LodgingUnavailable(
            "no lodging window covers the mandatory rest with a free bed",
            {"order_id": order.order_id, "point_id": order.relief.point_id,
             "rest_minutes": order.rest_minutes,
             "windows": [w.lodging_id for w in order.lodging]},
        )
    _, _, _, lodging_id, rest_start, rest_end, state = min(candidates)
    new_state = {"window": state["window"],
                 "bookings": [*state["bookings"], (rest_start, rest_end, order.order_id)]}
    grant = {"lodging_id": lodging_id, "rest_start": rest_start, "rest_end": rest_end}
    return grant, new_state


def evaluate(order: CrewConnectionOrder, *, crew_segments, relief_state,
             qualification_rules, lodging_states):
    """Joint validation of all four resources.

    Returns ``(plan, changes)`` where ``changes`` maps resource key to the full
    new resource value.  Either every change is applicable or an exception is
    raised -- nothing is applied here.
    """
    combined_segments, duty_totals_info = _check_duty(order, crew_segments)
    qualification = _check_qualification(order, qualification_rules)
    new_relief = _check_relief(order, relief_state)
    lodging_grant, new_lodging = _check_lodging(order, lodging_states)

    plan = {
        "order_id": order.order_id,
        "crew_id": order.crew_id,
        "sectors": [w.sector for w in order.work],
        "duty": duty_totals_info,
        "relief_point": order.relief.point_id,
        "relief_interval": [utc(order.relief.arrival), utc(order.relief.departure)],
        "qualification": qualification,
        "lodging": lodging_grant,
    }
    changes = {
        crew_key(order.crew_id): {"segments": combined_segments},
        relief_key(order.relief.point_id): new_relief,
    }
    if new_lodging is not None:
        changes[lodging_key(lodging_grant["lodging_id"])] = new_lodging
    return plan, changes
