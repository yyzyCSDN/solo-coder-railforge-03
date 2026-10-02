"""Atomic workflow for cross-sector crew connections with lodging guarantee.

Every order is evaluated as one unit against the generation-aware ledger:

* a *committed* order publishes one event containing the full plan and the
  per-resource effect, and appends one audit record;
* a *rejected* order (expired generation, illegal duty, over-capacity relief
  point, missing qualification, no lodging window) publishes a rejection
  event and an audit record **without mutating any resource**.

Committed events fully describe their effects, so a fresh service can
rebuild the ledger by replay and prove it matches the live state.
"""
from __future__ import annotations

from dataclasses import dataclass, field, is_dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Mapping

from railforge.audit.trail import Trail
from railforge.crew.connection import (
    ConnectionRejected,
    CrewConnectionOrder,
    GenerationExpired,
    LodgingWindow,
)
from railforge.crew.connection_ledger import ConnectionLedger
from railforge.crew.duty import DutySegment
from railforge.crew.qualification import QualificationRule
from railforge.events.stream import EventStream

TOPIC = "crew.connection.ordered"
PROVISION_TOPIC = "crew.connection.provisioned"


def _safe(value: Any) -> Any:
    if is_dataclass(value):
        return _safe(asdict(value))
    if isinstance(value, dict):
        return {str(k): _safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_safe(v) for v in value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value


@dataclass(frozen=True)
class ConnectionReceipt:
    operation_id: str
    status: str  # "committed" | "rejected"
    subject: str
    plan: dict | None
    from_versions: dict[str, int] = field(default_factory=dict)
    to_versions: dict[str, int] = field(default_factory=dict)
    event: Any = None
    audit_index: int = 0


def _serialize_order(order: CrewConnectionOrder) -> dict:
    return _safe({"order_id": order.order_id, "crew_id": order.crew_id,
                  "work": order.work, "relief": order.relief,
                  "qualification_scope": order.qualification_scope,
                  "qualification_value": order.qualification_value,
                  "limits": order.limits, "lodging": order.lodging,
                  "rest_required": order.rest_required,
                  "rest_minutes": order.rest_minutes, "basis": order.basis})


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def hydrate_value(key: str, value: Any) -> Any:
    """Reconstruct domain objects from the JSON-like payload of a past event."""
    if value is None:
        return None
    if key.startswith("crew:"):
        return {"segments": [DutySegment(_dt(s["start"]), _dt(s["end"]), s["driving"])
                             for s in value["segments"]]}
    if key.startswith("relief:"):
        return {"capacity": int(value["capacity"]),
                "bookings": [(_dt(a), _dt(b), oid) for a, b, oid in value["bookings"]]}
    if key.startswith("qualification:"):
        return {"rules": [QualificationRule(
            r["rule_id"], r["scope"], _dt(r["effective_from"]),
            _dt(r["effective_to"]) if r.get("effective_to") else None,
            int(r["priority"]), r["value"]) for r in value["rules"]]}
    if key.startswith("lodging:"):
        w = value["window"]
        win = LodgingWindow(w["lodging_id"], w["point_id"], _dt(w["start"]),
                            _dt(w["end"]), int(w["beds"]),
                            float(w["cost"]), float(w["risk"]))
        return {"window": win,
                "bookings": [(_dt(a), _dt(b), oid) for a, b, oid in value["bookings"]]}
    return value


class CrewConnectionService:
    def __init__(self, ledger: ConnectionLedger | None = None,
                 stream: EventStream | None = None, trail: Trail | None = None):
        self.ledger = ledger or ConnectionLedger()
        self.stream = stream or EventStream()
        self.trail = trail or Trail()
        self._receipts: dict[str, ConnectionReceipt] = {}
        self._attempts: dict[str, int] = {}

    def _record(self, operation_id: str, at: datetime, status: str,
                payload_extra: Mapping[str, Any]) -> tuple[Any, int]:
        payload = {"operation_id": operation_id, "status": status,
                   "control_path": "atomic", **dict(payload_extra)}
        event_id = operation_id if status == "committed" else f"{operation_id}:rejected:{self._attempts.get(operation_id, 0)}"
        event = self.stream.publish(event_id, at, TOPIC, operation_id, payload)
        record = self.trail.append("crew-connection", TOPIC, operation_id, at)
        return event, record.index

    def provision(self, operation_id: str, key: str, value: Any,
                  at: datetime | None = None) -> ConnectionReceipt:
        """Register or update a resource (crew / relief / qualification / lodging).

        Updates bump the generation, which invalidates every order that read
        the previous generation.
        """
        at = at or datetime.now(timezone.utc)
        prior = self._receipts.get(operation_id)
        if prior is not None:
            return prior
        version = self.ledger.register(key, value)
        event = self.stream.publish(
            operation_id, at, PROVISION_TOPIC, key,
            {"operation_id": operation_id, "status": "committed",
             "control_path": "atomic", "key": key, "version": version,
             "value": _safe(value)},
        )
        record = self.trail.append("crew-connection", PROVISION_TOPIC, key, at)
        receipt = ConnectionReceipt(operation_id, "committed", key,
                                    {"key": key, "version": version},
                                    {key: version}, {key: version},
                                    event, record.index)
        self._receipts[operation_id] = receipt
        return receipt

    def reserve(self, operation_id: str, order: CrewConnectionOrder,
                at: datetime | None = None) -> ConnectionReceipt:
        """Atomically reserve crew duty, relief slot, qualification and lodging."""
        at = at or datetime.now(timezone.utc)
        prior = self._receipts.get(operation_id)
        if prior is not None:
            return prior

        self._attempts[operation_id] = self._attempts.get(operation_id, 0) + 1
        order_payload = _serialize_order(order)
        try:
            commit = self.ledger.commit(order)
        except ConnectionRejected as rejected:
            event, audit_index = self._record(
                operation_id, at, "rejected",
                {"order": order_payload, "reject_code": rejected.code,
                 "reject_reason": str(rejected), "details": _safe(rejected.details)},
            )
            return ConnectionReceipt(operation_id, "rejected", order.order_id, None,
                                     event=event, audit_index=audit_index)

        event, audit_index = self._record(
            operation_id, at, "committed",
            {"order": order_payload, "plan": _safe(commit.plan),
             "effect": _safe({"changes": commit.changes,
                              "from_versions": commit.from_versions,
                              "to_versions": commit.to_versions})},
        )
        receipt = ConnectionReceipt(
            operation_id, "committed", order.order_id, _safe(commit.plan),
            dict(commit.from_versions), dict(commit.to_versions), event, audit_index,
        )
        self._receipts[operation_id] = receipt
        return receipt

    # ---- replay ----------------------------------------------------------
    def replay(self) -> ConnectionLedger:
        """Rebuild a ledger from committed events on this service's stream."""
        return replay_stream(self.stream)

    def verify_replay(self) -> bool:
        """True if a fresh replay of the event stream matches live state."""
        return self.replay().dump() == self.ledger.dump()


def replay_stream(stream: EventStream) -> ConnectionLedger:
    """Rebuild state from the ordered event stream.

    Only committed events carry effects; rejections mutate nothing.  The
    rebuilt ledger is returned so callers can compare it against the live one.
    """
    ledger = ConnectionLedger()
    for event in stream.snapshot():
        if event.topic == PROVISION_TOPIC and event.payload.get("status") == "committed":
            key = event.payload["key"]
            version = ledger.register(key, hydrate_value(key, event.payload["value"]))
            if version != event.payload["version"]:
                raise GenerationExpired(
                    "replay provision produced an unexpected generation",
                    {"key": key, "expected": event.payload["version"],
                     "actual": version},
                )
            continue
        if event.topic != TOPIC or event.payload.get("status") != "committed":
            continue
        effect = event.payload["effect"]
        changes = {k: hydrate_value(k, v) for k, v in effect["changes"].items()}
        ledger.replay_effect(
            event.payload["operation_id"],
            changes, effect["from_versions"], effect["to_versions"],
        )
    return ledger
