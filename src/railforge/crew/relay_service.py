from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock

from railforge.crew.relay_plan import (
    CREW, LODGING, POINT, HoldView, plan as build_plan, resource_id,
)
from railforge.crew.relay_store import Hold, RelayStore, StaleEpoch
from railforge.workflows.control import RailForgeWorkflowControl

REGISTERED_TOPIC = "crew.relay.resource.registered"
COMMITTED_TOPIC = "crew.relay.scheduled"
REJECTED_TOPIC = "crew.relay.rejected"


def _parse(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


@dataclass(frozen=True)
class ReplayReport:
    registered: int
    committed: int
    rejected: int
    digest: str
    store: RelayStore


class CrewRelayService:
    """Cross-section crew relay with lodging guarantee.

    Each order is planned jointly (duty legality, driving hours, point
    capacity, qualification, lodging window) against a pinned epoch
    snapshot and committed all-or-nothing: if ANY considered resource
    epoch has moved, the whole order is rejected and no partial schedule
    is left behind. Commits and rejections are both published to the
    hash-chained stream and audit trail, so state can be replayed.
    """

    def __init__(self, control: RailForgeWorkflowControl | None = None,
                 store: RelayStore | None = None):
        self.control = control or RailForgeWorkflowControl()
        self.store = store or RelayStore()
        self._lock = RLock()
        self._rejections: dict[str, int] = {}

    def snapshot_epochs(self, resource_ids=None) -> dict[str, int]:
        """Epoch snapshot a client can plan against and later pass back as pins."""
        return self.store.epochs(resource_ids)

    def submit(self, order, points, etas, facilities, limits, at, expected_epochs=None):
        """Plan and atomically commit one relay order.

        expected_epochs pins the plan to a client-side snapshot; resources
        missing from the pins are expected to be at epoch 0. Omit it to
        plan and commit against a fresh service-side snapshot.
        """
        with self._lock:
            previous = self.control.receipts.get(order.order_id)
            if previous is not None:
                if previous.topic != COMMITTED_TOPIC:
                    raise ValueError("order id reused for a different workflow")
                return previous
            self._register_resources(order, points, facilities, at)
            read_set = self._read_set(order, points, facilities)
            if expected_epochs is None:
                pins = self.store.epochs(read_set)
            else:
                pins = {rid: 0 for rid in read_set}
                pins.update(expected_epochs)
            usage = tuple(HoldView(h.resource_id, h.start, h.end) for h in self.store.holds())
            try:
                relay_plan = build_plan(order, points, etas, facilities, limits, usage, pins)
            except ValueError as exc:
                self._reject(order, at, "infeasible", str(exc))
                raise
            holds = tuple(Hold(h.resource_id, order.order_id, h.start, h.end)
                          for h in relay_plan.holds)
            evidence = {
                "train": order.train,
                "point_id": relay_plan.point_id,
                "facility_id": relay_plan.facility_id,
                "relief_crew": order.relief_crew.crew_id,
                "inbound_crew": order.inbound_crew_id,
            }

            def action():
                committed = self.store.commit(pins, holds)
                return {
                    "order_id": order.order_id,
                    "train": order.train,
                    "point_id": relay_plan.point_id,
                    "facility_id": relay_plan.facility_id,
                    "handover_at": relay_plan.handover_at,
                    "rest_from": relay_plan.rest_from,
                    "rest_to": relay_plan.rest_to,
                    "duty_hours": relay_plan.duty_hours,
                    "drive_hours": relay_plan.drive_hours,
                    "span_hours": relay_plan.span_hours,
                    "holds": [
                        {"resource_id": h.resource_id, "order_id": h.order_id,
                         "start": h.start, "end": h.end}
                        for h in holds
                    ],
                    "expected_epochs": dict(pins),
                    "committed_epochs": committed,
                }

            try:
                return self.control.run(order.order_id, COMMITTED_TOPIC, order.order_id,
                                        at, action, evidence)
            except StaleEpoch as exc:
                self._reject(order, at, "stale-epoch", str(exc))
                raise

    def replay(self) -> ReplayReport:
        """Rebuild store state from the event stream, re-validating every
        epoch check and capacity check. Any divergence raises loudly."""
        fresh = RelayStore()
        registered = committed = rejected = 0
        for event in sorted(self.control.stream.snapshot(), key=lambda e: e.sequence):
            if event.topic == REGISTERED_TOPIC:
                fresh.register(event.payload["resource_id"], event.payload["capacity"])
                registered += 1
            elif event.topic == COMMITTED_TOPIC:
                result = event.payload["result"]
                holds = [
                    Hold(h["resource_id"], h["order_id"], _parse(h["start"]), _parse(h["end"]))
                    for h in result["holds"]
                ]
                epochs = fresh.commit(result["expected_epochs"], holds)
                if epochs != result["committed_epochs"]:
                    raise RuntimeError("replay divergence: committed epochs do not match the log")
                committed += 1
            elif event.topic == REJECTED_TOPIC:
                rejected += 1
        return ReplayReport(registered, committed, rejected, fresh.digest(), fresh)

    def verify_replay(self) -> bool:
        return self.replay().digest == self.store.digest()

    def _read_set(self, order, points, facilities) -> list[str]:
        ids = {resource_id(POINT, p.point_id) for p in points}
        ids |= {resource_id(LODGING, f.facility_id) for f in facilities}
        ids.add(resource_id(CREW, order.relief_crew.crew_id))
        return sorted(ids)

    def _register_resources(self, order, points, facilities, at):
        wanted = {resource_id(POINT, p.point_id): p.capacity for p in points}
        for f in facilities:
            if f.beds < 1:
                raise ValueError("beds")
            wanted[resource_id(LODGING, f.facility_id)] = f.beds
        wanted[resource_id(CREW, order.relief_crew.crew_id)] = 1
        for rid in sorted(wanted):
            if self.store.registered(rid):
                self.store.register(rid, wanted[rid])  # revalidates capacity consistency
                continue
            epoch = self.store.register(rid, wanted[rid])
            self.control.stream.publish(f"register:{rid}", at, REGISTERED_TOPIC, rid,
                                        {"resource_id": rid, "capacity": wanted[rid], "epoch": epoch})
            self.control.trail.append("railforge-control", REGISTERED_TOPIC, rid, at)

    def _reject(self, order, at, kind, reason):
        attempt = self._rejections.get(order.order_id, 0) + 1
        self._rejections[order.order_id] = attempt
        payload = {
            "operation_id": order.order_id,
            "status": "rejected",
            "kind": kind,
            "reason": reason,
            "attempt": attempt,
        }
        self.control.stream.publish(f"{order.order_id}:rejected:{attempt}", at, REJECTED_TOPIC,
                                    order.order_id, payload)
        self.control.trail.append("railforge-control", REJECTED_TOPIC, order.order_id, at)
