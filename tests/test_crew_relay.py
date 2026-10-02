import threading
import unittest
from datetime import datetime, timedelta, timezone

from railforge.api import RailForgeService
from railforge.crew.duty import DutySegment
from railforge.crew.relay_plan import (
    CrewMember, DutyLimits, LodgingWindow, PointEta, RelayOrder, RelayPoint,
)
from railforge.crew.relay_service import (
    COMMITTED_TOPIC, REJECTED_TOPIC, CrewRelayService,
)
from railforge.crew.relay_store import CapacityExceeded, Hold, RelayStore, StaleEpoch

U = timezone.utc
T0 = datetime(2026, 3, 1, 8, 0, tzinfo=U)
LIMITS = DutyLimits(max_duty_hours=12.0, max_drive_hours=9.0, max_span_hours=14.0)

P1 = RelayPoint("P1", 1)
P2 = RelayPoint("P2", 1)
# F1 closes too early for the 6h rest after a 10:00 handover; F2 fits the 12:30 handover.
F1 = LodgingWindow("F1", "P1", T0 - timedelta(hours=2), T0 + timedelta(hours=7), 1)
F2 = LodgingWindow("F2", "P2", T0 + timedelta(hours=3), T0 + timedelta(hours=12), 1)
POINTS = [P1, P2]
FACS = [F1, F2]
ETAS = [PointEta("P1", T0 + timedelta(hours=2), 2.0),
        PointEta("P2", T0 + timedelta(hours=4.5), 4.5)]


def make_order(order_id, crew_id="C-OUT", quals=("route:K27", "traction:HXD3"),
               required=("route:K27",), rest_hours=6.0,
               duty_start=T0 - timedelta(hours=4)):
    return RelayOrder(
        order_id=order_id,
        train=f"TR-{order_id}",
        inbound_crew_id="C-IN",
        relief_crew=CrewMember(crew_id, frozenset(quals)),
        required_quals=frozenset(required),
        duty_rows=(DutySegment(duty_start, T0, True),),
        rest_hours=rest_hours,
        handover_minutes=30,
        report_lead_minutes=20,
        onward_drive_hours=4.0,
    )


def holds_of(service, order_id):
    return [h for h in service.store.holds() if h.order_id == order_id]


class JointPlanTests(unittest.TestCase):
    def test_joint_plan_selects_lodging_feasible_point(self):
        svc = CrewRelayService()
        receipt = svc.submit(make_order("R1"), POINTS, ETAS, FACS, LIMITS, T0)
        self.assertEqual(receipt.event.payload["status"], "committed")
        result = receipt.result
        # P1 is legal on duty and capacity but its lodging window cannot hold
        # the 6h rest, so the joint plan must land on P2 + F2.
        self.assertEqual(result["point_id"], "P2")
        self.assertEqual(result["facility_id"], "F2")
        self.assertAlmostEqual(result["duty_hours"], 8.5)
        self.assertAlmostEqual(result["drive_hours"], 8.5)
        resources = {h["resource_id"] for h in result["holds"]}
        self.assertEqual(resources, {"point:P2", "crew:C-OUT", "lodging:F2"})
        self.assertEqual(set(result["expected_epochs"]),
                         {"point:P1", "point:P2", "lodging:F1", "lodging:F2", "crew:C-OUT"})
        self.assertTrue(svc.verify_replay())

    def test_earliest_feasible_point_wins(self):
        wide_f1 = LodgingWindow("F1", "P1", T0 - timedelta(hours=2), T0 + timedelta(hours=10), 1)
        svc = CrewRelayService()
        receipt = svc.submit(make_order("R1"), POINTS, ETAS, [wide_f1, F2], LIMITS, T0)
        self.assertEqual(receipt.result["point_id"], "P1")
        self.assertEqual(receipt.result["facility_id"], "F1")

    def test_unqualified_relief_crew_rejected(self):
        svc = CrewRelayService()
        with self.assertRaises(ValueError):
            svc.submit(make_order("R2", required=("route:K99",)), POINTS, ETAS, FACS, LIMITS, T0)
        self.assertEqual(svc.store.holds(), ())
        kinds = [e.payload["kind"] for e in svc.control.stream.snapshot()
                 if e.topic == REJECTED_TOPIC]
        self.assertEqual(kinds, ["infeasible"])
        self.assertTrue(svc.control.trail.verify())
        self.assertTrue(svc.verify_replay())

    def test_driving_hours_limit_enforced(self):
        svc = CrewRelayService()
        etas = [PointEta("P2", T0 + timedelta(hours=10), 10.0)]
        with self.assertRaises(ValueError):
            svc.submit(make_order("R3"), POINTS, etas, FACS, LIMITS, T0)
        self.assertEqual(svc.store.holds(), ())

    def test_lodging_beds_exhausted_rejects_order(self):
        svc = CrewRelayService()
        svc.submit(make_order("A"), POINTS, ETAS, FACS, LIMITS, T0)  # occupies F2 12:30-18:30
        order_b = make_order("B", crew_id="C-OUT2", duty_start=T0 - timedelta(hours=2))
        etas_b = [PointEta("P2", T0 + timedelta(hours=5), 5.0)]  # rest 13:00-19:00 overlaps
        with self.assertRaises(ValueError):
            svc.submit(order_b, POINTS, etas_b, FACS, LIMITS, T0)
        self.assertEqual(holds_of(svc, "B"), [])


class StaleEpochTests(unittest.TestCase):
    def test_stale_epoch_rejects_entire_order(self):
        svc = CrewRelayService()
        svc.submit(make_order("A"), POINTS, ETAS, FACS, LIMITS, T0)
        pins = svc.snapshot_epochs()  # client-side planning snapshot
        # C commits afterwards and moves point:P1's epoch.
        svc.submit(make_order("C", crew_id="C-OUT3", rest_hours=0.0), POINTS,
                   [PointEta("P1", T0 + timedelta(hours=2), 2.0)], FACS, LIMITS, T0)
        holds_after_c = svc.store.holds()
        epochs_after_c = svc.snapshot_epochs()
        order_b = make_order("B", crew_id="C-OUT2", rest_hours=0.0)
        etas_b = [PointEta("P2", T0 + timedelta(hours=5), 5.0)]
        with self.assertRaises(StaleEpoch):
            svc.submit(order_b, POINTS, etas_b, FACS, LIMITS, T0, expected_epochs=pins)
        # The whole order was rejected: no point slot, no crew interval, no bed,
        # and no existing resource moved. (Registering the never-before-seen
        # relief crew as master data is not a schedule.)
        self.assertEqual(svc.store.holds(), holds_after_c)
        for rid, epoch in epochs_after_c.items():
            self.assertEqual(svc.snapshot_epochs([rid])[rid], epoch)
        self.assertEqual(holds_of(svc, "B"), [])
        rejected = [e for e in svc.control.stream.snapshot() if e.topic == REJECTED_TOPIC]
        self.assertEqual(rejected[-1].payload["kind"], "stale-epoch")
        self.assertTrue(svc.control.stream.verify_chain())
        self.assertTrue(svc.control.trail.verify())
        self.assertTrue(svc.verify_replay())
        # Rejection is not sticky: a retry against fresh epochs commits whole.
        receipt = svc.submit(order_b, POINTS, etas_b, FACS, LIMITS, T0)
        self.assertEqual(receipt.event.payload["status"], "committed")
        self.assertEqual(len(holds_of(svc, "B")), 2)
        self.assertTrue(svc.verify_replay())

    def test_rejected_order_leaves_no_partial_schedule(self):
        svc = CrewRelayService()
        svc.submit(make_order("A"), POINTS, ETAS, FACS, LIMITS, T0)  # takes P2 + F2 + C-OUT
        digest_after_a = svc.store.digest()
        # B collides with A at P2 on point, crew and beds; P1's window does not fit.
        with self.assertRaises(ValueError):
            svc.submit(make_order("B"), POINTS, ETAS, FACS, LIMITS, T0)
        self.assertEqual(svc.store.digest(), digest_after_a)
        self.assertEqual(holds_of(svc, "B"), [])
        self.assertEqual(len(holds_of(svc, "A")), 3)

    def test_concurrent_submit_exactly_one_commits(self):
        svc = CrewRelayService()
        p2 = RelayPoint("P2", 4)  # capacity is not the decider; epochs are
        svc.submit(make_order("SEED", crew_id="C-SEED", rest_hours=0.0), [P1, p2],
                   [PointEta("P1", T0 + timedelta(hours=2), 2.0)], FACS, LIMITS, T0)
        pins = svc.snapshot_epochs()
        barrier = threading.Barrier(4)
        outcomes = []

        def worker(i):
            barrier.wait()
            order = make_order(f"R{i}", crew_id=f"C-{i}", rest_hours=0.0)
            try:
                svc.submit(order, [P1, p2], [PointEta("P2", T0 + timedelta(hours=5), 5.0)],
                           FACS, LIMITS, T0, expected_epochs=pins)
                outcomes.append("committed")
            except StaleEpoch:
                outcomes.append("stale")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(outcomes), ["committed", "stale", "stale", "stale"])
        self.assertEqual(len(svc.store.holds()), 4)  # seed (2 holds) + single winner (2 holds)
        self.assertTrue(svc.control.stream.verify_chain())
        self.assertTrue(svc.control.trail.verify())
        self.assertTrue(svc.verify_replay())


class StateAuditReplayTests(unittest.TestCase):
    def test_replay_rebuilds_state_and_audit(self):
        svc = CrewRelayService()
        svc.submit(make_order("A"), POINTS, ETAS, FACS, LIMITS, T0)
        svc.submit(make_order("B", crew_id="C-OUT2", rest_hours=0.0), POINTS,
                   [PointEta("P2", T0 + timedelta(hours=5), 5.0)], FACS, LIMITS, T0)
        with self.assertRaises(ValueError):
            svc.submit(make_order("C", required=("route:K99",)), POINTS, ETAS, FACS, LIMITS, T0)
        report = svc.replay()
        self.assertEqual(report.committed, 2)
        self.assertEqual(report.rejected, 1)
        self.assertEqual(report.registered, 6)  # 2 points + 2 facilities + 2 relief crews
        self.assertEqual(report.digest, svc.store.digest())
        self.assertEqual(report.store.holds(), svc.store.holds())
        self.assertTrue(svc.control.stream.verify_chain())
        self.assertTrue(svc.control.trail.verify())
        # every commit, rejection and registration left exactly one audit record
        self.assertEqual(len(svc.control.trail._rows), len(svc.control.stream.snapshot()))
        actions = [r.action for r in svc.control.trail._rows]
        self.assertIn(COMMITTED_TOPIC, actions)
        self.assertIn(REJECTED_TOPIC, actions)

    def test_resubmission_is_idempotent(self):
        svc = CrewRelayService()
        order = make_order("A")
        first = svc.submit(order, POINTS, ETAS, FACS, LIMITS, T0)
        head = svc.control.stream.head
        digest = svc.store.digest()
        second = svc.submit(order, POINTS, ETAS, FACS, LIMITS, T0)
        self.assertIs(first, second)
        self.assertEqual(svc.control.stream.head, head)
        self.assertEqual(svc.store.digest(), digest)

    def test_service_facade_integration(self):
        service = RailForgeService()
        receipt = service.crew_relay.submit(make_order("A"), POINTS, ETAS, FACS, LIMITS, T0)
        self.assertEqual(receipt.event.payload["status"], "committed")
        health = service.health()
        self.assertEqual(health["status"], "ok")
        self.assertTrue(health["workflow"]["audit_valid"])
        self.assertIn("crew.relay_plan", health["capabilities"])
        self.assertTrue(service.crew_relay.verify_replay())


class RelayStoreTests(unittest.TestCase):
    def test_commit_is_all_or_nothing(self):
        store = RelayStore()
        store.register("point:P1", 1)
        store.register("lodging:F1", 1)
        pins = store.epochs()
        holds = [Hold("point:P1", "A", T0, T0 + timedelta(hours=1)),
                 Hold("lodging:F1", "A", T0, T0 + timedelta(hours=6))]
        committed = store.commit(pins, holds)
        self.assertEqual(committed, {"point:P1": 1, "lodging:F1": 1})
        # one stale pin rejects the whole batch; nothing is written
        with self.assertRaises(StaleEpoch):
            store.commit({"point:P1": 0, "lodging:F1": 1},
                         [Hold("lodging:F1", "B", T0 + timedelta(days=1),
                               T0 + timedelta(days=1, hours=6))])
        self.assertEqual([h.order_id for h in store.holds()], ["A", "A"])
        # capacity overflow with fresh pins also writes nothing
        with self.assertRaises(CapacityExceeded):
            store.commit({"point:P1": 1},
                         [Hold("point:P1", "C", T0 + timedelta(minutes=30),
                               T0 + timedelta(hours=2))])
        self.assertEqual([h.order_id for h in store.holds()], ["A", "A"])
        self.assertEqual(store.epochs(), {"point:P1": 1, "lodging:F1": 1})


if __name__ == "__main__":
    unittest.main()
