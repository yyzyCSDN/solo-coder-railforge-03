import threading
import unittest
from datetime import datetime, timezone, timedelta

from railforge.api import RailForgeService
from railforge.crew.connection import (
    CrewConnectionOrder,
    DutyLimits,
    GenerationExpired,
    LodgingWindow,
    ReliefUse,
    SectorWork,
    crew_key,
    lodging_key,
    qualification_key,
    relief_key,
    required_keys,
)
from railforge.crew.connection_workflow import (
    TOPIC,
    CrewConnectionService,
    replay_stream,
)
from railforge.crew.duty import DutySegment
from railforge.crew.qualification import QualificationRule

U = timezone.utc
T0 = datetime(2026, 5, 1, 6, tzinfo=U)
ARRIVAL = datetime(2026, 5, 1, 12, tzinfo=U)
DEPARTURE = datetime(2026, 5, 1, 12, 30, tzinfo=U)
LIMITS = DutyLimits(max_duty_hours=12, max_drive_hours=9, max_span_hours=14)


def window(lodging_id, point="RP1", beds=2, cost=100.0, risk=0.1,
           start=None, end=None):
    return LodgingWindow(
        lodging_id, point,
        start or datetime(2026, 5, 1, 12, tzinfo=U),
        end or datetime(2026, 5, 2, 12, tzinfo=U),
        beds, cost, risk,
    )


def build_service(relief_capacity=2, beds=2, max_drive=9.0, rules=None,
                  existing_drive_hours=2.0, extra_crews=()):
    svc = CrewConnectionService()
    at = T0
    for crew_id, drive_hours in (("C1", existing_drive_hours),
                                 *((c, existing_drive_hours) for c in extra_crews)):
        svc.provision(f"seed-crew-{crew_id}", crew_key(crew_id),
                      {"segments": [DutySegment(T0, T0 + timedelta(hours=drive_hours), True)]}, at)
    svc.provision("seed-relief", relief_key("RP1"),
                  {"capacity": relief_capacity, "bookings": []}, at)
    if rules is None:
        rules = [QualificationRule("q1", "routeA", datetime(2026, 1, 1, tzinfo=U), None, 1, "passenger")]
    svc.provision("seed-qual", qualification_key("routeA"), {"rules": rules}, at)
    svc.provision("seed-lodging", lodging_key("H1"),
                  {"window": window("H1", beds=beds), "bookings": []}, at)
    return svc


def make_order(svc, order_id="o1", *, lodging_ids=("H1",), rest_required=True,
               rest_minutes=480, work_hours=4.0, crew_id="C1", point="RP1",
               scope="routeA", value="passenger", basis=None,
               arrival=ARRIVAL, departure=DEPARTURE, start=None,
               driving=True, max_drive=9.0, max_span=14.0):
    start = start or T0 + timedelta(hours=2)
    order = CrewConnectionOrder(
        order_id=order_id,
        crew_id=crew_id,
        work=(SectorWork("S1", start, start + timedelta(hours=work_hours), driving),),
        relief=ReliefUse(point, arrival, departure),
        qualification_scope=scope,
        qualification_value=value,
        limits=DutyLimits(12, max_drive, max_span),
        lodging=tuple(window(wid) for wid in lodging_ids),
        rest_required=rest_required,
        rest_minutes=rest_minutes,
        basis=basis if basis is not None else svc.ledger.versions(
            [crew_key(crew_id), relief_key(point),
             qualification_key(scope), *(lodging_key(w) for w in lodging_ids)]),
    )
    return order


class JointValidationTests(unittest.TestCase):
    def test_commits_all_four_resources_together(self):
        svc = build_service()
        at = ARRIVAL
        receipt = svc.reserve("op-1", make_order(svc), at)
        self.assertEqual(receipt.status, "committed")
        plan = receipt.plan
        self.assertEqual(plan["sectors"], ["S1"])
        self.assertEqual(plan["relief_point"], "RP1")
        self.assertEqual(plan["qualification"]["rule_id"], "q1")
        self.assertEqual(plan["lodging"]["lodging_id"], "H1")
        # rest starts at departure and lasts rest_minutes
        self.assertEqual(plan["lodging"]["rest_start"], "2026-05-01T12:30:00+00:00")
        self.assertEqual(plan["lodging"]["rest_end"], "2026-05-01T20:30:00+00:00")
        # every touched resource advanced exactly one generation
        self.assertEqual(receipt.from_versions,
                         {crew_key("C1"): 0, relief_key("RP1"): 0, lodging_key("H1"): 0})
        self.assertEqual(receipt.to_versions,
                         {crew_key("C1"): 1, relief_key("RP1"): 1, lodging_key("H1"): 1})
        # qualification generation is observed but not bumped
        self.assertEqual(svc.ledger.read(qualification_key("routeA")).version, 0)

    def test_driving_time_violation_rejects_whole_order(self):
        svc = build_service(existing_drive_hours=6.0, max_drive=9.0)
        before = svc.ledger.dump()
        order = make_order(svc, work_hours=4.0, max_drive=9.0)  # 6 + 4 > 9
        receipt = svc.reserve("op-duty", order, ARRIVAL)
        self.assertEqual(receipt.status, "rejected")
        self.assertEqual(receipt.event.payload["reject_code"], "duty_not_legal")
        self.assertIsNone(receipt.plan)
        self.assertEqual(svc.ledger.dump(), before)  # nothing was half-written

    def test_relief_point_at_capacity_rejects(self):
        svc = build_service(relief_capacity=1, extra_crews=("C2",))
        first = svc.reserve("op-a", make_order(svc, "oa", crew_id="C1"), ARRIVAL)
        self.assertEqual(first.status, "committed")
        before = svc.ledger.dump()
        # second, overlapping connection by a different crew at the single-slot point
        order_b = make_order(
            svc, "ob", crew_id="C2",
            arrival=ARRIVAL + timedelta(minutes=10),
            departure=DEPARTURE + timedelta(minutes=10),
            basis=svc.ledger.versions(required_keys(make_order(
                svc, "ob", crew_id="C2", arrival=ARRIVAL + timedelta(minutes=10),
                departure=DEPARTURE + timedelta(minutes=10)))),
        )
        receipt = svc.reserve("op-b", order_b, ARRIVAL)
        self.assertEqual(receipt.status, "rejected")
        self.assertEqual(receipt.event.payload["reject_code"], "relief_capacity_exceeded")
        self.assertEqual(svc.ledger.dump(), before)

    def test_non_overlapping_relief_bookings_share_the_point(self):
        svc = build_service(relief_capacity=1, extra_crews=("C2",))
        a = make_order(svc, "oa", crew_id="C1", arrival=ARRIVAL, departure=DEPARTURE)
        self.assertEqual(svc.reserve("op-a", a, ARRIVAL).status, "committed")
        b = make_order(
            svc, "ob", crew_id="C2",
            arrival=DEPARTURE + timedelta(hours=1),
            departure=DEPARTURE + timedelta(hours=2),
            basis=svc.ledger.versions(required_keys(make_order(
                svc, "ob", crew_id="C2", arrival=DEPARTURE + timedelta(hours=1),
                departure=DEPARTURE + timedelta(hours=2)))),
        )
        self.assertEqual(svc.reserve("op-b", b, ARRIVAL).status, "committed")

    def test_qualification_denied_when_rule_expired_or_wrong_value(self):
        rules = [QualificationRule("q-old", "routeA", datetime(2026, 1, 1, tzinfo=U),
                                   datetime(2026, 4, 1, tzinfo=U), 1, "passenger")]
        svc = build_service(rules=rules)
        before = svc.ledger.dump()
        receipt = svc.reserve("op-q", make_order(svc), ARRIVAL)
        self.assertEqual(receipt.status, "rejected")
        self.assertEqual(receipt.event.payload["reject_code"], "qualification_denied")
        self.assertEqual(svc.ledger.dump(), before)

        svc2 = build_service()
        receipt2 = svc2.reserve("op-q2", make_order(svc2, value="hazmat"), ARRIVAL)
        self.assertEqual(receipt2.status, "rejected")
        self.assertEqual(receipt2.event.payload["reject_code"], "qualification_denied")

    def test_lodging_picks_cheapest_fitting_window(self):
        svc = build_service()
        svc.provision("seed-h2", lodging_key("H2"),
                      {"window": window("H2", cost=80.0), "bookings": []}, ARRIVAL)
        order = make_order(svc, lodging_ids=("H1", "H2"),
                           basis=svc.ledger.versions(
                               [crew_key("C1"), relief_key("RP1"),
                                qualification_key("routeA"),
                                lodging_key("H1"), lodging_key("H2")]))
        receipt = svc.reserve("op-l", order, ARRIVAL)
        self.assertEqual(receipt.status, "committed")
        self.assertEqual(receipt.plan["lodging"]["lodging_id"], "H2")

    def test_no_lodging_window_covers_mandatory_rest_rejects(self):
        svc = build_service()
        # window closes 2h after departure, rest needs 8h
        svc.provision("seed-short", lodging_key("HS"),
                      {"window": window("HS", start=ARRIVAL,
                                        end=DEPARTURE + timedelta(hours=2)),
                       "bookings": []}, ARRIVAL)
        basis = {crew_key("C1"): 0, relief_key("RP1"): 0,
                 qualification_key("routeA"): 0, lodging_key("HS"): 0}
        order = make_order(svc, lodging_ids=("HS",), basis=basis)
        before = svc.ledger.dump()
        receipt = svc.reserve("op-rest", order, ARRIVAL)
        self.assertEqual(receipt.status, "rejected")
        self.assertEqual(receipt.event.payload["reject_code"], "lodging_unavailable")
        self.assertEqual(svc.ledger.dump(), before)

    def test_last_bed_taken_rejects_and_leaves_no_partial_booking(self):
        svc = build_service(beds=1, extra_crews=("C2",))
        self.assertEqual(svc.reserve("op-a", make_order(svc, "oa"), ARRIVAL).status,
                         "committed")
        before = svc.ledger.dump()
        order_b = make_order(
            svc, "ob", crew_id="C2",
            basis=svc.ledger.versions(required_keys(make_order(svc, "ob", crew_id="C2"))))
        receipt = svc.reserve("op-b", order_b, ARRIVAL)
        self.assertEqual(receipt.status, "rejected")
        self.assertEqual(receipt.event.payload["reject_code"], "lodging_unavailable")
        # crew and relief must not have been advanced when lodging failed
        self.assertEqual(svc.ledger.dump(), before)

    def test_rest_not_required_skips_lodging(self):
        svc = build_service()
        order = make_order(svc, rest_required=False, lodging_ids=())
        # rebuild basis without any lodging key
        object.__setattr__(order, "basis", svc.ledger.versions(
            [crew_key("C1"), relief_key("RP1"), qualification_key("routeA")]))
        receipt = svc.reserve("op-norest", order, ARRIVAL)
        self.assertEqual(receipt.status, "committed")
        self.assertIsNone(receipt.plan["lodging"])
        self.assertNotIn(lodging_key("H1"), receipt.to_versions)


class GenerationAndAtomicityTests(unittest.TestCase):
    def test_stale_generation_on_any_resource_rejects_entire_order(self):
        for stale_key in ("crew", "relief", "qualification", "lodging"):
            svc = build_service()
            # bump one generation out from under the caller
            if stale_key == "crew":
                svc.ledger.register(crew_key("C1"),
                                    {"segments": [DutySegment(T0, T0 + timedelta(hours=1), True)]})
            elif stale_key == "relief":
                svc.ledger.register(relief_key("RP1"), {"capacity": 2, "bookings": []})
            elif stale_key == "qualification":
                svc.ledger.register(qualification_key("routeA"),
                                    {"rules": [QualificationRule("q2", "routeA",
                                                                 datetime(2026, 1, 1, tzinfo=U),
                                                                 None, 1, "passenger")]})
            else:
                svc.ledger.register(lodging_key("H1"),
                                    {"window": window("H1", beds=4), "bookings": []})
            stale_order = make_order(
                svc, basis={crew_key("C1"): 0, relief_key("RP1"): 0,
                            qualification_key("routeA"): 0, lodging_key("H1"): 0})
            before = svc.ledger.dump()
            receipt = svc.reserve(f"op-stale-{stale_key}", stale_order, ARRIVAL)
            self.assertEqual(receipt.status, "rejected", stale_key)
            self.assertEqual(receipt.event.payload["reject_code"], "generation_expired")
            self.assertEqual(svc.ledger.dump(), before, f"half-written state for {stale_key}")

    def test_missing_generation_in_basis_rejects(self):
        svc = build_service()
        order = make_order(svc, basis={crew_key("C1"): 0, relief_key("RP1"): 0})
        before = svc.ledger.dump()
        receipt = svc.reserve("op-nobasis", order, ARRIVAL)
        self.assertEqual(receipt.status, "rejected")
        self.assertEqual(receipt.event.payload["reject_code"], "basis_incomplete")
        self.assertIn("qualification:routeA", receipt.event.payload["details"]["missing"])
        self.assertEqual(svc.ledger.dump(), before)

    def test_rejected_order_can_be_retried_with_fresh_generation(self):
        svc = build_service()
        stale = make_order(svc, basis={crew_key("C1"): 0, relief_key("RP1"): 0,
                                       qualification_key("routeA"): 0,
                                       lodging_key("H1"): 0})
        svc.ledger.register(lodging_key("H1"), {"window": window("H1"), "bookings": []})
        rejected = svc.reserve("op-retry", stale, ARRIVAL)
        self.assertEqual(rejected.status, "rejected")
        fresh = make_order(svc)
        retried = svc.reserve("op-retry", fresh, ARRIVAL)
        # a rejected operation id is not cached: the retry is evaluated
        self.assertEqual(retried.status, "committed")

    def test_committed_operation_is_idempotent(self):
        svc = build_service()
        order = make_order(svc)
        first = svc.reserve("op-idem", order, ARRIVAL)
        second = svc.reserve("op-idem", order, ARRIVAL)
        self.assertIs(first, second)
        # only one committed order on the stream
        committed = [e for e in svc.stream.snapshot()
                     if e.topic == TOPIC and e.payload["status"] == "committed"]
        self.assertEqual(len(committed), 1)


class AuditAndReplayTests(unittest.TestCase):
    def test_commits_and_rejections_are_audited_and_chained(self):
        svc = build_service(extra_crews=("C2",))
        svc.reserve("ok-1", make_order(svc), ARRIVAL)
        svc.reserve("bad-1", make_order(svc, value="hazmat", crew_id="C2"), ARRIVAL)
        self.assertTrue(svc.stream.verify_chain())
        self.assertTrue(svc.trail.verify())
        events = svc.stream.snapshot()
        statuses = [e.payload["status"] for e in events if e.topic == TOPIC]
        self.assertEqual(statuses, ["committed", "rejected"])
        rejected = next(e for e in events if e.payload["status"] == "rejected")
        self.assertEqual(rejected.payload["control_path"], "atomic")
        self.assertEqual(rejected.payload["reject_code"], "qualification_denied")
        self.assertEqual(rejected.subject, "bad-1")
        # rejection got its own unique event id but no receipt caching
        self.assertTrue(rejected.event_id.startswith("bad-1:rejected:"))
        # one audit record per stream event (5 provisions + 2 orders)
        self.assertEqual(len(svc.trail._rows), 7)

    def test_replay_rebuilds_live_state_after_commits_and_rejections(self):
        svc = build_service(beds=1, extra_crews=("C2", "C3", "C4", "C5"))
        # first committed order consumes the single bed on day 1
        svc.reserve("ok-1", make_order(svc, "o1", crew_id="C1"), ARRIVAL)
        # illegal driving time: C2 asks for far more driving than the limits allow
        svc.reserve("reject-duty", make_order(
            svc, "o2", crew_id="C2", max_drive=1.0,
            basis=svc.ledger.versions(required_keys(make_order(
                svc, "o2", crew_id="C2", max_drive=1.0)))), ARRIVAL)
        # no bed left for C3's overlapping rest window
        svc.reserve("reject-bed", make_order(
            svc, "o3", crew_id="C3",
            basis=svc.ledger.versions(required_keys(make_order(
                svc, "o3", crew_id="C3")))), ARRIVAL)
        # master-data change bumps the qualification generation mid-life
        svc.provision("qual-v2", qualification_key("routeA"),
                      {"rules": [QualificationRule("q2", "routeA",
                                                   datetime(2026, 1, 1, tzinfo=U),
                                                   None, 1, "passenger")]}, ARRIVAL)
        # widen lodging and book a late connection on the fresh generations
        svc.provision("lodge-wide", lodging_key("H1"),
                      {"window": window("H1", start=datetime(2026, 5, 1, 12, tzinfo=U),
                                        end=datetime(2026, 5, 3, tzinfo=U), beds=1),
                       "bookings": svc.ledger.read(lodging_key("H1")).value["bookings"]},
                      ARRIVAL)
        late_arrival = ARRIVAL + timedelta(hours=10)
        late_departure = DEPARTURE + timedelta(hours=10)
        svc.reserve("ok-2", make_order(
            svc, "o4", crew_id="C4", max_span=48.0,
            start=datetime(2026, 5, 1, 20, tzinfo=U), work_hours=2.0,
            arrival=late_arrival, departure=late_departure,
            basis=svc.ledger.versions(required_keys(
                make_order(svc, "o4", crew_id="C4", max_span=48.0,
                           start=datetime(2026, 5, 1, 20, tzinfo=U), work_hours=2.0,
                           arrival=late_arrival, departure=late_departure)))),
                    ARRIVAL)

        self.assertTrue(svc.verify_replay())
        rebuilt = replay_stream(svc.stream)
        self.assertEqual(rebuilt.dump(), svc.ledger.dump())
        # rebuilt ledger behaves the same: a fresh basis read from replay commits too
        order = make_order(
            svc, "o5", crew_id="C5", max_span=72.0,
            start=datetime(2026, 5, 2, 2, 30, tzinfo=U), work_hours=4.0,
            arrival=datetime(2026, 5, 2, 6, 30, tzinfo=U),
            departure=datetime(2026, 5, 2, 7, tzinfo=U),
            basis=rebuilt.versions(required_keys(make_order(
                svc, "o5", crew_id="C5", max_span=72.0,
                start=datetime(2026, 5, 2, 2, 30, tzinfo=U), work_hours=4.0,
                arrival=datetime(2026, 5, 2, 6, 30, tzinfo=U),
                departure=datetime(2026, 5, 2, 7, tzinfo=U)))))
        self.assertEqual(rebuilt.commit(order).plan["order_id"], "o5")

    def test_replay_detects_gap_in_effect_versions(self):
        svc = build_service()
        svc.reserve("ok-1", make_order(svc), ARRIVAL)
        # tamper with the committed effect
        event = next(e for e in svc.stream.snapshot() if e.topic == TOPIC)
        event.payload["effect"]["from_versions"]["crew:C1"] = 9
        with self.assertRaises(GenerationExpired):
            replay_stream(svc.stream)


class ConflictTests(unittest.TestCase):
    def test_concurrent_orders_for_single_bed_resolve_without_partial_state(self):
        svc = build_service(beds=1, relief_capacity=1)
        barrier = threading.Barrier(4)

        orders, receipts = [], []

        def worker(oid):
            # each thread reads the same generation 0 before racing
            order = make_order(svc, oid)
            barrier.wait()
            receipts.append(svc.reserve(f"op-{oid}", order, ARRIVAL))

        threads = [threading.Thread(target=worker, args=(f"o{i}",)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        committed = [r for r in receipts if r.status == "committed"]
        rejected = [r for r in receipts if r.status == "rejected"]
        self.assertEqual(len(committed), 1)
        self.assertEqual(len(rejected), 3)
        self.assertTrue(all(r.event.payload["reject_code"] == "generation_expired"
                            for r in rejected))
        # exactly one bed/relief slot/crew addition consumed
        crew = svc.ledger.read(crew_key("C1")).value
        relief = svc.ledger.read(relief_key("RP1")).value
        lodge = svc.ledger.read(lodging_key("H1")).value
        self.assertEqual(len(relief["bookings"]), 1)
        self.assertEqual(len(lodge["bookings"]), 1)
        # original 2h duty row + exactly one 4h sector appended
        self.assertEqual(len(crew["segments"]), 2)
        self.assertTrue(svc.verify_replay())
        self.assertTrue(svc.stream.verify_chain())
        self.assertTrue(svc.trail.verify())

    def test_serial_conflict_is_resolvable_by_rereading_generation(self):
        svc = build_service(beds=1, relief_capacity=1)
        # widen the lodging window so a later same-day connection can still rest
        svc.provision("lodge-wide", lodging_key("H1"),
                      {"window": window("H1", start=datetime(2026, 5, 1, 12, tzinfo=U),
                                        end=datetime(2026, 5, 3, tzinfo=U), beds=1),
                       "bookings": []}, ARRIVAL)
        first = make_order(svc, "o1")
        self.assertEqual(svc.reserve("op-1", first, ARRIVAL).status, "committed")
        later_arrival = ARRIVAL + timedelta(hours=10)
        later_departure = DEPARTURE + timedelta(hours=10)
        stale_second = make_order(
            svc, "o2", max_span=48.0,
            arrival=later_arrival, departure=later_departure,
            basis={crew_key("C1"): 0, relief_key("RP1"): 0,
                   qualification_key("routeA"): 0, lodging_key("H1"): 0})
        rejected = svc.reserve("op-2", stale_second, ARRIVAL)
        self.assertEqual(rejected.status, "rejected")
        self.assertEqual(rejected.event.payload["reject_code"], "generation_expired")
        # caller re-reads generations and places a non-overlapping connection
        fresh = make_order(
            svc, "o2", max_span=48.0,
            start=datetime(2026, 5, 1, 20, tzinfo=U), work_hours=2.0,
            arrival=later_arrival, departure=later_departure,
            basis=svc.ledger.versions(required_keys(make_order(
                svc, "o2", max_span=48.0,
                start=datetime(2026, 5, 1, 20, tzinfo=U), work_hours=2.0,
                arrival=later_arrival, departure=later_departure))))
        self.assertEqual(svc.reserve("op-2b", fresh, ARRIVAL).status, "committed")
        self.assertTrue(svc.verify_replay())


class ServiceSurfaceTests(unittest.TestCase):
    def test_capability_catalog_lists_connection_modules(self):
        capabilities = RailForgeService().health()["capabilities"]
        for name in ("crew.connection", "crew.connection_ledger", "crew.connection_workflow"):
            self.assertIn(name, capabilities)

    def test_service_exposes_crew_connection_plane(self):
        service = RailForgeService()
        svc = service.crew_connections()
        self.assertIsInstance(svc, CrewConnectionService)
        self.assertIs(svc, service.crew_connections())
        self.assertIs(svc.stream, service.control.stream)
        self.assertIs(svc.trail, service.control.trail)
        svc.provision("s-crew", crew_key("C1"), {"segments": []}, T0)
        svc.provision("s-relief", relief_key("RP1"), {"capacity": 1, "bookings": []}, T0)
        svc.provision("s-qual", qualification_key("routeA"),
                      {"rules": [QualificationRule("q1", "routeA",
                                                   datetime(2026, 1, 1, tzinfo=U),
                                                   None, 1, "passenger")]}, T0)
        svc.provision("s-lodge", lodging_key("H1"),
                      {"window": window("H1", beds=1), "bookings": []}, T0)
        order = make_order(svc, work_hours=2.0,
                           start=datetime(2026, 5, 1, 6, tzinfo=U),
                           arrival=datetime(2026, 5, 1, 8, tzinfo=U),
                           departure=datetime(2026, 5, 1, 8, 30, tzinfo=U))
        receipt = svc.reserve("svc-op", order, T0)
        self.assertEqual(receipt.status, "committed")
        self.assertTrue(service.health()["workflow"]["audit_valid"])


if __name__ == "__main__":
    unittest.main()
