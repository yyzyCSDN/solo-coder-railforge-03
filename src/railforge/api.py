from __future__ import annotations

from railforge.braking.brake_percentage import brake_percentage
from railforge.consist.integrity import validate
from railforge.routing.clearance import route_ok
from railforge.signaling.block_occupancy import conflicts
from railforge.workflows.control import RailForgeWorkflowControl
from railforge.workflows.features import RailForgeFeatureService
from railforge.crew.connection_workflow import CrewConnectionService
from railforge.capability_catalog import load_capabilities


class RailForgeService:
    def __init__(self, control: RailForgeWorkflowControl | None = None):
        self.control = control or RailForgeWorkflowControl()
        self.features = RailForgeFeatureService(self.control)
        self._crew_connections: CrewConnectionService | None = None

    def health(self):
        return {
            "service": "railforge",
            "status": "ok",
            "runtime_dependencies": 0,
            "capabilities": load_capabilities(),
            "workflow": {"events": self.control.stream.head, "audit_valid": self.control.trail.verify()},
        }

    def block_conflicts(self, candidate, existing, clearance):
        return conflicts(candidate, existing, clearance)

    def validate_consist(self, rows):
        return validate(rows)

    def brake_percentage(self, rows, gradient):
        return brake_percentage(rows, gradient)

    def route_clearance(self, profile, segments):
        return route_ok(profile, segments)

    def workflow(self):
        return self.control

    def crew_connections(self):
        """Stateful cross-sector crew connection + lodging guarantee plane."""
        if self._crew_connections is None:
            self._crew_connections = CrewConnectionService(
                stream=self.control.stream, trail=self.control.trail)
        return self._crew_connections

