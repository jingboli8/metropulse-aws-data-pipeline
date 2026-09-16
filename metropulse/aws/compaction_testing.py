"""Deterministic fake Glue publisher used by offline compaction tests."""

from __future__ import annotations

from metropulse.compaction_models import PublicationConflict


class FakePartitionPublisher:
    """Model create, guarded replacement, no-op, and conflicts without AWS."""

    def __init__(self) -> None:
        self.locations: dict[tuple[str, str], str] = {}
        self.calls: list[tuple[str, str, str, str | None]] = []
        self.fail_before_mutation = False

    def publish(
        self,
        *,
        year: str,
        month: str,
        location: str,
        expected_current_location: str | None,
    ) -> str:
        self.calls.append((year, month, location, expected_current_location))
        identity = (year, month)
        current = self.locations.get(identity)
        if current == location:
            return "no_op"
        if self.fail_before_mutation:
            raise RuntimeError("injected publication failure")
        if current is None:
            if expected_current_location is not None:
                raise PublicationConflict("expected existing partition is absent")
            self.locations[identity] = location
            return "created"
        if expected_current_location != current:
            raise PublicationConflict("unexpected current partition location")
        self.locations[identity] = location
        return "updated"
