from __future__ import annotations

from types import SimpleNamespace

import pytest

from metropulse.audit_models import (
    AuditBounds,
    AuditMonthResult,
    PinnedObject,
    ScheduledAuditResult,
)
from metropulse.aws.audit_lambda_handler import AuditInvocationFailure
from metropulse.aws.audit_lambda_handler import handle_event as handle_audit
from metropulse.aws.compaction_lambda_handler import CompactionInvocationError
from metropulse.aws.compaction_lambda_handler import handle_event as handle_compaction
from metropulse.aws.operations_config import OperationsConfig
from metropulse.aws.operations_observability import OperationsObserver
from metropulse.aws.testing import InMemoryObjectStorage
from metropulse.compaction_models import CompactionBounds
from metropulse.selection_approval import selection_document_bytes


def _config(tmp_path) -> OperationsConfig:
    return OperationsConfig(
        environment="test",
        component_version="7.0.0-test",
        destination_bucket="lake",
        glue_database="metropulse_test",
        glue_table="metropt3_curated",
        compaction_bounds=CompactionBounds(),
        audit_bounds=AuditBounds(),
        audit_temp_directory=tmp_path,
    )


def test_compaction_handler_validates_pin_and_uses_unique_request_owner(
    compact_fixture, tmp_path, monkeypatch
) -> None:
    selection, _ = compact_fixture()
    run_id, key, body = selection_document_bytes(selection)
    storage = InMemoryObjectStorage()
    storage.seed("lake", key, body)
    calls: list[str] = []

    class FakeProcessor:
        def __init__(self, *_args, **_kwargs):
            pass

        def process(self, _selection, **kwargs):
            calls.append(kwargs["owner_token"])
            return {
                "location": "s3://lake/curated/",
                "newly_completed": False,
                "newly_published": False,
                "status": "verified_no_op",
            }

    monkeypatch.setattr(
        "metropulse.aws.compaction_lambda_handler.S3MonthlyCompactionProcessor", FakeProcessor
    )
    event = {
        "contract": "metropulse-compaction-invocation-v1",
        "month": "02",
        "processing_timestamp": "2026-09-18T00:00:00Z",
        "selection": {
            "bucket": "lake",
            "identity_kind": "sha256",
            "identity_value": __import__("hashlib").sha256(body).hexdigest(),
            "key": key,
        },
        "year": "2020",
    }
    records: list[dict[str, object]] = []
    observer = OperationsObserver("test", "7.0.0-test", records.append)
    for request_id in ("request-a", "request-b"):
        result = handle_compaction(
            event,
            SimpleNamespace(aws_request_id=request_id),
            config=_config(tmp_path),
            storage=storage,
            publisher=object(),
            observer=observer,
            monotonic=iter((1.0, 1.1)).__next__,
        )
        assert result["run_id"] == run_id
    assert calls == ["compaction:request-a", "compaction:request-b"]
    assert len(set(calls)) == 2
    dimensions = [
        item["_aws"]["CloudWatchMetrics"][0]["Dimensions"] for item in records if "_aws" in item
    ]
    assert dimensions and all(
        value == [["Environment", "ComponentVersion"]] for value in dimensions
    )


def test_compaction_handler_rejects_inline_or_unpinned_selection(tmp_path) -> None:
    observer = OperationsObserver("test", "7", lambda _value: None)
    with pytest.raises(CompactionInvocationError):
        handle_compaction(
            {
                "contract": "metropulse-compaction-invocation-v1",
                "month": "02",
                "processing_timestamp": "2026-09-18T00:00:00Z",
                "selection_document": {},
                "selection": {},
                "year": "2020",
            },
            SimpleNamespace(aws_request_id="request"),
            config=_config(tmp_path),
            storage=InMemoryObjectStorage(),
            publisher=object(),
            observer=observer,
            monotonic=iter((1.0, 1.1)).__next__,
        )


class _AuditProcessor:
    def __init__(self, result: ScheduledAuditResult) -> None:
        self.result = result
        self.references: list[PinnedObject] = []

    def process(self, reference: PinnedObject) -> ScheduledAuditResult:
        self.references.append(reference)
        return self.result


def _audit_result(*, failed: bool = False) -> ScheduledAuditResult:
    return ScheduledAuditResult(
        inventory_id="a" * 64,
        month_results=(AuditMonthResult("2020", "02", "b" * 64, 10, 100, "passed", ()),),
        fatal_failures=("missing object",) if failed else (),
        reconciliation_failures=(),
        publication_drift=(),
        quality_observations=({"kind": "known_gap"},),
        months_inspected=8,
        total_curated_rows=1_516_948,
        within_month_gap_count=327,
        cross_month_gap_count=4,
        global_gap_count=331,
        overlap_count=0,
        reversed_boundary_count=0,
        status="failed" if failed else "passed",
    )


def _audit_event() -> dict[str, object]:
    identity = "a" * 64
    return {
        "audit_time": "2026-09-18T00:00:00Z",
        "contract": "metropulse-scheduled-audit-invocation-v1",
        "inventory": {
            "bucket": "lake",
            "identity_kind": "sha256",
            "identity_value": "b" * 64,
            "key": f"control/audit/source=metropt3/inventory_id={identity}/inventory.json",
        },
        "verification_mode": "full_checksum",
    }


def test_known_331_gaps_are_observations_not_operational_failure(tmp_path) -> None:
    records: list[dict[str, object]] = []
    result = handle_audit(
        _audit_event(),
        SimpleNamespace(aws_request_id="audit-request"),
        config=_config(tmp_path),
        processor=_AuditProcessor(_audit_result()),
        observer=OperationsObserver("test", "7", records.append),
        monotonic=iter((1.0, 1.2)).__next__,
    )
    assert result["global_gap_count"] == 331
    metric = next(item for item in records if "_aws" in item)
    assert metric["AuditRunsFailed"] == 0
    assert metric["AuditRunsSucceeded"] == 1
    assert metric["CrossMonthSignificantGaps"] == 4
    assert metric["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [
        ["Environment", "ComponentVersion"]
    ]


def test_audit_failure_is_raised_only_after_metrics_and_logs(tmp_path) -> None:
    records: list[dict[str, object]] = []
    with pytest.raises(AuditInvocationFailure):
        handle_audit(
            _audit_event(),
            SimpleNamespace(aws_request_id="audit-request"),
            config=_config(tmp_path),
            processor=_AuditProcessor(_audit_result(failed=True)),
            observer=OperationsObserver("test", "7", records.append),
            monotonic=iter((1.0, 1.2)).__next__,
        )
    assert any(item.get("AuditRunsFailed") == 1 for item in records)
    assert records[-1]["event_name"] == "audit_failed"


def test_scheduler_context_is_optional_but_complete_when_present(tmp_path) -> None:
    event = _audit_event()
    event.pop("audit_time")
    event.update(
        {
            "attempt_number": "1",
            "execution_id": "execution",
            "schedule_arn": "arn:aws:scheduler:region:account:schedule/default/audit",
            "scheduled_time": "2026-09-18T06:00:00Z",
        }
    )
    result = handle_audit(
        event,
        SimpleNamespace(aws_request_id="audit-request"),
        config=_config(tmp_path),
        processor=_AuditProcessor(_audit_result()),
        observer=OperationsObserver("test", "7", lambda _value: None),
        monotonic=iter((1.0, 1.2)).__next__,
    )
    assert result["outcome"] == "passed"
