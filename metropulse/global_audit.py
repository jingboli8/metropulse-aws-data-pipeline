"""Cross-partition timestamp-continuity audit for completed daily manifests."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

from metropulse.local_io import write_text_atomic

GLOBAL_AUDIT_VERSION = "1.0.0"
EXPECTED_INTERVAL_MIN_SECONDS = 9
EXPECTED_INTERVAL_MAX_SECONDS = 13
SIGNIFICANT_GAP_SECONDS = 60


class GlobalAuditError(ValueError):
    """Daily manifests cannot form one trustworthy ordered audit."""


def _distribution_items(distribution: Counter[int]) -> list[dict[str, int]]:
    return [
        {"count": distribution[seconds], "seconds": seconds} for seconds in sorted(distribution)
    ]


def _interval_summary(distribution: Counter[int]) -> dict[str, int | float | None]:
    significant = [
        seconds
        for seconds, count in sorted(distribution.items())
        if seconds > SIGNIFICANT_GAP_SECONDS
        for _ in range(count)
    ]
    return {
        "abnormal_positive_interval_count": sum(
            count
            for seconds, count in distribution.items()
            if seconds > 0
            and not EXPECTED_INTERVAL_MIN_SECONDS <= seconds <= EXPECTED_INTERVAL_MAX_SECONDS
        ),
        "maximum_significant_gap_seconds": max(significant, default=None),
        "median_significant_gap_seconds": (statistics.median(significant) if significant else None),
        "minimum_significant_gap_seconds": min(significant, default=None),
        "significant_gap_count": len(significant),
        "total_delta_count": sum(distribution.values()),
    }


def _load_manifests(output_root: Path) -> list[dict[str, Any]]:
    paths = sorted(output_root.glob("control/source=metropt3/source_date=*/manifest.json"))
    manifests: list[dict[str, Any]] = []
    for path in paths:
        try:
            manifest = json.loads(path.read_text(encoding="utf-8"))
            date.fromisoformat(manifest["source_date"])
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise GlobalAuditError(f"Cannot read daily manifest {path.name}: {error}") from error
        manifests.append(manifest)
    manifests.sort(key=lambda item: item["source_date"])
    if not manifests:
        raise GlobalAuditError("No daily manifests were found.")
    dates = [manifest["source_date"] for manifest in manifests]
    if len(dates) != len(set(dates)):
        raise GlobalAuditError("Duplicate source dates were found in daily manifests.")
    return manifests


def _single_version(manifests: list[dict[str, Any]], field: str) -> str:
    versions = {str(manifest.get(field)) for manifest in manifests}
    if len(versions) != 1 or "None" in versions:
        raise GlobalAuditError(f"Daily manifests do not share one {field}.")
    return versions.pop()


def audit_partition_continuity(
    output_root: Path,
    *,
    expected_global_gap_count: int | None = None,
) -> dict[str, Any]:
    """Compose daily distributions with ordered boundary deltas without changing rows."""
    manifests = _load_manifests(output_root)
    pipeline_version = _single_version(manifests, "pipeline_version")
    schema_version = _single_version(manifests, "schema_version")
    within_distribution: Counter[int] = Counter()
    boundary_distribution: Counter[int] = Counter()
    source_row_count = 0
    valid_timestamp_count = 0
    consistency_findings: list[str] = []
    empty_partitions: list[str] = []

    for manifest in manifests:
        source_date = str(manifest["source_date"])
        source_row_count += int(manifest["input_row_count"])
        daily_valid_count = int(manifest.get("valid_timestamp_count", -1))
        valid_timestamp_count += max(daily_valid_count, 0)
        if daily_valid_count == 0:
            empty_partitions.append(source_date)
        if daily_valid_count != int(manifest["valid_row_count"]):
            consistency_findings.append(f"{source_date}: valid timestamp count mismatch")
        distribution = manifest["batch_metrics"]["sampling_interval_distribution"]
        for item in distribution:
            within_distribution[int(item["seconds"])] += int(item["count"])
        expected_within_count = max(
            int(manifest["batch_metrics"]["parseable_timestamp_count"]) - 1,
            0,
        )
        observed_within_count = sum(int(item["count"]) for item in distribution)
        if expected_within_count != observed_within_count:
            consistency_findings.append(f"{source_date}: within delta count mismatch")

    boundary_findings: list[dict[str, object]] = []
    missing_calendar_date_findings: list[dict[str, object]] = []
    overlap_findings: list[dict[str, object]] = []
    boundary_warnings: list[dict[str, object]] = []
    unassessable_boundary_count = 0
    for previous, current in zip(manifests, manifests[1:], strict=False):
        previous_date = date.fromisoformat(previous["source_date"])
        current_date = date.fromisoformat(current["source_date"])
        missing_date_count = max((current_date - previous_date).days - 1, 0)
        if missing_date_count:
            finding = {
                "current_source_date": current_date.isoformat(),
                "missing_calendar_date_count": missing_date_count,
                "previous_source_date": previous_date.isoformat(),
            }
            missing_calendar_date_findings.append(finding)
            boundary_warnings.append({"rule_id": "AUDIT_MISSING_CALENDAR_DATE", **finding})

        previous_last = previous.get("last_valid_event_timestamp_local")
        current_first = current.get("first_valid_event_timestamp_local")
        finding: dict[str, object] = {
            "current_source_date": current_date.isoformat(),
            "missing_calendar_date_count": missing_date_count,
            "previous_source_date": previous_date.isoformat(),
        }
        if previous_last is None or current_first is None:
            finding.update(
                {
                    "delta_seconds": None,
                    "status": "unassessable_empty_or_fully_quarantined_partition",
                }
            )
            unassessable_boundary_count += 1
            boundary_warnings.append({"rule_id": "AUDIT_BOUNDARY_UNASSESSABLE", **finding})
            boundary_findings.append(finding)
            continue
        previous_timestamp = datetime.fromisoformat(str(previous_last))
        current_timestamp = datetime.fromisoformat(str(current_first))
        if previous_timestamp.tzinfo is not None or current_timestamp.tzinfo is not None:
            raise GlobalAuditError("Boundary timestamps must remain timezone-naive.")
        delta = int((current_timestamp - previous_timestamp).total_seconds())
        boundary_distribution[delta] += 1
        status = "normal"
        if delta < 0:
            status = "reversed_overlap"
        elif delta == 0:
            status = "overlap"
        elif delta > SIGNIFICANT_GAP_SECONDS:
            status = "significant_gap"
        elif not EXPECTED_INTERVAL_MIN_SECONDS <= delta <= EXPECTED_INTERVAL_MAX_SECONDS:
            status = "abnormal_positive_interval"
        finding.update({"delta_seconds": delta, "status": status})
        boundary_findings.append(finding)
        if delta <= 0:
            overlap_findings.append(finding)
            boundary_warnings.append({"rule_id": "AUDIT_BOUNDARY_OVERLAP", **finding})
        if delta > SIGNIFICANT_GAP_SECONDS:
            boundary_warnings.append({"rule_id": "AUDIT_BOUNDARY_SIGNIFICANT_GAP", **finding})

    global_distribution = within_distribution + boundary_distribution
    within_summary = _interval_summary(within_distribution)
    boundary_summary = _interval_summary(boundary_distribution)
    global_summary = _interval_summary(global_distribution)
    delta_counts_match = global_summary["total_delta_count"] == (
        within_summary["total_delta_count"] + boundary_summary["total_delta_count"]
    )
    abnormal_counts_match = global_summary["abnormal_positive_interval_count"] == (
        within_summary["abnormal_positive_interval_count"]
        + boundary_summary["abnormal_positive_interval_count"]
    )
    gap_counts_match = global_summary["significant_gap_count"] == (
        within_summary["significant_gap_count"] + boundary_summary["significant_gap_count"]
    )
    expected_gap_matches = (
        expected_global_gap_count is None
        or global_summary["significant_gap_count"] == expected_global_gap_count
    )
    reconciliation_passes = (
        delta_counts_match
        and abnormal_counts_match
        and gap_counts_match
        and expected_gap_matches
        and not consistency_findings
    )
    if not reconciliation_passes:
        status = "failed"
    elif unassessable_boundary_count:
        status = "incomplete"
    else:
        status = "passed"

    return {
        "audit_version": GLOBAL_AUDIT_VERSION,
        "boundary_findings": boundary_findings,
        "boundary_interval_metrics": boundary_summary,
        "boundary_sampling_distribution": _distribution_items(boundary_distribution),
        "boundary_warnings": boundary_warnings,
        "empty_or_fully_quarantined_partitions": empty_partitions,
        "global_interval_metrics": global_summary,
        "global_sampling_distribution": _distribution_items(global_distribution),
        "missing_calendar_date_findings": missing_calendar_date_findings,
        "ordered_date_range": {
            "end": manifests[-1]["source_date"],
            "start": manifests[0]["source_date"],
        },
        "overlapping_or_reversed_boundary_findings": overlap_findings,
        "partition_count": len(manifests),
        "pipeline_version": pipeline_version,
        "reconciliation": {
            "abnormal_interval_counts_match": abnormal_counts_match,
            "consistency_findings": consistency_findings,
            "delta_counts_match": delta_counts_match,
            "expected_global_gap_count": expected_global_gap_count,
            "expected_global_gap_count_matches": expected_gap_matches,
            "significant_gap_counts_match": gap_counts_match,
            "status": status,
            "unassessable_boundary_count": unassessable_boundary_count,
        },
        "schema_version": schema_version,
        "source_row_count": source_row_count,
        "valid_timestamp_count": valid_timestamp_count,
        "within_partition_interval_metrics": within_summary,
        "within_partition_sampling_distribution": _distribution_items(within_distribution),
    }


def global_audit_text(report: Mapping[str, object]) -> str:
    """Serialize a global audit deterministically."""
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def main() -> int:
    """Write an ignored machine-readable cross-partition audit report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=Path("data/local-lake"))
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--expected-global-gap-count", type=int)
    arguments = parser.parse_args()
    try:
        report = audit_partition_continuity(
            arguments.output_root,
            expected_global_gap_count=arguments.expected_global_gap_count,
        )
    except GlobalAuditError as error:
        parser.error(str(error))
    write_text_atomic(arguments.evidence, global_audit_text(report))
    print(global_audit_text(report), end="")
    return 0 if report["reconciliation"]["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
