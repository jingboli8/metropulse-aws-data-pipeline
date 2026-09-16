from __future__ import annotations

from dataclasses import replace

from metropulse.compaction_identity import WriterContract, monthly_run_id, run_identity_payload


def test_run_id_is_order_independent_and_canonical(compact_fixture) -> None:
    selection, _ = compact_fixture()
    shuffled = replace(selection, inputs=tuple(reversed(selection.inputs)))
    assert monthly_run_id(selection) == monthly_run_id(shuffled)
    assert len(monthly_run_id(selection)) == 64
    assert run_identity_payload(selection)["contract"] == "metropulse-monthly-compaction-run-v1"
    payload = run_identity_payload(selection)
    assert set(payload) == {
        "compactor_version",
        "contract",
        "inputs",
        "manifest_versions",
        "month",
        "output_contract_version",
        "pipeline_versions",
        "schema_versions",
        "source_name",
        "writer_contract",
        "year",
    }
    assert set(payload["inputs"][0]) == {
        "completion_marker",
        "first_valid_timestamp",
        "input_row_count",
        "last_valid_timestamp",
        "manifest_version",
        "pipeline_version",
        "processing_identity",
        "quarantine_row_count",
        "schema_version",
        "source_date",
        "staging",
        "valid_row_count",
    }
    assert set(payload["writer_contract"]) == {
        "allow_truncated_timestamps",
        "coerce_timestamps",
        "compression",
        "parquet_format_version",
        "pyarrow_version",
        "row_group_size",
        "timestamp_unit",
        "use_dictionary",
        "write_statistics",
    }


def test_every_material_identity_group_changes_run_id(compact_fixture) -> None:
    selection, _ = compact_fixture()
    original = monthly_run_id(selection)
    item = selection.inputs[0]
    changes = [
        replace(item, processing_identity="changed"),
        replace(item, completion_marker_identity_value="c" * 64),
        replace(item, staging_key="staging/changed.parquet"),
        replace(item, staging_sha256="d" * 64),
        replace(item, staging_byte_size=item.staging_byte_size + 1),
        replace(item, valid_row_count=2, input_row_count=2),
        replace(item, pipeline_version="2.0.1"),
    ]
    for changed in changes:
        assert (
            monthly_run_id(replace(selection, inputs=(changed, *selection.inputs[1:]))) != original
        )
    assert monthly_run_id(selection, compactor_version="2") != original
    assert monthly_run_id(selection, output_contract_version="2") != original
    assert (
        monthly_run_id(selection, writer_contract=replace(WriterContract(), row_group_size=32))
        != original
    )
