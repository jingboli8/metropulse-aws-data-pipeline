"""Thin, injected Glue partition publisher with explicit conflict semantics."""

from __future__ import annotations

from contextlib import suppress
from copy import deepcopy
from typing import Any

from metropulse.compaction_models import PublicationConflict
from metropulse.schema import FIELD_DEFINITIONS


class GluePartitionPublisher:
    """Create or update one year/month partition through an injected Glue client."""

    def __init__(self, client: Any, *, database: str, table: str) -> None:
        self.client = client
        self.database = database
        self.table = table

    def publish(
        self,
        *,
        year: str,
        month: str,
        location: str,
        expected_current_location: str | None,
    ) -> str:
        """Publish exact values/location, returning created, updated, or no_op."""
        _validate_location(year, month, location)
        table = self.client.get_table(DatabaseName=self.database, Name=self.table)["Table"]
        if (
            table.get("Name", self.table) != self.table
            or table.get("DatabaseName", self.database) != self.database
        ):
            raise PublicationConflict("Glue table identity does not match the configured target")
        _validate_table(table)
        values = [year, month]
        current = self._get_partition(values)
        if current is not None:
            current_location = current["StorageDescriptor"]["Location"]
            if current_location == location:
                return "no_op"
            if expected_current_location is None or current_location != expected_current_location:
                raise PublicationConflict("Glue partition has an unexpected current location")
            descriptor = deepcopy(current["StorageDescriptor"])
            descriptor["Location"] = location
            with suppress(Exception):
                # Resolve an ambiguous response only through the exact read-back below.
                self.client.update_partition(
                    DatabaseName=self.database,
                    TableName=self.table,
                    PartitionValueList=values,
                    PartitionInput={"StorageDescriptor": descriptor, "Values": values},
                )
            outcome = "updated"
        else:
            if expected_current_location is not None:
                raise PublicationConflict("expected an existing Glue partition")
            descriptor = deepcopy(table["StorageDescriptor"])
            descriptor["Location"] = location
            try:
                self.client.create_partition(
                    DatabaseName=self.database,
                    TableName=self.table,
                    PartitionInput={"StorageDescriptor": descriptor, "Values": values},
                )
                outcome = "created"
            except Exception:
                # Create races and ambiguous responses are accepted only after exact read-back.
                outcome = "created"
        observed = self._get_partition(values)
        if observed is None or observed.get("Values") != values:
            raise PublicationConflict("Glue partition read-back is missing or has wrong values")
        if observed["StorageDescriptor"].get("Location") != location:
            raise PublicationConflict("Glue partition read-back has an unexpected location")
        return outcome

    def _get_partition(self, values: list[str]) -> dict[str, Any] | None:
        try:
            return self.client.get_partition(
                DatabaseName=self.database, TableName=self.table, PartitionValues=values
            )["Partition"]
        except Exception as error:
            code = getattr(error, "response", {}).get("Error", {}).get("Code")
            if code in {"EntityNotFoundException", "404"}:
                return None
            raise


class GluePartitionReader:
    """Read exact Glue year/month partitions through an injected client."""

    def __init__(self, client: Any, *, database: str, table: str) -> None:
        self.client = client
        self.database = database
        self.table = table

    def get_partition(self, *, year: str, month: str) -> dict[str, Any] | None:
        """Return one exact partition after validating the configured table schema."""
        table = self.client.get_table(DatabaseName=self.database, Name=self.table)["Table"]
        _validate_table(table)
        try:
            return self.client.get_partition(
                DatabaseName=self.database,
                TableName=self.table,
                PartitionValues=[year, month],
            )["Partition"]
        except Exception as error:
            code = getattr(error, "response", {}).get("Error", {}).get("Code")
            if code in {"EntityNotFoundException", "404"}:
                return None
            raise


def _validate_table(table: dict[str, Any]) -> None:
    expected_columns = [
        {"Name": field.normalized_name, "Type": field.athena_type} for field in FIELD_DEFINITIONS
    ]
    actual_columns = [
        {"Name": value.get("Name"), "Type": value.get("Type")}
        for value in table.get("StorageDescriptor", {}).get("Columns", [])
    ]
    partitions = [
        {"Name": value.get("Name"), "Type": value.get("Type")}
        for value in table.get("PartitionKeys", [])
    ]
    if actual_columns != expected_columns or partitions != [
        {"Name": "year", "Type": "string"},
        {"Name": "month", "Type": "string"},
    ]:
        raise PublicationConflict("Glue table schema does not match the curated contract")


def _validate_location(year: str, month: str, location: str) -> None:
    expected = f"/year={year}/month={month}/run_id="
    if not location.startswith("s3://") or expected not in location or not location.endswith("/"):
        raise PublicationConflict("Glue partition location is not an exact completed run directory")
