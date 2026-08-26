"""Emit JSONL status when a parquet dataset is logically empty."""

from __future__ import annotations

import json
from enum import StrEnum
from pathlib import Path

import duckdb


class ValidationStage(StrEnum):
    """Supported validation stages for empty-dataset status records."""

    MANIFEST = "MANIFEST"
    LOCUS_BREAKER = "LOCUS_BREAKER"
    LOCUS_COLLECTION = "LOCUS_COLLECTION"


class EmptyDatasetReason(StrEnum):
    """Reasons emitted by the empty-status checker."""

    EMPTY_DATASET = "EMPTY_DATASET"


def emit_empty_status(
    run_id: str,
    path: Path,
    validation_stage: ValidationStage,
    logical_path: str | None = None,
) -> str | None:
    """Return a JSONL status record when the parquet dataset has zero logical rows."""
    row_count = parquet_row_count(path)
    if row_count > 0:
        return None

    return json.dumps(
        {
            "runId": run_id,
            "path": logical_path or str(path),
            "validationStage": validation_stage.value,
            "reason": EmptyDatasetReason.EMPTY_DATASET.value,
        }
    )


def parquet_row_count(path: Path) -> int:
    """Count logical rows from parquet metadata for a flat file or partitioned directory."""
    parquet_inputs = _parquet_metadata_inputs(path)
    if not parquet_inputs:
        return 0
    try:
        with duckdb.connect() as con:
            row = con.execute(
                """
                SELECT coalesce(sum(row_group_num_rows), 0)::BIGINT
                FROM (
                    SELECT file_name, row_group_id, max(row_group_num_rows) AS row_group_num_rows
                    FROM parquet_metadata(?)
                    GROUP BY 1, 2
                )
                """,
                [parquet_inputs],
            ).fetchone()
    except duckdb.Error as error:
        raise RuntimeError(f"Unable to inspect parquet metadata for {path}: {error}") from error

    if row is None:
        raise RuntimeError(f"DuckDB returned no parquet metadata rows for {path}")
    return int(row[0])


def _parquet_metadata_inputs(path: Path) -> list[str]:
    """Return parquet file paths suitable for DuckDB parquet_metadata()."""
    if not path.exists():
        raise FileNotFoundError(f"Input path {path} does not exist")
    if path.is_file():
        return [path.as_posix()]
    if not path.is_dir():
        raise ValueError(f"Input path {path} must be a parquet file or directory")

    return sorted(file.as_posix() for file in path.rglob("*.parquet") if file.is_file())
