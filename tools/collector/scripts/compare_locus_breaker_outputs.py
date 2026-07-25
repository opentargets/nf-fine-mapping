#!/usr/bin/env python3
"""Compare collector and Gentropy LocusBreaker outputs by logical row values.

The collector rewrite writes one flat parquet file per study. Gentropy writes a
Hive-partitioned dataset under each study directory, with `studyLocusId` encoded
as a directory partition. This script normalizes both layouts before comparing.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

TOP_LEVEL_COLUMNS = (
    "studyLocusId",
    "studyId",
    "variantId",
    "chromosome",
    "position",
    "beta",
    "sampleSize",
    "pValueMantissa",
    "pValueExponent",
    "effectAlleleFrequencyFromSource",
    "standardError",
    "qualityControls",
    "locusStart",
    "locusEnd",
    "locus",
)

LOCUS_COMPARE_FIELDS = (
    "variantId",
    "beta",
    "pValueMantissa",
    "pValueExponent",
    "standardError",
)

COLLECTOR_NULL_LOCUS_FIELDS = (
    "is95CredibleSet",
    "is99CredibleSet",
    "logBF",
    "posteriorProbability",
    "r2Overall",
)

SORT_FIELDS = (
    "studyId",
    "chromosome_sort_group",
    "chromosome_sort_number",
    "chromosome",
    "locusStart",
    "locusEnd",
    "position",
    "variantId",
    "studyLocusId",
)

DIFF_KEY_FIELDS = (
    "studyLocusId",
    "studyId",
    "variantId",
    "chromosome",
    "position",
    "locusStart",
    "locusEnd",
)


@dataclass(frozen=True)
class DatasetPath:
    path: Path
    modified_ns: int


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def study_ids_from_manifest(manifest: Path) -> list[str]:
    with manifest.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        return [row["studyId"] for row in reader if row.get("studyId")]


def study_ids_from_outputs(collector_dir: Path, gentropy_index: dict[str, Path]) -> list[str]:
    collector_ids = {path.stem for path in collector_dir.glob("*.parquet")}
    gentropy_ids = set(gentropy_index)
    return sorted(collector_ids | gentropy_ids)


def dataset_modified_ns(path: Path) -> int:
    parquet_files_found = parquet_files(path)
    if parquet_files_found:
        return max(file.stat().st_mtime_ns for file in parquet_files_found)
    return path.stat().st_mtime_ns


def add_gentropy_dataset(index: dict[str, DatasetPath], study_id: str, path: Path) -> None:
    candidate = DatasetPath(path=path, modified_ns=dataset_modified_ns(path))
    current = index.get(study_id)
    if current is None or candidate.modified_ns > current.modified_ns:
        index[study_id] = candidate


def add_gentropy_root(index: dict[str, DatasetPath], root: Path) -> None:
    if not root.exists():
        return

    for study_path in sorted(path for path in root.iterdir() if path.is_dir()):
        if parquet_files(study_path):
            add_gentropy_dataset(index, study_path.name, study_path)


def gentropy_index_from_paths(gentropy_dir: Path | None, gentropy_work_dir: Path | None) -> dict[str, Path]:
    index: dict[str, DatasetPath] = {}

    if gentropy_dir is not None:
        add_gentropy_root(index, gentropy_dir)

    if gentropy_work_dir is not None and gentropy_work_dir.exists():
        for root in sorted(gentropy_work_dir.rglob("gentropy_locus_breaker_clumped_study_locus")):
            if root.is_dir():
                add_gentropy_root(index, root)

    return {study_id: dataset.path for study_id, dataset in sorted(index.items())}


def parquet_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(p for p in path.rglob("*.parquet") if not p.name.startswith("."))


def read_rows(con: duckdb.DuckDBPyConnection, path: Path, *, hive_partitioning: bool) -> list[dict[str, Any]]:
    files = parquet_files(path)
    if not files:
        raise FileNotFoundError(f"No parquet files found under {path}")

    files_sql = "[" + ", ".join(sql_literal(file.as_posix()) for file in files) + "]"
    hive_sql = "true" if hive_partitioning else "false"
    query = f"""
        SELECT *
        FROM read_parquet({files_sql}, hive_partitioning={hive_sql})
    """
    rows = con.execute(query).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row, strict=True)) for row in rows]


def is_nan(value: Any) -> bool:
    return isinstance(value, float) and math.isnan(value)


def normalize_scalar(value: Any) -> Any:
    if is_nan(value):
        return None
    if isinstance(value, float):
        return round(value, 10)
    return value


def normalize_quality_controls(value: Any) -> Any:
    if value is None:
        return None
    return sorted(str(item) for item in value)


def normalize_locus_entry(entry: Any, *, source: str) -> dict[str, Any]:
    if not isinstance(entry, dict):
        # DuckDB returns STRUCT values as dicts. Keep this explicit so any
        # backend representation change fails loudly in the diff.
        raise TypeError(f"Unexpected locus entry from {source}: {entry!r}")

    normalized = {field: normalize_scalar(entry.get(field)) for field in LOCUS_COMPARE_FIELDS}

    if source == "collector":
        non_null_extra = {
            field: normalize_scalar(entry.get(field)) for field in COLLECTOR_NULL_LOCUS_FIELDS if normalize_scalar(entry.get(field)) is not None
        }
        if non_null_extra:
            normalized["__unexpected_non_null_collector_fields"] = non_null_extra

    return normalized


def normalize_locus(value: Any, *, source: str) -> Any:
    if value is None:
        return None
    entries = [normalize_locus_entry(entry, source=source) for entry in value]
    return sorted(
        entries,
        key=lambda row: (
            row.get("variantId") or "",
            -999999 if row.get("pValueExponent") is None else row.get("pValueExponent"),
            -1.0 if row.get("pValueMantissa") is None else row.get("pValueMantissa"),
            -1e30 if row.get("beta") is None else row.get("beta"),
            -1e30 if row.get("standardError") is None else row.get("standardError"),
        ),
    )


def chromosome_sort_values(chromosome: Any) -> tuple[int, int | None]:
    if chromosome is None:
        return (1, None)
    try:
        return (0, int(str(chromosome)))
    except ValueError:
        return (1, None)


def normalize_row(row: dict[str, Any], *, source: str) -> dict[str, Any]:
    missing = [column for column in TOP_LEVEL_COLUMNS if column not in row]
    if missing:
        raise KeyError(f"{source} row is missing columns: {missing}")

    chrom_group, chrom_number = chromosome_sort_values(row.get("chromosome"))
    normalized = {column: normalize_scalar(row.get(column)) for column in TOP_LEVEL_COLUMNS if column not in {"qualityControls", "locus"}}
    normalized["qualityControls"] = normalize_quality_controls(row.get("qualityControls"))
    normalized["locus"] = normalize_locus(row.get("locus"), source=source)
    normalized["chromosome_sort_group"] = chrom_group
    normalized["chromosome_sort_number"] = chrom_number
    return normalized


def sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(field) for field in SORT_FIELDS)


def normalized_rows(rows: Iterable[dict[str, Any]], *, source: str) -> list[dict[str, Any]]:
    normalized = [normalize_row(row, source=source) for row in rows]
    return sorted(normalized, key=sort_key)


def comparable_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: value for key, value in row.items() if key not in {"chromosome_sort_group", "chromosome_sort_number"}} for row in rows]


def values_equal(left: Any, right: Any, *, float_abs_tolerance: float) -> bool:
    if left == right:
        return True
    if isinstance(left, float) and isinstance(right, float):
        return abs(left - right) <= float_abs_tolerance
    return False


def first_difference(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
    *,
    float_abs_tolerance: float,
) -> dict[str, Any] | None:
    if len(left) != len(right):
        return {"type": "row_count", "collector": len(left), "gentropy": len(right)}

    for index, (collector_row, gentropy_row) in enumerate(zip(left, right, strict=True)):
        fields = sorted(set(collector_row) | set(gentropy_row))
        differences = {
            field: {
                "collector": collector_row.get(field),
                "gentropy": gentropy_row.get(field),
                "absolute_difference": abs(collector_row.get(field) - gentropy_row.get(field))
                if isinstance(collector_row.get(field), float) and isinstance(gentropy_row.get(field), float)
                else None,
            }
            for field in fields
            if not values_equal(collector_row.get(field), gentropy_row.get(field), float_abs_tolerance=float_abs_tolerance)
        }
        if differences:
            return {
                "type": "row_value",
                "index": index,
                "key": {field: collector_row.get(field) for field in DIFF_KEY_FIELDS},
                "differences": differences,
            }
    return None


def compare_study(
    con: duckdb.DuckDBPyConnection,
    collector_path: Path,
    gentropy_path: Path,
    *,
    float_abs_tolerance: float,
) -> dict[str, Any]:
    collector_rows = comparable_rows(normalized_rows(read_rows(con, collector_path, hive_partitioning=False), source="collector"))
    gentropy_rows = comparable_rows(normalized_rows(read_rows(con, gentropy_path, hive_partitioning=True), source="gentropy"))
    difference = first_difference(collector_rows, gentropy_rows, float_abs_tolerance=float_abs_tolerance)
    return {
        "collector_rows": len(collector_rows),
        "gentropy_rows": len(gentropy_rows),
        "match": difference is None,
        "difference": difference,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collector-dir", type=Path, required=True, help="Directory containing collector flat <studyId>.parquet files")
    parser.add_argument("--gentropy-dir", type=Path, help="Directory containing Gentropy per-study partitioned datasets")
    parser.add_argument(
        "--gentropy-work-dir",
        type=Path,
        help="Optional Nextflow work directory used to discover unpublished Gentropy per-task datasets",
    )
    parser.add_argument("--manifest", type=Path, help="Optional manifest TSV used to define studyIds to compare")
    parser.add_argument("--study-id", action="append", help="Study ID to compare. Can be repeated. Overrides manifest discovery.")
    parser.add_argument("--json", type=Path, help="Optional path to write full JSON comparison report")
    parser.add_argument("--max-diff-json-chars", type=int, default=6000, help="Maximum inline JSON chars per failed study on stdout")
    parser.add_argument(
        "--float-abs-tolerance",
        type=float,
        default=0.0,
        help="Absolute tolerance for comparing FLOAT/DOUBLE values. Defaults to strict equality.",
    )
    args = parser.parse_args()

    gentropy_index = gentropy_index_from_paths(args.gentropy_dir, args.gentropy_work_dir)
    if not gentropy_index:
        parser.error("No Gentropy datasets found. Provide --gentropy-dir and/or --gentropy-work-dir.")

    if args.study_id:
        study_ids = sorted(args.study_id)
    elif args.manifest:
        study_ids = study_ids_from_manifest(args.manifest)
    else:
        study_ids = study_ids_from_outputs(args.collector_dir, gentropy_index)

    report: dict[str, Any] = {"studies": {}, "summary": {"checked": 0, "matched": 0, "failed": 0, "missing": 0}}
    report["float_abs_tolerance"] = args.float_abs_tolerance
    con = duckdb.connect()
    try:
        for study_id in study_ids:
            collector_path = args.collector_dir / f"{study_id}.parquet"
            gentropy_path = gentropy_index.get(study_id)
            study_report: dict[str, Any]

            if not collector_path.exists() or gentropy_path is None:
                study_report = {
                    "match": False,
                    "missing": {
                        "collector": not collector_path.exists(),
                        "gentropy": gentropy_path is None,
                    },
                }
                report["summary"]["missing"] += 1
            else:
                try:
                    study_report = compare_study(
                        con,
                        collector_path,
                        gentropy_path,
                        float_abs_tolerance=args.float_abs_tolerance,
                    )
                except Exception as error:  # noqa: BLE001 - this is a diagnostic script
                    study_report = {"match": False, "error": repr(error)}

            report["studies"][study_id] = study_report
            report["summary"]["checked"] += 1
            if study_report.get("match"):
                report["summary"]["matched"] += 1
                print(f"PASS {study_id}: {study_report['collector_rows']} rows")
            else:
                report["summary"]["failed"] += 1
                diff_text = json.dumps(study_report, indent=2, sort_keys=True)
                if len(diff_text) > args.max_diff_json_chars:
                    diff_text = diff_text[: args.max_diff_json_chars] + "\n... <truncated>"
                print(f"FAIL {study_id}: {diff_text}")
    finally:
        con.close()

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(report, indent=2, sort_keys=True))

    print("SUMMARY " + json.dumps(report["summary"], sort_keys=True))
    return 0 if report["summary"]["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
