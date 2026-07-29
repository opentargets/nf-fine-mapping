"""Compare Hailing Ducks and Gentropy pairwise-LD datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
from pydantic import BaseModel, ConfigDict, Field


class LdParityConfig(BaseModel):
    """Inputs and tolerance for an LD parity report."""

    model_config = ConfigDict(frozen=True)

    hailing_path: Path
    gentropy_path: Path
    report_path: Path
    hailing_stats_path: Path | None = None
    gentropy_stats_path: Path | None = None
    tolerance: float = Field(default=1e-8, ge=0)


def _quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parquet_glob(path: Path) -> str:
    return (path / "**" / "*.parquet").as_posix() if path.is_dir() else path.as_posix()


def _declared_ancestries(path: Path | None) -> set[str]:
    if path is None:
        return set()
    ancestries: set[str] = set()
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        ancestry = record.get("ancestry")
        if isinstance(ancestry, str):
            ancestries.add(ancestry)
    return ancestries


def _normalised_variant_sql(column: str) -> str:
    return f"CASE WHEN starts_with(CAST({column} AS VARCHAR), 'chr') THEN substr(CAST({column} AS VARCHAR), 4) ELSE CAST({column} AS VARCHAR) END"


def _create_normalised_table(
    con: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    source_path: Path,
) -> None:
    variant_i = _normalised_variant_sql("variantIdI")
    variant_j = _normalised_variant_sql("variantIdJ")
    source = _quote_sql_string(_parquet_glob(source_path))
    con.execute(
        f"""
        CREATE TEMP TABLE {table_name} AS
        WITH normalised AS (
            SELECT
                CAST(ancestry AS VARCHAR) AS ancestry,
                least({variant_i}, {variant_j}) AS variantIdI,
                greatest({variant_i}, {variant_j}) AS variantIdJ,
                CAST(r AS DOUBLE) AS r
            FROM read_parquet({source}, union_by_name = true, hive_partitioning = true)
        )
        SELECT ancestry, variantIdI, variantIdJ, min(r) AS r
        FROM normalised
        GROUP BY ancestry, variantIdI, variantIdJ
        HAVING count(DISTINCT r) = 1
        """
    )
    conflict = con.execute(
        f"""
        WITH normalised AS (
            SELECT
                CAST(ancestry AS VARCHAR) AS ancestry,
                least({variant_i}, {variant_j}) AS variantIdI,
                greatest({variant_i}, {variant_j}) AS variantIdJ,
                CAST(r AS DOUBLE) AS r
            FROM read_parquet({source}, union_by_name = true, hive_partitioning = true)
        )
        SELECT ancestry, variantIdI, variantIdJ
        FROM normalised
        GROUP BY ancestry, variantIdI, variantIdJ
        HAVING count(DISTINCT r) > 1
        LIMIT 1
        """
    ).fetchone()
    if conflict is not None:
        ancestry, variant_id_i, variant_id_j = conflict
        raise RuntimeError(f"Conflicting LD values in {source_path} for {ancestry}: {variant_id_i}, {variant_id_j}")


def _comparison_rows(con: duckdb.DuckDBPyConnection, tolerance: float) -> list[tuple[Any, ...]]:
    return con.execute(
        """
        WITH compared AS (
            SELECT
                coalesce(h.ancestry, g.ancestry) AS ancestry,
                coalesce(h.variantIdI, g.variantIdI) AS variantIdI,
                coalesce(h.variantIdJ, g.variantIdJ) AS variantIdJ,
                h.r AS hailing_r,
                g.r AS gentropy_r
            FROM hailing AS h
            FULL OUTER JOIN gentropy AS g
              USING (ancestry, variantIdI, variantIdJ)
        )
        SELECT
            ancestry,
            count(hailing_r)::BIGINT AS hailing_pairs,
            count(gentropy_r)::BIGINT AS gentropy_pairs,
            count(*) FILTER (WHERE hailing_r IS NOT NULL AND gentropy_r IS NOT NULL)::BIGINT AS shared_pairs,
            count(*) FILTER (WHERE hailing_r IS NOT NULL AND gentropy_r IS NULL)::BIGINT AS hailing_only_pairs,
            count(*) FILTER (WHERE hailing_r IS NULL AND gentropy_r IS NOT NULL)::BIGINT AS gentropy_only_pairs,
            count(*) FILTER (
                WHERE hailing_r IS NOT NULL
                  AND gentropy_r IS NOT NULL
                  AND abs(hailing_r - gentropy_r) > ?
            )::BIGINT AS shared_value_mismatches,
            max(abs(hailing_r - gentropy_r)) FILTER (
                WHERE hailing_r IS NOT NULL AND gentropy_r IS NOT NULL
            ) AS max_abs_ld_difference
        FROM compared
        GROUP BY ancestry
        ORDER BY ancestry
        """,
        [tolerance],
    ).fetchall()


def _diagonal_differences(con: duckdb.DuckDBPyConnection, tolerance: float) -> dict[str, int]:
    row = con.execute(
        """
        WITH compared AS (
            SELECT
                coalesce(h.variantIdI, g.variantIdI) AS variantIdI,
                coalesce(h.variantIdJ, g.variantIdJ) AS variantIdJ,
                h.r AS hailing_r,
                g.r AS gentropy_r
            FROM hailing AS h
            FULL OUTER JOIN gentropy AS g
              USING (ancestry, variantIdI, variantIdJ)
        )
        SELECT
            count(*) FILTER (
                WHERE variantIdI = variantIdJ AND hailing_r IS NOT NULL AND gentropy_r IS NULL
            )::BIGINT,
            count(*) FILTER (
                WHERE variantIdI = variantIdJ AND hailing_r IS NULL AND gentropy_r IS NOT NULL
            )::BIGINT,
            count(*) FILTER (
                WHERE variantIdI = variantIdJ
                  AND hailing_r IS NOT NULL
                  AND gentropy_r IS NOT NULL
                  AND abs(hailing_r - gentropy_r) > ?
            )::BIGINT
        FROM compared
        """,
        [tolerance],
    ).fetchone()
    if row is None:
        raise RuntimeError("Failed to calculate diagonal LD differences")
    return {
        "hailing_only": row[0],
        "gentropy_only": row[1],
        "shared_value_mismatches": row[2],
    }


def compare_ld_outputs(config: LdParityConfig) -> dict[str, Any]:
    """Write and return an order-independent LD parity report."""
    for path in (config.hailing_path, config.gentropy_path):
        if not path.exists():
            raise FileNotFoundError(path)
    for path in (config.hailing_stats_path, config.gentropy_stats_path):
        if path is not None and not path.exists():
            raise FileNotFoundError(path)

    declared_ancestries = _declared_ancestries(config.hailing_stats_path) | _declared_ancestries(config.gentropy_stats_path)
    with duckdb.connect() as con:
        _create_normalised_table(con, table_name="hailing", source_path=config.hailing_path)
        _create_normalised_table(con, table_name="gentropy", source_path=config.gentropy_path)
        rows = _comparison_rows(con, config.tolerance)
        diagonal_differences = _diagonal_differences(con, config.tolerance)

    ancestry_reports = {
        ancestry: {
            "ancestry": ancestry,
            "hailing_pairs": hailing_pairs,
            "gentropy_pairs": gentropy_pairs,
            "shared_pairs": shared_pairs,
            "hailing_only_pairs": hailing_only_pairs,
            "gentropy_only_pairs": gentropy_only_pairs,
            "shared_value_mismatches": shared_value_mismatches,
            "max_abs_ld_difference": max_abs_ld_difference,
            "hailing_zero_pairs": hailing_pairs == 0,
            "gentropy_zero_pairs": gentropy_pairs == 0,
        }
        for (
            ancestry,
            hailing_pairs,
            gentropy_pairs,
            shared_pairs,
            hailing_only_pairs,
            gentropy_only_pairs,
            shared_value_mismatches,
            max_abs_ld_difference,
        ) in rows
    }
    for ancestry in declared_ancestries - ancestry_reports.keys():
        ancestry_reports[ancestry] = {
            "ancestry": ancestry,
            "hailing_pairs": 0,
            "gentropy_pairs": 0,
            "shared_pairs": 0,
            "hailing_only_pairs": 0,
            "gentropy_only_pairs": 0,
            "shared_value_mismatches": 0,
            "max_abs_ld_difference": None,
            "hailing_zero_pairs": True,
            "gentropy_zero_pairs": True,
        }

    ancestries = [ancestry_reports[ancestry] for ancestry in sorted(ancestry_reports)]
    shared_differences = [row["max_abs_ld_difference"] for row in ancestries if row["max_abs_ld_difference"] is not None]
    report: dict[str, Any] = {
        "tolerance": config.tolerance,
        "totals": {
            "hailing_pairs": sum(row["hailing_pairs"] for row in ancestries),
            "gentropy_pairs": sum(row["gentropy_pairs"] for row in ancestries),
            "shared_pairs": sum(row["shared_pairs"] for row in ancestries),
            "hailing_only_pairs": sum(row["hailing_only_pairs"] for row in ancestries),
            "gentropy_only_pairs": sum(row["gentropy_only_pairs"] for row in ancestries),
            "shared_value_mismatches": sum(row["shared_value_mismatches"] for row in ancestries),
            "max_abs_ld_difference": max(shared_differences, default=None),
        },
        "diagonal_differences": diagonal_differences,
        "ancestries": ancestries,
    }
    config.report_path.parent.mkdir(parents=True, exist_ok=True)
    config.report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report
