"""Tests for collector-native LocusBreaker core semantics."""

from __future__ import annotations

from hashlib import md5
from pathlib import Path

import duckdb
import pytest

from collector.locus_breaker import LocusBreakerConfig, run_locus_breaker, split_pvalue

SUMSTAT_COLUMNS = (
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
)


def sql_literal(value: object) -> str:
    """Return a DuckDB SQL literal for test fixture values."""
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, float):
        return f"{value}::FLOAT"
    return str(value)


def write_sumstats(path: Path, rows: list[tuple[object, ...]]) -> None:
    """Write rows with the locus_breaker input schema to parquet."""
    values = ",\n                    ".join("(" + ", ".join(sql_literal(value) for value in row) + ")" for row in rows)
    columns = ",\n                    ".join(SUMSTAT_COLUMNS)

    con = duckdb.connect()
    try:
        con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    {values}
                ) AS t(
                    {columns}
                )
            ) TO '{path}' (FORMAT 'parquet')
            """
        )
    finally:
        con.close()


def read_lbc_rows(path: Path, order_by: str = "studyId, chromosome, locusStart, locusEnd, position, variantId"):
    con = duckdb.connect()
    try:
        return con.execute(
            f"""
            SELECT studyLocusId, studyId, variantId, chromosome, position, locusStart, locusEnd, qualityControls, locus
            FROM read_parquet('{path}')
            ORDER BY {order_by}
            """
        ).fetchall()
    finally:
        con.close()


def test_split_pvalue_matches_gentropy_rounding_examples():
    assert split_pvalue(0.00001234) == (1.234, -5)
    assert split_pvalue(1) == (1.0, 0)
    assert split_pvalue(0.123) == (1.23, -1)
    assert split_pvalue(0.99) == (9.9, -1)


@pytest.mark.parametrize("pvalue", [-0.1, 1.1])
def test_split_pvalue_rejects_values_outside_unit_interval(pvalue: float):
    with pytest.raises(ValueError, match="P-value must be between 0 and 1"):
        split_pvalue(pvalue)


def test_lbc_uses_mantissa_exponent_filters_and_rank_ties(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "1_100_A_C", "1", 100, 0.1, 1000, 9.0, -9, 0.1, 0.05),
            ("GCST_A", "1_110_A_C", "1", 110, 0.2, 1000, 9.0, -9, 0.2, 0.06),
            ("GCST_A", "1_120_A_C", "1", 120, 0.3, 1000, 1.001, -8, 0.3, 0.07),
            ("GCST_A", "1_130_A_C", "1", 130, 0.4, 1000, 1.0, -7, 0.4, 0.08),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-5,
            lbc_distance_cutoff=100,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=10,
            collect_locus=False,
            remove_mhc=False,
        ),
    )

    rows = read_lbc_rows(output_path)
    assert [row[2] for row in rows] == ["1_100_A_C", "1_110_A_C"]
    assert {row[5:7] for row in rows} == {(90, 140)}
    assert all(row[7] is None for row in rows)
    assert all(row[8] is None for row in rows)


def test_lbc_lead_cutoff_is_strict_after_ranking(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "1_100_A_C", "1", 100, 0.1, 1000, 1.0, -8, 0.1, 0.05),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-5,
            lbc_distance_cutoff=100,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=10,
            collect_locus=False,
            remove_mhc=False,
        ),
    )
    assert read_lbc_rows(output_path) == []


def test_lbc_splits_on_distance_greater_than_cutoff_and_applies_flanking_floor(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "1_5_A_C", "1", 5, 0.1, 1000, 1.0, -9, 0.1, 0.05),
            ("GCST_A", "1_25_A_C", "1", 25, 0.2, 1000, 2.0, -8, 0.2, 0.06),
            ("GCST_A", "1_60_A_C", "1", 60, 0.3, 1000, 1.0, -9, 0.3, 0.07),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-5,
            lbc_distance_cutoff=30,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=10,
            collect_locus=False,
            remove_mhc=False,
        ),
    )

    rows = read_lbc_rows(output_path)
    assert [(row[2], row[5], row[6]) for row in rows] == [
        ("1_5_A_C", 0, 35),
        ("1_60_A_C", 50, 70),
    ]


def test_lbc_clumps_independently_by_study_and_chromosome(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "1_100_A_C", "1", 100, 0.1, 1000, 1.0, -9, 0.1, 0.05),
            ("GCST_A", "2_100_A_C", "2", 100, 0.2, 1000, 1.0, -9, 0.2, 0.06),
            ("GCST_B", "1_105_A_C", "1", 105, 0.3, 1000, 1.0, -9, 0.3, 0.07),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-5,
            lbc_distance_cutoff=100,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=10,
            collect_locus=False,
            remove_mhc=False,
        ),
    )

    rows = read_lbc_rows(output_path)
    assert [(row[1], row[2], row[3], row[5], row[6]) for row in rows] == [
        ("GCST_A", "1_100_A_C", "1", 90, 110),
        ("GCST_A", "2_100_A_C", "2", 90, 110),
        ("GCST_B", "1_105_A_C", "1", 95, 115),
    ]


def test_lbc_study_locus_id_matches_gentropy_md5_concat_semantics(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST000001", "1_1000_A_C", "1", 1000, 0.1, 1000, 1.0, -9, 0.1, 0.05),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-5,
            lbc_distance_cutoff=100,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=10,
            collect_locus=False,
            remove_mhc=False,
        ),
    )

    rows = read_lbc_rows(output_path)
    assert rows[0][0] == md5(b"GCST0000011_1000_A_C").hexdigest()


def test_wbc_is_skipped_when_lbc_produces_no_loci(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "1_100_A_C", "1", 100, 0.1, 1000, 1.0, -6, 0.1, 0.05),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-5,
            lbc_distance_cutoff=100,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=0,
            large_loci_size=100,
            wbc_clump_distance=50,
            wbc_pvalue_threshold=1e-5,
            collect_locus=False,
            remove_mhc=False,
        ),
    )
    assert read_lbc_rows(output_path) == []


def test_wbc_is_skipped_when_lbc_has_no_large_loci(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "1_100_A_C", "1", 100, 0.1, 1000, 1.0, -9, 0.1, 0.05),
            ("GCST_A", "1_150_A_C", "1", 150, 0.2, 1000, 2.0, -9, 0.2, 0.06),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-5,
            lbc_distance_cutoff=100,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=0,
            large_loci_size=1_000,
            wbc_clump_distance=50,
            wbc_pvalue_threshold=1e-5,
            collect_locus=False,
            remove_mhc=False,
        ),
    )

    rows = read_lbc_rows(output_path)
    assert [(row[2], row[5], row[6], row[7]) for row in rows] == [("1_100_A_C", 100, 150, None)]


def test_wbc_replaces_large_lbc_loci_with_centered_leads_and_retains_small_loci(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "1_100_A_C", "1", 100, 0.1, 1000, 1.0, -10, 0.1, 0.05),
            ("GCST_A", "1_149_A_C", "1", 149, 0.2, 1000, 2.0, -10, 0.2, 0.06),
            ("GCST_A", "1_150_A_C", "1", 150, 0.3, 1000, 3.0, -10, 0.3, 0.07),
            ("GCST_A", "1_260_A_C", "1", 260, 0.4, 1000, 1.001, -5, 0.4, 0.08),
            ("GCST_A", "1_1000_A_C", "1", 1000, 0.5, 1000, 1.0, -9, 0.5, 0.09),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-4,
            lbc_distance_cutoff=500,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=0,
            large_loci_size=100,
            wbc_clump_distance=50,
            wbc_pvalue_threshold=1e-5,
            collect_locus=False,
            remove_mhc=False,
        ),
    )

    rows = read_lbc_rows(output_path)
    assert [(row[2], row[5], row[6], row[7]) for row in rows] == [
        ("1_100_A_C", 50, 150, []),
        ("1_150_A_C", 100, 200, []),
        ("1_1000_A_C", 1000, 1000, None),
    ]


def test_wbc_replacement_preserves_source_float_values(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "1_100_A_C", "1", 100, 0.1, 1000, 1.57, -26, 0.91635, 0.05),
            ("GCST_A", "1_260_A_C", "1", 260, 0.2, 1000, 2.0, -10, 0.2, 0.06),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-4,
            lbc_distance_cutoff=500,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=0,
            large_loci_size=100,
            wbc_clump_distance=50,
            wbc_pvalue_threshold=1e-5,
            collect_locus=False,
            remove_mhc=False,
        ),
    )

    con = duckdb.connect()
    try:
        source_values = con.execute(
            f"""
            SELECT
                printf('%.10f', CAST(pValueMantissa AS DOUBLE)),
                printf('%.10f', CAST(effectAlleleFrequencyFromSource AS DOUBLE))
            FROM read_parquet('{input_path}')
            WHERE variantId = '1_100_A_C'
            """
        ).fetchone()
        output_values = con.execute(
            f"""
            SELECT
                printf('%.10f', CAST(pValueMantissa AS DOUBLE)),
                printf('%.10f', CAST(effectAlleleFrequencyFromSource AS DOUBLE))
            FROM read_parquet('{output_path}')
            WHERE variantId = '1_100_A_C'
            """
        ).fetchone()
    finally:
        con.close()

    assert source_values == ("1.5700000525", "0.9163500071")
    assert output_values == source_values


def test_wbc_candidates_are_filtered_by_mantissa_exponent_threshold(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "1_100_A_C", "1", 100, 0.1, 1000, 1.0, -10, 0.1, 0.05),
            ("GCST_A", "1_200_A_C", "1", 200, 0.2, 1000, 1.001, -5, 0.2, 0.06),
            ("GCST_A", "1_300_A_C", "1", 300, 0.3, 1000, 2.0, -10, 0.3, 0.07),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-4,
            lbc_distance_cutoff=500,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=0,
            large_loci_size=100,
            wbc_clump_distance=50,
            wbc_pvalue_threshold=1e-5,
            collect_locus=False,
            remove_mhc=False,
        ),
    )

    rows = read_lbc_rows(output_path)
    assert [row[2] for row in rows] == ["1_100_A_C", "1_300_A_C"]


def test_mhc_exclusion_happens_after_large_locus_replacement(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "6_30000000_A_C", "6", 30_000_000, 0.1, 1000, 1.0, -10, 0.1, 0.05),
            ("GCST_A", "1_100_A_C", "1", 100, 0.2, 1000, 1.0, -9, 0.2, 0.06),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-5,
            lbc_distance_cutoff=100,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=100,
            large_loci_size=100,
            wbc_clump_distance=50,
            wbc_pvalue_threshold=1e-5,
            collect_locus=False,
            remove_mhc=True,
        ),
    )

    rows = read_lbc_rows(output_path)
    assert [(row[2], row[3], row[5], row[6]) for row in rows] == [("1_100_A_C", "1", 50, 150)]


def test_collect_locus_removes_duplicated_sumstats_rows(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "1_100_A_C", "1", 100, 0.1, 1000, 1.0, -9, 0.1, 0.05),
            ("GCST_A", "1_105_A_C", "1", 105, None, 1000, None, None, 0.2, None),
            ("GCST_A", "1_105_A_C", "1", 105, 0.3, 1000, 1.0, -3, 0.3, 0.07),
            ("GCST_A", "1_500_A_C", "1", 500, 0.4, 1000, 1.0, -3, 0.4, 0.08),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-5,
            lbc_distance_cutoff=100,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=10,
            collect_locus=True,
            remove_mhc=False,
        ),
    )

    con = duckdb.connect()
    try:
        locus = con.execute(f"SELECT locus FROM read_parquet('{output_path}')").fetchone()[0]
        row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()
    finally:
        con.close()

    assert row_count == (1,)
    assert [entry["variantId"] for entry in locus] == ["1_100_A_C"]
    assert all(entry["variantId"] != "1_105_A_C" for entry in locus)
    assert locus[0]["is95CredibleSet"] is None
    assert locus[0]["r2Overall"] is None


def test_output_rows_use_normalized_chromosome_sort_key_without_changing_values(tmp_path: Path):
    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "10_100_A_C", "10", 100, 0.1, 1000, 1.0, -9, 0.1, 0.05),
            ("GCST_A", "2_100_A_C", "2", 100, 0.2, 1000, 1.0, -9, 0.2, 0.06),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-5,
            lbc_distance_cutoff=100,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=0,
            collect_locus=False,
            remove_mhc=False,
        ),
    )

    con = duckdb.connect()
    try:
        rows = con.execute(f"SELECT variantId, chromosome FROM read_parquet('{output_path}')").fetchall()
    finally:
        con.close()
    assert rows == [("2_100_A_C", "2"), ("10_100_A_C", "10")]


def test_final_parquet_is_readable_by_duckdb_and_polars(tmp_path: Path):
    import polars as pl

    input_path = tmp_path / "sumstats.parquet"
    output_path = tmp_path / "study_locus.parquet"
    write_sumstats(
        input_path,
        [
            ("GCST_A", "1_100_A_C", "1", 100, 0.1, 1000, 1.0, -9, 0.1, 0.05),
        ],
    )

    run_locus_breaker(
        input_path,
        output_path,
        LocusBreakerConfig(
            lbc_baseline_pvalue=1e-5,
            lbc_distance_cutoff=100,
            lbc_pvalue_threshold=1e-8,
            lbc_flanking_distance=0,
            collect_locus=True,
            remove_mhc=False,
        ),
    )

    con = duckdb.connect()
    try:
        duckdb_columns = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{output_path}')").fetchall()]
    finally:
        con.close()
    polars_frame = pl.read_parquet(output_path)

    assert duckdb_columns == [
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
    ]
    assert polars_frame.height == 1
    assert "locus" in polars_frame.columns
