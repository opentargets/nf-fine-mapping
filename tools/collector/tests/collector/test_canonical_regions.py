import hashlib

# ruff: noqa: E501
import json
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from collector import app
from collector.canonical_regions import (
    CanonicalRegion,
    CanonicalRegionInput,
    CollectCanonicalRegionsConfig,
    SourceLocus,
    create_regional_variants_table,
    prepare_collect_canonical_region_inputs,
    validate_summary_statistics_inputs,
)

runner = CliRunner()


def _regional_test_inputs(tmp_path: Path) -> tuple[tuple[CanonicalRegionInput, ...], list[CanonicalRegion]]:
    sum_a = _write_sumstats_dataset_with_rows(
        tmp_path / "a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[("1_110_A_G", 110, 0.1, -8, 1.0, 0.2, 0.01), ("1_500_A_G", 500, 0.1, -7, 1.0, 0.2, 0.01)],
    )
    sum_b = _write_sumstats_dataset_with_rows(
        tmp_path / "b.sumstats.parquet",
        study_id="STUDY_B",
        rows=[("1_120_A_G", 120, 0.1, -8, 1.0, 0.3, 0.01)],
    )
    prepared = (
        CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=sum_a, summary_statistics_path=sum_a),
        CanonicalRegionInput(study_id="STUDY_B", ancestry="AFR", locus_breaker_path=sum_b, summary_statistics_path=sum_b),
    )
    regions = [
        CanonicalRegion(
            chromosome="1",
            region_start=100,
            region_end=200,
            quality_controls=(),
            input_loci=(
                SourceLocus("STUDY_A", "a-locus", "EUR", "1", 100, 150),
                SourceLocus("STUDY_B", "b-locus", "AFR", "1", 110, 160),
            ),
        )
    ]
    return prepared, regions


def _write_locus_breaker_dataset(path: Path, *, study_ids: list[str]) -> Path:
    rows = []
    for index, study_id in enumerate(study_ids, start=1):
        rows.append(
            f"""
            SELECT
                'sl-{study_id}-{index}'::VARCHAR AS studyLocusId,
                '{study_id}'::VARCHAR AS studyId,
                '1_{100 * index}_A_G'::VARCHAR AS variantId,
                '1'::VARCHAR AS chromosome,
                {100 * index}::INTEGER AS position,
                0.1::DOUBLE AS beta,
                1000::INTEGER AS sampleSize,
                1.0::FLOAT AS pValueMantissa,
                -8::INTEGER AS pValueExponent,
                0.2::FLOAT AS effectAlleleFrequencyFromSource,
                0.01::DOUBLE AS standardError,
                []::VARCHAR[] AS qualityControls,
                {100 * index}::INTEGER AS locusStart,
                {100 * index + 10}::INTEGER AS locusEnd,
                []::STRUCT(
                    is95CredibleSet BOOLEAN,
                    is99CredibleSet BOOLEAN,
                    logBF DOUBLE,
                    posteriorProbability DOUBLE,
                    variantId VARCHAR,
                    pValueMantissa FLOAT,
                    pValueExponent INTEGER,
                    beta DOUBLE,
                    standardError DOUBLE,
                    r2Overall DOUBLE
                )[] AS locus
            """
        )
    with duckdb.connect() as con:
        con.execute(f"COPY ({' UNION ALL '.join(rows)}) TO '{path}' (FORMAT PARQUET)")
    return path


def _write_locus_breaker_dataset_with_loci(path: Path, *, study_id: str, loci: list[tuple[str, int, int]]) -> Path:
    rows = []
    for study_locus_id, locus_start, locus_end in loci:
        rows.append(
            f"""
            SELECT
                '{study_locus_id}'::VARCHAR AS studyLocusId,
                '{study_id}'::VARCHAR AS studyId,
                '1_{locus_start}_A_G'::VARCHAR AS variantId,
                '1'::VARCHAR AS chromosome,
                {locus_start}::INTEGER AS position,
                0.1::DOUBLE AS beta,
                1000::INTEGER AS sampleSize,
                1.0::FLOAT AS pValueMantissa,
                -8::INTEGER AS pValueExponent,
                0.2::FLOAT AS effectAlleleFrequencyFromSource,
                0.01::DOUBLE AS standardError,
                []::VARCHAR[] AS qualityControls,
                {locus_start}::INTEGER AS locusStart,
                {locus_end}::INTEGER AS locusEnd,
                []::STRUCT(
                    is95CredibleSet BOOLEAN,
                    is99CredibleSet BOOLEAN,
                    logBF DOUBLE,
                    posteriorProbability DOUBLE,
                    variantId VARCHAR,
                    pValueMantissa FLOAT,
                    pValueExponent INTEGER,
                    beta DOUBLE,
                    standardError DOUBLE,
                    r2Overall DOUBLE
                )[] AS locus
            """
        )
    with duckdb.connect() as con:
        con.execute(f"COPY ({' UNION ALL '.join(rows)}) TO '{path}' (FORMAT PARQUET)")
    return path


def _write_sumstats_dataset(path: Path, *, study_ids: list[str]) -> Path:
    rows = []
    for index, study_id in enumerate(study_ids, start=1):
        rows.append(
            f"""
            SELECT
                '{study_id}'::VARCHAR AS studyId,
                '1_{200 * index}_A_G'::VARCHAR AS variantId,
                '1'::VARCHAR AS chromosome,
                {200 * index}::INTEGER AS position,
                0.1::DOUBLE AS beta,
                1000::INTEGER AS sampleSize,
                1.0::FLOAT AS pValueMantissa,
                -8::INTEGER AS pValueExponent,
                0.2::FLOAT AS effectAlleleFrequencyFromSource,
                0.01::DOUBLE AS standardError
            """
        )
    with duckdb.connect() as con:
        con.execute(f"COPY ({' UNION ALL '.join(rows)}) TO '{path}' (FORMAT PARQUET)")
    return path


def _write_single_sumstats_dataset(path: Path, *, study_id: str) -> Path:
    return _write_sumstats_dataset(path, study_ids=[study_id])


def _write_sumstats_dataset_with_rows(
    path: Path,
    *,
    study_id: str,
    rows: list[tuple[str, int, float, int, float, float, float]],
) -> Path:
    selects = []
    for variant_id, position, beta, pvalue_exponent, pvalue_mantissa, effect_allele_frequency, standard_error in rows:
        selects.append(
            f"""
            SELECT
                '{study_id}'::VARCHAR AS studyId,
                '{variant_id}'::VARCHAR AS variantId,
                '1'::VARCHAR AS chromosome,
                {position}::INTEGER AS position,
                {beta}::DOUBLE AS beta,
                1000::INTEGER AS sampleSize,
                {pvalue_mantissa}::FLOAT AS pValueMantissa,
                {pvalue_exponent}::INTEGER AS pValueExponent,
                {effect_allele_frequency}::FLOAT AS effectAlleleFrequencyFromSource,
                {standard_error}::DOUBLE AS standardError
            """
        )
    with duckdb.connect() as con:
        con.execute(f"COPY ({' UNION ALL '.join(selects)}) TO '{path}' (FORMAT PARQUET)")
    return path


def _valid_config(tmp_path: Path) -> CollectCanonicalRegionsConfig:
    locus_breaker_a = _write_locus_breaker_dataset(tmp_path / "study_b.locus.parquet", study_ids=["STUDY_B"])
    locus_breaker_b = _write_locus_breaker_dataset(tmp_path / "study_a.locus.parquet", study_ids=["STUDY_A"])
    sumstats_a = _write_sumstats_dataset(tmp_path / "study_b.sumstats.parquet", study_ids=["STUDY_B"])
    sumstats_b = _write_sumstats_dataset(tmp_path / "study_a.sumstats.parquet", study_ids=["STUDY_A"])
    return CollectCanonicalRegionsConfig(
        run_id="run-1",
        locus_breaker_paths=(locus_breaker_a, locus_breaker_b),
        ancestries=("AFR", "EUR"),
        summary_statistics_paths=(sumstats_a, sumstats_b),
        fine_mapping_locus_set_output_dir=tmp_path / "fine_mapping_locus_sets",
        stats_parquet_output=tmp_path / "stats" / "run-1.stat.parquet",
        stats_json_output=tmp_path / "stats" / "run-1.stat.json",
    )


def test_prepare_collect_canonical_region_inputs_sorts_by_study_id_and_preserves_alignment(tmp_path: Path) -> None:
    prepared = prepare_collect_canonical_region_inputs(_valid_config(tmp_path))

    assert [(record.study_id, record.ancestry, record.locus_breaker_path.name, record.summary_statistics_path.name) for record in prepared] == [
        ("STUDY_A", "EUR", "study_a.locus.parquet", "study_a.sumstats.parquet"),
        ("STUDY_B", "AFR", "study_b.locus.parquet", "study_b.sumstats.parquet"),
    ]


def test_prepare_collect_canonical_region_inputs_rejects_multiple_studies_per_locus_breaker_input(tmp_path: Path) -> None:
    config = _valid_config(tmp_path)
    multi_study_path = _write_locus_breaker_dataset(tmp_path / "multi.locus.parquet", study_ids=["STUDY_A", "STUDY_X"])
    config = config.model_copy(update={"locus_breaker_paths": (multi_study_path, config.locus_breaker_paths[1])})

    with pytest.raises(ValueError, match="exactly one distinct studyId") as excinfo:
        prepare_collect_canonical_region_inputs(config)
    assert "LocusBreaker input" in str(excinfo.value)


def test_prepare_collect_canonical_region_inputs_rejects_mismatched_study_ids(tmp_path: Path) -> None:
    config = _valid_config(tmp_path)
    wrong_sumstats = _write_sumstats_dataset(tmp_path / "wrong.sumstats.parquet", study_ids=["STUDY_X"])
    config = config.model_copy(update={"summary_statistics_paths": (wrong_sumstats, config.summary_statistics_paths[1])})

    with pytest.raises(ValueError, match="matching studyId") as excinfo:
        prepare_collect_canonical_region_inputs(config)
    assert "STUDY_X" in str(excinfo.value)
    assert "STUDY_B" in str(excinfo.value)


def test_collect_canonical_regions_cli_rejects_fewer_than_two_input_triples(tmp_path: Path) -> None:
    locus_breaker_path = _write_locus_breaker_dataset(tmp_path / "single.locus.parquet", study_ids=["STUDY_A"])
    sumstats_path = _write_sumstats_dataset(tmp_path / "single.sumstats.parquet", study_ids=["STUDY_A"])

    result = runner.invoke(
        app,
        [
            "collect_canonical_regions",
            "--run_id",
            "run-1",
            "--locus_breaker",
            str(locus_breaker_path),
            "--ancestry",
            "EUR",
            "--summary_statistics",
            str(sumstats_path),
            "--fine_mapping_locus_set_output_dir",
            str(tmp_path / "fine_mapping_locus_sets"),
            "--stats_parquet_output",
            str(tmp_path / "stats" / "run-1.stat.parquet"),
            "--stats_json_output",
            str(tmp_path / "stats" / "run-1.stat.json"),
        ],
    )

    assert result.exit_code != 0
    assert "at least two" in result.output.lower()


def test_collect_canonical_regions_cli_rejects_duplicate_ancestries(tmp_path: Path) -> None:
    locus_breaker_a = _write_locus_breaker_dataset(tmp_path / "study_a.locus.parquet", study_ids=["STUDY_A"])
    locus_breaker_b = _write_locus_breaker_dataset(tmp_path / "study_b.locus.parquet", study_ids=["STUDY_B"])
    sumstats_a = _write_sumstats_dataset(tmp_path / "study_a.sumstats.parquet", study_ids=["STUDY_A"])
    sumstats_b = _write_sumstats_dataset(tmp_path / "study_b.sumstats.parquet", study_ids=["STUDY_B"])

    result = runner.invoke(
        app,
        [
            "collect_canonical_regions",
            "--run_id",
            "run-1",
            "--locus_breaker",
            str(locus_breaker_a),
            "--locus_breaker",
            str(locus_breaker_b),
            "--ancestry",
            "EUR",
            "--ancestry",
            "EUR",
            "--summary_statistics",
            str(sumstats_a),
            "--summary_statistics",
            str(sumstats_b),
            "--fine_mapping_locus_set_output_dir",
            str(tmp_path / "fine_mapping_locus_sets"),
            "--stats_parquet_output",
            str(tmp_path / "stats" / "run-1.stat.parquet"),
            "--stats_json_output",
            str(tmp_path / "stats" / "run-1.stat.json"),
        ],
    )

    assert result.exit_code != 0
    assert "ancestry" in result.output
    assert "distinct" in result.output


def test_collect_canonical_regions_cli_rejects_unequal_array_lengths(tmp_path: Path) -> None:
    locus_breaker_a = _write_locus_breaker_dataset(tmp_path / "study_a.locus.parquet", study_ids=["STUDY_A"])
    locus_breaker_b = _write_locus_breaker_dataset(tmp_path / "study_b.locus.parquet", study_ids=["STUDY_B"])
    sumstats_a = _write_sumstats_dataset(tmp_path / "study_a.sumstats.parquet", study_ids=["STUDY_A"])
    sumstats_b = _write_sumstats_dataset(tmp_path / "study_b.sumstats.parquet", study_ids=["STUDY_B"])

    result = runner.invoke(
        app,
        [
            "collect_canonical_regions",
            "--run_id",
            "run-1",
            "--locus_breaker",
            str(locus_breaker_a),
            "--locus_breaker",
            str(locus_breaker_b),
            "--ancestry",
            "EUR",
            "--summary_statistics",
            str(sumstats_a),
            "--summary_statistics",
            str(sumstats_b),
            "--fine_mapping_locus_set_output_dir",
            str(tmp_path / "fine_mapping_locus_sets"),
            "--stats_parquet_output",
            str(tmp_path / "stats" / "run-1.stat.parquet"),
            "--stats_json_output",
            str(tmp_path / "stats" / "run-1.stat.json"),
        ],
    )

    assert result.exit_code != 0
    assert "equal length" in result.output


def test_collect_canonical_regions_cli_writes_transitive_inclusive_regions_to_stats_parquet(tmp_path: Path) -> None:
    locus_breaker_c = _write_locus_breaker_dataset_with_loci(tmp_path / "study_c.locus.parquet", study_id="STUDY_C", loci=[("c_locus", 250, 320)])
    locus_breaker_a = _write_locus_breaker_dataset_with_loci(tmp_path / "study_a.locus.parquet", study_id="STUDY_A", loci=[("a_locus", 100, 200)])
    locus_breaker_b = _write_locus_breaker_dataset_with_loci(tmp_path / "study_b.locus.parquet", study_id="STUDY_B", loci=[("b_locus", 200, 260)])
    sumstats_c = _write_single_sumstats_dataset(tmp_path / "study_c.sumstats.parquet", study_id="STUDY_C")
    sumstats_a = _write_single_sumstats_dataset(tmp_path / "study_a.sumstats.parquet", study_id="STUDY_A")
    sumstats_b = _write_single_sumstats_dataset(tmp_path / "study_b.sumstats.parquet", study_id="STUDY_B")
    stats_parquet_output = tmp_path / "stats" / "run-1.stat.parquet"

    result = runner.invoke(
        app,
        [
            "collect_canonical_regions",
            "--run_id",
            "run-1",
            "--locus_breaker",
            str(locus_breaker_c),
            "--locus_breaker",
            str(locus_breaker_a),
            "--locus_breaker",
            str(locus_breaker_b),
            "--ancestry",
            "AFR",
            "--ancestry",
            "EUR",
            "--ancestry",
            "CSA",
            "--summary_statistics",
            str(sumstats_c),
            "--summary_statistics",
            str(sumstats_a),
            "--summary_statistics",
            str(sumstats_b),
            "--fine_mapping_locus_set_output_dir",
            str(tmp_path / "fine_mapping_locus_sets"),
            "--stats_parquet_output",
            str(stats_parquet_output),
            "--stats_json_output",
            str(tmp_path / "stats" / "run-1.stat.json"),
        ],
    )

    assert result.exit_code == 0, result.output

    with duckdb.connect() as con:
        rows = con.execute(
            f"""
            SELECT
                chromosome,
                locusStart,
                locusEnd,
                list_transform(inputLoci, item -> item.studyId) AS studyIds,
                list_transform(inputLoci, item -> item.studyLocusId) AS studyLocusIds,
                list_transform(components, item -> item.studyId) AS componentStudies
            FROM read_parquet('{stats_parquet_output}')
            ORDER BY chromosome, locusStart, locusEnd
            """
        ).fetchall()

    assert rows == [
        ("1", 100, 320, ["STUDY_A", "STUDY_B", "STUDY_C"], ["a_locus", "b_locus", "c_locus"], ["STUDY_A", "STUDY_B", "STUDY_C"]),
    ]


def test_collect_canonical_regions_cli_splits_overlap_chain_before_cap_exceedance(tmp_path: Path) -> None:
    locus_breaker_a = _write_locus_breaker_dataset_with_loci(tmp_path / "study_a.locus.parquet", study_id="STUDY_A", loci=[("a_locus", 100, 200)])
    locus_breaker_b = _write_locus_breaker_dataset_with_loci(tmp_path / "study_b.locus.parquet", study_id="STUDY_B", loci=[("b_locus", 180, 259)])
    locus_breaker_c = _write_locus_breaker_dataset_with_loci(tmp_path / "study_c.locus.parquet", study_id="STUDY_C", loci=[("c_locus", 240, 319)])
    sumstats_a = _write_single_sumstats_dataset(tmp_path / "study_a.sumstats.parquet", study_id="STUDY_A")
    sumstats_b = _write_single_sumstats_dataset(tmp_path / "study_b.sumstats.parquet", study_id="STUDY_B")
    sumstats_c = _write_single_sumstats_dataset(tmp_path / "study_c.sumstats.parquet", study_id="STUDY_C")
    stats_parquet_output = tmp_path / "stats" / "run-1.stat.parquet"

    result = runner.invoke(
        app,
        [
            "collect_canonical_regions",
            "--run_id",
            "run-1",
            "--locus_breaker",
            str(locus_breaker_a),
            "--locus_breaker",
            str(locus_breaker_b),
            "--locus_breaker",
            str(locus_breaker_c),
            "--ancestry",
            "EUR",
            "--ancestry",
            "CSA",
            "--ancestry",
            "AFR",
            "--summary_statistics",
            str(sumstats_a),
            "--summary_statistics",
            str(sumstats_b),
            "--summary_statistics",
            str(sumstats_c),
            "--fine_mapping_locus_set_output_dir",
            str(tmp_path / "fine_mapping_locus_sets"),
            "--stats_parquet_output",
            str(stats_parquet_output),
            "--stats_json_output",
            str(tmp_path / "stats" / "run-1.stat.json"),
            "--max_region_span_bp",
            "140",
        ],
    )

    assert result.exit_code == 0, result.output

    with duckdb.connect() as con:
        rows = con.execute(
            f"""
            SELECT locusStart, locusEnd, list_transform(inputLoci, item -> item.studyLocusId) AS studyLocusIds
            FROM read_parquet('{stats_parquet_output}')
            ORDER BY locusStart, locusEnd
            """
        ).fetchall()

    assert rows == [
        (100, 200, ["a_locus"]),
        (180, 319, ["b_locus", "c_locus"]),
    ]


def test_collect_canonical_regions_cli_emits_oversized_source_locus_as_standalone_region(tmp_path: Path) -> None:
    locus_breaker_a = _write_locus_breaker_dataset_with_loci(tmp_path / "study_a.locus.parquet", study_id="STUDY_A", loci=[("a_large", 100, 260)])
    locus_breaker_b = _write_locus_breaker_dataset_with_loci(tmp_path / "study_b.locus.parquet", study_id="STUDY_B", loci=[("b_small", 150, 180)])
    sumstats_a = _write_single_sumstats_dataset(tmp_path / "study_a.sumstats.parquet", study_id="STUDY_A")
    sumstats_b = _write_single_sumstats_dataset(tmp_path / "study_b.sumstats.parquet", study_id="STUDY_B")
    stats_parquet_output = tmp_path / "stats" / "run-1.stat.parquet"

    result = runner.invoke(
        app,
        [
            "collect_canonical_regions",
            "--run_id",
            "run-1",
            "--locus_breaker",
            str(locus_breaker_a),
            "--locus_breaker",
            str(locus_breaker_b),
            "--ancestry",
            "EUR",
            "--ancestry",
            "AFR",
            "--summary_statistics",
            str(sumstats_a),
            "--summary_statistics",
            str(sumstats_b),
            "--fine_mapping_locus_set_output_dir",
            str(tmp_path / "fine_mapping_locus_sets"),
            "--stats_parquet_output",
            str(stats_parquet_output),
            "--stats_json_output",
            str(tmp_path / "stats" / "run-1.stat.json"),
            "--max_region_span_bp",
            "100",
        ],
    )

    assert result.exit_code == 0, result.output

    with duckdb.connect() as con:
        rows = con.execute(
            f"""
            SELECT locusStart, locusEnd, list_transform(components, item -> item.qualityControls) AS qualityControls, list_transform(inputLoci, item -> item.studyLocusId) AS studyLocusIds  -- noqa: E501
            FROM read_parquet('{stats_parquet_output}')
            ORDER BY locusStart, locusEnd
            """
        ).fetchall()

    assert rows == [
        (100, 260, [[]], ["a_large"]),
        (150, 180, [["NO_VARIANTS_IN_LOCUS"]], ["b_small"]),
    ]


def test_collect_canonical_regions_cli_materializes_per_ancestry_locus_set_with_strict_maf_and_deterministic_ids(
    tmp_path: Path,
) -> None:
    locus_breaker_a = _write_locus_breaker_dataset_with_loci(tmp_path / "study_a.locus.parquet", study_id="STUDY_A", loci=[("a_locus", 100, 200)])
    locus_breaker_b = _write_locus_breaker_dataset_with_loci(tmp_path / "study_b.locus.parquet", study_id="STUDY_B", loci=[("b_locus", 180, 220)])
    sumstats_a = _write_sumstats_dataset_with_rows(
        tmp_path / "study_a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[
            ("1_110_A_G", 110, 0.20, -8, 5.0, 0.20, 0.02),
            ("1_140_C_T", 140, 0.10, -8, 5.0, 0.30, 0.03),
            ("1_150_G_A", 150, 0.30, -9, 1.0, 0.01, 0.02),
        ],
    )
    sumstats_b = _write_sumstats_dataset_with_rows(
        tmp_path / "study_b.sumstats.parquet",
        study_id="STUDY_B",
        rows=[
            ("1_125_A_C", 125, -0.40, -7, 2.0, 0.40, 0.04),
            ("1_130_A_G", 130, -0.50, -7, 2.0, 0.30, 0.05),
            ("1_210_T_C", 210, 0.60, -6, 8.0, 0.99, 0.06),
        ],
    )
    output_dir = tmp_path / "fine_mapping_locus_sets"
    stats_json_output = tmp_path / "stats" / "run-1.stat.json"

    result = runner.invoke(
        app,
        [
            "collect_canonical_regions",
            "--run_id",
            "run-1",
            "--locus_breaker",
            str(locus_breaker_a),
            "--locus_breaker",
            str(locus_breaker_b),
            "--ancestry",
            "EUR",
            "--ancestry",
            "AFR",
            "--summary_statistics",
            str(sumstats_a),
            "--summary_statistics",
            str(sumstats_b),
            "--fine_mapping_locus_set_output_dir",
            str(output_dir),
            "--stats_parquet_output",
            str(tmp_path / "stats" / "run-1.stat.parquet"),
            "--stats_json_output",
            str(stats_json_output),
        ],
    )

    assert result.exit_code == 0, result.output

    files = sorted(output_dir.glob("*.parquet"))
    assert len(files) == 1

    expected_study_locus_ids = {
        "STUDY_A": hashlib.md5(b"STUDY_A1_110_A_G", usedforsecurity=False).hexdigest(),
        "STUDY_B": hashlib.md5(b"STUDY_B1_125_A_C", usedforsecurity=False).hexdigest(),
    }
    expected_locus_set_id = hashlib.md5(
        "|".join(sorted(expected_study_locus_ids.values())).encode(),
        usedforsecurity=False,
    ).hexdigest()

    with duckdb.connect() as con:
        rows = con.execute(
            f"""
            SELECT
                fineMappingLocusSetId,
                studyId,
                studyLocusId,
                chromosome,
                locusStart,
                locusEnd,
                list_transform(locus, item -> item.variantId) AS locusVariants
            FROM read_parquet('{files[0]}')
            ORDER BY studyId
            """
        ).fetchall()

    assert rows == [
        (
            expected_locus_set_id,
            "STUDY_A",
            expected_study_locus_ids["STUDY_A"],
            "1",
            100,
            220,
            ["1_110_A_G", "1_140_C_T"],
        ),
        (
            expected_locus_set_id,
            "STUDY_B",
            expected_study_locus_ids["STUDY_B"],
            "1",
            100,
            220,
            ["1_125_A_C", "1_130_A_G"],
        ),
    ]

    stats = json.loads(stats_json_output.read_text())
    assert stats["nPublishedLocusSets"] == 1


def test_collect_canonical_regions_cli_records_fatal_no_variants_in_locus_stats_when_publication_is_blocked(
    tmp_path: Path,
) -> None:
    locus_breaker_a = _write_locus_breaker_dataset_with_loci(
        tmp_path / "study_a.locus.parquet",
        study_id="STUDY_A",
        loci=[("a_locus", 100, 200)],
    )
    locus_breaker_b = _write_locus_breaker_dataset_with_loci(
        tmp_path / "study_b.locus.parquet",
        study_id="STUDY_B",
        loci=[("b_locus", 180, 220)],
    )
    sumstats_a = _write_sumstats_dataset_with_rows(
        tmp_path / "study_a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[
            ("1_110_A_G", 110, 0.20, -8, 5.0, 0.20, 0.02),
            ("1_140_C_T", 140, 0.10, -8, 5.0, 0.30, 0.03),
            ("1_150_G_A", 150, 0.30, -9, 1.0, 0.01, 0.02),
        ],
    )
    sumstats_b = _write_sumstats_dataset_with_rows(
        tmp_path / "study_b.sumstats.parquet",
        study_id="STUDY_B",
        rows=[
            ("1_125_A_C", 125, -0.40, -7, 2.0, 0.009, 0.04),
            ("1_130_A_G", 130, -0.50, -7, 2.0, 0.991, 0.05),
            ("1_210_T_C", 210, 0.60, -6, 8.0, 0.99, 0.06),
        ],
    )
    output_dir = tmp_path / "fine_mapping_locus_sets"
    stats_json_output = tmp_path / "stats" / "run-1.stat.json"
    stats_parquet_output = tmp_path / "stats" / "run-1.stat.parquet"

    result = runner.invoke(
        app,
        [
            "collect_canonical_regions",
            "--run_id",
            "run-1",
            "--locus_breaker",
            str(locus_breaker_a),
            "--locus_breaker",
            str(locus_breaker_b),
            "--ancestry",
            "EUR",
            "--ancestry",
            "AFR",
            "--summary_statistics",
            str(sumstats_a),
            "--summary_statistics",
            str(sumstats_b),
            "--fine_mapping_locus_set_output_dir",
            str(output_dir),
            "--stats_parquet_output",
            str(stats_parquet_output),
            "--stats_json_output",
            str(stats_json_output),
        ],
    )

    assert result.exit_code == 0, result.output
    assert list(output_dir.glob("*.parquet")) == []

    with duckdb.connect() as con:
        rows = con.execute(
            f"""
            SELECT
                fineMappingLocusSetId,
                chromosome,
                locusStart,
                locusEnd,
                nVariants,
                nVariantsAboveMafCutoff,
                list_transform(inputLoci, item -> item.studyLocusId) AS inputStudyLocusIds,
                list_transform(
                    components,
                    item -> struct_pack(
                        studyId := item.studyId,
                        studyLocusId := item.studyLocusId,
                        nVariants := item.nVariants,
                        nVariantsBelowMafCutoff := item.nVariantsBelowMafCutoff,
                        qualityControls := item.qualityControls
                    )
                ) AS componentStats
            FROM read_parquet('{stats_parquet_output}')
            """
        ).fetchall()

    assert rows == [
        (
            None,
            "1",
            100,
            220,
            6,
            2,
            ["a_locus", "b_locus"],
            [
                {
                    "studyId": "STUDY_A",
                    "studyLocusId": "a_locus",
                    "nVariants": 3,
                    "nVariantsBelowMafCutoff": 1,
                    "qualityControls": [],
                },
                {
                    "studyId": "STUDY_B",
                    "studyLocusId": "b_locus",
                    "nVariants": 3,
                    "nVariantsBelowMafCutoff": 3,
                    "qualityControls": ["NO_VARIANTS_IN_LOCUS"],
                },
            ],
        )
    ]

    stats = json.loads(stats_json_output.read_text())
    assert stats["nCandidateLocusSets"] == 1
    assert stats["nPublishedLocusSets"] == 0
    assert set(stats["timingsSeconds"]) == {"inputValidation", "regionDiscovery", "locusMaterialization", "statistics"}
    assert all(value >= 0 for value in stats["timingsSeconds"].values())


def test_collect_canonical_regions_missing_eaf_invalidates_run_without_publishing(tmp_path: Path) -> None:
    locus_a = _write_locus_breaker_dataset_with_loci(tmp_path / "a.locus.parquet", study_id="STUDY_A", loci=[("a", 100, 200)])
    locus_b = _write_locus_breaker_dataset_with_loci(tmp_path / "b.locus.parquet", study_id="STUDY_B", loci=[("b", 150, 250)])
    sum_a = _write_single_sumstats_dataset(tmp_path / "a.sumstats.parquet", study_id="STUDY_A")
    sum_b = tmp_path / "b.sumstats.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"COPY (SELECT 'STUDY_B' AS studyId, '1_160_A_G' AS variantId, '1' AS chromosome, 160 AS position, 1.0 AS pValueMantissa, -8 AS pValueExponent, NULL::FLOAT AS effectAlleleFrequencyFromSource, 0.1 AS beta, 0.01 AS standardError) TO '{sum_b}' (FORMAT PARQUET)"
        )
    output_dir = tmp_path / "sets"
    result = runner.invoke(
        app,
        [
            "collect_canonical_regions",
            "--run_id",
            "run-1",
            "--locus_breaker",
            str(locus_a),
            "--locus_breaker",
            str(locus_b),
            "--ancestry",
            "EUR",
            "--ancestry",
            "AFR",
            "--summary_statistics",
            str(sum_a),
            "--summary_statistics",
            str(sum_b),
            "--fine_mapping_locus_set_output_dir",
            str(output_dir),
            "--stats_parquet_output",
            str(tmp_path / "stats.parquet"),
            "--stats_json_output",
            str(tmp_path / "stats.json"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert not list(output_dir.glob("*.parquet"))
    report = json.loads((tmp_path / "stats.json").read_text())
    assert report["studiesWithMissingEAF"] == ["STUDY_B"]
    assert report["runQualityControls"] == ["MISSING_EFFECT_ALLELE_FREQUENCY_FROM_SOURCE"]


def test_regional_staging_keeps_only_projected_variants_inside_regions(tmp_path: Path) -> None:
    prepared, regions = _regional_test_inputs(tmp_path)
    with duckdb.connect() as con:
        validate_summary_statistics_inputs(con, prepared)
        table_name = create_regional_variants_table(con, prepared, regions)
        rows = con.execute(f"SELECT studyId, ancestry, variantId, position FROM {table_name} ORDER BY studyId, position").fetchall()
    assert rows == [("STUDY_A", "EUR", "1_110_A_G", 110), ("STUDY_B", "AFR", "1_120_A_G", 120)]


def test_summary_validation_rejects_duplicate_variants(tmp_path: Path) -> None:
    path = _write_sumstats_dataset_with_rows(
        tmp_path / "duplicate.sumstats.parquet",
        study_id="STUDY_A",
        rows=[("1_110_A_G", 110, 0.1, -8, 1.0, 0.2, 0.01), ("1_110_A_G", 110, 0.2, -7, 1.0, 0.2, 0.01)],
    )
    prepared = (CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=path, summary_statistics_path=path),)
    with duckdb.connect() as con, pytest.raises(ValueError, match="duplicate variantId"):
        validate_summary_statistics_inputs(con, prepared)


def test_summary_validation_rejects_all_null_eaf(tmp_path: Path) -> None:
    path = tmp_path / "null-eaf.sumstats.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"COPY (SELECT 'STUDY_A'::VARCHAR AS studyId, '1_110_A_G'::VARCHAR AS variantId, '1'::VARCHAR AS chromosome, 110::INTEGER AS position, 1.0::FLOAT AS pValueMantissa, -8::INTEGER AS pValueExponent, NULL::FLOAT AS effectAlleleFrequencyFromSource, 0.1::DOUBLE AS beta, 0.01::DOUBLE AS standardError) TO '{path}' (FORMAT PARQUET)"
        )
    prepared = (CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=path, summary_statistics_path=path),)
    with duckdb.connect() as con, pytest.raises(ValueError, match="incomplete effectAlleleFrequencyFromSource"):
        validate_summary_statistics_inputs(con, prepared)
