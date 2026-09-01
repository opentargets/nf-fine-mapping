import hashlib

# ruff: noqa: E501
import json
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from collector import app
from collector.canonical_regions import (
    OVERSIZED_SOURCE_LOCUS_QC,
    CanonicalRegion,
    CanonicalRegionInput,
    CollectCanonicalRegionsConfig,
    SourceLocus,
    _read_source_loci,
    _resolve_overlap,
    _ResolvingLocus,
    _studies_with_missing_eaf,
    _sweep_canonical_regions,
    build_regional_output_tables,
    create_regional_variants_table,
    prepare_collect_canonical_region_inputs,
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
                SourceLocus("STUDY_A", "a-locus", "EUR", "1", 100, 150, 125),
                SourceLocus("STUDY_B", "b-locus", "AFR", "1", 110, 160, 135),
            ),
        )
    ]
    return prepared, regions


def test_sweep_merges_two_loci_that_agree_into_one_region():
    loci = [
        SourceLocus("STUDY_A", "a1", "EUR", "1", 1_000_000, 2_200_000, 2_000_000),
        SourceLocus("STUDY_B", "b1", "AFR", "1", 1_800_000, 3_000_000, 1_900_000),
    ]
    regions = _sweep_canonical_regions(loci, 3_000_000)
    assert len(regions) == 1
    assert (regions[0].region_start, regions[0].region_end) == (1_800_000, 2_200_000)


def test_sweep_starts_a_new_region_on_disagreement_even_with_no_position_gap():
    loci = [
        SourceLocus("STUDY_A", "a1", "EUR", "1", 100, 200, lead_position=190),
        SourceLocus("STUDY_B", "b1", "AFR", "1", 150, 250, lead_position=190),
        SourceLocus("STUDY_C", "c1", "NFE", "1", 160, 300, lead_position=280),
    ]
    regions = _sweep_canonical_regions(loci, 3_000_000)
    assert len(regions) == 2
    # A (100-200, lead 190) and B (150-250, lead 190) both have their lead
    # inside their [150, 200] intersection, so they agree and both trim to
    # that shared span -- 150, not 160 (A's own start never enters this
    # calculation once it merges with B; C's 160 start only starts mattering
    # when B-vs-C is resolved next, and that comparison cannot move a
    # boundary that has already been fixed by an earlier agreement).
    assert (regions[0].region_start, regions[0].region_end) == (150, 200)
    assert (regions[1].region_start, regions[1].region_end) == (201, 300)


def test_sweep_never_emits_overlapping_regions_on_a_dense_chain():
    loci = [
        SourceLocus("STUDY_A", "a1", "EUR", "1", 0, 1_500_000, lead_position=100_000),
        SourceLocus("STUDY_B", "b1", "AFR", "1", 400_000, 1_900_000, lead_position=1_800_000),
        SourceLocus("STUDY_C", "c1", "NFE", "1", 1_700_000, 3_200_000, lead_position=1_750_000),
        SourceLocus("STUDY_D", "d1", "EAS", "1", 1_600_000, 3_100_000, lead_position=3_050_000),
    ]
    regions = _sweep_canonical_regions(loci, 3_000_000)
    for earlier, later in zip(regions, regions[1:], strict=False):  # noqa: RUF007
        assert earlier.region_end < later.region_start
    # Without this, the assertion above passes vacuously against a
    # regression to always-merge-into-one (a single-region result makes the
    # zip() loop iterate zero times) -- it would even still pass against the
    # old, retired union-based sweep. Pin down that this scenario genuinely
    # produces two disjoint regions, not one.
    assert len(regions) == 2


def test_sweep_never_lets_a_stale_contained_member_reinflate_a_region_past_a_later_trim():
    # STUDY_B is fully contained by STUDY_A's span, so A-B agree via
    # containment and B's current bounds widen to match A's (100, 300).
    # STUDY_C then overlaps only B's tail, and only C's lead falls in that
    # overlap, so B is trimmed back down to (100, 249) -- but A itself is
    # never revisited. A region-builder that takes min/max over every group
    # member's bounds (rather than tracking one authoritative envelope)
    # would let A's stale, still-300 bound silently re-widen the region past
    # B's trim, producing (100, 300) and (250, 400) -- which overlap at
    # [250, 300]. The correct region is (100, 249), matching the actual
    # trimmed envelope.
    loci = [
        SourceLocus("STUDY_A", "a1", "EUR", "1", 100, 300, lead_position=120),
        SourceLocus("STUDY_B", "b1", "AFR", "1", 150, 200, lead_position=170),
        SourceLocus("STUDY_C", "c1", "NFE", "1", 250, 400, lead_position=280),
    ]
    regions = _sweep_canonical_regions(loci, 3_000_000)
    for earlier, later in zip(regions, regions[1:], strict=False):  # noqa: RUF007
        assert earlier.region_end < later.region_start
    assert len(regions) == 2
    assert (regions[0].region_start, regions[0].region_end) == (100, 249)
    assert (regions[1].region_start, regions[1].region_end) == (250, 400)


def test_sweep_still_splits_on_a_genuine_position_gap():
    loci = [
        SourceLocus("STUDY_A", "a1", "EUR", "1", 100, 200, lead_position=150),
        SourceLocus("STUDY_B", "b1", "AFR", "1", 5_000, 5_200, lead_position=5_100),
    ]
    regions = _sweep_canonical_regions(loci, 3_000_000)
    assert len(regions) == 2
    assert (regions[0].region_start, regions[0].region_end) == (100, 200)
    assert (regions[1].region_start, regions[1].region_end) == (5_000, 5_200)


def test_sweep_flags_a_region_containing_an_oversized_source_locus():
    loci = [
        SourceLocus("STUDY_A", "a1", "EUR", "1", 100, 400, lead_position=200),
    ]
    regions = _sweep_canonical_regions(loci, 100)
    assert len(regions) == 1
    assert regions[0].quality_controls == (OVERSIZED_SOURCE_LOCUS_QC,)


def test_sweep_does_not_flag_a_normally_sized_region():
    loci = [
        SourceLocus("STUDY_A", "a1", "EUR", "1", 100, 200, lead_position=150),
        SourceLocus("STUDY_B", "b1", "AFR", "1", 5_000, 5_200, lead_position=5_100),
    ]
    regions = _sweep_canonical_regions(loci, 3_000_000)
    assert len(regions) == 2
    assert regions[0].quality_controls == ()
    assert regions[1].quality_controls == ()


def test_sweep_never_emits_overlapping_or_misordered_regions_on_random_multi_locus_input():
    import random

    rng = random.Random(20260826)
    for _ in range(5_000):
        n_loci = rng.randint(1, 8)
        loci = []
        for i in range(n_loci):
            start = rng.randint(0, 200)
            end = start + rng.randint(0, 100)
            lead = rng.randint(start, end)
            loci.append(SourceLocus(f"STUDY_{i}", f"locus_{i}", "EUR", "1", start, end, lead))
        loci.sort(key=lambda locus: (locus.locus_start, locus.locus_end, locus.source_key))
        regions = _sweep_canonical_regions(loci, 1_000_000)
        for earlier, later in zip(regions, regions[1:], strict=False):  # noqa: RUF007
            assert earlier.chromosome != later.chromosome or earlier.region_end < later.region_start
        for region in regions:
            for member in region.input_loci:
                assert region.region_start <= member.lead_position <= region.region_end
        seen_source_keys: set[tuple[str, str]] = set()
        for region in regions:
            for member in region.input_loci:
                key = (member.study_id, member.study_locus_id)
                assert key not in seen_source_keys, f"{key} appeared in more than one region"
                seen_source_keys.add(key)


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
    if not selects:
        selects.append(
            """
            SELECT
                NULL::VARCHAR AS studyId,
                NULL::VARCHAR AS variantId,
                NULL::VARCHAR AS chromosome,
                NULL::INTEGER AS position,
                NULL::DOUBLE AS beta,
                NULL::INTEGER AS sampleSize,
                NULL::FLOAT AS pValueMantissa,
                NULL::INTEGER AS pValueExponent,
                NULL::FLOAT AS effectAlleleFrequencyFromSource,
                NULL::DOUBLE AS standardError
            WHERE FALSE
            """
        )
    with duckdb.connect() as con:
        con.execute(f"COPY ({' UNION ALL '.join(selects)}) TO '{path}' (FORMAT PARQUET)")
    return path


def test_read_source_loci_computes_each_locus_own_lead_from_its_own_bounds(tmp_path: Path) -> None:
    locus_a = _write_locus_breaker_dataset_with_loci(tmp_path / "a.locus.parquet", study_id="STUDY_A", loci=[("a1", 100, 300)])
    sum_a = _write_sumstats_dataset_with_rows(
        tmp_path / "a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[
            ("1_150_A_G", 150, 0.1, -6, 1.0, 0.2, 0.01),
            ("1_250_A_G", 250, 0.1, -9, 1.0, 0.2, 0.01),
        ],
    )
    prepared = (CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=locus_a, summary_statistics_path=sum_a),)
    loci = _read_source_loci(prepared, min_maf=0.01)
    assert len(loci) == 1
    assert loci[0].lead_position == 250


def test_read_source_loci_ties_break_by_position_then_variant_id(tmp_path: Path) -> None:
    locus_a = _write_locus_breaker_dataset_with_loci(tmp_path / "a.locus.parquet", study_id="STUDY_A", loci=[("a1", 100, 300)])
    sum_a = _write_sumstats_dataset_with_rows(
        tmp_path / "a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[
            ("1_250_A_G", 250, 0.1, -9, 1.0, 0.2, 0.01),
            ("1_150_A_G", 150, 0.1, -9, 1.0, 0.2, 0.01),
        ],
    )
    prepared = (CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=locus_a, summary_statistics_path=sum_a),)
    loci = _read_source_loci(prepared, min_maf=0.01)
    assert loci[0].lead_position == 150


def test_read_source_loci_excludes_a_locus_with_no_qualifying_variant(tmp_path: Path) -> None:
    locus_a = _write_locus_breaker_dataset_with_loci(tmp_path / "a.locus.parquet", study_id="STUDY_A", loci=[("a1", 100, 300)])
    sum_a = _write_sumstats_dataset_with_rows(tmp_path / "a.sumstats.parquet", study_id="STUDY_A", rows=[])
    prepared = (CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=locus_a, summary_statistics_path=sum_a),)
    loci = _read_source_loci(prepared, min_maf=0.01)
    assert loci == []


def test_read_source_loci_excludes_variants_below_the_maf_cutoff(tmp_path: Path) -> None:
    locus_a = _write_locus_breaker_dataset_with_loci(tmp_path / "a.locus.parquet", study_id="STUDY_A", loci=[("a1", 100, 300)])
    sum_a = _write_sumstats_dataset_with_rows(
        tmp_path / "a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[("1_150_A_G", 150, 0.1, -9, 1.0, 0.001, 0.01)],
    )
    prepared = (CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=locus_a, summary_statistics_path=sum_a),)
    loci = _read_source_loci(prepared, min_maf=0.01)
    assert loci == []


def _resolving(study_id: str, study_locus_id: str, start: int, end: int, lead: int) -> _ResolvingLocus:
    source = SourceLocus(study_id, study_locus_id, "EUR", "1", start, end, lead)
    return _ResolvingLocus(source=source, current_start=start, current_end=end)


def test_resolve_overlap_both_leads_in_intersection_crops_both_to_it():
    left = _resolving("STUDY_A", "a1", 1_000_000, 2_200_000, lead=2_000_000)
    right = _resolving("STUDY_B", "b1", 1_800_000, 3_000_000, lead=1_900_000)
    agreed = _resolve_overlap(left, right)
    assert agreed is True
    assert (left.current_start, left.current_end) == (1_800_000, 2_200_000)
    assert (right.current_start, right.current_end) == (1_800_000, 2_200_000)


def test_resolve_overlap_only_left_lead_in_intersection_trims_right_start():
    left = _resolving("STUDY_A", "a1", 1_000_000, 2_200_000, lead=2_000_000)
    right = _resolving("STUDY_B", "b1", 1_800_000, 3_000_000, lead=2_800_000)
    agreed = _resolve_overlap(left, right)
    assert agreed is False
    assert (left.current_start, left.current_end) == (1_000_000, 2_200_000)
    assert (right.current_start, right.current_end) == (2_200_001, 3_000_000)


def test_resolve_overlap_only_right_lead_in_intersection_trims_left_end():
    left = _resolving("STUDY_A", "a1", 1_000_000, 2_200_000, lead=1_500_000)
    right = _resolving("STUDY_B", "b1", 1_800_000, 3_000_000, lead=1_900_000)
    agreed = _resolve_overlap(left, right)
    assert agreed is False
    assert (left.current_start, left.current_end) == (1_000_000, 1_799_999)
    assert (right.current_start, right.current_end) == (1_800_000, 3_000_000)


def test_resolve_overlap_neither_lead_in_intersection_trims_both_and_leaves_a_gap():
    left = _resolving("STUDY_A", "a1", 1_000_000, 2_200_000, lead=1_500_000)
    right = _resolving("STUDY_B", "b1", 1_800_000, 3_000_000, lead=2_800_000)
    agreed = _resolve_overlap(left, right)
    assert agreed is False
    assert (left.current_start, left.current_end) == (1_000_000, 1_799_999)
    assert (right.current_start, right.current_end) == (2_200_001, 3_000_000)


def test_resolve_overlap_full_containment_picks_the_larger_locus():
    left = _resolving("STUDY_A", "a1", 0, 10_000_000, lead=9_000_000)
    right = _resolving("STUDY_B", "b1", 4_000_000, 4_500_000, lead=4_200_000)
    agreed = _resolve_overlap(left, right)
    assert agreed is True
    assert (left.current_start, left.current_end) == (0, 10_000_000)
    assert (right.current_start, right.current_end) == (0, 10_000_000)


def test_resolve_overlap_full_containment_the_other_direction():
    left = _resolving("STUDY_A", "a1", 4_000_000, 4_500_000, lead=4_200_000)
    right = _resolving("STUDY_B", "b1", 0, 10_000_000, lead=9_000_000)
    agreed = _resolve_overlap(left, right)
    assert agreed is True
    assert (left.current_start, left.current_end) == (0, 10_000_000)
    assert (right.current_start, right.current_end) == (0, 10_000_000)


def test_resolve_overlap_identical_bounds_is_treated_as_containment():
    left = _resolving("STUDY_A", "a1", 1_000_000, 2_000_000, lead=1_500_000)
    right = _resolving("STUDY_B", "b1", 1_000_000, 2_000_000, lead=1_600_000)
    agreed = _resolve_overlap(left, right)
    assert agreed is True
    assert (left.current_start, left.current_end) == (1_000_000, 2_000_000)
    assert (right.current_start, right.current_end) == (1_000_000, 2_000_000)


def test_resolve_overlap_never_trims_a_locus_past_its_own_lead():
    import random

    rng = random.Random(20260825)
    for _ in range(20_000):
        left_start = rng.randint(0, 1_000)
        left_end = left_start + rng.randint(0, 2_000)
        left_lead = rng.randint(left_start, left_end)
        right_start = rng.randint(0, 1_000)
        right_end = right_start + rng.randint(0, 2_000)
        right_lead = rng.randint(right_start, right_end)
        left = _resolving("STUDY_A", "a1", left_start, left_end, left_lead)
        right = _resolving("STUDY_B", "b1", right_start, right_end, right_lead)
        overlaps = left.current_start <= right.current_end and right.current_start <= left.current_end
        if not overlaps:
            continue
        _resolve_overlap(left, right)
        assert left.current_start <= left.source.lead_position <= left.current_end
        assert right.current_start <= right.source.lead_position <= right.current_end
        assert left.current_start <= left.current_end
        assert right.current_start <= right.current_end


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


def test_read_source_loci_selects_each_locus_lead_setwise_with_maf_and_tie_breaking(tmp_path: Path) -> None:
    locus_breaker = _write_locus_breaker_dataset_with_loci(
        tmp_path / "study_a.locus.parquet",
        study_id="STUDY_A",
        loci=[
            ("locus_one", 100, 180),
            ("locus_two", 180, 260),
            ("locus_without_qualifying_variant", 300, 320),
        ],
    )
    summary_statistics = _write_sumstats_dataset_with_rows(
        tmp_path / "study_a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[
            ("1_110_A_G", 110, 0.20, -7, 1.0, 0.20, 0.01),
            ("1_120_A_G", 120, 0.10, -8, 1.0, 0.20, 0.01),
            ("1_180_A_G", 180, 0.05, -8, 1.0, 0.20, 0.01),
            ("1_200_A_G", 200, 0.01, -8, 0.01, 0.20, 0.01),
            ("1_310_A_G", 310, 0.01, -8, 1.0, 0.01, 0.01),
        ],
    )
    prepared = (
        CanonicalRegionInput(
            study_id="STUDY_A",
            ancestry="EUR",
            locus_breaker_path=locus_breaker,
            summary_statistics_path=summary_statistics,
        ),
    )

    assert [(locus.study_locus_id, locus.lead_position) for locus in _read_source_loci(prepared, 0.01)] == [
        ("locus_one", 120),
        ("locus_two", 200),
    ]


def test_collect_canonical_regions_cli_accepts_a_single_input_triple(tmp_path: Path) -> None:
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

    assert result.exit_code == 0, result.output
    assert (tmp_path / "stats" / "run-1.stat.json").exists()
    assert (tmp_path / "stats" / "run-1.stat.parquet").exists()


def test_collect_canonical_regions_config_rejects_zero_input_triples(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="At least one") as excinfo:
        CollectCanonicalRegionsConfig(
            run_id="run-1",
            locus_breaker_paths=(),
            ancestries=(),
            summary_statistics_paths=(),
            fine_mapping_locus_set_output_dir=tmp_path / "fine_mapping_locus_sets",
            stats_parquet_output=tmp_path / "stats" / "run-1.stat.parquet",
            stats_json_output=tmp_path / "stats" / "run-1.stat.json",
        )
    assert "at least one" in str(excinfo.value).lower()


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


def test_collect_canonical_regions_cli_rejects_variant_overlap_threshold_above_one(tmp_path: Path) -> None:
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
            "AFR",
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
            "--canonical_region_min_variant_overlap_proportion",
            "1.1",
        ],
    )

    assert result.exit_code != 0
    assert "canonical_region_min_variant_overlap_proportion" in result.output
    assert "no such option" not in result.output.lower()


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
    # Under the lead-aware sweep, a chain of merely-overlapping loci no
    # longer transitively unions into one region -- only a chain of
    # *agreeing* loci does. So all three studies here share the same lead
    # position (200), which each study's own bounds happen to contain, and
    # each consecutive pair's overlap contains that shared lead: A-B agree
    # and crop to their [150, 250] intersection, then that result (tracked
    # as the group's single running envelope, not each locus's individually
    # remembered bounds) is compared against C and agrees again, cropping
    # further to [180, 250]. The final region is the true 3-way
    # intersection of all three spans -- max(100, 150, 180) to
    # min(250, 300, 320) = (180, 250) -- because each sequential pairwise
    # agreement narrows the group's shared envelope rather than getting
    # stuck after the first.
    locus_breaker_c = _write_locus_breaker_dataset_with_loci(tmp_path / "study_c.locus.parquet", study_id="STUDY_C", loci=[("c_locus", 180, 320)])
    locus_breaker_a = _write_locus_breaker_dataset_with_loci(tmp_path / "study_a.locus.parquet", study_id="STUDY_A", loci=[("a_locus", 100, 250)])
    locus_breaker_b = _write_locus_breaker_dataset_with_loci(tmp_path / "study_b.locus.parquet", study_id="STUDY_B", loci=[("b_locus", 150, 300)])
    sumstats_c = _write_sumstats_dataset_with_rows(
        tmp_path / "study_c.sumstats.parquet", study_id="STUDY_C", rows=[("1_200_A_G", 200, 0.1, -8, 1.0, 0.2, 0.01)]
    )
    sumstats_a = _write_sumstats_dataset_with_rows(
        tmp_path / "study_a.sumstats.parquet", study_id="STUDY_A", rows=[("1_200_A_G", 200, 0.1, -8, 1.0, 0.2, 0.01)]
    )
    sumstats_b = _write_sumstats_dataset_with_rows(
        tmp_path / "study_b.sumstats.parquet", study_id="STUDY_B", rows=[("1_200_A_G", 200, 0.1, -8, 1.0, 0.2, 0.01)]
    )
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
        ("1", 180, 250, ["STUDY_A", "STUDY_B", "STUDY_C"], ["a_locus", "b_locus", "c_locus"], ["STUDY_A", "STUDY_B", "STUDY_C"]),
    ]


def test_collect_canonical_regions_cli_materializes_per_ancestry_locus_set_with_strict_maf_and_deterministic_ids(
    tmp_path: Path,
) -> None:
    locus_breaker_a = _write_locus_breaker_dataset_with_loci(tmp_path / "study_a.locus.parquet", study_id="STUDY_A", loci=[("a_locus", 100, 200)])
    # b_locus's own bounds start at 125 (rather than 180) so that STUDY_B's
    # already-qualifying "1_125_A_C" variant (MAF 0.40, already part of the
    # asserted published locus below) falls inside its own bounds and gives
    # it a lead; its only other own-bounds-and-MAF-qualifying alternative,
    # "1_210_T_C", sits exactly at the MAF cutoff (0.01) and must not qualify.
    #
    # a_locus's own qualifying lead is "1_110_A_G" (position 110), which sits
    # outside the [125, 200] overlap with b_locus, while b_locus's own
    # qualifying lead ("1_125_A_C", position 125) sits inside it. Only
    # b_locus's lead is in the shared zone, so the pairwise resolution rules
    # this a disagreement (two distinct signals, not one): a_locus is trimmed
    # back to (100, 124) and published separately, while b_locus keeps its
    # own bounds (125, 220) and becomes the region asserted below.
    locus_breaker_b = _write_locus_breaker_dataset_with_loci(tmp_path / "study_b.locus.parquet", study_id="STUDY_B", loci=[("b_locus", 125, 220)])
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

    # STUDY_A's published lead within the (125, 220) region is "1_140_C_T"
    # (position 140), not its own-bounds lead "1_110_A_G": position 110 falls
    # outside the published region once a_locus is trimmed away from the
    # disagreement above, and every study is still cross-joined against every
    # region regardless of sweep membership, so STUDY_A's qualifying variant
    # inside b_locus's own region is what ends up published for it.
    expected_study_locus_ids = {
        "STUDY_A": hashlib.md5(b"STUDY_A|1_140_C_T", usedforsecurity=False).hexdigest(),
        "STUDY_B": hashlib.md5(b"STUDY_B|1_125_A_C", usedforsecurity=False).hexdigest(),
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
            125,
            220,
            ["1_140_C_T"],
        ),
        (
            expected_locus_set_id,
            "STUDY_B",
            expected_study_locus_ids["STUDY_B"],
            "1",
            125,
            220,
            ["1_125_A_C", "1_130_A_G"],
        ),
    ]

    stats = json.loads(stats_json_output.read_text())
    assert stats["nPublishedLocusSets"] == 1
    # Two candidate regions now: a_locus's disagreement-trimmed (100, 124)
    # and the published (125, 220).
    assert stats["candidateLocusSizeBp"] == {"n": 2, "mean": 60.5, "min": 25, "max": 96}
    assert stats["publishedLocusSizeBp"] == {"n": 1, "mean": 96.0, "min": 96, "max": 96}


def test_collect_canonical_regions_cli_records_overlap_metrics_without_trimming_passing_outputs(tmp_path: Path) -> None:
    locus_breaker_a = _write_locus_breaker_dataset_with_loci(
        tmp_path / "study_a.locus.parquet",
        study_id="STUDY_A",
        loci=[("a_locus", 100, 220)],
    )
    locus_breaker_b = _write_locus_breaker_dataset_with_loci(
        tmp_path / "study_b.locus.parquet",
        study_id="STUDY_B",
        loci=[("b_locus", 120, 240)],
    )
    locus_breaker_c = _write_locus_breaker_dataset_with_loci(
        tmp_path / "study_c.locus.parquet",
        study_id="STUDY_C",
        loci=[("c_locus", 130, 230)],
    )
    sumstats_a = _write_sumstats_dataset_with_rows(
        tmp_path / "study_a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[
            ("1_150_A_G", 150, 0.10, -8, 1.0, 0.20, 0.01),
            ("1_160_C_T", 160, 0.20, -7, 1.0, 0.25, 0.01),
        ],
    )
    sumstats_b = _write_sumstats_dataset_with_rows(
        tmp_path / "study_b.sumstats.parquet",
        study_id="STUDY_B",
        rows=[("1_150_A_G", 150, 0.15, -8, 1.0, 0.30, 0.01)],
    )
    sumstats_c = _write_sumstats_dataset_with_rows(
        tmp_path / "study_c.sumstats.parquet",
        study_id="STUDY_C",
        rows=[
            ("1_150_A_G", 150, 0.12, -8, 1.0, 0.35, 0.01),
            ("1_160_C_T", 160, 0.18, -7, 1.0, 0.22, 0.01),
        ],
    )
    output_dir = tmp_path / "fine_mapping_locus_sets"
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
            "AFR",
            "--ancestry",
            "EAS",
            "--summary_statistics",
            str(sumstats_a),
            "--summary_statistics",
            str(sumstats_b),
            "--summary_statistics",
            str(sumstats_c),
            "--fine_mapping_locus_set_output_dir",
            str(output_dir),
            "--stats_parquet_output",
            str(stats_parquet_output),
            "--stats_json_output",
            str(tmp_path / "stats" / "run-1.stat.json"),
            "--canonical_region_min_variant_overlap_proportion",
            "0.5",
        ],
    )

    assert result.exit_code == 0, result.output

    files = sorted(output_dir.glob("*.parquet"))
    assert len(files) == 1

    with duckdb.connect() as con:
        stats_rows = con.execute(
            f"""
            SELECT
                fineMappingLocusSetId IS NOT NULL AS isPublished,
                nIntersectionVariants,
                nUnionVariants,
                variantOverlapProportion,
                minimumVariantOverlapProportion,
                qualityControls
            FROM read_parquet('{stats_parquet_output}')
            """
        ).fetchall()
        locus_rows = con.execute(
            f"""
            SELECT
                studyId,
                list_transform(locus, item -> item.variantId) AS locusVariants
            FROM read_parquet('{files[0]}')
            ORDER BY studyId
            """
        ).fetchall()

    assert stats_rows == [(True, 1, 2, 0.5, 0.5, [])]
    assert locus_rows == [
        ("STUDY_A", ["1_150_A_G", "1_160_C_T"]),
        ("STUDY_B", ["1_150_A_G"]),
        ("STUDY_C", ["1_150_A_G", "1_160_C_T"]),
    ]


@pytest.mark.parametrize("threshold", [0.0, 1.0])
def test_collect_canonical_regions_cli_accepts_variant_overlap_threshold_endpoints(tmp_path: Path, threshold: float) -> None:
    locus_breaker_a = _write_locus_breaker_dataset_with_loci(
        tmp_path / "study_a.locus.parquet",
        study_id="STUDY_A",
        loci=[("a_locus", 100, 200)],
    )
    locus_breaker_b = _write_locus_breaker_dataset_with_loci(
        tmp_path / "study_b.locus.parquet",
        study_id="STUDY_B",
        loci=[("b_locus", 110, 210)],
    )
    sumstats_a = _write_sumstats_dataset_with_rows(
        tmp_path / "study_a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[("1_150_A_G", 150, 0.10, -8, 1.0, 0.20, 0.01)],
    )
    sumstats_b = _write_sumstats_dataset_with_rows(
        tmp_path / "study_b.sumstats.parquet",
        study_id="STUDY_B",
        rows=[("1_150_A_G", 150, 0.15, -8, 1.0, 0.25, 0.01)],
    )
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
            "--canonical_region_min_variant_overlap_proportion",
            str(threshold),
        ],
    )

    assert result.exit_code == 0, result.output

    with duckdb.connect() as con:
        thresholds = con.execute(
            f"""
            SELECT minimumVariantOverlapProportion
            FROM read_parquet('{stats_parquet_output}')
            """
        ).fetchall()

    assert thresholds == [(threshold,)]


def test_collect_canonical_regions_cli_uses_exact_variant_ids_for_overlap_metrics(tmp_path: Path) -> None:
    locus_breaker_a = _write_locus_breaker_dataset_with_loci(
        tmp_path / "study_a.locus.parquet",
        study_id="STUDY_A",
        loci=[("a_locus", 100, 200)],
    )
    locus_breaker_b = _write_locus_breaker_dataset_with_loci(
        tmp_path / "study_b.locus.parquet",
        study_id="STUDY_B",
        loci=[("b_locus", 110, 210)],
    )
    sumstats_a = _write_sumstats_dataset_with_rows(
        tmp_path / "study_a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[
            ("1_150_A_G", 150, 0.10, -8, 1.0, 0.20, 0.01),
            ("1_160_C_T", 160, 0.10, -7, 1.0, 0.25, 0.01),
        ],
    )
    sumstats_b = _write_sumstats_dataset_with_rows(
        tmp_path / "study_b.sumstats.parquet",
        study_id="STUDY_B",
        rows=[
            ("1_150_A_T", 150, 0.15, -8, 1.0, 0.30, 0.01),
            ("1_160_C_T", 160, 0.15, -7, 1.0, 0.35, 0.01),
        ],
    )
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
            "--canonical_region_min_variant_overlap_proportion",
            "0.0",
        ],
    )

    assert result.exit_code == 0, result.output

    with duckdb.connect() as con:
        overlap_rows = con.execute(
            f"""
            SELECT
                nIntersectionVariants,
                nUnionVariants,
                variantOverlapProportion
            FROM read_parquet('{stats_parquet_output}')
            """
        ).fetchall()

    assert overlap_rows == [(1, 3, 1 / 3)]


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
    # STUDY_C's own locus never contributes a lead: its one variant (below)
    # fails MAF, so c_locus never enters the sweep, and its far-away bounds
    # (500-550) can't overlap the a/b region either way. It exists purely to
    # exercise the pre-existing "every prepared input is cross-joined
    # against every region regardless of sweep membership" behavior.
    #
    # a_locus's own qualifying lead (position 110) sits outside its overlap
    # with b_locus ([180, 200]), while b_locus's own qualifying lead
    # (position 200) sits inside it -- so pairwise resolution treats them as
    # two distinct signals rather than one merged region: a_locus is trimmed
    # to (100, 179) and b_locus keeps (180, 220). Each of those two regions
    # is missing a MAF-qualifying variant from at least one of the other two
    # studies (STUDY_C always, plus STUDY_A or STUDY_B depending on the
    # region), so NO_VARIANTS_IN_LOCUS blocks publication of both.
    locus_breaker_c = _write_locus_breaker_dataset_with_loci(
        tmp_path / "study_c.locus.parquet",
        study_id="STUDY_C",
        loci=[("c_locus", 500, 550)],
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
            # Genuine own-bounds (180-220) MAF-qualifying lead (MAF 0.30),
            # so b_locus actually survives Task-1 filtering and re-enters
            # the sweep with a real lead of its own, instead of being
            # dropped and only coincidentally reproducing the same blocked
            # outcome via its other, out-of-bounds variants.
            ("1_200_G_T", 200, 0.05, -6, 1.0, 0.30, 0.03),
        ],
    )
    sumstats_c = _write_sumstats_dataset_with_rows(
        tmp_path / "study_c.sumstats.parquet",
        study_id="STUDY_C",
        rows=[("1_520_A_G", 520, 0.1, -8, 1.0, 0.001, 0.01)],
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
            "--locus_breaker",
            str(locus_breaker_c),
            "--ancestry",
            "EUR",
            "--ancestry",
            "AFR",
            "--ancestry",
            "CSA",
            "--summary_statistics",
            str(sumstats_a),
            "--summary_statistics",
            str(sumstats_b),
            "--summary_statistics",
            str(sumstats_c),
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

    assert rows == []

    stats = json.loads(stats_json_output.read_text())
    # Two candidate regions now (a_locus's disagreement-trimmed (100, 179)
    # and b_locus's (180, 220)), both blocked by NO_VARIANTS_IN_LOCUS.
    assert stats["nCandidateLocusSets"] == 2
    assert stats["nPublishedLocusSets"] == 0
    assert stats["nNotPromotedLocusSets"] == 2
    assert stats["notPromotedReasons"] == {"NO_VARIANTS_IN_LOCUS": 2}
    assert set(stats["timingsSeconds"]) == {"inputValidation", "regionDiscovery", "locusMaterialization", "statistics"}
    assert all(value >= 0 for value in stats["timingsSeconds"].values())
    assert stats["candidateLocusSizeBp"]["n"] == 2
    assert stats["publishedLocusSizeBp"] == {"n": 0, "mean": None, "min": None, "max": None}


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
        table_name = create_regional_variants_table(con, prepared, regions)
        rows = con.execute(f"SELECT studyId, ancestry, variantId, position FROM {table_name} ORDER BY studyId, position").fetchall()
    assert rows == [("STUDY_A", "EUR", "1_110_A_G", 110), ("STUDY_B", "AFR", "1_120_A_G", 120)]


def test_build_regional_output_tables_derives_stats_and_published_loci_from_staged_variants(tmp_path: Path) -> None:
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
    prepared = (
        CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=locus_breaker_a, summary_statistics_path=sumstats_a),
        CanonicalRegionInput(study_id="STUDY_B", ancestry="AFR", locus_breaker_path=locus_breaker_b, summary_statistics_path=sumstats_b),
    )
    regions = [
        CanonicalRegion(
            chromosome="1",
            region_start=100,
            region_end=220,
            quality_controls=(),
            input_loci=(
                SourceLocus("STUDY_A", "a_locus", "EUR", "1", 100, 200, 150),
                SourceLocus("STUDY_B", "b_locus", "AFR", "1", 180, 220, 200),
            ),
        )
    ]

    expected_study_locus_ids = {
        "STUDY_A": hashlib.md5(b"STUDY_A|1_110_A_G", usedforsecurity=False).hexdigest(),
        "STUDY_B": hashlib.md5(b"STUDY_B|1_125_A_C", usedforsecurity=False).hexdigest(),
    }
    expected_locus_set_id = hashlib.md5(
        "|".join(sorted(expected_study_locus_ids.values())).encode(),
        usedforsecurity=False,
    ).hexdigest()

    with duckdb.connect() as con:
        staged_variants = create_regional_variants_table(con, prepared, regions)
        stats_table, loci_table = build_regional_output_tables(con, prepared, regions, staged_variants)
        stats_rows = con.execute(
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
            FROM {stats_table}
            """
        ).fetchall()
        locus_rows = con.execute(
            f"""
            SELECT
                fineMappingLocusSetId,
                studyId,
                studyLocusId,
                chromosome,
                locusStart,
                locusEnd,
                list_transform(locus, item -> item.variantId) AS locusVariants
            FROM {loci_table}
            ORDER BY studyId
            """
        ).fetchall()

    assert stats_rows == [
        (
            expected_locus_set_id,
            "1",
            100,
            220,
            6,
            4,
            ["a_locus", "b_locus"],
            [
                {
                    "studyId": "STUDY_A",
                    "studyLocusId": expected_study_locus_ids["STUDY_A"],
                    "nVariants": 3,
                    "nVariantsBelowMafCutoff": 1,
                    "qualityControls": [],
                },
                {
                    "studyId": "STUDY_B",
                    "studyLocusId": expected_study_locus_ids["STUDY_B"],
                    "nVariants": 3,
                    "nVariantsBelowMafCutoff": 1,
                    "qualityControls": [],
                },
            ],
        )
    ]
    assert locus_rows == [
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


def test_duplicate_variants_are_silently_dropped_from_eaf_check(tmp_path: Path) -> None:
    # Both copies of 1_110_A_G are dropped; only 1_120_A_G survives deduplication.
    # EAF is present on the surviving row so the study must not appear as missing.
    path = _write_sumstats_dataset_with_rows(
        tmp_path / "duplicate.sumstats.parquet",
        study_id="STUDY_A",
        rows=[
            ("1_110_A_G", 110, 0.1, -8, 1.0, 0.2, 0.01),
            ("1_110_A_G", 110, 0.2, -7, 1.0, 0.2, 0.01),
            ("1_120_A_G", 120, 0.1, -8, 1.0, 0.3, 0.01),
        ],
    )
    prepared = (CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=path, summary_statistics_path=path),)
    assert _studies_with_missing_eaf(prepared) == []


def test_duplicate_variants_are_excluded_from_staged_variants(tmp_path: Path) -> None:
    # 1_110_A_G appears twice for STUDY_A; both copies must be dropped entirely.
    locus = _write_locus_breaker_dataset_with_loci(tmp_path / "a.locus.parquet", study_id="STUDY_A", loci=[("a", 100, 200)])
    sumstats = _write_sumstats_dataset_with_rows(
        tmp_path / "a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[
            ("1_110_A_G", 110, 0.1, -8, 1.0, 0.2, 0.01),
            ("1_110_A_G", 110, 0.2, -7, 1.0, 0.2, 0.01),
            ("1_150_C_T", 150, 0.1, -9, 1.0, 0.3, 0.01),
        ],
    )
    locus_b = _write_locus_breaker_dataset_with_loci(tmp_path / "b.locus.parquet", study_id="STUDY_B", loci=[("b", 100, 200)])
    sumstats_b = _write_sumstats_dataset_with_rows(
        tmp_path / "b.sumstats.parquet", study_id="STUDY_B", rows=[("1_130_A_G", 130, 0.1, -8, 1.0, 0.2, 0.01)]
    )
    prepared = (
        CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=locus, summary_statistics_path=sumstats),
        CanonicalRegionInput(study_id="STUDY_B", ancestry="AFR", locus_breaker_path=locus_b, summary_statistics_path=sumstats_b),
    )
    regions = [
        CanonicalRegion(
            chromosome="1",
            region_start=100,
            region_end=200,
            quality_controls=(),
            input_loci=(SourceLocus("STUDY_A", "a", "EUR", "1", 100, 200, 150), SourceLocus("STUDY_B", "b", "AFR", "1", 100, 200, 130)),
        )
    ]
    with duckdb.connect() as con:
        table = create_regional_variants_table(con, prepared, regions)
        rows = con.execute(f"SELECT studyId, variantId FROM {table} ORDER BY studyId, variantId").fetchall()
    assert rows == [("STUDY_A", "1_150_C_T"), ("STUDY_B", "1_130_A_G")]


def test_studies_with_missing_eaf_detects_null_eaf(tmp_path: Path) -> None:
    path = tmp_path / "null-eaf.sumstats.parquet"
    with duckdb.connect() as con:
        con.execute(
            f"COPY (SELECT 'STUDY_A'::VARCHAR AS studyId, '1_110_A_G'::VARCHAR AS variantId, '1'::VARCHAR AS chromosome, 110::INTEGER AS position, 1.0::FLOAT AS pValueMantissa, -8::INTEGER AS pValueExponent, NULL::FLOAT AS effectAlleleFrequencyFromSource, 0.1::DOUBLE AS beta, 0.01::DOUBLE AS standardError) TO '{path}' (FORMAT PARQUET)"
        )
    prepared = (CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=path, summary_statistics_path=path),)
    assert _studies_with_missing_eaf(prepared) == ["STUDY_A"]


def test_collect_canonical_regions_materializes_all_inputs_for_single_ancestry_boundary(tmp_path: Path) -> None:
    locus_a = _write_locus_breaker_dataset_with_loci(tmp_path / "a.locus.parquet", study_id="STUDY_A", loci=[("a_locus", 100, 200)])
    locus_b = _write_locus_breaker_dataset_with_loci(tmp_path / "b.locus.parquet", study_id="STUDY_B", loci=[("b_locus", 1000, 1100)])
    sum_a = _write_sumstats_dataset_with_rows(
        tmp_path / "a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[("1_120_A_G", 120, 0.1, -8, 1.0, 0.2, 0.01)],
    )
    sum_b = _write_sumstats_dataset_with_rows(
        tmp_path / "b.sumstats.parquet",
        study_id="STUDY_B",
        rows=[("1_130_A_G", 130, 0.1, -8, 1.0, 0.2, 0.01)],
    )
    output_dir = tmp_path / "fine_mapping_locus_sets"
    stats_path = tmp_path / "stats.parquet"

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
            str(stats_path),
            "--stats_json_output",
            str(tmp_path / "stats.json"),
        ],
    )

    assert result.exit_code == 0, result.output
    with duckdb.connect() as con:
        stats = con.execute(
            f"SELECT fineMappingLocusSetId, list_transform(components, item -> item.studyId) FROM read_parquet('{stats_path}')"
        ).fetchall()
        output_rows = con.execute(f"SELECT studyId FROM read_parquet('{next(output_dir.glob('*.parquet'))}') ORDER BY studyId").fetchall()

    assert len(stats) == 1
    assert stats[0][0] is not None
    assert stats[0][1] == ["STUDY_A", "STUDY_B"]
    assert output_rows == [("STUDY_A",), ("STUDY_B",)]


def test_build_regional_output_tables_consolidates_duplicate_set_ids_using_intersection(tmp_path: Path) -> None:
    locus_a = _write_locus_breaker_dataset_with_loci(tmp_path / "a.locus.parquet", study_id="STUDY_A", loci=[("a", 100, 200)])
    locus_b = _write_locus_breaker_dataset_with_loci(tmp_path / "b.locus.parquet", study_id="STUDY_B", loci=[("b", 100, 200)])
    sum_a = _write_sumstats_dataset_with_rows(
        tmp_path / "a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[
            ("1_160_A_G", 160, 0.1, -9, 1.0, 0.2, 0.01),
            ("1_180_A_G", 180, 0.1, -8, 1.0, 0.2, 0.01),
        ],
    )
    sum_b = _write_sumstats_dataset_with_rows(
        tmp_path / "b.sumstats.parquet",
        study_id="STUDY_B",
        rows=[
            ("1_160_C_T", 160, 0.1, -9, 1.0, 0.2, 0.01),
            ("1_180_C_T", 180, 0.1, -8, 1.0, 0.2, 0.01),
        ],
    )
    prepared = (
        CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=locus_a, summary_statistics_path=sum_a),
        CanonicalRegionInput(study_id="STUDY_B", ancestry="AFR", locus_breaker_path=locus_b, summary_statistics_path=sum_b),
    )
    regions = [
        CanonicalRegion(
            chromosome="1",
            region_start=100,
            region_end=200,
            quality_controls=("SOURCE_QC",),
            input_loci=(SourceLocus("STUDY_A", "a1", "EUR", "1", 100, 200, 150), SourceLocus("STUDY_B", "b1", "AFR", "1", 100, 200, 150)),
        ),
        CanonicalRegion(
            chromosome="1",
            region_start=150,
            region_end=250,
            quality_controls=(),
            input_loci=(SourceLocus("STUDY_A", "a2", "EUR", "1", 150, 250, 200), SourceLocus("STUDY_B", "b2", "AFR", "1", 150, 250, 200)),
        ),
    ]

    with duckdb.connect() as con:
        staged = create_regional_variants_table(con, prepared, regions)
        stats_table, loci_table = build_regional_output_tables(con, prepared, regions, staged)
        stats = con.execute(
            f"SELECT fineMappingLocusSetId, locusStart, locusEnd, list_transform(components, item -> item.qualityControls) FROM {stats_table}"
        ).fetchall()
        loci = con.execute(
            f"SELECT locusStart, locusEnd, qualityControls, list_transform(locus, item -> item.variantId) FROM {loci_table} ORDER BY studyId"
        ).fetchall()

    assert len(stats) == 1
    assert stats[0][1:3] == (150, 200)
    assert all("MULTIPLE_FINE_MAPPING_LOCUS_SETS_OVERLAP_THE_SAME_SIGNAL" in qc for qc in stats[0][3])
    assert all(row[0:2] == (150, 200) for row in loci)
    assert all("MULTIPLE_FINE_MAPPING_LOCUS_SETS_OVERLAP_THE_SAME_SIGNAL" in row[2] for row in loci)
    assert all("SOURCE_QC" in row[2] for row in loci)
    assert [row[3] for row in loci] == [["1_160_A_G", "1_180_A_G"], ["1_160_C_T", "1_180_C_T"]]


def test_build_regional_output_tables_forwards_region_level_quality_controls_to_components(tmp_path: Path) -> None:
    locus_a = _write_locus_breaker_dataset_with_loci(tmp_path / "a.locus.parquet", study_id="STUDY_A", loci=[("a", 100, 260)])
    locus_b = _write_locus_breaker_dataset_with_loci(tmp_path / "b.locus.parquet", study_id="STUDY_B", loci=[("b", 150, 180)])
    sum_a = _write_sumstats_dataset_with_rows(
        tmp_path / "a.sumstats.parquet",
        study_id="STUDY_A",
        rows=[("1_200_A_G", 200, 0.1, -8, 1.0, 0.2, 0.01)],
    )
    sum_b = _write_sumstats_dataset_with_rows(
        tmp_path / "b.sumstats.parquet",
        study_id="STUDY_B",
        rows=[("1_200_C_T", 200, 0.1, -8, 1.0, 0.2, 0.01)],
    )
    prepared = (
        CanonicalRegionInput(study_id="STUDY_A", ancestry="EUR", locus_breaker_path=locus_a, summary_statistics_path=sum_a),
        CanonicalRegionInput(study_id="STUDY_B", ancestry="AFR", locus_breaker_path=locus_b, summary_statistics_path=sum_b),
    )
    regions = [
        CanonicalRegion(
            chromosome="1",
            region_start=100,
            region_end=260,
            # These two tags are arbitrary here -- this test only checks that
            # build_regional_output_tables forwards whatever quality-control
            # tags a region already carries down to its components; it does
            # not exercise how those tags got onto the region in the first
            # place (that used to be the now-removed span-cap mechanism).
            quality_controls=(OVERSIZED_SOURCE_LOCUS_QC, "SOME_OTHER_REGION_LEVEL_QC"),
            input_loci=(
                SourceLocus("STUDY_A", "a", "EUR", "1", 100, 260, 180),
                SourceLocus("STUDY_B", "b", "AFR", "1", 150, 180, 165),
            ),
        )
    ]

    with duckdb.connect() as con:
        staged = create_regional_variants_table(con, prepared, regions)
        stats_table, _loci_table = build_regional_output_tables(con, prepared, regions, staged)
        component_qc = con.execute(f"SELECT list_transform(components, item -> item.qualityControls) FROM {stats_table}").fetchone()[0]

    assert component_qc == [
        ["SOME_OTHER_REGION_LEVEL_QC", OVERSIZED_SOURCE_LOCUS_QC],
        ["SOME_OTHER_REGION_LEVEL_QC", OVERSIZED_SOURCE_LOCUS_QC],
    ]
