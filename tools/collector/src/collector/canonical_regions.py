"""Canonical-region collector input validation and bounded region sweep."""

# ruff: noqa: E501

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import cast

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from collector.schema import CANONICAL_REGION_INPUT_LOCUS_SCHEMA, CANONICAL_REGION_STATS_SCHEMA, COLLECTED_LOCUS_SCHEMA

OVERSIZED_SOURCE_LOCUS_QC = "SOURCE_LOCUS_EXCEEDS_MAX_REGION_SPAN"
MAF_CUTOFF = 0.01


class CanonicalRegionInput(BaseModel):
    """One aligned canonical-region input triple after study-ID validation."""

    model_config = ConfigDict(frozen=True)

    study_id: str = Field(min_length=1)
    ancestry: str = Field(min_length=1)
    locus_breaker_path: Path
    summary_statistics_path: Path


class CollectCanonicalRegionsConfig(BaseModel):
    """Path and cardinality contract for the collect_canonical_regions command."""

    model_config = ConfigDict(frozen=True)

    run_id: str = Field(min_length=1)
    locus_breaker_paths: tuple[Path, ...]
    ancestries: tuple[str, ...]
    summary_statistics_paths: tuple[Path, ...]
    fine_mapping_locus_set_output_dir: Path
    stats_parquet_output: Path
    stats_json_output: Path
    max_region_span_bp: int = Field(default=3_000_000, ge=1)

    @model_validator(mode="after")
    def _validate_parallel_arrays(self) -> CollectCanonicalRegionsConfig:
        if len(self.locus_breaker_paths) < 2:
            raise ValueError("At least two input triples are required")
        expected_length = len(self.locus_breaker_paths)
        if len(self.ancestries) != expected_length or len(self.summary_statistics_paths) != expected_length:
            raise ValueError("locus_breaker, ancestry, and summary_statistics arrays must have equal length")
        _assert_distinct(self.locus_breaker_paths, "LocusBreaker paths")
        _assert_distinct(self.summary_statistics_paths, "summary-statistics paths")
        _assert_distinct(self.ancestries, "ancestry labels")
        return self


@dataclass(frozen=True)
class SourceLocus:
    """One input locus used by the canonical-region sweep."""

    study_id: str
    study_locus_id: str
    ancestry: str
    chromosome: str
    locus_start: int
    locus_end: int

    @property
    def source_key(self) -> tuple[str, str]:
        """Return the stable source ordering key."""
        return (self.study_id, self.study_locus_id)

    @property
    def inclusive_span_bp(self) -> int:
        """Return the inclusive span of the source locus."""
        return self.locus_end - self.locus_start + 1


@dataclass(frozen=True)
class CanonicalRegion:
    """One merged canonical region."""

    chromosome: str
    region_start: int
    region_end: int
    quality_controls: tuple[str, ...]
    input_loci: tuple[SourceLocus, ...]

    @property
    def canonical_region_id(self) -> str:
        """Return a deterministic identifier from region bounds and provenance."""
        payload = "|".join(
            [
                self.chromosome,
                str(self.region_start),
                str(self.region_end),
                *[f"{locus.study_id}:{locus.study_locus_id}" for locus in self.input_loci],
            ]
        )
        return hashlib.md5(payload.encode(), usedforsecurity=False).hexdigest()


def _assert_distinct(values: tuple[object, ...], label: str) -> None:
    normalized = [str(value) for value in values]
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{label} must contain distinct values")


def _quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parquet_glob(path: Path) -> str:
    return (path / "**" / "*.parquet").as_posix() if path.is_dir() else path.as_posix()


def _read_parquet_sql(path: Path) -> str:
    return f"read_parquet({_quote_sql_string(_parquet_glob(path))}, union_by_name = true, hive_partitioning = true)"


def _prepare_output_paths(config: CollectCanonicalRegionsConfig) -> None:
    config.fine_mapping_locus_set_output_dir.mkdir(parents=True, exist_ok=True)
    for output_path in config.fine_mapping_locus_set_output_dir.glob("*.parquet"):
        output_path.unlink()
    for output_path in (config.stats_parquet_output, config.stats_json_output):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            if output_path.is_dir():
                raise IsADirectoryError(f"Output path is a directory: {output_path}")
            output_path.unlink()


def _single_study_id(con: duckdb.DuckDBPyConnection, path: Path, dataset_label: str) -> str:
    if not path.exists():
        raise FileNotFoundError(path)
    rows = con.execute(
        f"""
        SELECT DISTINCT CAST(studyId AS VARCHAR) AS studyId
        FROM {_read_parquet_sql(path)}
        WHERE studyId IS NOT NULL
        ORDER BY studyId
        """
    ).fetchall()
    if len(rows) != 1:
        raise ValueError(f"{dataset_label} {path} must contain exactly one distinct studyId")
    return rows[0][0]


def prepare_collect_canonical_region_inputs(config: CollectCanonicalRegionsConfig) -> tuple[CanonicalRegionInput, ...]:
    """Validate, align, and sort the canonical-region input triples by studyId."""
    prepared: list[CanonicalRegionInput] = []
    with duckdb.connect() as con:
        for locus_breaker_path, ancestry, summary_statistics_path in zip(
            config.locus_breaker_paths,
            config.ancestries,
            config.summary_statistics_paths,
            strict=True,
        ):
            locus_breaker_study_id = _single_study_id(con, locus_breaker_path, "LocusBreaker input")
            summary_statistics_study_id = _single_study_id(con, summary_statistics_path, "summary-statistics input")
            if locus_breaker_study_id != summary_statistics_study_id:
                raise ValueError(
                    "Each LocusBreaker input and summary-statistics input must have exactly one matching studyId; "
                    f"got {locus_breaker_study_id} and {summary_statistics_study_id}"
                )
            prepared.append(
                CanonicalRegionInput(
                    study_id=locus_breaker_study_id,
                    ancestry=ancestry,
                    locus_breaker_path=locus_breaker_path,
                    summary_statistics_path=summary_statistics_path,
                )
            )
    return tuple(sorted(prepared, key=lambda record: record.study_id))


def _chromosome_sort_key(chromosome: str) -> tuple[int, int | str]:
    normalized = chromosome.removeprefix("chr").removeprefix("CHR")
    if normalized.isdigit():
        return (0, int(normalized))
    return (1, normalized)


def _read_source_loci(prepared_inputs: tuple[CanonicalRegionInput, ...]) -> list[SourceLocus]:
    loci: list[SourceLocus] = []
    with duckdb.connect() as con:
        for prepared_input in prepared_inputs:
            rows = con.execute(
                f"""
                SELECT
                    CAST(studyLocusId AS VARCHAR) AS studyLocusId,
                    CAST(chromosome AS VARCHAR) AS chromosome,
                    CAST(locusStart AS INTEGER) AS locusStart,
                    CAST(locusEnd AS INTEGER) AS locusEnd
                FROM {_read_parquet_sql(prepared_input.locus_breaker_path)}
                ORDER BY
                    CASE WHEN try_cast(chromosome AS INTEGER) IS NULL THEN 1 ELSE 0 END,
                    try_cast(chromosome AS INTEGER),
                    chromosome,
                    locusStart,
                    locusEnd,
                    studyLocusId
                """
            ).fetchall()
            loci.extend(
                [
                    SourceLocus(
                        study_id=prepared_input.study_id,
                        study_locus_id=study_locus_id,
                        ancestry=prepared_input.ancestry,
                        chromosome=chromosome,
                        locus_start=locus_start,
                        locus_end=locus_end,
                    )
                    for study_locus_id, chromosome, locus_start, locus_end in rows
                ]
            )
    return sorted(
        loci,
        key=lambda locus: (
            _chromosome_sort_key(locus.chromosome),
            locus.locus_start,
            locus.locus_end,
            locus.source_key,
        ),
    )


def _build_region(input_loci: list[SourceLocus], quality_controls: tuple[str, ...] = ()) -> CanonicalRegion:
    sorted_input_loci = tuple(sorted(input_loci, key=lambda locus: locus.source_key))
    return CanonicalRegion(
        chromosome=sorted_input_loci[0].chromosome,
        region_start=min(locus.locus_start for locus in sorted_input_loci),
        region_end=max(locus.locus_end for locus in sorted_input_loci),
        quality_controls=quality_controls,
        input_loci=sorted_input_loci,
    )


def _sweep_canonical_regions(source_loci: list[SourceLocus], max_region_span_bp: int) -> list[CanonicalRegion]:
    regions: list[CanonicalRegion] = []
    current: list[SourceLocus] = []

    def flush_current() -> None:
        if current:
            regions.append(_build_region(current))
            current.clear()

    for locus in source_loci:
        if locus.inclusive_span_bp > max_region_span_bp:
            flush_current()
            regions.append(_build_region([locus], quality_controls=(OVERSIZED_SOURCE_LOCUS_QC,)))
            continue

        if not current:
            current.append(locus)
            continue

        current_region = _build_region(current)
        overlaps_current = locus.chromosome == current_region.chromosome and locus.locus_start <= current_region.region_end
        merged_span_bp = max(current_region.region_end, locus.locus_end) - min(current_region.region_start, locus.locus_start) + 1
        if overlaps_current and merged_span_bp <= max_region_span_bp:
            current.append(locus)
            continue

        flush_current()
        current.append(locus)

    flush_current()
    return regions


def _input_loci_sql(region: CanonicalRegion) -> str:
    input_locus_type = CANONICAL_REGION_INPUT_LOCUS_SCHEMA.sql_type()
    items_sql = ", ".join(
        [
            "struct_pack("
            f"studyId := {_quote_sql_string(locus.study_id)}, "
            f"studyLocusId := {_quote_sql_string(locus.study_locus_id)}, "
            f"ancestry := {_quote_sql_string(locus.ancestry)}"
            ")"
            for locus in region.input_loci
        ]
    )
    return f"[{items_sql}]::{input_locus_type}[]"


def _write_stats_parquet(path: Path, prepared_inputs: tuple[CanonicalRegionInput, ...], regions: list[CanonicalRegion]) -> None:
    with duckdb.connect() as con:
        if not regions:
            con.execute(
                f"""
                COPY (
                    {CANONICAL_REGION_STATS_SCHEMA.empty_select_sql()}
                ) TO {_quote_sql_string(path.as_posix())} (FORMAT PARQUET)
                """
            )
            return

        stats = []
        for region in regions:
            components = []
            total_raw = 0
            total_above = 0
            lead_ids = []
            source_loci_by_study = {locus.study_id: locus for locus in region.input_loci}
            for prepared in prepared_inputs:
                source_locus = source_loci_by_study.get(prepared.study_id)
                if source_locus is None:
                    continue
                raw, above = con.execute(
                    f"""
                    SELECT count(DISTINCT variantId), count(DISTINCT variantId) FILTER (WHERE least(effectAlleleFrequencyFromSource, 1.0 - effectAlleleFrequencyFromSource) > {MAF_CUTOFF})  -- noqa: E501
                    FROM {_read_parquet_sql(prepared.summary_statistics_path)}
                    WHERE CAST(chromosome AS VARCHAR) = {_quote_sql_string(region.chromosome)}
                      AND CAST(position AS INTEGER) BETWEEN {region.region_start} AND {region.region_end}
                    """
                ).fetchone() or (0, 0)
                raw, above = int(raw or 0), int(above or 0)
                total_raw += raw
                total_above += above
                controls = ["NO_VARIANTS_IN_LOCUS"] if above == 0 else []
                if above:
                    lead_row = con.execute(
                        f"""SELECT variantId FROM {_read_parquet_sql(prepared.summary_statistics_path)}
                        WHERE CAST(chromosome AS VARCHAR) = {_quote_sql_string(region.chromosome)}
                          AND CAST(position AS INTEGER) BETWEEN {region.region_start} AND {region.region_end}
                          AND least(effectAlleleFrequencyFromSource, 1.0 - effectAlleleFrequencyFromSource) > {MAF_CUTOFF}
                        ORDER BY pValueExponent, pValueMantissa, variantId LIMIT 1"""
                    ).fetchone() or (None,)
                    lead = lead_row[0]
                    if lead is None:
                        continue
                    lead_ids.append(_deterministic_study_locus_id(prepared.study_id, lead))
                components.append((prepared.study_id, source_locus.study_locus_id, raw, raw - above, controls))
            locus_set_id = _deterministic_fine_mapping_locus_set_id(lead_ids) if len(lead_ids) == len(prepared_inputs) else None
            input_sql = (
                "["
                + ", ".join(
                    f"struct_pack(studyId := {_quote_sql_string(source.study_id)}, studyLocusId := {_quote_sql_string(source.study_locus_id)})"
                    for source in region.input_loci
                )
                + "]"
            )
            component_sql = (
                "["
                + ", ".join(
                    f"struct_pack(studyId := {_quote_sql_string(study_id)}, studyLocusId := {_quote_sql_string(study_locus_id)}, nVariants := {raw}, nVariantsBelowMafCutoff := {below}, qualityControls := [{', '.join(_quote_sql_string(q) for q in controls)}]::VARCHAR[])"
                    for study_id, study_locus_id, raw, below, controls in components
                )
                + "]"
            )
            stats.append(
                f"SELECT {(_quote_sql_string(locus_set_id) if locus_set_id else 'CAST(NULL AS VARCHAR)')} AS fineMappingLocusSetId, {_quote_sql_string(region.chromosome)} AS chromosome, {region.region_start}::INTEGER AS locusStart, {region.region_end}::INTEGER AS locusEnd, {total_raw}::INTEGER AS nVariants, {total_above}::INTEGER AS nVariantsAboveMafCutoff, {input_sql}::{CANONICAL_REGION_STATS_SCHEMA.fields[6].sql_type()} AS inputLoci, {component_sql}::{CANONICAL_REGION_STATS_SCHEMA.fields[7].sql_type()} AS components"
            )
        region_sql = " UNION ALL ".join(stats)
        con.execute(
            f"""
            COPY (
                {region_sql}
                ORDER BY chromosome, locusStart, locusEnd, fineMappingLocusSetId
            ) TO {_quote_sql_string(path.as_posix())} (FORMAT PARQUET)
            """
        )


def _write_stats_json(
    path: Path,
    config: CollectCanonicalRegionsConfig,
    regions: list[CanonicalRegion],
    timings_seconds: dict[str, float] | None = None,
) -> None:
    payload = {
        "runId": config.run_id,
        "inputTuples": [
            {
                "studyId": prepared.study_id,
                "ancestry": prepared.ancestry,
                "locusBreakerPath": str(prepared.locus_breaker_path),
                "summaryStatisticsPath": str(prepared.summary_statistics_path),
            }
            for prepared in prepare_collect_canonical_region_inputs(config)
        ],
        "nCandidateLocusSets": len(regions),
        "nPublishedLocusSets": 0,
        "studiesWithMissingEAF": [],
        "runQualityControls": [],
        "timingsSeconds": timings_seconds or {},
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _studies_with_missing_eaf(
    prepared_inputs: tuple[CanonicalRegionInput, ...],
) -> list[str]:
    """Return studies whose source summary statistics contain a null EAF."""
    missing: list[str] = []
    with duckdb.connect() as con:
        for prepared in prepared_inputs:
            count_row = con.execute(
                f"""
                SELECT count(*)
                FROM {_read_parquet_sql(prepared.summary_statistics_path)}
                WHERE effectAlleleFrequencyFromSource IS NULL
                """
            ).fetchone() or (0,)
            count = count_row[0]
            if count:
                missing.append(prepared.study_id)
    return sorted(missing)


def _write_invalid_run_stats(
    path: Path,
    config: CollectCanonicalRegionsConfig,
    prepared_inputs: tuple[CanonicalRegionInput, ...],
    studies_with_missing_eaf: list[str],
) -> None:
    """Emit the compact run report for a fatal preflight QC result."""
    payload = {
        "runId": config.run_id,
        "inputTuples": [
            {
                "studyId": prepared.study_id,
                "ancestry": prepared.ancestry,
                "locusBreakerPath": str(prepared.locus_breaker_path),
                "summaryStatisticsPath": str(prepared.summary_statistics_path),
            }
            for prepared in prepared_inputs
        ],
        "studiesWithMissingEAF": studies_with_missing_eaf,
        "runQualityControls": ["MISSING_EFFECT_ALLELE_FREQUENCY_FROM_SOURCE"],
        "nCandidateLocusSets": 0,
        "nPublishedLocusSets": 0,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _deterministic_study_locus_id(study_id: str, variant_id: str) -> str:
    return hashlib.md5(f"{study_id}{variant_id}".encode(), usedforsecurity=False).hexdigest()


def _deterministic_fine_mapping_locus_set_id(study_locus_ids: list[str]) -> str:
    payload = "|".join(sorted(study_locus_ids))
    return hashlib.md5(payload.encode(), usedforsecurity=False).hexdigest()


def _materialize_variants(
    con: duckdb.DuckDBPyConnection,
    summary_statistics_path: Path,
    region: CanonicalRegion,
) -> list[tuple[str, float, int, float | None, float | None]]:
    return con.execute(
        f"""
        SELECT
            CAST(variantId AS VARCHAR) AS variantId,
            CAST(pValueMantissa AS FLOAT) AS pValueMantissa,
            CAST(pValueExponent AS INTEGER) AS pValueExponent,
            CAST(beta AS DOUBLE) AS beta,
            CAST(standardError AS DOUBLE) AS standardError
        FROM {_read_parquet_sql(summary_statistics_path)}
        WHERE CAST(chromosome AS VARCHAR) = {_quote_sql_string(region.chromosome)}
          AND CAST(position AS INTEGER) BETWEEN {region.region_start} AND {region.region_end}
          AND effectAlleleFrequencyFromSource IS NOT NULL
          AND least(
                CAST(effectAlleleFrequencyFromSource AS DOUBLE),
                1.0::DOUBLE - CAST(effectAlleleFrequencyFromSource AS DOUBLE)
          ) > {MAF_CUTOFF}
        ORDER BY CAST(position AS INTEGER), variantId
        """
    ).fetchall()


def _write_fine_mapping_locus_sets(
    config: CollectCanonicalRegionsConfig,
    prepared_inputs: tuple[CanonicalRegionInput, ...],
    regions: list[CanonicalRegion],
) -> int:
    published_count = 0
    locus_type = COLLECTED_LOCUS_SCHEMA.fields[-1].sql_type()

    with duckdb.connect() as con:
        for region in regions:
            materialized_rows: list[dict[str, object]] = []
            for prepared in prepared_inputs:
                variants = _materialize_variants(con, prepared.summary_statistics_path, region)
                if not variants:
                    materialized_rows = []
                    break

                lead_variant_id, _lead_mantissa, _lead_exponent, _lead_beta, _lead_standard_error = min(
                    variants,
                    key=lambda row: (
                        row[2],
                        row[1],
                        row[0],
                    ),
                )
                materialized_rows.append(
                    {
                        "studyId": prepared.study_id,
                        "studyLocusId": _deterministic_study_locus_id(prepared.study_id, lead_variant_id),
                        "variants": variants,
                    }
                )

            if not materialized_rows:
                continue

            fine_mapping_locus_set_id = _deterministic_fine_mapping_locus_set_id(
                [row["studyLocusId"] for row in materialized_rows if isinstance(row["studyLocusId"], str)]
            )
            quality_controls_sql = "[" + ", ".join(_quote_sql_string(item) for item in region.quality_controls) + "]::VARCHAR[]"
            row_sql = []
            for row in materialized_rows:
                variants = cast(list[tuple[str, float, int, float | None, float | None]], row["variants"])
                variants_sql = ", ".join(
                    [
                        "struct_pack("
                        f"variantId := {_quote_sql_string(variant_id)}, "
                        f"pValueMantissa := {pvalue_mantissa}::FLOAT, "
                        f"pValueExponent := {pvalue_exponent}::INTEGER, "
                        f"beta := {beta if beta is not None else 'NULL'}::DOUBLE, "
                        f"standardError := {standard_error if standard_error is not None else 'NULL'}::DOUBLE"
                        ")"
                        for variant_id, pvalue_mantissa, pvalue_exponent, beta, standard_error in variants
                    ]
                )
                row_sql.append(
                    f"""
                    SELECT
                        {_quote_sql_string(fine_mapping_locus_set_id)} AS fineMappingLocusSetId,
                        {_quote_sql_string(str(row["studyLocusId"]))} AS studyLocusId,
                        {_quote_sql_string(str(row["studyId"]))} AS studyId,
                        {_quote_sql_string(region.chromosome)} AS chromosome,
                        {region.region_start}::INTEGER AS locusStart,
                        {region.region_end}::INTEGER AS locusEnd,
                        {quality_controls_sql} AS qualityControls,
                        [{variants_sql}]::{locus_type} AS locus
                    """
                )

            output_path = config.fine_mapping_locus_set_output_dir / f"{fine_mapping_locus_set_id}.parquet"
            con.execute(
                f"""
                COPY (
                    {" UNION ALL ".join(row_sql)}
                    ORDER BY studyId, studyLocusId
                ) TO {_quote_sql_string(output_path.as_posix())} (FORMAT PARQUET)
                """
            )
            published_count += 1

    return published_count


def run_collect_canonical_regions(config: CollectCanonicalRegionsConfig) -> tuple[CanonicalRegionInput, ...]:
    """Validate inputs, sweep bounded canonical regions, and emit provisional outputs."""
    _prepare_output_paths(config)
    validation_started = perf_counter()
    prepared_inputs = prepare_collect_canonical_region_inputs(config)
    studies_with_missing_eaf = _studies_with_missing_eaf(prepared_inputs)
    if studies_with_missing_eaf:
        _write_stats_parquet(config.stats_parquet_output, prepared_inputs, [])
        _write_invalid_run_stats(config.stats_json_output, config, prepared_inputs, studies_with_missing_eaf)
        return prepared_inputs
    timings: dict[str, float] = {"inputValidation": round(perf_counter() - validation_started, 6)}
    started = perf_counter()
    source_loci = _read_source_loci(prepared_inputs)
    regions = _sweep_canonical_regions(source_loci, config.max_region_span_bp)
    timings["regionDiscovery"] = round(perf_counter() - started, 6)
    started = perf_counter()
    published_count = _write_fine_mapping_locus_sets(config, prepared_inputs, regions)
    timings["locusMaterialization"] = round(perf_counter() - started, 6)
    started = perf_counter()
    _write_stats_parquet(config.stats_parquet_output, prepared_inputs, regions)
    timings["statistics"] = round(perf_counter() - started, 6)
    _write_stats_json(config.stats_json_output, config, regions, timings)
    if config.stats_json_output.exists():
        payload = json.loads(config.stats_json_output.read_text())
        payload["nPublishedLocusSets"] = published_count
        config.stats_json_output.write_text(json.dumps(payload, indent=2) + "\n")
    return prepared_inputs
