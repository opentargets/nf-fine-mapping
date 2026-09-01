"""Canonical-region collector input validation and bounded region sweep."""

# ruff: noqa: E501

from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

import duckdb
from pydantic import BaseModel, ConfigDict, Field, model_validator

from collector.schema import CANONICAL_REGION_INPUT_LOCUS_SCHEMA, CANONICAL_REGION_STATS_SCHEMA, COLLECTED_LOCUS_SCHEMA

OVERSIZED_SOURCE_LOCUS_QC = "SOURCE_LOCUS_EXCEEDS_MAX_REGION_SPAN"
DUPLICATE_FINE_MAPPING_SET_QC = "MULTIPLE_FINE_MAPPING_LOCUS_SETS_OVERLAP_THE_SAME_SIGNAL"
INSUFFICIENT_VARIANT_OVERLAP_QC = "INSUFFICIENT_VARIANT_OVERLAP"
DEFAULT_CANONICAL_REGION_MIN_MAF = 0.01
DEFAULT_CANONICAL_REGION_MIN_VARIANT_OVERLAP_PROPORTION = 0.5
DISK_EXHAUSTION_EXIT_CODE = 75


class DiskExhaustionError(RuntimeError):
    """Raised when DuckDB cannot allocate/write temporary storage."""


def _connect_duckdb() -> duckdb.DuckDBPyConnection:
    """Open DuckDB and direct temporary files to task-local storage when configured."""
    con = duckdb.connect()
    temp_directory = os.environ.get("DUCKDB_TMPDIR") or os.environ.get("TMPDIR")
    if temp_directory:
        Path(temp_directory).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory = {_quote_sql_string(temp_directory)}")
    return con


@contextmanager
def _managed_duckdb():
    """Convert temporary-storage DuckDB failures to the retryable collector error."""
    con = None
    try:
        con = _connect_duckdb()
        yield con
    except (duckdb.Error, OSError) as error:
        _raise_disk_error(error)
    finally:
        if con is not None:
            con.close()


def _raise_disk_error(error: Exception) -> None:
    message = str(error).lower()
    if any(marker in message for marker in ("no space left", "out of disk", "disk full", "could not write")):
        raise DiskExhaustionError(str(error)) from error
    raise error


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
    canonical_region_min_maf: float = Field(default=DEFAULT_CANONICAL_REGION_MIN_MAF, ge=0, lt=0.5)
    canonical_region_min_variant_overlap_proportion: float = Field(
        default=DEFAULT_CANONICAL_REGION_MIN_VARIANT_OVERLAP_PROPORTION,
        ge=0,
        le=1,
    )
    canonical_region_max_region_span_bp: int = Field(default=3_000_000, ge=1)

    @model_validator(mode="after")
    def _validate_parallel_arrays(self) -> CollectCanonicalRegionsConfig:
        if len(self.locus_breaker_paths) < 1:
            raise ValueError("At least one input triple is required")
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
    lead_position: int

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


def create_regional_variants_table(
    con: duckdb.DuckDBPyConnection,
    prepared_inputs: tuple[CanonicalRegionInput, ...],
    regions: list[CanonicalRegion],
    table_name: str = "region_variants",
) -> str:
    """Read summary statistics once into a compact temporary region-scoped relation."""
    if not regions:
        raise ValueError("At least one canonical region is required")
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    region_rows = " UNION ALL ".join(
        f"SELECT {_quote_sql_string(region.canonical_region_id)} AS canonicalRegionId, {_quote_sql_string(region.chromosome)} AS chromosome, {region.region_start}::INTEGER AS locusStart, {region.region_end}::INTEGER AS locusEnd"
        for region in regions
    )
    con.execute("DROP TABLE IF EXISTS _canonical_regions_for_join")
    con.execute(
        f"""
        CREATE TEMP TABLE _canonical_regions_for_join AS
        {region_rows}
        """
    )
    inputs = " UNION ALL ".join(
        f"""
        SELECT
            {_quote_sql_string(prepared.study_id)} AS studyId,
            {_quote_sql_string(prepared.ancestry)} AS ancestry,
            CAST(variantId AS VARCHAR) AS variantId,
            CAST(chromosome AS VARCHAR) AS chromosome,
            CAST(position AS INTEGER) AS position,
            CAST(pValueMantissa AS FLOAT) AS pValueMantissa,
            CAST(pValueExponent AS INTEGER) AS pValueExponent,
            CAST(effectAlleleFrequencyFromSource AS FLOAT) AS effectAlleleFrequencyFromSource,
            CAST(beta AS DOUBLE) AS beta,
            CAST(standardError AS DOUBLE) AS standardError
        FROM {_deduplicated_sumstats_sql(prepared.summary_statistics_path)}
        """
        for prepared in prepared_inputs
    )
    con.execute(
        f"""
        CREATE TEMP TABLE {table_name} AS
        SELECT
            regions.canonicalRegionId,
            stats.studyId,
            stats.ancestry,
            stats.variantId,
            stats.chromosome,
            stats.position,
            stats.pValueMantissa,
            stats.pValueExponent,
            stats.effectAlleleFrequencyFromSource,
            stats.beta,
            stats.standardError
        FROM ({inputs}) AS stats
        INNER JOIN _canonical_regions_for_join AS regions
          ON stats.chromosome = regions.chromosome
         AND stats.position BETWEEN regions.locusStart AND regions.locusEnd
        """
    )
    return table_name


def _create_region_metadata_tables(
    con: duckdb.DuckDBPyConnection,
    regions: list[CanonicalRegion],
    metadata_table_name: str = "canonical_region_metadata",
    inputs_table_name: str = "canonical_region_inputs",
) -> tuple[str, str]:
    """Materialize canonical-region metadata once for downstream SQL output generation."""
    con.execute(f"DROP TABLE IF EXISTS {metadata_table_name}")
    con.execute(f"DROP TABLE IF EXISTS {inputs_table_name}")
    if not regions:
        con.execute(
            f"""
            CREATE TEMP TABLE {metadata_table_name} AS
            SELECT
                CAST(NULL AS VARCHAR) AS canonicalRegionId,
                CAST(NULL AS VARCHAR) AS chromosome,
                CAST(NULL AS INTEGER) AS locusStart,
                CAST(NULL AS INTEGER) AS locusEnd,
                CAST(NULL AS VARCHAR[]) AS qualityControls,
                CAST(NULL AS {CANONICAL_REGION_STATS_SCHEMA.fields[6].sql_type()}) AS inputLoci
            WHERE false
            """
        )
        con.execute(
            f"""
            CREATE TEMP TABLE {inputs_table_name} AS
            SELECT
                CAST(NULL AS VARCHAR) AS canonicalRegionId,
                CAST(NULL AS VARCHAR) AS studyId,
                CAST(NULL AS VARCHAR) AS studyLocusId,
                CAST(NULL AS VARCHAR) AS ancestry
            WHERE false
            """
        )
        return metadata_table_name, inputs_table_name

    metadata_rows = []
    input_rows = []
    for region in regions:
        quality_controls_sql = "[" + ", ".join(_quote_sql_string(item) for item in region.quality_controls) + "]::VARCHAR[]"
        metadata_rows.append(
            f"""
            SELECT
                {_quote_sql_string(region.canonical_region_id)} AS canonicalRegionId,
                {_quote_sql_string(region.chromosome)} AS chromosome,
                {region.region_start}::INTEGER AS locusStart,
                {region.region_end}::INTEGER AS locusEnd,
                {quality_controls_sql} AS qualityControls,
                {_input_loci_sql(region)} AS inputLoci
            """
        )
        input_rows.extend(
            f"""
            SELECT
                {_quote_sql_string(region.canonical_region_id)} AS canonicalRegionId,
                {_quote_sql_string(locus.study_id)} AS studyId,
                {_quote_sql_string(locus.study_locus_id)} AS studyLocusId,
                {_quote_sql_string(locus.ancestry)} AS ancestry
            """
            for locus in region.input_loci
        )
    con.execute(
        f"""
        CREATE TEMP TABLE {metadata_table_name} AS
        {" UNION ALL ".join(metadata_rows)}
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE {inputs_table_name} AS
        {" UNION ALL ".join(input_rows)}
        """
    )
    return metadata_table_name, inputs_table_name


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


def _deduplicated_sumstats_sql(path: Path) -> str:
    """Return summary statistics with ambiguous variantId rows removed, matching locus_breaker semantics."""
    return f"(SELECT * FROM {_read_parquet_sql(path)} QUALIFY count(*) OVER (PARTITION BY CAST(variantId AS VARCHAR)) = 1)"


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
    with _managed_duckdb() as con:
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


def _read_source_loci(prepared_inputs: tuple[CanonicalRegionInput, ...], min_maf: float) -> list[SourceLocus]:
    locus_inputs = " UNION ALL ".join(
        f"""
        SELECT
            {_quote_sql_string(prepared_input.study_id)} AS studyId,
            CAST(studyLocusId AS VARCHAR) AS studyLocusId,
            CAST(chromosome AS VARCHAR) AS chromosome,
            CAST(locusStart AS INTEGER) AS locusStart,
            CAST(locusEnd AS INTEGER) AS locusEnd
        FROM {_read_parquet_sql(prepared_input.locus_breaker_path)}
        """
        for prepared_input in prepared_inputs
    )
    summary_statistics = " UNION ALL ".join(
        f"""
        SELECT
            {_quote_sql_string(prepared_input.study_id)} AS studyId,
            CAST(variantId AS VARCHAR) AS variantId,
            CAST(chromosome AS VARCHAR) AS chromosome,
            CAST(position AS INTEGER) AS position,
            CAST(pValueMantissa AS FLOAT) AS pValueMantissa,
            CAST(pValueExponent AS INTEGER) AS pValueExponent,
            CAST(effectAlleleFrequencyFromSource AS DOUBLE) AS effectAlleleFrequencyFromSource
        FROM {_deduplicated_sumstats_sql(prepared_input.summary_statistics_path)}
        """
        for prepared_input in prepared_inputs
    )
    with _managed_duckdb() as con:
        rows = con.execute(
            f"""
            WITH candidate_leads AS (
                SELECT
                    loci.studyId,
                    loci.studyLocusId,
                    loci.chromosome,
                    loci.locusStart,
                    loci.locusEnd,
                    stats.position,
                    ROW_NUMBER() OVER (
                        PARTITION BY loci.studyId, loci.studyLocusId
                        ORDER BY stats.pValueExponent, stats.pValueMantissa, stats.position, stats.variantId
                    ) AS leadRank
                FROM ({locus_inputs}) AS loci
                INNER JOIN ({summary_statistics}) AS stats
                  ON stats.studyId = loci.studyId
                 AND stats.chromosome = loci.chromosome
                 AND stats.position BETWEEN loci.locusStart AND loci.locusEnd
                 AND least(stats.effectAlleleFrequencyFromSource, 1.0::DOUBLE - stats.effectAlleleFrequencyFromSource) > {min_maf}
            )
            SELECT studyId, studyLocusId, chromosome, locusStart, locusEnd, position
            FROM candidate_leads
            WHERE leadRank = 1
            """
        ).fetchall()

    ancestry_by_study = {prepared_input.study_id: prepared_input.ancestry for prepared_input in prepared_inputs}
    loci = [
        SourceLocus(
            study_id=study_id,
            study_locus_id=study_locus_id,
            ancestry=ancestry_by_study[study_id],
            chromosome=chromosome,
            locus_start=int(locus_start),
            locus_end=int(locus_end),
            lead_position=int(position),
        )
        for study_id, study_locus_id, chromosome, locus_start, locus_end, position in rows
    ]
    return sorted(
        loci,
        key=lambda locus: (
            _chromosome_sort_key(locus.chromosome),
            locus.locus_start,
            locus.locus_end,
            locus.source_key,
        ),
    )


@dataclass
class _ResolvingLocus:
    """One source locus's live, possibly-trimmed position during the resolution sweep."""

    source: SourceLocus
    current_start: int
    current_end: int


def _resolve_overlap(left: _ResolvingLocus, right: _ResolvingLocus) -> bool:
    """Resolve two overlapping loci in place using their fixed leads. Returns True if they agree."""
    if left.current_start <= right.current_start and right.current_end <= left.current_end:
        right.current_start = left.current_start
        right.current_end = left.current_end
        return True
    if right.current_start <= left.current_start and left.current_end <= right.current_end:
        left.current_start = right.current_start
        left.current_end = right.current_end
        return True

    intersection_start = max(left.current_start, right.current_start)
    intersection_end = min(left.current_end, right.current_end)
    if intersection_start > intersection_end:
        raise RuntimeError(
            f"_resolve_overlap called on non-overlapping loci: "
            f"left=({left.current_start},{left.current_end}) right=({right.current_start},{right.current_end})"
        )
    left_lead_in = intersection_start <= left.source.lead_position <= intersection_end
    right_lead_in = intersection_start <= right.source.lead_position <= intersection_end

    if left_lead_in and right_lead_in:
        left.current_start = right.current_start = intersection_start
        left.current_end = right.current_end = intersection_end
        return True

    # Neither locus contains the other (both containment checks above already
    # returned), so the overlap is a genuine stagger: whichever locus starts
    # first also ends first. Trimming must follow that geometry rather than the
    # `left`/`right` argument order, or a "later" locus passed in as `left` can
    # get its current_start pushed past its own current_end.
    earlier, later = (left, right) if left.current_start <= right.current_start else (right, left)
    earlier_lead_in = left_lead_in if earlier is left else right_lead_in
    later_lead_in = right_lead_in if later is right else left_lead_in

    if earlier_lead_in:
        later.current_start = intersection_end + 1
        return False
    if later_lead_in:
        earlier.current_end = intersection_start - 1
        return False
    earlier.current_end = intersection_start - 1
    later.current_start = intersection_end + 1
    return False


def _build_region_from_group(group: list[_ResolvingLocus], envelope_start: int, envelope_end: int, max_region_span_bp: int) -> CanonicalRegion:
    contributing = [item.source for item in group if envelope_start <= item.source.lead_position <= envelope_end]
    sorted_sources = tuple(sorted(contributing, key=lambda locus: locus.source_key))
    quality_controls = (OVERSIZED_SOURCE_LOCUS_QC,) if any(locus.inclusive_span_bp > max_region_span_bp for locus in sorted_sources) else ()
    return CanonicalRegion(
        chromosome=group[0].source.chromosome,
        region_start=envelope_start,
        region_end=envelope_end,
        quality_controls=quality_controls,
        input_loci=sorted_sources,
    )


def _sweep_canonical_regions(source_loci: list[SourceLocus], max_region_span_bp: int) -> list[CanonicalRegion]:
    regions: list[CanonicalRegion] = []
    current_group: list[_ResolvingLocus] = []
    envelope_start: int | None = None
    envelope_end: int | None = None
    published_floor: dict[str, int] = {}

    def flush_current() -> None:
        nonlocal current_group, envelope_start, envelope_end
        if current_group:
            if envelope_start is None or envelope_end is None:
                raise RuntimeError("Canonical-region sweep lost the active region envelope")
            region = _build_region_from_group(current_group, envelope_start, envelope_end, max_region_span_bp)
            regions.append(region)
            published_floor[region.chromosome] = max(published_floor.get(region.chromosome, region.region_end), region.region_end)
        current_group = []
        envelope_start = None
        envelope_end = None

    for locus in source_loci:
        # A locus whose own lead already belongs to an earlier, already-
        # published region on this chromosome cannot be represented without
        # reaching back into territory that region has already claimed --
        # drop it entirely rather than let it distort a later comparison.
        floor = published_floor.get(locus.chromosome)
        if floor is not None and locus.lead_position <= floor:
            continue
        effective_start = locus.locus_start if floor is None else max(locus.locus_start, floor + 1)
        resolving = _ResolvingLocus(source=locus, current_start=effective_start, current_end=locus.locus_end)

        if not current_group:
            current_group = [resolving]
            envelope_start, envelope_end = resolving.current_start, resolving.current_end
            continue

        if envelope_start is None or envelope_end is None:
            raise RuntimeError("Canonical-region sweep lost the active region envelope")
        last = current_group[-1]
        # Symmetric: a locus that ends before the envelope starts does not
        # overlap it either, even though it may still start before the
        # envelope's own end -- checking only one side let a genuinely
        # disjoint pair reach _resolve_overlap with an inverted intersection.
        overlaps = (
            resolving.source.chromosome == last.source.chromosome
            and resolving.current_start <= envelope_end
            and envelope_start <= resolving.current_end
        )
        if not overlaps:
            flush_current()
            current_group = [resolving]
            envelope_start, envelope_end = resolving.current_start, resolving.current_end
            continue

        # `last` may hold a stale individual bound left over from an earlier
        # containment-widening elsewhere in this group; resync it to the
        # group's true current envelope before resolving, so that whatever
        # _resolve_overlap does to `last` is guaranteed to represent a
        # change to the group's envelope, not just to one member's private,
        # possibly-outdated bounds.
        last.current_start, last.current_end = envelope_start, envelope_end
        agreed = _resolve_overlap(last, resolving)

        if agreed:
            current_group.append(resolving)
            envelope_start, envelope_end = last.current_start, last.current_end
            continue

        # Disagreement: adopt whatever _resolve_overlap trimmed `last` (the
        # group's envelope-holder, just resynced above) down to as the
        # group's final bounds -- never a min/max over every member's
        # historical individual bounds. This is what prevents an earlier
        # member widened by a containment merge (and never revisited again)
        # from silently re-inflating the region past a trim applied only to
        # `last`.
        envelope_start, envelope_end = last.current_start, last.current_end
        flush_current()
        current_group = [resolving]
        envelope_start, envelope_end = resolving.current_start, resolving.current_end

    flush_current()
    # Loci are fed in ascending-start order, but a locus that is excluded or
    # start-clamped by the published-floor guard above can cause a later
    # group to flush before an earlier-starting-but-later-processed group
    # does; sort explicitly so callers get a well-defined, position-ordered
    # result rather than relying on emission order by accident.
    return sorted(regions, key=lambda region: (region.chromosome, region.region_start))


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


def build_regional_output_tables(
    con: duckdb.DuckDBPyConnection,
    prepared_inputs: tuple[CanonicalRegionInput, ...],
    regions: list[CanonicalRegion],
    region_variants_table: str = "region_variants",
    stats_table_name: str = "canonical_region_stats_output",
    loci_table_name: str = "published_locus_rows",
    min_maf: float = DEFAULT_CANONICAL_REGION_MIN_MAF,
    min_variant_overlap_proportion: float = DEFAULT_CANONICAL_REGION_MIN_VARIANT_OVERLAP_PROPORTION,
) -> tuple[str, str]:
    """Derive canonical-region stats and published-locus rows from staged variants."""
    con.execute(f"DROP TABLE IF EXISTS {stats_table_name}")
    con.execute(f"DROP TABLE IF EXISTS {loci_table_name}")
    if not regions:
        con.execute(f"CREATE TEMP TABLE {stats_table_name} AS {CANONICAL_REGION_STATS_SCHEMA.empty_select_sql()}")
        con.execute(f"CREATE TEMP TABLE {loci_table_name} AS {COLLECTED_LOCUS_SCHEMA.empty_select_sql()}")
        return stats_table_name, loci_table_name

    metadata_table_name, _inputs_table_name = _create_region_metadata_tables(con, regions)
    component_rows = " UNION ALL ".join(
        f"SELECT {_quote_sql_string(region.canonical_region_id)} AS canonicalRegionId, {_quote_sql_string(prepared.study_id)} AS studyId"
        for region in regions
        for prepared in prepared_inputs
    )
    component_inputs_table_name = "canonical_region_component_inputs"
    con.execute(f"DROP TABLE IF EXISTS {component_inputs_table_name}")
    con.execute(f"CREATE TEMP TABLE {component_inputs_table_name} AS {component_rows}")
    maf_sql = (
        "least("
        "CAST(staged.effectAlleleFrequencyFromSource AS DOUBLE), "
        "1.0::DOUBLE - CAST(staged.effectAlleleFrequencyFromSource AS DOUBLE)"
        f") > {min_maf}"
    )
    locus_type = COLLECTED_LOCUS_SCHEMA.fields[-1].sql_type()
    component_type = CANONICAL_REGION_STATS_SCHEMA.fields[7].sql_type()
    con.execute("DROP TABLE IF EXISTS canonical_region_component_stats")
    con.execute(
        f"""
        CREATE TEMP TABLE canonical_region_component_stats AS
        SELECT
            inputs.canonicalRegionId,
            inputs.studyId,
            count(DISTINCT staged.variantId)::INTEGER AS nVariants,
            count(DISTINCT staged.variantId) FILTER (WHERE {maf_sql})::INTEGER AS nVariantsAboveMafCutoff,
            (
                list(staged.variantId ORDER BY staged.pValueExponent, staged.pValueMantissa, staged.variantId)
                FILTER (WHERE {maf_sql})
            )[1] AS leadVariantId
        FROM {component_inputs_table_name} AS inputs
        LEFT JOIN {region_variants_table} AS staged
          ON staged.canonicalRegionId = inputs.canonicalRegionId
         AND staged.studyId = inputs.studyId
        GROUP BY inputs.canonicalRegionId, inputs.studyId
        """
    )
    con.execute("DROP TABLE IF EXISTS canonical_region_status")
    con.execute(
        f"""
        CREATE TEMP TABLE canonical_region_status AS
        SELECT
            metadata.canonicalRegionId,
            metadata.chromosome,
            metadata.locusStart,
            metadata.locusEnd,
            metadata.qualityControls,
            metadata.inputLoci,
            sum(components.nVariants)::INTEGER AS nVariants,
            sum(components.nVariantsAboveMafCutoff)::INTEGER AS nVariantsAboveMafCutoff,
            list(
                struct_pack(
                    studyId := components.studyId,
                    studyLocusId := md5(components.studyId || '|' || components.leadVariantId),
                    nVariants := components.nVariants,
                    nVariantsBelowMafCutoff := components.nVariants - components.nVariantsAboveMafCutoff,
                    qualityControls := CASE WHEN components.nVariantsAboveMafCutoff = 0 THEN ['NO_VARIANTS_IN_LOCUS']::VARCHAR[] ELSE []::VARCHAR[] END
                )
                ORDER BY components.studyId
            )::{component_type} AS components,
                CASE
                    WHEN count(*) FILTER (WHERE components.leadVariantId IS NOT NULL) = count(*)
                    THEN md5(
                        array_to_string(
                            list_sort(
                                list(md5(components.studyId || '|' || components.leadVariantId))
                                FILTER (WHERE components.leadVariantId IS NOT NULL)
                            ),
                            '|'
                        )
                    )
                    ELSE NULL
                END AS fineMappingLocusSetId
        FROM {metadata_table_name} AS metadata
        INNER JOIN canonical_region_component_stats AS components
          ON components.canonicalRegionId = metadata.canonicalRegionId
        GROUP BY
            metadata.canonicalRegionId,
            metadata.chromosome,
            metadata.locusStart,
            metadata.locusEnd,
            metadata.qualityControls,
            metadata.inputLoci
        """
    )
    con.execute("DROP TABLE IF EXISTS canonical_region_merged_bounds")
    con.execute(
        f"""
        CREATE TEMP TABLE canonical_region_merged_bounds AS
        SELECT
            fineMappingLocusSetId,
            any_value(chromosome) AS chromosome,
            max(locusStart)::INTEGER AS locusStart,
            min(locusEnd)::INTEGER AS locusEnd,
            list_sort(list_distinct(flatten(list(inputLoci)))) AS inputLoci,
            CASE
                WHEN count(*) > 1 THEN list_sort(list_distinct(list_concat(
                    flatten(list(qualityControls)),
                    ['{DUPLICATE_FINE_MAPPING_SET_QC}']::VARCHAR[]
                )))
                ELSE list_sort(list_distinct(flatten(list(qualityControls))))
            END AS qualityControls
        FROM canonical_region_status
        WHERE fineMappingLocusSetId IS NOT NULL
        GROUP BY fineMappingLocusSetId
        """
    )
    con.execute("DROP TABLE IF EXISTS canonical_region_final_variants")
    con.execute(
        f"""
        CREATE TEMP TABLE canonical_region_final_variants AS
        SELECT DISTINCT
            merged.fineMappingLocusSetId,
            staged.studyId,
            staged.variantId,
            staged.chromosome,
            staged.position,
            staged.pValueMantissa,
            staged.pValueExponent,
            staged.effectAlleleFrequencyFromSource,
            staged.beta,
            staged.standardError
        FROM canonical_region_merged_bounds AS merged
        INNER JOIN canonical_region_status AS original
          ON original.fineMappingLocusSetId = merged.fineMappingLocusSetId
        INNER JOIN {region_variants_table} AS staged
          ON staged.canonicalRegionId = original.canonicalRegionId
         AND staged.position BETWEEN merged.locusStart AND merged.locusEnd
        """
    )
    con.execute("DROP TABLE IF EXISTS canonical_region_final_components")
    con.execute(
        f"""
        CREATE TEMP TABLE canonical_region_final_components AS
        SELECT DISTINCT
            merged.fineMappingLocusSetId,
            prepared.studyId
        FROM canonical_region_merged_bounds AS merged
        INNER JOIN canonical_region_status AS original
          ON original.fineMappingLocusSetId = merged.fineMappingLocusSetId
        CROSS JOIN (VALUES {", ".join(f"({_quote_sql_string(prepared.study_id)})" for prepared in prepared_inputs)}) AS prepared(studyId)
        """
    )
    con.execute("DROP TABLE IF EXISTS canonical_region_final_component_stats")
    con.execute(
        f"""
        CREATE TEMP TABLE canonical_region_final_component_stats AS
        SELECT
            components.fineMappingLocusSetId,
            components.studyId,
            count(DISTINCT variants.variantId)::INTEGER AS nVariants,
            count(DISTINCT variants.variantId) FILTER (WHERE least(CAST(variants.effectAlleleFrequencyFromSource AS DOUBLE), 1.0::DOUBLE - CAST(variants.effectAlleleFrequencyFromSource AS DOUBLE)) > {min_maf})::INTEGER AS nVariantsAboveMafCutoff,
            (list(variants.variantId ORDER BY variants.pValueExponent, variants.pValueMantissa, variants.variantId) FILTER (WHERE least(CAST(variants.effectAlleleFrequencyFromSource AS DOUBLE), 1.0::DOUBLE - CAST(variants.effectAlleleFrequencyFromSource AS DOUBLE)) > {min_maf}))[1] AS leadVariantId
        FROM canonical_region_final_components AS components
        LEFT JOIN canonical_region_final_variants AS variants
          ON variants.fineMappingLocusSetId = components.fineMappingLocusSetId
         AND variants.studyId = components.studyId
        GROUP BY components.fineMappingLocusSetId, components.studyId
        """
    )
    con.execute("DROP TABLE IF EXISTS canonical_region_final_variant_membership")
    con.execute(
        f"""
        CREATE TEMP TABLE canonical_region_final_variant_membership AS
        SELECT DISTINCT
            fineMappingLocusSetId,
            studyId,
            variantId
        FROM canonical_region_final_variants
        WHERE least(
            CAST(effectAlleleFrequencyFromSource AS DOUBLE),
            1.0::DOUBLE - CAST(effectAlleleFrequencyFromSource AS DOUBLE)
        ) > {min_maf}
        """
    )
    con.execute("DROP TABLE IF EXISTS canonical_region_final_overlap_stats")
    con.execute(
        """
        CREATE TEMP TABLE canonical_region_final_overlap_stats AS
        WITH component_counts AS (
            SELECT
                fineMappingLocusSetId,
                count(*)::INTEGER AS nComponents
            FROM canonical_region_final_component_stats
            GROUP BY fineMappingLocusSetId
        ),
        variant_membership AS (
            SELECT
                fineMappingLocusSetId,
                variantId,
                count(DISTINCT studyId)::INTEGER AS nStudiesWithVariant
            FROM canonical_region_final_variant_membership
            GROUP BY fineMappingLocusSetId, variantId
        )
        SELECT
            component_counts.fineMappingLocusSetId,
            count(DISTINCT variant_membership.variantId)::INTEGER AS nUnionVariants,
            count(DISTINCT variant_membership.variantId) FILTER (
                WHERE variant_membership.nStudiesWithVariant = component_counts.nComponents
            )::INTEGER AS nIntersectionVariants,
            CASE
                WHEN count(DISTINCT variant_membership.variantId) = 0 THEN NULL
                ELSE count(DISTINCT variant_membership.variantId) FILTER (
                    WHERE variant_membership.nStudiesWithVariant = component_counts.nComponents
                )::DOUBLE / count(DISTINCT variant_membership.variantId)::DOUBLE
            END AS variantOverlapProportion
        FROM component_counts
        LEFT JOIN variant_membership
          ON variant_membership.fineMappingLocusSetId = component_counts.fineMappingLocusSetId
        GROUP BY component_counts.fineMappingLocusSetId, component_counts.nComponents
        """
    )
    con.execute("DROP TABLE IF EXISTS canonical_region_final_status")
    con.execute(
        f"""
        CREATE TEMP TABLE canonical_region_final_status AS
        SELECT
            merged.fineMappingLocusSetId,
            merged.chromosome,
            merged.locusStart,
            merged.locusEnd,
            merged.inputLoci,
            sum(components.nVariants)::INTEGER AS nVariants,
            sum(components.nVariantsAboveMafCutoff)::INTEGER AS nVariantsAboveMafCutoff,
            list(struct_pack(
                studyId := components.studyId,
                studyLocusId := md5(components.studyId || '|' || components.leadVariantId),
                nVariants := components.nVariants,
                nVariantsBelowMafCutoff := components.nVariants - components.nVariantsAboveMafCutoff,
                qualityControls := list_sort(list_distinct(list_concat(
                    merged.qualityControls,
                    CASE WHEN components.nVariantsAboveMafCutoff = 0 THEN ['NO_VARIANTS_IN_LOCUS']::VARCHAR[] ELSE []::VARCHAR[] END
                )))
            ) ORDER BY components.studyId)::{component_type} AS components,
            overlap.nIntersectionVariants,
            overlap.nUnionVariants,
            overlap.variantOverlapProportion,
            list_sort(list_distinct(list_concat(
                merged.qualityControls,
                CASE
                    WHEN overlap.variantOverlapProportion < {min_variant_overlap_proportion}
                    THEN ['{INSUFFICIENT_VARIANT_OVERLAP_QC}']::VARCHAR[]
                    ELSE []::VARCHAR[]
                END
            ))) AS qualityControls,
            CASE WHEN count(*) FILTER (WHERE components.leadVariantId IS NOT NULL) = count(*) THEN merged.fineMappingLocusSetId ELSE NULL END AS publishedFineMappingLocusSetId
        FROM canonical_region_merged_bounds AS merged
        INNER JOIN canonical_region_final_component_stats AS components
          ON components.fineMappingLocusSetId = merged.fineMappingLocusSetId
        INNER JOIN canonical_region_final_overlap_stats AS overlap
          ON overlap.fineMappingLocusSetId = merged.fineMappingLocusSetId
        GROUP BY
            merged.fineMappingLocusSetId,
            merged.chromosome,
            merged.locusStart,
            merged.locusEnd,
            merged.qualityControls,
            merged.inputLoci,
            overlap.nIntersectionVariants,
            overlap.nUnionVariants,
            overlap.variantOverlapProportion
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE {stats_table_name} AS
        SELECT
            publishedFineMappingLocusSetId AS fineMappingLocusSetId,
            chromosome,
            locusStart,
            locusEnd,
            nVariants,
            nVariantsAboveMafCutoff,
            inputLoci,
            components,
            nIntersectionVariants,
            nUnionVariants,
            variantOverlapProportion,
            {min_variant_overlap_proportion}::DOUBLE AS minimumVariantOverlapProportion,
            qualityControls
        FROM canonical_region_final_status
        WHERE publishedFineMappingLocusSetId IS NOT NULL
        ORDER BY locusStart, locusEnd, fineMappingLocusSetId
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE {loci_table_name} AS
        SELECT
            status.publishedFineMappingLocusSetId AS fineMappingLocusSetId,
            md5(components.studyId || '|' || components.leadVariantId) AS studyLocusId,
            components.studyId,
            variants.chromosome,
            status.locusStart,
            status.locusEnd,
            status.qualityControls,
            list(
                struct_pack(
                    variantId := variants.variantId,
                    pValueMantissa := variants.pValueMantissa,
                    pValueExponent := variants.pValueExponent,
                    beta := variants.beta,
                    standardError := variants.standardError
                )
                ORDER BY variants.position, variants.variantId
            )::{locus_type} AS locus
        FROM canonical_region_final_status AS status
        INNER JOIN canonical_region_final_component_stats AS components
          ON components.fineMappingLocusSetId = status.fineMappingLocusSetId
        INNER JOIN canonical_region_final_variants AS variants
          ON variants.fineMappingLocusSetId = status.fineMappingLocusSetId
         AND variants.studyId = components.studyId
         AND least(CAST(variants.effectAlleleFrequencyFromSource AS DOUBLE), 1.0::DOUBLE - CAST(variants.effectAlleleFrequencyFromSource AS DOUBLE)) > {min_maf}
        WHERE status.publishedFineMappingLocusSetId IS NOT NULL
        GROUP BY
            status.publishedFineMappingLocusSetId,
            components.studyId,
            components.leadVariantId,
            variants.chromosome,
            status.locusStart,
            status.locusEnd,
            status.qualityControls
        ORDER BY fineMappingLocusSetId, studyId, studyLocusId
        """
    )
    return stats_table_name, loci_table_name


def _write_stats_json(
    path: Path,
    config: CollectCanonicalRegionsConfig,
    prepared_inputs: tuple[CanonicalRegionInput, ...],
    regions: list[CanonicalRegion],
    timings_seconds: dict[str, float] | None = None,
    published_locus_sizes: list[int] | None = None,
) -> None:
    def size_summary(sizes: list[int]) -> dict[str, float | int | None]:
        if not sizes:
            return {"n": 0, "mean": None, "min": None, "max": None}
        return {"n": len(sizes), "mean": sum(sizes) / len(sizes), "min": min(sizes), "max": max(sizes)}

    payload = {
        "runId": config.run_id,
        "canonicalRegionMinMaf": config.canonical_region_min_maf,
        "canonicalRegionMaxRegionSpanBp": config.canonical_region_max_region_span_bp,
        "inputTuples": [
            {
                "studyId": prepared.study_id,
                "ancestry": prepared.ancestry,
                "locusBreakerPath": str(prepared.locus_breaker_path),
                "summaryStatisticsPath": str(prepared.summary_statistics_path),
            }
            for prepared in prepared_inputs
        ],
        "nCandidateLocusSets": len(regions),
        "nPublishedLocusSets": 0,
        "nNotPromotedLocusSets": len(regions),
        "notPromotedReasons": {"NO_VARIANTS_IN_LOCUS": len(regions)} if regions else {},
        "studiesWithMissingEAF": [],
        "runQualityControls": [],
        "timingsSeconds": timings_seconds or {},
        "candidateLocusSizeBp": size_summary([region.region_end - region.region_start + 1 for region in regions]),
        "publishedLocusSizeBp": size_summary(published_locus_sizes or []),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _studies_with_missing_eaf(
    prepared_inputs: tuple[CanonicalRegionInput, ...],
) -> list[str]:
    """Return studies whose source summary statistics contain a null EAF."""
    missing: list[str] = []
    with _managed_duckdb() as con:
        for prepared in prepared_inputs:
            count_row = con.execute(
                f"""
                SELECT
                    count(*)::BIGINT AS n_rows,
                    count(effectAlleleFrequencyFromSource)::BIGINT AS n_eaf
                FROM {_deduplicated_sumstats_sql(prepared.summary_statistics_path)}
                """
            ).fetchone() or (0, 0)
            n_rows, n_eaf = (int(value or 0) for value in count_row)
            if n_rows and n_eaf != n_rows:
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
        "canonicalRegionMinMaf": config.canonical_region_min_maf,
        "canonicalRegionMaxRegionSpanBp": config.canonical_region_max_region_span_bp,
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
        "nNotPromotedLocusSets": 0,
        "notPromotedReasons": {},
        "candidateLocusSizeBp": {"n": 0, "mean": None, "min": None, "max": None},
        "publishedLocusSizeBp": {"n": 0, "mean": None, "min": None, "max": None},
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _deterministic_study_locus_id(study_id: str, variant_id: str) -> str:
    return hashlib.md5(f"{study_id}|{variant_id}".encode(), usedforsecurity=False).hexdigest()


def _deterministic_fine_mapping_locus_set_id(study_locus_ids: list[str]) -> str:
    payload = "|".join(sorted(study_locus_ids))
    return hashlib.md5(payload.encode(), usedforsecurity=False).hexdigest()


def _write_empty_stats_parquet(path: Path) -> None:
    with _managed_duckdb() as con:
        con.execute(
            f"""
            COPY (
                {CANONICAL_REGION_STATS_SCHEMA.empty_select_sql()}
            ) TO {_quote_sql_string(path.as_posix())} (FORMAT PARQUET)
            """
        )


def _write_stats_parquet_from_table(con: duckdb.DuckDBPyConnection, stats_table_name: str, path: Path) -> None:
    con.execute(
        f"""
        COPY (
            SELECT * FROM {stats_table_name}
            ORDER BY chromosome, locusStart, locusEnd, fineMappingLocusSetId
        ) TO {_quote_sql_string(path.as_posix())} (FORMAT PARQUET)
        """
    )


def _write_fine_mapping_locus_sets_from_table(
    con: duckdb.DuckDBPyConnection,
    output_dir: Path,
    loci_table_name: str,
) -> tuple[int, list[int]]:
    published_ids = [
        row[0]
        for row in con.execute(
            f"""
            SELECT DISTINCT fineMappingLocusSetId
            FROM {loci_table_name}
            ORDER BY fineMappingLocusSetId
            """
        ).fetchall()
    ]
    for fine_mapping_locus_set_id in published_ids:
        output_path = output_dir / f"{fine_mapping_locus_set_id}.parquet"
        con.execute(
            f"""
            COPY (
                SELECT * FROM {loci_table_name}
                WHERE fineMappingLocusSetId = {_quote_sql_string(fine_mapping_locus_set_id)}
                ORDER BY studyId, studyLocusId
            ) TO {_quote_sql_string(output_path.as_posix())} (FORMAT PARQUET)
            """
        )
    published_sizes = [
        int(row[0])
        for row in con.execute(f"SELECT max(locusEnd) - min(locusStart) + 1 FROM {loci_table_name} GROUP BY fineMappingLocusSetId").fetchall()
    ]
    return len(published_ids), published_sizes


def run_collect_canonical_regions(config: CollectCanonicalRegionsConfig) -> tuple[CanonicalRegionInput, ...]:
    """Validate inputs, sweep bounded canonical regions, and emit provisional outputs."""
    _prepare_output_paths(config)
    validation_started = perf_counter()
    prepared_inputs = prepare_collect_canonical_region_inputs(config)
    studies_with_missing_eaf = _studies_with_missing_eaf(prepared_inputs)
    if studies_with_missing_eaf:
        _write_empty_stats_parquet(config.stats_parquet_output)
        _write_invalid_run_stats(config.stats_json_output, config, prepared_inputs, studies_with_missing_eaf)
        return prepared_inputs
    timings: dict[str, float] = {"inputValidation": round(perf_counter() - validation_started, 6)}
    started = perf_counter()
    source_loci = _read_source_loci(prepared_inputs, config.canonical_region_min_maf)
    regions = _sweep_canonical_regions(source_loci, config.canonical_region_max_region_span_bp)
    timings["regionDiscovery"] = round(perf_counter() - started, 6)
    started = perf_counter()
    with _managed_duckdb() as con:
        region_variants_table = create_regional_variants_table(con, prepared_inputs, regions) if regions else ""
        stats_table_name, loci_table_name = build_regional_output_tables(
            con,
            prepared_inputs,
            regions,
            region_variants_table=region_variants_table or "region_variants",
            min_maf=config.canonical_region_min_maf,
            min_variant_overlap_proportion=config.canonical_region_min_variant_overlap_proportion,
        )
        published_count, published_locus_sizes = _write_fine_mapping_locus_sets_from_table(
            con,
            config.fine_mapping_locus_set_output_dir,
            loci_table_name,
        )
        timings["locusMaterialization"] = round(perf_counter() - started, 6)
        started = perf_counter()
        _write_stats_parquet_from_table(con, stats_table_name, config.stats_parquet_output)
    timings["statistics"] = round(perf_counter() - started, 6)
    _write_stats_json(config.stats_json_output, config, prepared_inputs, regions, timings, published_locus_sizes)
    if config.stats_json_output.exists():
        payload = json.loads(config.stats_json_output.read_text())
        payload["nPublishedLocusSets"] = published_count
        payload["nNotPromotedLocusSets"] = len(regions) - published_count
        payload["notPromotedReasons"] = {"NO_VARIANTS_IN_LOCUS": len(regions) - published_count} if len(regions) > published_count else {}
        config.stats_json_output.write_text(json.dumps(payload, indent=2) + "\n")
    return prepared_inputs
