"""Adapt Hailing Ducks native LD output to the Gentropy LD contract."""

from __future__ import annotations

import json
import resource
import subprocess
import tempfile
from collections.abc import Callable
from pathlib import Path
from time import perf_counter

import duckdb
from pydantic import BaseModel, ConfigDict, Field


class HailingLdReference(BaseModel):
    """Native Hail references for one ancestry."""

    model_config = ConfigDict(frozen=True)

    ancestry: str = Field(min_length=1)
    ht_path: str = Field(min_length=1)
    bm_path: str = Field(min_length=1)


class HailingStudyMetadata(BaseModel):
    """Study-to-ancestry mapping used to select one LD reference."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    study_id: str = Field(alias="studyId", min_length=1)
    ancestry: str = Field(min_length=1)


class HailingLdConfig(BaseModel):
    """Input and output contract for Hailing Ducks LD annotation."""

    model_config = ConfigDict(frozen=True)

    input_path: Path
    study_metadata_path: Path
    output_path: Path
    stats_output: Path
    references: tuple[HailingLdReference, ...] = Field(min_length=1)
    native_contig_prefix: str = "chr"
    max_cached_blocks: int = Field(default=8, ge=1)


def _quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parquet_glob(path: Path) -> str:
    return (path / "**" / "*.parquet").as_posix() if path.is_dir() else path.as_posix()


def _native_contig_sql(chromosome_sql: str, prefix: str) -> str:
    quoted_prefix = _quote_sql_string(prefix)
    return (
        f"CASE WHEN starts_with(CAST({chromosome_sql} AS VARCHAR), {quoted_prefix}) "
        f"THEN CAST({chromosome_sql} AS VARCHAR) "
        f"ELSE concat({quoted_prefix}, CAST({chromosome_sql} AS VARCHAR)) END"
    )


def _native_variant_sql(variant_id_sql: str, prefix: str) -> str:
    native_contig = _native_contig_sql(f"regexp_extract(CAST({variant_id_sql} AS VARCHAR), '^([^_]+)_', 1)", prefix)
    return f"concat({native_contig}, '_', regexp_extract(CAST({variant_id_sql} AS VARCHAR), '^[^_]+_(.*)$', 1))"


def _prepare_request_files(
    con: duckdb.DuckDBPyConnection,
    input_path: Path,
    requests_path: Path,
    mapping_path: Path,
    native_contig_prefix: str,
) -> None:
    """Create the batch request and original-ID mapping files."""
    source = _quote_sql_string(_parquet_glob(input_path))
    native_variant = _native_variant_sql("locus_variant.variantId", native_contig_prefix)
    read_sql = f"read_parquet({source}, union_by_name = true, hive_partitioning = true)"
    input_columns = {
        row[0]
        for row in con.execute(f"DESCRIBE SELECT * FROM {read_sql}").fetchall()
    }
    locus_id_sql = (
        "CAST(fineMappingLocusSetId AS VARCHAR)"
        if "fineMappingLocusSetId" in input_columns
        else "CAST(studyLocusId AS VARCHAR)"
    )
    con.execute(
        f"""
        COPY (
            WITH expanded AS (
                SELECT DISTINCT
                    {locus_id_sql} AS locus_id,
                    CAST(chromosome AS VARCHAR) AS chromosome,
                    CAST(locusStart AS INTEGER) AS locusStart,
                    CAST(locusEnd AS INTEGER) AS locusEnd,
                    {native_variant} AS native_variant_id
                FROM {read_sql}, UNNEST(locus) AS expanded(locus_variant)
                WHERE locus IS NOT NULL
            )
            SELECT
                locus_id,
                concat(
                    {_native_contig_sql("chromosome", native_contig_prefix)},
                    ':',
                    CAST(min(locusStart) AS VARCHAR),
                    '-',
                    CAST(max(locusEnd) AS VARCHAR)
                ) AS locus,
                list(native_variant_id ORDER BY native_variant_id) AS variant_ids
            FROM expanded
            GROUP BY locus_id, chromosome
        ) TO {_quote_sql_string(requests_path.as_posix())} (FORMAT PARQUET)
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT DISTINCT
                {locus_id_sql} AS locus_id,
                {native_variant} AS native_variant_id,
                CAST(locus_variant.variantId AS VARCHAR) AS original_variant_id
            FROM {read_sql}, UNNEST(locus) AS expanded(locus_variant)
            WHERE locus IS NOT NULL
        ) TO {_quote_sql_string(mapping_path.as_posix())} (FORMAT PARQUET)
        """
    )


def _materialize_reference(
    _con: duckdb.DuckDBPyConnection,
    reference: HailingLdReference,
    requests_path: Path,
    ld_path: Path,
    status_path: Path,
    max_cached_blocks: int,
) -> None:
    """Materialise one ancestry through the Hailing Ducks extension."""
    s3_settings = "SET s3_region = 'us-east-1';" if reference.ht_path.startswith("s3://") or reference.bm_path.startswith("s3://") else ""
    sql = f"""
        {s3_settings}
        SELECT * FROM hail_ld_materialize(
            {_quote_sql_string(reference.ht_path)},
            {_quote_sql_string(reference.bm_path)},
            {_quote_sql_string(requests_path.as_posix())},
            {_quote_sql_string(ld_path.as_posix())},
            {_quote_sql_string(status_path.as_posix())},
            max_cached_blocks := {max_cached_blocks}
        )
        """
    subprocess.run(  # noqa: S603 - fixed executable and argv invocation; no shell expansion
        ["/usr/local/bin/duckdb", "-c", sql],
        check=True,
    )


def _adapt_reference_output(
    con: duckdb.DuckDBPyConnection,
    reference: HailingLdReference,
    ld_path: Path,
    status_path: Path,
    mapping_path: Path,
    output_path: Path,
) -> None:
    """Write one ancestry's resolved pairs and diagonals to Parquet."""
    ancestry = _quote_sql_string(reference.ancestry)
    con.execute(
        f"""
        COPY (
            SELECT
                {ancestry} AS ancestry,
                map_i.original_variant_id AS variantIdI,
                map_j.original_variant_id AS variantIdJ,
                CAST(pairs.r AS DOUBLE) AS r
            FROM read_parquet({_quote_sql_string(ld_path.as_posix())}) AS pairs
            JOIN read_parquet({_quote_sql_string(status_path.as_posix())}) AS status_i
              ON status_i.locus_id = pairs.locus_id
             AND status_i.idx = pairs.idx_i
             AND status_i.status_code IN (0, 1)
            JOIN read_parquet({_quote_sql_string(status_path.as_posix())}) AS status_j
              ON status_j.locus_id = pairs.locus_id
             AND status_j.idx = pairs.idx_j
             AND status_j.status_code IN (0, 1)
            JOIN read_parquet({_quote_sql_string(mapping_path.as_posix())}) AS map_i
              ON map_i.locus_id = status_i.locus_id
             AND map_i.native_variant_id = status_i.requested_variant_id
            JOIN read_parquet({_quote_sql_string(mapping_path.as_posix())}) AS map_j
              ON map_j.locus_id = status_j.locus_id
             AND map_j.native_variant_id = status_j.requested_variant_id
            UNION ALL
            SELECT
                {ancestry} AS ancestry,
                mapping.original_variant_id AS variantIdI,
                mapping.original_variant_id AS variantIdJ,
                1.0::DOUBLE AS r
            FROM read_parquet({_quote_sql_string(status_path.as_posix())}) AS status
            JOIN read_parquet({_quote_sql_string(mapping_path.as_posix())}) AS mapping
              ON mapping.locus_id = status.locus_id
             AND mapping.native_variant_id = status.requested_variant_id
            WHERE status.status_code IN (0, 1)
        ) TO {_quote_sql_string(output_path.as_posix())} (FORMAT PARQUET)
        """
    )


def _assert_no_conflicting_pairs(con: duckdb.DuckDBPyConnection, ancestry: str, path: Path) -> None:
    """Reject duplicate pair keys whose LD values disagree."""
    conflict = con.execute(
        """
        SELECT variantIdI, variantIdJ
        FROM read_parquet(?)
        GROUP BY variantIdI, variantIdJ
        HAVING count(DISTINCT r) > 1
        LIMIT 1
        """,
        [path.as_posix()],
    ).fetchone()
    if conflict is not None:
        variant_id_i, variant_id_j = conflict
        raise RuntimeError(f"Conflicting LD values for {ancestry}: {variant_id_i}, {variant_id_j}")


def run_hailing_ld(config: HailingLdConfig, materialize: Callable[..., None] = _materialize_reference) -> None:
    """Resolve and write Hailing Ducks LD in Gentropy-compatible form."""
    for path in (config.input_path, config.study_metadata_path):
        if not path.exists():
            raise FileNotFoundError(path)
    if not config.input_path.is_file() and not config.input_path.is_dir():
        raise ValueError(f"Input path is not a file or directory: {config.input_path}")
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    config.stats_output.parent.mkdir(parents=True, exist_ok=True)

    for path in (config.output_path, config.stats_output):
        if path.exists():
            if path.is_dir():
                raise IsADirectoryError(path)
            path.unlink()

    with tempfile.TemporaryDirectory(prefix="collector-hailing-ld-") as temporary_directory:
        temporary_path = Path(temporary_directory)
        study_ancestries: dict[str, str] = {}
        for line_number, line in enumerate(config.study_metadata_path.read_text().splitlines(), start=1):
            if not line.strip():
                continue
            try:
                record = HailingStudyMetadata.model_validate_json(line)
            except ValueError as error:
                raise ValueError(f"Invalid study metadata at line {line_number}: {error}") from error
            previous_ancestry = study_ancestries.setdefault(record.study_id, record.ancestry)
            if previous_ancestry != record.ancestry:
                raise ValueError(f"Conflicting ancestry metadata for studyId {record.study_id}")

        with duckdb.connect() as con:
            input_study_ids = {
                row[0]
                for row in con.execute(
                    f"""
                    SELECT DISTINCT CAST(studyId AS VARCHAR)
                    FROM read_parquet({_quote_sql_string(_parquet_glob(config.input_path))}, union_by_name = true, hive_partitioning = true)
                    """
                ).fetchall()
            }
            missing_study_ids = sorted(input_study_ids - study_ancestries.keys())
            if missing_study_ids:
                raise ValueError(f"Study metadata is missing studyId(s): {', '.join(missing_study_ids)}")
            reference_ancestries = [reference.ancestry for reference in config.references]
            if len(reference_ancestries) != len(set(reference_ancestries)):
                raise ValueError("Hailing LD references contain duplicate ancestries")
            missing_ancestries = sorted({study_ancestries[study_id] for study_id in input_study_ids} - set(reference_ancestries))
            if missing_ancestries:
                raise ValueError(f"Hailing LD references are missing ancestry(s): {', '.join(missing_ancestries)}")

            statistics: list[dict[str, float | int | str]] = []
            adapted_paths: list[Path] = []
            for reference in config.references:
                study_ids = tuple(sorted(study_id for study_id in input_study_ids if study_ancestries[study_id] == reference.ancestry))
                if not study_ids:
                    statistics.append(
                        {
                            "ancestry": reference.ancestry,
                            "n_requested_variants": 0,
                            "n_resolved_variants": 0,
                            "n_unresolved_variants": 0,
                            "n_unsupported_variants": 0,
                            "n_ld_pairs": 0,
                            "native_materialize_seconds": 0.0,
                            "adapter_seconds": 0.0,
                            "peak_child_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                        }
                    )
                    continue
                requests_path = temporary_path / f"{reference.ancestry}.requests.parquet"
                mapping_path = temporary_path / f"{reference.ancestry}.mapping.parquet"
                _prepare_request_files(
                    con,
                    config.input_path,
                    requests_path,
                    mapping_path,
                    config.native_contig_prefix,
                )
                ld_path = temporary_path / f"{reference.ancestry}.ld.parquet"
                status_path = temporary_path / f"{reference.ancestry}.status.parquet"
                adapted_path = temporary_path / f"{reference.ancestry}.adapted.parquet"
                materialize_started = perf_counter()
                materialize(con, reference, requests_path, ld_path, status_path, config.max_cached_blocks)
                native_materialize_seconds = perf_counter() - materialize_started
                adapter_started = perf_counter()
                _adapt_reference_output(con, reference, ld_path, status_path, mapping_path, adapted_path)
                _assert_no_conflicting_pairs(con, reference.ancestry, adapted_path)
                adapted_paths.append(adapted_path)
                statistics_row = con.execute(
                    """
                    SELECT
                        count(*)::BIGINT,
                        count(*) FILTER (WHERE status_code IN (0, 1))::BIGINT,
                        count(*) FILTER (WHERE status_code = 5)::BIGINT,
                        (
                            SELECT count(*)::BIGINT
                            FROM (
                                SELECT variantIdI, variantIdJ
                                FROM read_parquet(?)
                                GROUP BY variantIdI, variantIdJ
                            )
                        )
                    FROM read_parquet(?)
                    """,
                    [adapted_path.as_posix(), status_path.as_posix()],
                ).fetchone()
                if statistics_row is None:
                    raise RuntimeError(f"Failed to calculate Hailing Ducks statistics for {reference.ancestry}")
                requested, resolved, unsupported, ld_pairs = statistics_row
                adapter_seconds = perf_counter() - adapter_started
                statistics.append(
                    {
                        "ancestry": reference.ancestry,
                        "n_requested_variants": requested,
                        "n_resolved_variants": resolved,
                        "n_unresolved_variants": requested - resolved - unsupported,
                        "n_unsupported_variants": unsupported,
                        "n_ld_pairs": ld_pairs,
                        "native_materialize_seconds": round(native_materialize_seconds, 6),
                        "adapter_seconds": round(adapter_seconds, 6),
                        "peak_child_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                    }
                )

            combined_output_started = perf_counter()
            if adapted_paths:
                adapted_sources = ", ".join(_quote_sql_string(path.as_posix()) for path in adapted_paths)
                con.execute(
                    f"""
                    COPY (
                        SELECT ancestry, variantIdI, variantIdJ, min(r) AS r
                        FROM read_parquet([{adapted_sources}])
                        GROUP BY ancestry, variantIdI, variantIdJ
                        ORDER BY ancestry, variantIdI, variantIdJ
                    ) TO {_quote_sql_string(config.output_path.as_posix())} (FORMAT PARQUET)
                    """
                )
            else:
                con.execute(
                    f"""
                    COPY (
                        SELECT
                            NULL::VARCHAR AS ancestry,
                            NULL::VARCHAR AS variantIdI,
                            NULL::VARCHAR AS variantIdJ,
                            NULL::DOUBLE AS r
                        WHERE false
                    ) TO {_quote_sql_string(config.output_path.as_posix())} (FORMAT PARQUET)
                    """
                )
            combined_output_seconds = round(perf_counter() - combined_output_started, 6)
            for record in statistics:
                record["combined_output_seconds"] = combined_output_seconds
    config.stats_output.write_text("".join(json.dumps(record) + "\n" for record in statistics))
