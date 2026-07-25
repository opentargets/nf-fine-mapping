"""Annotate collected loci with long-format LD pairs."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
from pydantic import BaseModel, ConfigDict, Field, field_validator

FINE_MAPPING_LOCI_FILENAME = "fine_mapping_loci.parquet"
LD_PAIRS_FILENAME = "ld_pairs.parquet"


class StudyLocusLDAnnotationMetadata(BaseModel):
    """Run metadata required for LD-annotation output."""

    model_config = ConfigDict(frozen=True)

    study_id: str = Field(alias="studyId")
    ancestry: str
    sample_size: int = Field(alias="sampleSize")

    @field_validator("ancestry")
    @classmethod
    def normalize_ancestry(cls, value: str) -> str:
        """Normalize pipeline ancestry aliases to PanUKBB LD labels."""
        lowered_value = value.lower()
        if lowered_value in {"nfe", "eur"}:
            return "EUR"
        upper_value = value.upper()
        if upper_value in {"AFR", "CSA"}:
            return upper_value
        raise ValueError(f"Unsupported PanUKBB ancestry: {value}")


class StudyLocusLDAnnotationConfig(BaseModel):
    """Path contract for the study_locus_ld_annotation command."""

    model_config = ConfigDict(frozen=True)

    input_path: Path
    metadata_json: Path
    ld_index_path: Path
    ld_pairs_input_path: Path
    output_dir: Path


def _quote_sql_string(value: str) -> str:
    """Return a single-quoted SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _parquet_glob(path: Path) -> str:
    """Return the parquet reader path for a file or directory dataset."""
    if path.is_dir():
        return (path / "**" / "*.parquet").as_posix()
    return path.as_posix()


def _read_parquet_sql(input_path: Path) -> str:
    """Return a DuckDB read_parquet expression for the StudyLocus input."""
    return f"read_parquet({_quote_sql_string(_parquet_glob(input_path))}, union_by_name = true, hive_partitioning = true)"


def _load_metadata(path: Path) -> tuple[StudyLocusLDAnnotationMetadata, ...]:
    """Return validated per-study run metadata from a JSON file."""
    raw_metadata = json.loads(path.read_text())
    if isinstance(raw_metadata, dict):
        raw_metadata = [raw_metadata]
    if not isinstance(raw_metadata, list):
        raise TypeError("Metadata JSON must contain an object or a list of objects")
    metadata = tuple(StudyLocusLDAnnotationMetadata.model_validate(item) for item in raw_metadata)
    if not metadata:
        raise ValueError("Metadata JSON must contain at least one study")
    study_ids = [item.study_id for item in metadata]
    if len(study_ids) != len(set(study_ids)):
        raise ValueError("Metadata JSON must contain each studyId at most once")
    return metadata


def _prepare_output_paths(output_dir: Path) -> tuple[Path, Path]:
    """Create the output directory and remove stale parquet outputs."""
    if output_dir.exists() and not output_dir.is_dir():
        raise NotADirectoryError(f"Output directory path is a file: {output_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    fine_mapping_output = output_dir / FINE_MAPPING_LOCI_FILENAME
    ld_pairs_output = output_dir / LD_PAIRS_FILENAME
    for path in (fine_mapping_output, ld_pairs_output):
        if path.exists():
            if path.is_dir():
                raise IsADirectoryError(f"Output path is a directory: {path}")
            path.unlink()
    return fine_mapping_output, ld_pairs_output


def run_study_locus_ld_annotation(config: StudyLocusLDAnnotationConfig) -> None:
    """Write flattened fine-mapping loci and filtered long-format LD pairs."""
    if not config.input_path.exists():
        raise FileNotFoundError(f"Input path does not exist: {config.input_path}")
    if not config.metadata_json.exists():
        raise FileNotFoundError(f"Metadata JSON path does not exist: {config.metadata_json}")
    if config.metadata_json.suffix != ".json":
        raise ValueError("Metadata file should have a .json extension")
    if not config.ld_index_path.exists():
        raise FileNotFoundError(f"LD index path does not exist: {config.ld_index_path}")
    if not config.ld_pairs_input_path.exists():
        raise FileNotFoundError(f"LD pairs input path does not exist: {config.ld_pairs_input_path}")

    metadata = _load_metadata(config.metadata_json)
    fine_mapping_output, ld_pairs_output = _prepare_output_paths(config.output_dir)
    metadata_values_sql = ", ".join(
        f"({_quote_sql_string(item.study_id)}, {_quote_sql_string(item.ancestry)}, {item.sample_size}::INTEGER)" for item in metadata
    )

    with duckdb.connect() as con:
        con.execute(
            f"""
            CREATE TEMP TABLE study_metadata AS
            SELECT *
            FROM (VALUES {metadata_values_sql}) AS metadata(studyId, ancestry, sampleSize)
            """
        )
        con.execute(
            f"""
            CREATE TEMP TABLE input_loci AS
            SELECT *
            FROM {_read_parquet_sql(config.input_path)}
            """
        )
        con.execute(
            f"""
            CREATE TEMP TABLE ld_index AS
            SELECT DISTINCT
                CAST(variantId AS VARCHAR) AS variantId,
                CAST(population AS VARCHAR) AS ancestry
            FROM {_read_parquet_sql(config.ld_index_path)}
            """
        )
        con.execute(
            f"""
            CREATE TEMP TABLE source_ld_pairs AS
            SELECT
                CAST(ancestry AS VARCHAR) AS ancestry,
                CAST(variantIdI AS VARCHAR) AS variantIdI,
                CAST(variantIdJ AS VARCHAR) AS variantIdJ,
                CAST(r AS DOUBLE) AS r
            FROM {_read_parquet_sql(config.ld_pairs_input_path)}
            """
        )
        metadata_mismatch = con.execute(
            """
            WITH input_studies AS (
                SELECT DISTINCT studyId
                FROM input_loci
            ),
            metadata_studies AS (
                SELECT DISTINCT studyId
                FROM study_metadata
            )
            SELECT
                (SELECT count(*) FROM input_studies) AS inputStudyCount,
                (SELECT count(*) FROM metadata_studies) AS metadataStudyCount,
                (SELECT count(*) FROM input_studies INNER JOIN metadata_studies USING (studyId)) AS matchedStudyCount
            """
        ).fetchone()
        if metadata_mismatch is None:
            raise RuntimeError("DuckDB returned no metadata mismatch counts")
        input_study_count, metadata_study_count, matched_study_count = metadata_mismatch
        if input_study_count != metadata_study_count or input_study_count != matched_study_count:
            raise ValueError(
                "Metadata studyId values must exactly match input StudyLocus studyId values "
                f"({input_study_count} input studies, {metadata_study_count} metadata studies, {matched_study_count} matched)."
            )
        con.execute(
            """
            CREATE TEMP TABLE flattened_loci AS
            SELECT
                CAST(input_loci.fineMappingLocusSetId AS VARCHAR) AS fineMappingLocusSetId,
                CAST(input_loci.studyLocusId AS VARCHAR) AS studyLocusId,
                CAST(input_loci.studyId AS VARCHAR) AS studyId,
                CAST(study_metadata.ancestry AS VARCHAR) AS ancestry,
                CAST(study_metadata.sampleSize AS INTEGER) AS sampleSize,
                CAST(locus_variant.variantId AS VARCHAR) AS variantId,
                CAST(locus_variant.beta AS DOUBLE) AS beta,
                CAST(locus_variant.standardError AS DOUBLE) AS standardError,
                CASE
                    WHEN locus_variant.standardError IS NULL OR locus_variant.standardError = 0 THEN CAST(NULL AS DOUBLE)
                    ELSE CAST(locus_variant.beta / locus_variant.standardError AS DOUBLE)
                END AS z
            FROM input_loci
            INNER JOIN study_metadata
                ON input_loci.studyId = study_metadata.studyId
            CROSS JOIN UNNEST(locus) AS locus_items(locus_variant)
            """
        )
        con.execute(
            f"""
            COPY (
                SELECT
                    fineMappingLocusSetId,
                    studyLocusId,
                    studyId,
                    ancestry,
                    sampleSize,
                    variantId,
                    beta,
                    standardError,
                    z
                FROM flattened_loci
                ORDER BY fineMappingLocusSetId, studyLocusId, variantId
            ) TO {_quote_sql_string(fine_mapping_output.as_posix())} (FORMAT PARQUET)
            """
        )
        con.execute(
            f"""
            COPY (
                WITH requested_ld_variants AS (
                    SELECT DISTINCT
                        requested_ancestries.ancestry,
                        requested_variants.variantId
                    FROM (
                        SELECT DISTINCT ancestry
                        FROM flattened_loci
                    ) AS requested_ancestries
                    CROSS JOIN (
                        SELECT DISTINCT variantId
                        FROM flattened_loci
                    ) AS requested_variants
                    INNER JOIN ld_index
                        ON requested_ancestries.ancestry = ld_index.ancestry
                        AND requested_variants.variantId = ld_index.variantId
                ),
                symmetric_ld_pairs AS (
                    SELECT ancestry, variantIdI, variantIdJ, r
                    FROM source_ld_pairs
                    UNION ALL
                    SELECT ancestry, variantIdJ AS variantIdI, variantIdI AS variantIdJ, r
                    FROM source_ld_pairs
                    WHERE variantIdI != variantIdJ
                ),
                unique_ld_pairs AS (
                    SELECT
                        ancestry,
                        variantIdI,
                        variantIdJ,
                        any_value(r) AS r
                    FROM symmetric_ld_pairs
                    GROUP BY ancestry, variantIdI, variantIdJ
                )
                SELECT
                    unique_ld_pairs.ancestry AS ancestry,
                    unique_ld_pairs.variantIdI AS variantIdI,
                    unique_ld_pairs.variantIdJ AS variantIdJ,
                    unique_ld_pairs.r AS r
                FROM unique_ld_pairs
                INNER JOIN requested_ld_variants AS left_variants
                    ON unique_ld_pairs.ancestry = left_variants.ancestry
                    AND unique_ld_pairs.variantIdI = left_variants.variantId
                INNER JOIN requested_ld_variants AS right_variants
                    ON unique_ld_pairs.ancestry = right_variants.ancestry
                    AND unique_ld_pairs.variantIdJ = right_variants.variantId
                ORDER BY ancestry, variantIdI, variantIdJ
            ) TO {_quote_sql_string(ld_pairs_output.as_posix())} (FORMAT PARQUET)
            """
        )
