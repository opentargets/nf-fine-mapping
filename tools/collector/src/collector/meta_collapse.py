"""Collapse a multi-ancestry locus set into a single-population comparator arm.

Produces the ``meta`` and ``single`` arms of the multi-ancestry resolution
benchmark (``docs/prd/meta-collapse.md``). Both consume the artifacts that
``HAILING_DUCKS_LD_ANNOTATION`` already produced for the joint arm and emit the
same two contracts, so the fine-mapping process runs unmodified and ``L``, the
purity threshold, the convergence criteria and the output contract are
identical across arms by construction rather than by discipline.

The meta arm
------------
Per-variant inverse-variance weights, normalised so that they square to one:

    u[a,i] = (1 / se[a,i]) / sqrt( sum_{b in O(i)} 1 / se[b,i]^2 )

where ``O(i)`` is the set of ancestry arms in which variant *i* is observed, and
``u[a,i] = 0`` for every other arm. Then

    beta_meta[i] = sum_a (beta[a,i] / se[a,i]^2) / sum_a (1 / se[a,i]^2)
    se_meta[i]   = 1 / sqrt( sum_a 1 / se[a,i]^2 )
    z_meta[i]    = sum_a u[a,i] * z[a,i]

and, because samples are independent across ancestries,

    R_meta[i,j] = sum_a u[a,i] * u[a,j] * R[a,i,j]

Three properties follow, and each is asserted rather than assumed:

1. ``R_meta[i,i] = sum_a u[a,i]^2 = 1`` exactly. Checked to
   ``diagonal_tolerance``; this is a free end-to-end test of both the weight
   normalisation and the two joins.
2. Positive semi-definiteness is preserved, since ``R_meta`` is a sum of
   ``D_a R_a D_a`` terms. Reference-panel matrices are frequently not PSD, so
   ``R_meta`` inherits that; it is recorded, not repaired.
3. When a variant is absent from an arm its weight is zero and the term drops
   out. The resulting smaller ``|R_meta[i,j]|`` is the true covariance of two
   statistics computed on partially different samples, not an artifact, so no
   per-pair renormalisation is applied.

Why not the sample-size-weighted form
-------------------------------------
``R_meta = sum_a (N_a / N) R_a`` is the form usually written down, and it is
wrong here. ``se[a,i]`` depends on allele frequency and allele frequency differs
across ancestries, so the weights are genuinely per-variant. Checked by Monte
Carlo in ``docs/benchmarks/verify_meta_ld.py`` (three ancestries at
N = 400,000 / 80,000 / 4,000, distinct LD and distinct MAF per arm, 400,000
draws): the per-variant form above is accurate to 0.8x the Monte Carlo standard
error, while the sample-size-weighted form is off by 8.5x it, with a maximum
error of 0.10 in correlation. The discrepancy is largest exactly where allele
frequencies differ most, which is the regime the pipeline exists to exploit.

The single arm
--------------
Selects one ancestry arm and filters both contracts to it. No arithmetic; it
exists so that all arms traverse identical code downstream.

Pair orientation
----------------
``variantIdI``/``variantIdJ`` are canonicalised with ``least``/``greatest``
before grouping. If two ancestries stored the same unordered pair in opposite
orientations, a naive ``GROUP BY`` would place them in different groups and
halve the resulting correlation, silently.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field

MetaCollapseMode = Literal["meta", "single"]

DEFAULT_META_ANCESTRY = "meta"
DEFAULT_DIAGONAL_TOLERANCE = 1e-9
DEFAULT_MAX_MISSING_PAIR_FRACTION = 0.001


class MetaCollapseConfig(BaseModel):
    """Inputs, outputs and guard rails for one locus-set collapse."""

    model_config = ConfigDict(frozen=True)

    input_path: Path
    ld_path: Path
    study_metadata_path: Path
    output_path: Path
    ld_output_path: Path
    stats_output_path: Path
    mode: MetaCollapseMode = "meta"
    run_id: str = Field(min_length=1)
    fine_mapping_locus_set_id: str = Field(min_length=1)
    target_ancestry: str | None = None
    meta_ancestry: str = DEFAULT_META_ANCESTRY
    diagonal_tolerance: float = Field(default=DEFAULT_DIAGONAL_TOLERANCE, ge=0)
    max_missing_pair_fraction: float = Field(default=DEFAULT_MAX_MISSING_PAIR_FRACTION, ge=0, le=1)


class MetaCollapseError(RuntimeError):
    """Raised when a collapse violates one of its own invariants."""


def _quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parquet_glob(path: Path) -> str:
    return (path / "**" / "*.parquet").as_posix() if path.is_dir() else path.as_posix()


def _read_study_metadata(path: Path) -> dict[str, dict[str, Any]]:
    """Map studyId to its ancestry and sample size, from the pipeline's JSONL."""
    mapping: dict[str, dict[str, Any]] = {}
    for line_number, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
            mapping[str(record["studyId"])] = {
                "ancestry": str(record["ancestry"]),
                "sampleSize": float(record["sampleSize"]),
            }
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError(f"Invalid study metadata at line {line_number} of {path}") from error
    if not mapping:
        raise ValueError(f"Study metadata {path} contains no records")
    return mapping


def _register_metadata(con: duckdb.DuckDBPyConnection, metadata: dict[str, dict[str, Any]]) -> None:
    con.execute(
        """
        CREATE TABLE study_metadata(
            studyId VARCHAR,
            ancestry VARCHAR,
            sampleSize DOUBLE
        )
        """
    )
    con.executemany(
        "INSERT INTO study_metadata VALUES (?, ?, ?)",
        [(study_id, entry["ancestry"], entry["sampleSize"]) for study_id, entry in sorted(metadata.items())],
    )


def _create_exploded_locus(con: duckdb.DuckDBPyConnection, input_glob: str) -> None:
    """One row per (ancestry, variantId), from the nested locus column."""
    con.execute(
        f"""
        CREATE TABLE locus_variants AS
        SELECT
            metadata.ancestry                 AS ancestry,
            metadata.sampleSize               AS sampleSize,
            locus_rows.studyId                AS studyId,
            locus_rows.studyLocusId           AS studyLocusId,
            locus_rows.fineMappingLocusSetId  AS fineMappingLocusSetId,
            locus_rows.chromosome             AS chromosome,
            locus_rows.locusStart             AS locusStart,
            locus_rows.locusEnd               AS locusEnd,
            locus_rows.qualityControls        AS qualityControls,
            CAST(entry.variantId AS VARCHAR)      AS variantId,
            CAST(entry.beta AS DOUBLE)            AS beta,
            CAST(entry.standardError AS DOUBLE)   AS standardError,
            CAST(entry.pValueMantissa AS FLOAT)   AS pValueMantissa,
            CAST(entry.pValueExponent AS INTEGER) AS pValueExponent
        FROM read_parquet({_quote_sql_string(input_glob)}) AS locus_rows,
             UNNEST(locus_rows.locus) AS exploded(entry)
        JOIN study_metadata AS metadata
          ON metadata.studyId = locus_rows.studyId
        WHERE entry.standardError IS NOT NULL
          AND entry.standardError > 0
          AND entry.beta IS NOT NULL
        """
    )


def _create_weights(con: duckdb.DuckDBPyConnection) -> None:
    """u[a,i], normalised over the arms in which variant i is observed."""
    con.execute(
        """
        CREATE TABLE variant_weights AS
        SELECT
            ancestry,
            variantId,
            (1.0 / standardError)
                / sqrt(SUM(1.0 / (standardError * standardError)) OVER (PARTITION BY variantId))
                AS u
        FROM locus_variants
        """
    )


def _create_canonical_ld(con: duckdb.DuckDBPyConnection, ld_glob: str) -> None:
    """Pairwise LD with a canonical (i <= j) orientation."""
    con.execute(
        f"""
        CREATE TABLE canonical_ld AS
        SELECT
            CAST(ancestry AS VARCHAR)                       AS ancestry,
            least(CAST(variantIdI AS VARCHAR), CAST(variantIdJ AS VARCHAR))    AS variantIdI,
            greatest(CAST(variantIdI AS VARCHAR), CAST(variantIdJ AS VARCHAR)) AS variantIdJ,
            CAST(r AS DOUBLE)                               AS r
        FROM read_parquet({_quote_sql_string(ld_glob)})
        """
    )


def _collapse_ld(con: duckdb.DuckDBPyConnection, meta_ancestry: str) -> None:
    """R_meta[i,j] = sum_a u[a,i] * u[a,j] * R[a,i,j]."""
    con.execute(
        f"""
        CREATE TABLE collapsed_ld AS
        SELECT
            {_quote_sql_string(meta_ancestry)} AS ancestry,
            ld.variantIdI                      AS variantIdI,
            ld.variantIdJ                      AS variantIdJ,
            SUM(wi.u * wj.u * ld.r)            AS r
        FROM canonical_ld AS ld
        JOIN variant_weights AS wi
          ON wi.ancestry = ld.ancestry AND wi.variantId = ld.variantIdI
        JOIN variant_weights AS wj
          ON wj.ancestry = ld.ancestry AND wj.variantId = ld.variantIdJ
        GROUP BY 1, 2, 3
        """
    )


def _collapse_locus(con: duckdb.DuckDBPyConnection, config: MetaCollapseConfig, sample_size_total: float) -> None:
    """One collapsed row carrying the IVW effect sizes as a nested locus list.

    qualityControls are unioned across arms: a flag raised by any contributing
    study is carried by the collapsed arm, so downstream QC filters cannot pass
    the comparator on evidence the joint arm was failed for.
    """
    meta_study_id = f"{config.run_id}__{config.mode}"
    con.execute(
        f"""
        CREATE TABLE collapsed_variants AS
        SELECT
            variantId,
            SUM(beta / (standardError * standardError))
                / SUM(1.0 / (standardError * standardError))     AS beta,
            1.0 / sqrt(SUM(1.0 / (standardError * standardError))) AS standardError
        FROM locus_variants
        GROUP BY variantId
        """
    )
    con.execute(
        f"""
        CREATE TABLE collapsed_locus AS
        SELECT
            {_quote_sql_string(config.fine_mapping_locus_set_id)} AS fineMappingLocusSetId,
            {_quote_sql_string(meta_study_id)}                    AS studyLocusId,
            {_quote_sql_string(meta_study_id)}                    AS studyId,
            (SELECT any_value(chromosome) FROM locus_variants)    AS chromosome,
            (SELECT min(locusStart) FROM locus_variants)          AS locusStart,
            (SELECT max(locusEnd) FROM locus_variants)            AS locusEnd,
            (
                SELECT COALESCE(list_sort(list_distinct(flatten(list(qualityControls)))), [])
                FROM (SELECT DISTINCT studyId, qualityControls FROM locus_variants)
            )                                                     AS qualityControls,
            (
                SELECT list(
                    STRUCT_PACK(
                        variantId := variantId,
                        pValueMantissa := CAST(NULL AS FLOAT),
                        pValueExponent := CAST(NULL AS INTEGER),
                        beta := beta,
                        standardError := standardError
                    )
                    ORDER BY variantId
                )
                FROM collapsed_variants
            )                                                     AS locus
        """
    )
    con.execute(
        "CREATE TABLE collapsed_sample_size AS SELECT ? AS sampleSizeTotal",
        [sample_size_total],
    )


def _filter_single_arm(con: duckdb.DuckDBPyConnection, input_glob: str, ancestry: str) -> None:
    """Keep one ancestry's locus row and its LD, unchanged."""
    con.execute(
        f"""
        CREATE TABLE collapsed_locus AS
        SELECT locus_rows.*
        FROM read_parquet({_quote_sql_string(input_glob)}) AS locus_rows
        JOIN study_metadata AS metadata
          ON metadata.studyId = locus_rows.studyId
        WHERE metadata.ancestry = {_quote_sql_string(ancestry)}
        """
    )
    con.execute(
        f"""
        CREATE TABLE collapsed_ld AS
        SELECT ancestry, variantIdI, variantIdJ, r
        FROM canonical_ld
        WHERE ancestry = {_quote_sql_string(ancestry)}
        """
    )


def _diagonal_deviation(con: duckdb.DuckDBPyConnection) -> float:
    row = con.execute(
        """
        SELECT COALESCE(max(abs(r - 1.0)), 0.0)
        FROM collapsed_ld
        WHERE variantIdI = variantIdJ
        """
    ).fetchone()
    return float(row[0]) if row else 0.0


def _missing_pair_count(con: duckdb.DuckDBPyConnection) -> tuple[int, int]:
    """Pairs absent from an arm's LD although both variants are present there.

    Each arm's LD is a complete upper triangle including the diagonal, so the
    expected row count is C(n, 2) + n for an arm contributing n variants. Any
    shortfall means pairs were dropped, and a dropped pair is treated as zero
    correlation by the sum in ``_collapse_ld`` -- which understates LD and makes
    the fine-mapper over-split. Counted here so it can be refused.
    """
    rows = con.execute(
        """
        WITH arm_variants AS (
            SELECT ancestry, count(DISTINCT variantId) AS n
            FROM variant_weights
            GROUP BY ancestry
        ),
        arm_pairs AS (
            SELECT ancestry, count(*) AS observed
            FROM canonical_ld
            GROUP BY ancestry
        )
        SELECT
            COALESCE(SUM(CAST(v.n AS BIGINT) * (CAST(v.n AS BIGINT) + 1) / 2), 0) AS expected,
            COALESCE(SUM(p.observed), 0)                                          AS observed
        FROM arm_variants AS v
        LEFT JOIN arm_pairs AS p ON p.ancestry = v.ancestry
        """
    ).fetchone()
    expected = int(rows[0]) if rows else 0
    observed = int(rows[1]) if rows else 0
    return max(expected - observed, 0), expected


def _collect_stats(con: duckdb.DuckDBPyConnection, config: MetaCollapseConfig, sample_size_total: float) -> dict[str, Any]:
    arms = con.execute("SELECT count(DISTINCT ancestry) FROM locus_variants").fetchone()[0]
    union_variants = con.execute("SELECT count(DISTINCT variantId) FROM locus_variants").fetchone()[0]
    in_all_arms = con.execute(
        """
        SELECT count(*) FROM (
            SELECT variantId
            FROM locus_variants
            GROUP BY variantId
            HAVING count(DISTINCT ancestry) = (SELECT count(DISTINCT ancestry) FROM locus_variants)
        )
        """
    ).fetchone()[0]
    single_arm_only = con.execute(
        """
        SELECT count(*) FROM (
            SELECT variantId FROM locus_variants GROUP BY variantId HAVING count(DISTINCT ancestry) = 1
        )
        """
    ).fetchone()[0]
    pairs_in = con.execute("SELECT count(*) FROM canonical_ld").fetchone()[0]
    pairs_out = con.execute("SELECT count(*) FROM collapsed_ld").fetchone()[0]
    missing_pairs, expected_pairs = _missing_pair_count(con)
    deviation = _diagonal_deviation(con)

    return {
        "runId": config.run_id,
        "fineMappingLocusSetId": config.fine_mapping_locus_set_id,
        "mode": config.mode,
        "targetAncestry": config.target_ancestry,
        "nAncestryArms": int(arms),
        "nVariantsUnion": int(union_variants),
        "nVariantsInAllArms": int(in_all_arms),
        "nVariantsSingleArmOnly": int(single_arm_only),
        "nPairsIn": int(pairs_in),
        "nPairsOut": int(pairs_out),
        "nPairsExpected": int(expected_pairs),
        "nPairsMissingWithBothVariantsPresent": int(missing_pairs),
        "missingPairFraction": (missing_pairs / expected_pairs) if expected_pairs else 0.0,
        "maxAbsDiagonalDeviation": deviation,
        "sampleSizeTotal": sample_size_total,
        "pairOrderCanonicalisationApplied": True,
        "ldCombinationRule": "R_meta[i,j] = sum_a u[a,i] * u[a,j] * R[a,i,j], u per-variant inverse-variance",
    }


def _write_outputs(con: duckdb.DuckDBPyConnection, config: MetaCollapseConfig) -> None:
    """Deterministic output: sorted before COPY so bytes are reproducible."""
    for path in (config.output_path, config.ld_output_path, config.stats_output_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT * FROM collapsed_locus ORDER BY fineMappingLocusSetId, studyId) "
        f"TO {_quote_sql_string(config.output_path.as_posix())} (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY (SELECT ancestry, variantIdI, variantIdJ, r FROM collapsed_ld "
        f"ORDER BY ancestry, variantIdI, variantIdJ) "
        f"TO {_quote_sql_string(config.ld_output_path.as_posix())} (FORMAT PARQUET)"
    )


def run_meta_collapse(config: MetaCollapseConfig) -> dict[str, Any]:
    """Collapse one locus set into a single-population arm and return its stats."""
    if not config.input_path.exists():
        raise FileNotFoundError(f"Fine-mapping locus set not found: {config.input_path}")
    if not config.ld_path.exists():
        raise FileNotFoundError(f"MultiAncestryPairwiseLD not found: {config.ld_path}")

    metadata = _read_study_metadata(config.study_metadata_path)
    input_glob = _parquet_glob(config.input_path)
    ld_glob = _parquet_glob(config.ld_path)

    with duckdb.connect() as con:
        _register_metadata(con, metadata)
        _create_exploded_locus(con, input_glob)
        _create_weights(con)
        _create_canonical_ld(con, ld_glob)

        if con.execute("SELECT count(*) FROM locus_variants").fetchone()[0] == 0:
            raise MetaCollapseError(f"Locus set {config.fine_mapping_locus_set_id} has no usable variants")

        if config.mode == "meta":
            sample_size_total = float(
                con.execute(
                    "SELECT COALESCE(SUM(sampleSize), 0) FROM ("
                    "  SELECT DISTINCT studyId, sampleSize FROM locus_variants"
                    ")"
                ).fetchone()[0]
            )
            _collapse_ld(con, config.meta_ancestry)
            _collapse_locus(con, config, sample_size_total)
        else:
            ancestry = config.target_ancestry
            if not ancestry:
                raise ValueError("mode 'single' requires --target_ancestry")
            declared = {entry["ancestry"] for entry in metadata.values()}
            if ancestry not in declared:
                raise ValueError(f"Ancestry {ancestry!r} is not present in the study metadata: {sorted(declared)}")
            sample_size_total = float(
                sum(entry["sampleSize"] for entry in metadata.values() if entry["ancestry"] == ancestry)
            )
            _filter_single_arm(con, input_glob, ancestry)

        stats = _collect_stats(con, config, sample_size_total)
        _write_outputs(con, config)

    config.stats_output_path.write_text(json.dumps(stats, sort_keys=True) + "\n")

    if math.isnan(stats["maxAbsDiagonalDeviation"]) or stats["maxAbsDiagonalDeviation"] > config.diagonal_tolerance:
        raise MetaCollapseError(
            f"Collapsed LD diagonal deviates from 1.0 by {stats['maxAbsDiagonalDeviation']:.3e}, "
            f"above the tolerance {config.diagonal_tolerance:.3e}. The weights or the joins are wrong; "
            f"see {config.stats_output_path}."
        )
    if stats["missingPairFraction"] > config.max_missing_pair_fraction:
        raise MetaCollapseError(
            f"{stats['nPairsMissingWithBothVariantsPresent']} LD pairs are absent although both variants are "
            f"present in the arm ({stats['missingPairFraction']:.4%} of expected, above "
            f"{config.max_missing_pair_fraction:.4%}). Absent pairs are summed as zero correlation, which "
            f"understates LD; refusing rather than reporting an over-split credible set."
        )
    return stats
