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


def _restrict_to_ld_backed_arms(con: duckdb.DuckDBPyConnection) -> None:
    """Keep, per variant, only the arms whose LD can actually support it.

    ``diag(R_meta)[i] = sum_a u[a,i]^2 * R_a[i,i]`` equals 1 only when every arm
    carrying weight for variant *i* also supplies a diagonal row for *i*. The
    reference panel resolves a subset of the requested variants, and the subset
    differs by ancestry, so a variant can sit in two arms' summary statistics
    while appearing in only one arm's LD. Its weight then sums to less than 1
    and the diagonal falls short by exactly the missing arm's ``u^2`` -- 0.189
    on the locus that first exposed this.

    The fix has to move ``z`` and ``R`` together. ``R_meta`` must be the
    covariance of the very statistics handed to the fine-mapper, so an arm that
    cannot contribute to ``R`` must not contribute to ``z`` either. Weighting
    ``z`` over all arms while weighting ``R`` over LD-backed arms only would be
    internally inconsistent and would quietly misspecify the likelihood.

    A variant absent from *every* arm's LD keeps its full set of arms: it can
    never enter ``R_meta``, so the diagonal invariant is unaffected, and
    dropping it would make the meta arm's variant set differ from the joint
    arm's -- which the comparison depends on holding fixed.

    ``locus_variants_all`` is retained so the exclusions can be counted.
    """
    con.execute(
        """
        CREATE TABLE ld_presence AS
        SELECT DISTINCT ancestry, variantIdI AS variantId FROM canonical_ld
        UNION
        SELECT DISTINCT ancestry, variantIdJ AS variantId FROM canonical_ld
        """
    )
    con.execute(
        """
        CREATE TABLE contributing_locus_variants AS
        WITH flagged AS (
            SELECT
                lv.*,
                CASE WHEN p.variantId IS NULL THEN FALSE ELSE TRUE END AS in_ld
            FROM locus_variants AS lv
            LEFT JOIN ld_presence AS p
              ON p.ancestry = lv.ancestry AND p.variantId = lv.variantId
        ),
        scoped AS (
            SELECT *, bool_or(in_ld) OVER (PARTITION BY variantId) AS any_arm_in_ld
            FROM flagged
        )
        SELECT * EXCLUDE (in_ld, any_arm_in_ld)
        FROM scoped
        WHERE in_ld OR NOT any_arm_in_ld
        """
    )
    con.execute("ALTER TABLE locus_variants RENAME TO locus_variants_all")
    con.execute("ALTER TABLE contributing_locus_variants RENAME TO locus_variants")


def _create_weights(con: duckdb.DuckDBPyConnection) -> None:
    """u[a,i], normalised over the arms that contribute to variant i.

    Contributing arms are those selected by ``_restrict_to_ld_backed_arms``, so
    ``sum_a u[a,i]^2 = 1`` holds over exactly the arms that also supply LD, and
    the collapsed diagonal is 1 by construction.
    """
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


def _missing_pair_count(con: duckdb.DuckDBPyConnection) -> tuple[int, int, int]:
    """Pairs absent from an arm's LD although both variants are present there.

    An arm's LD is a complete upper triangle including the diagonal **over the
    variants the reference panel could resolve**, so the expected row count is
    C(m, 2) + m for an arm whose LD mentions m variants. A shortfall means pairs
    were dropped between resolvable variants, and a dropped pair is summed as
    zero correlation by ``_collapse_ld`` -- understating LD and making the
    fine-mapper over-split. That is what this refuses.

    The denominator is deliberately the variants present in the LD, not the
    variants in the locus set. Hailing Ducks resolves a subset: across the 404
    ancestry x locus records of the 26.09 test3 run, ``n_ld_pairs`` equals
    C(n_resolved, 2) + n_resolved in 404 cases and C(n_requested, 2) +
    n_requested in only 52. Counting requested variants therefore reports up to
    52% of pairs "missing" on a locus where nothing was dropped at all -- 12.7%
    across that whole run.

    Variants in the locus set that the panel could not resolve are reported
    separately as ``nVariantsAbsentFromLd`` and are not an error: the joint arm
    is handed exactly the same locus set and the same LD, so every arm sees the
    identical variant/LD mismatch. Consistency across arms is what the
    comparison requires.
    """
    rows = con.execute(
        """
        WITH ld_variants AS (
            SELECT DISTINCT ancestry, variantIdI AS variantId FROM canonical_ld
            UNION
            SELECT DISTINCT ancestry, variantIdJ AS variantId FROM canonical_ld
        ),
        arm_resolved AS (
            SELECT ancestry, count(*) AS m FROM ld_variants GROUP BY ancestry
        ),
        arm_pairs AS (
            SELECT ancestry, count(*) AS observed FROM canonical_ld GROUP BY ancestry
        )
        SELECT
            COALESCE(SUM(CAST(r.m AS BIGINT) * (CAST(r.m AS BIGINT) + 1) / 2), 0) AS expected,
            COALESCE(SUM(p.observed), 0)                                          AS observed,
            (
                SELECT count(DISTINCT w.variantId)
                FROM variant_weights AS w
                WHERE NOT EXISTS (
                    SELECT 1 FROM ld_variants AS l
                    WHERE l.ancestry = w.ancestry AND l.variantId = w.variantId
                )
            )                                                                     AS absent_from_ld
        FROM arm_resolved AS r
        LEFT JOIN arm_pairs AS p ON p.ancestry = r.ancestry
        """
    ).fetchone()
    expected = int(rows[0]) if rows else 0
    observed = int(rows[1]) if rows else 0
    absent = int(rows[2]) if rows else 0
    return max(expected - observed, 0), expected, absent


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
    missing_pairs, expected_pairs, absent_from_ld = _missing_pair_count(con)
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
        # Locus-set variants the LD panel could not resolve. Reported, never
        # fatal: the joint arm receives the same locus set and the same LD, so
        # every arm sees an identical variant/LD mismatch.
        "nVariantsAbsentFromLd": int(absent_from_ld),
        # (arm, variant) pairs excluded from the collapse because that arm's LD
        # could not resolve the variant while another arm's could. Excluded from
        # z and R together, so R_meta stays the covariance of the z it accompanies.
        "nArmVariantContributionsDropped": int(
            con.execute(
                "SELECT (SELECT count(*) FROM locus_variants_all) - (SELECT count(*) FROM locus_variants)"
            ).fetchone()[0]
        ),
        "maxAbsDiagonalDeviation": deviation,
        "sampleSizeTotal": sample_size_total,
        "pairOrderCanonicalisationApplied": True,
        "ldCombinationRule": "R_meta[i,j] = sum_a u[a,i] * u[a,j] * R[a,i,j], u per-variant inverse-variance",
    }


def _write_outputs(con: duckdb.DuckDBPyConnection, config: MetaCollapseConfig) -> None:
    """Deterministic output: sorted before COPY so bytes are reproducible.

    Correlations are clamped to [-1, 1] on the way out. ``R_meta[i,j]`` is
    bounded by 1 in exact arithmetic -- the diagonal is ``sum_a u[a,i]^2 = 1``
    and Cauchy-Schwarz bounds the off-diagonals -- but summing three or more
    float64 products lands a hair outside, e.g. 1.0000000000000004, which
    MultiSuSiE rejects with "Invalid LD value". The clamp corrects 4e-16 of
    accumulated rounding and nothing else.

    Deliberately applied here and not in ``_collapse_ld``: the diagonal guard in
    ``_collect_stats`` reads the *unclamped* table, so it still measures the true
    deviation. Clamping first would make that check trivially pass and hide a
    genuinely wrong weighting.
    """
    for path in (config.output_path, config.ld_output_path, config.stats_output_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT * FROM collapsed_locus ORDER BY fineMappingLocusSetId, studyId) "
        f"TO {_quote_sql_string(config.output_path.as_posix())} (FORMAT PARQUET)"
    )
    con.execute(
        f"COPY (SELECT ancestry, variantIdI, variantIdJ, "
        f"       greatest(-1.0, least(1.0, r)) AS r "
        f"FROM collapsed_ld ORDER BY ancestry, variantIdI, variantIdJ) "
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
        # Order matters: the LD has to be read before the weights, because which
        # arms a variant is weighted over depends on which arms resolved it.
        _create_canonical_ld(con, ld_glob)
        _restrict_to_ld_backed_arms(con)
        _create_weights(con)

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
