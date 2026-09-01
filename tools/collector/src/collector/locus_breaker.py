"""Collector-native LocusBreaker command surface."""

from __future__ import annotations

from math import floor, log10
from pathlib import Path
from typing import cast

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from collector.schema import STUDY_LOCUS_SCHEMA, ListSchema

MHC_CHROMOSOME = "6"
MHC_START = 25_726_063
MHC_END = 33_400_556

OUTPUT_ORDER_SQL = """
studyId,
CASE WHEN try_cast(chromosome AS INTEGER) IS NULL THEN 1 ELSE 0 END,
try_cast(chromosome AS INTEGER),
chromosome,
locusStart,
locusEnd,
position,
variantId,
studyLocusId
"""

WBC_SOURCE_COLUMNS = (
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

TOP_LEVEL_SELECT_SQL = """
studyLocusId,
studyId,
variantId,
chromosome,
position,
beta,
sampleSize,
pValueMantissa,
pValueExponent,
effectAlleleFrequencyFromSource,
standardError,
qualityControls,
locusStart,
locusEnd,
locus
"""


class LocusBreakerConfig(BaseModel):
    """Parameters exposed by the collector locus_breaker command."""

    model_config = ConfigDict(frozen=True)

    lbc_baseline_pvalue: float = Field(default=1.0e-5, gt=0)
    lbc_distance_cutoff: int = Field(default=250_000, gt=0)
    lbc_pvalue_threshold: float = Field(default=1.0e-8, gt=0)
    lbc_flanking_distance: int = Field(default=100_000, ge=0)
    large_loci_size: int = Field(default=1_500_000, gt=0)
    wbc_clump_distance: int = Field(default=500_000, gt=0)
    wbc_pvalue_threshold: float = Field(default=1.0e-5, gt=0)
    collect_locus: bool = True
    remove_mhc: bool = True


def split_pvalue(pvalue: float) -> tuple[float, int]:
    """Split a p-value into Gentropy-compatible mantissa and exponent."""
    if pvalue < 0.0 or pvalue > 1.0:
        raise ValueError("P-value must be between 0 and 1")

    exponent = floor(log10(pvalue)) if pvalue != 0 else 0
    mantissa = round(pvalue / 10**exponent, 3)
    return mantissa, exponent


def _quote_sql_string(value: str) -> str:
    """Return a single-quoted SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _read_parquet_sql(input_path: Path) -> str:
    """Return a DuckDB read_parquet expression for a file or directory dataset."""
    path = input_path.as_posix()
    if input_path.is_dir():
        path = f"{path}/*.parquet"
    return f"read_parquet({_quote_sql_string(path)})"


def _unique_sumstats_sql(source: str) -> str:
    """Return sum statistics with ambiguous study/variant rows removed."""
    return f"""
SELECT *
FROM {source}
QUALIFY count(*) OVER (PARTITION BY studyId, variantId) = 1
"""


def _study_locus_id_sql(study_id: str = "studyId", variant_id: str = "variantId") -> str:
    """Return Gentropy-compatible studyLocusId SQL expression."""
    return f"md5(coalesce(cast({study_id} AS VARCHAR), 'None') || coalesce(cast({variant_id} AS VARCHAR), 'None'))"


def _pvalue_filter_sql(pvalue: float) -> str:
    """Return Gentropy-compatible p-value mantissa/exponent filter SQL."""
    mantissa, exponent = split_pvalue(pvalue)
    return f"(pValueExponent < {exponent} OR (pValueExponent = {exponent} AND pValueMantissa <= {mantissa}))"


def _candidate_select_sql(source: str, pvalue_threshold: float) -> str:
    """Return sorted WBC candidate SQL after Gentropy-compatible p-value filtering."""
    filter_sql = _pvalue_filter_sql(pvalue_threshold)
    return f"""
SELECT
    cast(studyId AS VARCHAR) AS studyId,
    cast(variantId AS VARCHAR) AS variantId,
    cast(chromosome AS VARCHAR) AS chromosome,
    cast(position AS INTEGER) AS position,
    cast(beta AS DOUBLE) AS beta,
    cast(sampleSize AS INTEGER) AS sampleSize,
    cast(pValueMantissa AS FLOAT) AS pValueMantissa,
    cast(pValueExponent AS INTEGER) AS pValueExponent,
    cast(effectAlleleFrequencyFromSource AS FLOAT) AS effectAlleleFrequencyFromSource,
    cast(standardError AS DOUBLE) AS standardError
FROM {source}
WHERE {filter_sql}
ORDER BY studyId, chromosome, position, variantId
"""


def _wbc_lead_rows(
    con: duckdb.DuckDBPyConnection,
    source: str,
    config: LocusBreakerConfig,
) -> list[dict[str, object]]:
    """Return WBC lead rows using Gentropy's cluster and greedy pruning semantics."""
    rows = con.execute(_candidate_select_sql(source, config.wbc_pvalue_threshold)).fetchall()
    candidates = [dict(zip(WBC_SOURCE_COLUMNS, row, strict=True)) for row in rows]
    if not candidates:
        return []

    clusters: list[list[dict[str, object]]] = []
    current_cluster: list[dict[str, object]] = []
    current_group: tuple[object, object] | None = None
    previous_position: int | None = None

    for candidate in candidates:
        group = (candidate["studyId"], candidate["chromosome"])
        position = int(candidate["position"])
        starts_new_cluster = current_group != group or previous_position is None or position - previous_position > config.wbc_clump_distance
        if starts_new_cluster:
            if current_cluster:
                clusters.append(current_cluster)
            current_cluster = []
            current_group = group
        current_cluster.append(candidate)
        previous_position = position

    if current_cluster:
        clusters.append(current_cluster)

    leads: list[dict[str, object]] = []
    for cluster in clusters:
        ordered_cluster = sorted(
            cluster,
            key=lambda row: (
                int(row["pValueExponent"]),
                float(row["pValueMantissa"]),
                int(row["position"]),
                str(row["variantId"]),
            ),
        )
        selected_positions: list[int] = []
        for candidate in ordered_cluster:
            position = int(cast(int, candidate["position"]))
            if any(abs(lead_position - position) < config.wbc_clump_distance for lead_position in selected_positions):
                continue
            selected_positions.append(position)
            leads.append(candidate)

    return leads


def _sql_literal(value: object) -> str:
    """Return a DuckDB SQL literal for values materialized from Python."""
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return _quote_sql_string(value)
    if isinstance(value, float):
        return repr(value)
    return str(value)


def _wbc_sql_literal(column: str, value: object) -> str:
    """Return a DuckDB SQL literal for WBC values materialized from Python."""
    if isinstance(value, float) and column in {"pValueMantissa", "effectAlleleFrequencyFromSource"}:
        return _quote_sql_string(repr(value))
    return _sql_literal(value)


def _create_wbc_leads_table(con: duckdb.DuckDBPyConnection, leads: list[dict[str, object]]) -> None:
    """Create a temporary DuckDB table containing WBC lead candidate rows."""
    if not leads:
        con.execute(
            "CREATE TEMP TABLE wbc_leads AS "
            "SELECT "
            "CAST(NULL AS VARCHAR) AS studyId, "
            "CAST(NULL AS VARCHAR) AS variantId, "
            "CAST(NULL AS VARCHAR) AS chromosome, "
            "CAST(NULL AS INTEGER) AS position, "
            "CAST(NULL AS DOUBLE) AS beta, "
            "CAST(NULL AS INTEGER) AS sampleSize, "
            "CAST(NULL AS FLOAT) AS pValueMantissa, "
            "CAST(NULL AS INTEGER) AS pValueExponent, "
            "CAST(NULL AS FLOAT) AS effectAlleleFrequencyFromSource, "
            "CAST(NULL AS DOUBLE) AS standardError "
            "WHERE false"
        )
        return

    values_sql = ",\n        ".join("(" + ", ".join(_wbc_sql_literal(column, row[column]) for column in WBC_SOURCE_COLUMNS) + ")" for row in leads)
    columns_sql = ", ".join(WBC_SOURCE_COLUMNS)
    con.execute(
        f"""
        CREATE TEMP TABLE wbc_leads AS
        SELECT * FROM (VALUES
            {values_sql}
        ) AS t({columns_sql})
        """
    )


def processed_locus_breaker_sql(config: LocusBreakerConfig) -> str:
    """Return SQL that replaces large LBC loci with WBC leads."""
    half_large_locus = config.large_loci_size // 2
    locus_type = STUDY_LOCUS_SCHEMA.fields[-1].sql_type()
    return f"""
WITH large_loci AS (
    SELECT *
    FROM lbc
    WHERE locusEnd - locusStart > {config.large_loci_size}
),
small_loci AS (
    SELECT *
    FROM lbc
    WHERE locusEnd - locusStart <= {config.large_loci_size}
),
replacement_loci AS (
    SELECT
        {_study_locus_id_sql("w.studyId", "w.variantId")} AS studyLocusId,
        cast(w.studyId AS VARCHAR) AS studyId,
        cast(w.variantId AS VARCHAR) AS variantId,
        cast(w.chromosome AS VARCHAR) AS chromosome,
        cast(w.position AS INTEGER) AS position,
        cast(w.beta AS DOUBLE) AS beta,
        cast(w.sampleSize AS INTEGER) AS sampleSize,
        cast(w.pValueMantissa AS FLOAT) AS pValueMantissa,
        cast(w.pValueExponent AS INTEGER) AS pValueExponent,
        cast(w.effectAlleleFrequencyFromSource AS FLOAT) AS effectAlleleFrequencyFromSource,
        cast(w.standardError AS DOUBLE) AS standardError,
        cast([] AS VARCHAR[]) AS qualityControls,
        cast(w.position - {half_large_locus} AS INTEGER) AS locusStart,
        cast(w.position + {half_large_locus} AS INTEGER) AS locusEnd,
        cast(NULL AS {locus_type}) AS locus
    FROM wbc_leads AS w
    WHERE EXISTS (
        SELECT 1
        FROM large_loci AS ll
        WHERE w.studyId = ll.studyId
          AND w.chromosome = ll.chromosome
          AND w.position BETWEEN ll.locusStart AND ll.locusEnd
    )
)
SELECT * FROM small_loci
UNION ALL
SELECT * FROM replacement_loci
"""


def lbc_core_sql(source: str, config: LocusBreakerConfig) -> str:
    """Return SQL implementing Gentropy's locus-breaker clumping core."""
    baseline_filter = _pvalue_filter_sql(config.lbc_baseline_pvalue)
    neglog_pvalue_cutoff = -log10(config.lbc_pvalue_threshold)
    flank = config.lbc_flanking_distance

    return f"""
WITH baseline_filtered AS (
    SELECT
        *,
        -1 * (log10(pValueMantissa) + pValueExponent) AS negLogPValue
    FROM {source}
    WHERE {baseline_filter}
),
with_previous_position AS (
    SELECT
        *,
        lag(position) OVER (
            PARTITION BY studyId, chromosome
            ORDER BY position
        ) AS previousPosition
    FROM baseline_filtered
),
with_locus_breaks AS (
    SELECT
        *,
        CASE
            WHEN previousPosition IS NULL
                OR position - previousPosition > {config.lbc_distance_cutoff}
            THEN position
            ELSE NULL
        END AS locusBreakPosition
    FROM with_previous_position
),
with_locus_start AS (
    SELECT
        *,
        CASE
            WHEN last_value(locusBreakPosition IGNORE NULLS) OVER (
                PARTITION BY studyId, chromosome
                ORDER BY position
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) - {flank} > 0
            THEN CAST(last_value(locusBreakPosition IGNORE NULLS) OVER (
                PARTITION BY studyId, chromosome
                ORDER BY position
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) - {flank} AS INTEGER)
            ELSE 0
        END AS locusStart
    FROM with_locus_breaks
),
with_locus_end AS (
    SELECT
        *,
        CAST(max(position + {flank}) OVER (
            PARTITION BY studyId, chromosome, locusStart
            ORDER BY position
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS INTEGER) AS locusEnd
    FROM with_locus_start
),
with_lead_rank AS (
    SELECT
        *,
        rank() OVER (
            PARTITION BY studyId, chromosome, locusStart, locusEnd
            ORDER BY negLogPValue DESC
        ) AS leadRank
    FROM with_locus_end
)
SELECT
    {_study_locus_id_sql()} AS studyLocusId,
    cast(studyId AS VARCHAR) AS studyId,
    cast(variantId AS VARCHAR) AS variantId,
    cast(chromosome AS VARCHAR) AS chromosome,
    cast(position AS INTEGER) AS position,
    cast(beta AS DOUBLE) AS beta,
    cast(sampleSize AS INTEGER) AS sampleSize,
    cast(pValueMantissa AS FLOAT) AS pValueMantissa,
    cast(pValueExponent AS INTEGER) AS pValueExponent,
    cast(effectAlleleFrequencyFromSource AS FLOAT) AS effectAlleleFrequencyFromSource,
    cast(standardError AS DOUBLE) AS standardError,
    cast(NULL AS VARCHAR[]) AS qualityControls,
    cast(locusStart AS INTEGER) AS locusStart,
    cast(locusEnd AS INTEGER) AS locusEnd,
    cast(NULL AS {STUDY_LOCUS_SCHEMA.fields[-1].sql_type()}) AS locus
FROM with_lead_rank
WHERE leadRank = 1
  AND negLogPValue > {neglog_pvalue_cutoff}
"""


def mhc_filtered_loci_sql(config: LocusBreakerConfig) -> str:
    """Return SQL applying Gentropy MHC overlap exclusion after LBC/WBC replacement."""
    if not config.remove_mhc:
        return "SELECT * FROM processed_loci"

    return f"""
SELECT *
FROM processed_loci
WHERE NOT (
    chromosome = {_quote_sql_string(MHC_CHROMOSOME)}
    AND locusStart <= {MHC_END}
    AND locusEnd >= {MHC_START}
)
"""


def _locus_struct_sql(alias: str = "s") -> str:
    """Return the nested locus struct expression in agreed field order."""
    locus_schema = cast(ListSchema, STUDY_LOCUS_SCHEMA.fields[-1].duckdb_type)
    locus_struct_type = locus_schema.item_schema.sql_type()
    return f"""
CAST(
    struct_pack(
        is95CredibleSet := CAST(NULL AS BOOLEAN),
        is99CredibleSet := CAST(NULL AS BOOLEAN),
        logBF := CAST(NULL AS DOUBLE),
        posteriorProbability := CAST(NULL AS DOUBLE),
        variantId := cast({alias}.variantId AS VARCHAR),
        pValueMantissa := cast({alias}.pValueMantissa AS FLOAT),
        pValueExponent := cast({alias}.pValueExponent AS INTEGER),
        beta := cast({alias}.beta AS DOUBLE),
        standardError := cast({alias}.standardError AS DOUBLE),
        r2Overall := CAST(NULL AS DOUBLE)
    ) AS {locus_struct_type}
)
"""


def final_loci_sql(source: str, config: LocusBreakerConfig) -> str:
    """Return SQL for the final flat output, optionally collecting locus arrays."""
    if not config.collect_locus:
        return f"SELECT {TOP_LEVEL_SELECT_SQL} FROM mhc_filtered_loci"

    locus_type = STUDY_LOCUS_SCHEMA.fields[-1].sql_type()
    locus_struct = _locus_struct_sql("s")
    return f"""
WITH source_sumstats AS (
    SELECT
        cast(studyId AS VARCHAR) AS studyId,
        cast(variantId AS VARCHAR) AS variantId,
        cast(chromosome AS VARCHAR) AS chromosome,
        cast(position AS INTEGER) AS position,
        cast(beta AS DOUBLE) AS beta,
        cast(pValueMantissa AS FLOAT) AS pValueMantissa,
        cast(pValueExponent AS INTEGER) AS pValueExponent,
        cast(standardError AS DOUBLE) AS standardError
    FROM {source}
),
collected_loci AS (
    SELECT
        f.studyLocusId,
        cast(
            list(
                {locus_struct}
                ORDER BY
                    s.position,
                    s.variantId,
                    s.pValueExponent,
                    s.pValueMantissa,
                    s.beta,
                    s.standardError
            ) AS {locus_type}
        ) AS locus
    FROM mhc_filtered_loci AS f
    JOIN source_sumstats AS s
      ON s.studyId = f.studyId
     AND s.chromosome = f.chromosome
     AND s.position >= f.locusStart
     AND s.position <= f.locusEnd
    GROUP BY f.studyLocusId
)
SELECT
    f.studyLocusId,
    f.studyId,
    f.variantId,
    f.chromosome,
    f.position,
    f.beta,
    f.sampleSize,
    f.pValueMantissa,
    f.pValueExponent,
    f.effectAlleleFrequencyFromSource,
    f.standardError,
    f.qualityControls,
    f.locusStart,
    f.locusEnd,
    coalesce(c.locus, cast([] AS {locus_type})) AS locus
FROM mhc_filtered_loci AS f
LEFT JOIN collected_loci AS c
  ON f.studyLocusId = c.studyLocusId
"""


def run_locus_breaker(input_path: Path, output_path: Path, config: LocusBreakerConfig) -> None:
    """Run the LocusBreaker command."""
    if not input_path.exists():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")
    if output_path.suffix != ".parquet":
        raise ValueError("Output file should have a .parquet extension")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect() as con:
        con.execute("CREATE TEMP TABLE source_sumstats AS " + _unique_sumstats_sql(_read_parquet_sql(input_path)))
        source = "source_sumstats"
        con.execute("CREATE TEMP TABLE lbc AS " + lbc_core_sql(source, config))
        large_loci_count_row = con.execute(f"SELECT COUNT(*) FROM lbc WHERE locusEnd - locusStart > {config.large_loci_size}").fetchone()
        if large_loci_count_row is None:
            raise RuntimeError("DuckDB returned no large-loci count")
        large_loci_count = large_loci_count_row[0]
        if large_loci_count > 0:
            _create_wbc_leads_table(con, _wbc_lead_rows(con, source, config))
            con.execute("CREATE TEMP TABLE processed_loci AS " + processed_locus_breaker_sql(config))
        else:
            con.execute("CREATE TEMP TABLE processed_loci AS SELECT * FROM lbc")

        con.execute("CREATE TEMP TABLE mhc_filtered_loci AS " + mhc_filtered_loci_sql(config))
        con.execute("CREATE TEMP TABLE final_loci AS " + final_loci_sql(source, config))

        if output_path.exists():
            output_path.unlink()

        con.execute(
            f"COPY (SELECT {TOP_LEVEL_SELECT_SQL} FROM final_loci ORDER BY {OUTPUT_ORDER_SQL}) "
            f"TO {_quote_sql_string(output_path.as_posix())} (FORMAT PARQUET)"
        )
