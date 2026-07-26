#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

CHR1_STUDY_LOCUS_GLOB="${CHR1_STUDY_LOCUS_GLOB:-${PROJECT_DIR}/testdata/output/locus_breaker_clumped_study_locus/*.parquet}"
FULL_TEST_STUDY_LOCUS_GLOB="${FULL_TEST_STUDY_LOCUS_GLOB:-${PROJECT_DIR}/testdata/output_full/collected_loci/full_overlaps/*.parquet}"
FILTER_DIR="${FILTER_DIR:-${PROJECT_DIR}/data/reference/panukbb/filters}"

if ! command -v duckdb >/dev/null 2>&1; then
    echo "duckdb is required to build PanUKBB variant filters." >&2
    exit 1
fi

mkdir -p "${FILTER_DIR}"

duckdb -c "
COPY (
    SELECT DISTINCT
        locus_variant.variantid AS variantId,
        chromosome
    FROM read_parquet('${CHR1_STUDY_LOCUS_GLOB}') AS study_locus,
         UNNEST(study_locus.locus) AS unnested(locus_variant)
    WHERE locus_variant.variantid IS NOT NULL
      AND chromosome IS NOT NULL
) TO '${FILTER_DIR}/chr1_variants.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD);
"

duckdb -c "
COPY (
    SELECT DISTINCT
        locus_variant.variantid AS variantId,
        chromosome
    FROM read_parquet('${FULL_TEST_STUDY_LOCUS_GLOB}') AS study_locus,
         UNNEST(study_locus.locus) AS unnested(locus_variant)
    WHERE locus_variant.variantid IS NOT NULL
      AND chromosome IS NOT NULL
) TO '${FILTER_DIR}/full_test_variants.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD);
"

duckdb -c "
SELECT
    'chr1' AS filter_name,
    count(*) AS row_count,
    count(DISTINCT variantId) AS variant_count,
    count(DISTINCT chromosome) AS chromosome_count
FROM read_parquet('${FILTER_DIR}/chr1_variants.parquet')
UNION ALL
SELECT
    'full_test' AS filter_name,
    count(*) AS row_count,
    count(DISTINCT variantId) AS variant_count,
    count(DISTINCT chromosome) AS chromosome_count
FROM read_parquet('${FILTER_DIR}/full_test_variants.parquet');
"
