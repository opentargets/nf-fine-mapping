#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

FILTER_DIR="${FILTER_DIR:-${PROJECT_DIR}/data/reference/panukbb/filters}"
VARIANT_INDEX_DIR="${VARIANT_INDEX_DIR:-${PROJECT_DIR}/data/reference/panukbb/variant_index}"
VARIANT_INDEX_PATH="${VARIANT_INDEX_PATH:-${VARIANT_INDEX_DIR}/study_locus_variants.parquet}"

if ! command -v duckdb >/dev/null 2>&1; then
    echo "duckdb is required to build the local StudyLocus VariantIndex." >&2
    exit 1
fi

for filter_name in chr1 full_test; do
    filter_path="${FILTER_DIR}/${filter_name}_variants.parquet"
    if [ ! -f "${filter_path}" ]; then
        echo "Missing variant filter: ${filter_path}" >&2
        echo "Run scripts/reference_data/build_panukbb_variant_filters.sh first." >&2
        exit 1
    fi
done

mkdir -p "${VARIANT_INDEX_DIR}"

duckdb -c "
COPY (
    WITH requested_variants AS (
        SELECT variantId, chromosome
        FROM read_parquet('${FILTER_DIR}/chr1_variants.parquet')
        UNION
        SELECT variantId, chromosome
        FROM read_parquet('${FILTER_DIR}/full_test_variants.parquet')
    ),
    parsed AS (
        SELECT
            variantId,
            chromosome AS sourceChromosome,
            split_part(variantId, '_', 1) AS parsedChromosome,
            try_cast(split_part(variantId, '_', 2) AS INTEGER) AS position,
            split_part(variantId, '_', 3) AS referenceAllele,
            split_part(variantId, '_', 4) AS alternateAllele,
            split_part(variantId, '_', 5) AS unexpectedToken
        FROM requested_variants
    )
    SELECT DISTINCT
        variantId,
        parsedChromosome AS chromosome,
        position,
        referenceAllele,
        alternateAllele
    FROM parsed
    WHERE position IS NOT NULL
      AND sourceChromosome = parsedChromosome
      AND referenceAllele <> ''
      AND alternateAllele <> ''
      AND unexpectedToken = ''
) TO '${VARIANT_INDEX_PATH}'
  (FORMAT PARQUET, COMPRESSION ZSTD);
"

duckdb -c "
WITH requested_variants AS (
    SELECT variantId, chromosome
    FROM read_parquet('${FILTER_DIR}/chr1_variants.parquet')
    UNION
    SELECT variantId, chromosome
    FROM read_parquet('${FILTER_DIR}/full_test_variants.parquet')
),
variant_index AS (
    SELECT variantId
    FROM read_parquet('${VARIANT_INDEX_PATH}')
)
SELECT
    count(*) AS requested_variant_count,
    (SELECT count(*) FROM variant_index) AS variant_index_count,
    count(*) - (SELECT count(*) FROM variant_index) AS dropped_unparseable_variant_count
FROM requested_variants;
"
