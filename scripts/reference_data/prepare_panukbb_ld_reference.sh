#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

GENTROPY_DIR="${GENTROPY_DIR:-${PROJECT_DIR}/.worktrees/gentropy-issue-4-long-ld-pairs}"
REFERENCE_DATA_ROOT="${REFERENCE_DATA_ROOT:-${PROJECT_DIR}/data/reference/panukbb}"
VARIANT_ANNOTATION_PATH="${VARIANT_ANNOTATION_PATH:-${REFERENCE_DATA_ROOT}/variant_index/study_locus_variants.parquet}"
PANUKBB_HT_PATH="${PANUKBB_HT_PATH:-}"
if [ -z "${PANUKBB_HT_PATH}" ]; then
    PANUKBB_HT_PATH="${REFERENCE_DATA_ROOT}/raw/UKBB.{POP}.ldadj.variant.b38.ht"
fi
PANUKBB_POPS="${PANUKBB_POPS:-[AFR,CSA,EUR]}"
FILTER_SCOPE="${FILTER_SCOPE:-chr1}"
SDKMAN_JAVA_VERSION="${SDKMAN_JAVA_VERSION:-11.0.23-tem}"
PRINT_CONFIG_ONLY="${PRINT_CONFIG_ONLY:-0}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-8g}"
SPARK_DRIVER_MAX_RESULT_SIZE="${SPARK_DRIVER_MAX_RESULT_SIZE:-2g}"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-400}"
SPARK_SQL_AUTO_BROADCAST_JOIN_THRESHOLD="${SPARK_SQL_AUTO_BROADCAST_JOIN_THRESHOLD:--1}"

SDKMAN_INIT="${SDKMAN_INIT:-${SDKMAN_DIR:-${HOME}/.sdkman}/bin/sdkman-init.sh}"
if [ ! -f "${SDKMAN_INIT}" ]; then
    echo "SDKMAN init script not found: ${SDKMAN_INIT}" >&2
    exit 1
fi

# SDKMAN's init script references shell-specific variables that may be unset
# under bash with nounset enabled.
set +u
# shellcheck source=/dev/null
source "${SDKMAN_INIT}"
sdk use java "${SDKMAN_JAVA_VERSION}" >/dev/null
set -u

if [ ! -d "${GENTROPY_DIR}" ]; then
    echo "Gentropy worktree not found: ${GENTROPY_DIR}" >&2
    exit 1
fi

if [ ! -f "${REFERENCE_DATA_ROOT}/filters/chr1_variants.parquet" ]; then
    echo "Missing chr1 filter. Run scripts/reference_data/build_panukbb_variant_filters.sh after locus breaker/collection outputs exist." >&2
    exit 1
fi

if [ ! -f "${VARIANT_ANNOTATION_PATH}" ]; then
    echo "Missing local VariantIndex: ${VARIANT_ANNOTATION_PATH}" >&2
    echo "Run scripts/reference_data/build_study_locus_variant_index.sh first." >&2
    exit 1
fi

for population in AFR CSA EUR; do
    hail_table_path="${PANUKBB_HT_PATH/\{POP\}/${population}}"
    if [ ! -d "${hail_table_path}" ]; then
        echo "Missing local PanUKBB Hail Table: ${hail_table_path}" >&2
        echo "Run scripts/reference_data/download_panukbb_variant_tables.sh first." >&2
        exit 1
    fi
done

IFS=',' read -r -a FILTERS <<< "${FILTER_SCOPE}"

hydra_args=(
    "step=pan_ukbb_variant_index"
    "step.session.write_mode=overwrite"
    "step.session.start_hail=true"
    "+step.session.extended_spark_conf={spark.driver.memory:${SPARK_DRIVER_MEMORY},spark.driver.maxResultSize:${SPARK_DRIVER_MAX_RESULT_SIZE},spark.sql.shuffle.partitions:${SPARK_SQL_SHUFFLE_PARTITIONS},spark.sql.autoBroadcastJoinThreshold:${SPARK_SQL_AUTO_BROADCAST_JOIN_THRESHOLD}}"
    "step.variant_annotation_path=${VARIANT_ANNOTATION_PATH}"
    "step.pan_ukbb_ht_path=\"${PANUKBB_HT_PATH}\""
    "step.pan_ukbb_pops=${PANUKBB_POPS}"
    "step.ukbb_annotation_path=\"${REFERENCE_DATA_ROOT}/UKBB.{POP}.aligned.parquet\""
)

if [ "${#FILTERS[@]}" -gt 0 ] && [ "${FILTER_SCOPE}" != "none" ]; then
    hydra_args+=(
        "step.filtered_ukbb_annotation_path=\"${REFERENCE_DATA_ROOT}/{FILTER}/UKBB.{POP}.aligned.parquet\""
    )

    for filter_name in "${FILTERS[@]}"; do
        filter_path="${REFERENCE_DATA_ROOT}/filters/${filter_name}_variants.parquet"
        if [ ! -f "${filter_path}" ]; then
            echo "Missing filter parquet: ${filter_path}" >&2
            exit 1
        fi
        hydra_args+=("+step.variant_filter_paths.${filter_name}=${filter_path}")
    done
fi

(
    cd "${GENTROPY_DIR}"
    java -version
    gentropy_cli_args=()
    if [ "${PRINT_CONFIG_ONLY}" = "1" ]; then
        gentropy_cli_args+=("--cfg" "job")
    fi
    uv run --no-sync gentropy "${gentropy_cli_args[@]}" "${hydra_args[@]}"
)
