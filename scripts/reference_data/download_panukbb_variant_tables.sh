#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

REFERENCE_DATA_ROOT="${REFERENCE_DATA_ROOT:-${PROJECT_DIR}/data/reference/panukbb}"
PANUKBB_VARIANT_TABLE_S3_TEMPLATE="${PANUKBB_VARIANT_TABLE_S3_TEMPLATE:-}"
if [ -z "${PANUKBB_VARIANT_TABLE_S3_TEMPLATE}" ]; then
    PANUKBB_VARIANT_TABLE_S3_TEMPLATE='s3://pan-ukb-us-east-1/ld_release/UKBB.{POP}.ldadj.variant.b38.ht/'
fi
PANUKBB_POPS="${PANUKBB_POPS:-AFR CSA EUR}"
DRY_RUN="${DRY_RUN:-0}"
AWS_SYNC_EXTRA_ARGS="${AWS_SYNC_EXTRA_ARGS:---only-show-errors}"

main() {
    if ! command -v aws >/dev/null 2>&1; then
        echo "aws CLI is required to download PanUKBB Hail variant tables." >&2
        exit 1
    fi

    if [[ "${PANUKBB_VARIANT_TABLE_S3_TEMPLATE}" == *".bm/"* ]] || [[ "${PANUKBB_VARIANT_TABLE_S3_TEMPLATE}" != *".variant.b38.ht/"* ]]; then
        echo "Refusing unsafe PanUKBB source template: ${PANUKBB_VARIANT_TABLE_S3_TEMPLATE}" >&2
        echo "This script only downloads small .variant.b38.ht/ Hail Tables, never .bm/ BlockMatrices." >&2
        exit 1
    fi

    mkdir -p "${REFERENCE_DATA_ROOT}/raw"

    read -r -a sync_extra_args <<< "${AWS_SYNC_EXTRA_ARGS}"

    for population in ${PANUKBB_POPS}; do
        placeholder="{POP}"
        source_path="${PANUKBB_VARIANT_TABLE_S3_TEMPLATE//$placeholder/${population}}"
        destination_path="${REFERENCE_DATA_ROOT}/raw/UKBB.${population}.ldadj.variant.b38.ht"

        if [[ "${source_path}" == *".bm/"* ]] || [[ "${destination_path}" == *".bm"* ]]; then
            echo "Refusing to download a BlockMatrix path: ${source_path}" >&2
            exit 1
        fi

        echo "Inspecting ${source_path}"
        object_count="$(
            aws s3 ls "${source_path}" --no-sign-request --recursive --summarize --human-readable \
                | tee /tmp/panukbb_${population}_variant_table_ls.txt \
                | tail -5 \
                | tee /dev/stderr \
                | awk '/Total Objects:/ { print $3 }'
        )"
        if [ "${object_count:-0}" = "0" ]; then
            echo "No objects found at ${source_path}" >&2
            exit 1
        fi

        if [ "${DRY_RUN}" = "1" ]; then
            echo "DRY_RUN=1; not downloading ${source_path}"
            continue
        fi

        echo "Downloading ${source_path} -> ${destination_path}"
        aws s3 sync "${source_path}" "${destination_path}" --no-sign-request "${sync_extra_args[@]}"
    done
}

main "$@"
