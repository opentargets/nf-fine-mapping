#!/usr/bin/env python3
"""Profile one largest fine-mapping locus set with the Gentropy LD backend.

Run this inside the Gentropy container. The default inputs select the largest
fine-mapping locus set parquet under ``testdata/work_full`` and pair it with
the local full-test Pan-UKBB VariantIndex files so the Gentropy LD path can be
profiled against canonical pipeline outputs. The script deliberately times the
Hail stages separately so that BlockMatrix extraction can be distinguished from
Spark startup, index filtering, and output writing.

Example::

    docker run --rm -it \
      -v "$PWD:/workspace" -w /workspace \
      gentropy:3.4.0-dev.1-ld-pair-extraction-v2 \
      python scripts/debug_largest_ld_annotation.py

The default runs the complete selected locus set, including all study-locus
rows in the parquet and one combined parquet write.  Use
``--stop-after entries_count`` for a shorter bottleneck measurement, or
``--stop-after spark_count`` to include the Hail-to-Spark conversion without
writing output.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import time
from pathlib import Path
from typing import Any

from gentropy.common.session import Session
from gentropy.datasource.pan_ukbb_ld.ld import PanUKBBLDMatrix


LOGGER = logging.getLogger(__name__)


DEFAULT_LD_REFERENCES = {
    "nfe": {
        "vi_path": "data/reference/panukbb/full_test/UKBB.EUR.aligned.parquet",
        "bm_path": "s3a://pan-ukb-us-east-1/ld_release/UKBB.EUR.ldadj.bm",
    },
    "eas": {
        "vi_path": "data/reference/panukbb/full_test/UKBB.CSA.aligned.parquet",
        "bm_path": "s3a://pan-ukb-us-east-1/ld_release/UKBB.CSA.ldadj.bm",
    },
    "afr": {
        "vi_path": "data/reference/panukbb/full_test/UKBB.AFR.aligned.parquet",
        "bm_path": "s3a://pan-ukb-us-east-1/ld_release/UKBB.AFR.ldadj.bm",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--locus-root",
        type=Path,
        default=Path("testdata/work_full"),
        help="Root containing fine_mapping_locus_sets parquet files.",
    )
    parser.add_argument(
        "--locus-path",
        type=Path,
        help="Profile this parquet directly instead of selecting the largest one.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("testdata/manifest.full.tsv"),
        help="Manifest used to resolve ancestry labels for study IDs.",
    )
    parser.add_argument(
        "--study-id",
        help="Profile only this study locus from the selected locus-set parquet.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("testdata/debug_ld_annotation_output"),
        help="Output path used only with --stop-after write.",
    )
    parser.add_argument("--spark-uri", default="local[2]")
    parser.add_argument("--driver-memory", default="24g")
    parser.add_argument("--executor-memory", default="24g")
    parser.add_argument(
        "--stop-after",
        choices=("index", "bm_read", "entries_count", "spark_count", "write"),
        default="write",
        help="Last phase to execute; write runs the complete selected locus set.",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING"),
        default="INFO",
        help="Python log level for timestamped phase logging.",
    )
    return parser.parse_args()


def timed(metrics: list[dict[str, Any]], name: str, start: float, **values: Any) -> None:
    metric = {"phase": name, "seconds": round(time.perf_counter() - start, 3), **values}
    metrics.append(metric)
    LOGGER.info("phase complete: %s", json.dumps(metric, sort_keys=True))


def manifest_metadata(path: Path) -> dict[str, dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = csv.DictReader(handle, delimiter="\t")
        return {
            row["studyId"]: {
                "ancestry": row["majorAncestry"],
                "sampleSize": int(row["effectiveSampleSize"]),
            }
            for row in rows
        }


def candidate_paths(root: Path) -> list[Path]:
    paths = sorted(root.glob("**/fine_mapping_locus_sets/*.parquet"))
    if not paths:
        raise FileNotFoundError(f"No fine_mapping_locus_sets parquet files found under {root}")
    return paths


def select_largest(session: Session, paths: list[Path]) -> tuple[Path, int]:
    from pyspark.sql import functions as F

    frame = session.spark.read.parquet(*(str(path) for path in paths)).withColumn("_source", F.input_file_name())
    ranked = (
        frame.select("_source", F.size("locus").alias("n_variants"))
        .groupBy("_source")
        .agg(F.sum("n_variants").alias("n_variants"))
        .orderBy(F.col("n_variants").desc())
    )
    row = ranked.first()
    if row is None:
        raise ValueError("No locus rows found in candidate parquet files")
    source = str(row["_source"])
    if source.startswith("file:"):
        source = source.removeprefix("file:")
    return Path(source), int(row["n_variants"])


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    metrics: list[dict[str, Any]] = []
    metadata = manifest_metadata(args.manifest)
    session = Session(
        spark_uri=args.spark_uri,
        app_name="debug-largest-ld-annotation",
        start_hail=True,
        output_partitions=1,
        log_level="ERROR",
        add_s3_connector=True,
        s3_configuration={"anonymous": True, "s3_host_url": "s3.us-east-1.amazonaws.com"},
        extended_spark_conf={
            "spark.driver.memory": args.driver_memory,
            "spark.executor.memory": args.executor_memory,
            "spark.driver.maxResultSize": "2g",
        },
        extended_hail_conf={"quiet": True, "log": "/dev/null"},
    )

    try:
        selection_start = time.perf_counter()
        if args.locus_path:
            locus_path = args.locus_path
            selected_variant_count = -1
        else:
            locus_path, selected_variant_count = select_largest(session, candidate_paths(args.locus_root))
        timed(metrics, "select_locus", selection_start, locus_path=str(locus_path), selected_variant_count=selected_variant_count)

        locus_frame = session.spark.read.parquet(str(locus_path)).select("studyId", "locus")
        rows = locus_frame.collect()
        if args.study_id:
            rows = [row for row in rows if row["studyId"] == args.study_id]
            if not rows:
                raise ValueError(f"Study ID {args.study_id} was not found in {locus_path}")
        locus_counts = {row["studyId"]: len(row["locus"] or []) for row in rows}
        selection = {"selected_locus": str(locus_path), "study_locus_variant_counts": locus_counts}
        LOGGER.info("selected locus set: %s", json.dumps(selection, sort_keys=True))
        print(json.dumps(selection, sort_keys=True))

        if args.stop_after == "index":
            return

        spark_outputs: list[Any] = []
        for row in rows:
            study_id = row["studyId"]
            study_metadata = metadata[study_id]
            ancestry = study_metadata["ancestry"]
            reference = DEFAULT_LD_REFERENCES[ancestry]
            variants = list(dict.fromkeys(variant["variantId"] for variant in row["locus"] or []))

            index_start = time.perf_counter()
            index = session.spark.read.parquet(reference["vi_path"])
            locus_index = index.filter(index.variantId.isin(variants)).dropDuplicates(["variantId"])
            index_rows = locus_index.select("idx", "variantId", "alleleOrder").sort("idx", "variantId").collect()
            timed(
                metrics,
                f"index_{study_id}",
                index_start,
                ancestry=ancestry,
                requested_variants=len(variants),
                matched_variants=len(index_rows),
                min_idx=min((item["idx"] for item in index_rows), default=None),
                max_idx=max((item["idx"] for item in index_rows), default=None),
                matrix_index_span=(
                    max((item["idx"] for item in index_rows), default=0)
                    - min((item["idx"] for item in index_rows), default=0)
                    + 1
                    if index_rows
                    else 0
                ),
                possible_upper_triangle_pairs=len(index_rows) * (len(index_rows) - 1) // 2,
            )
            print(json.dumps(metrics[-1], sort_keys=True))

            indices = [item["idx"] for item in index_rows]
            matrix_start = time.perf_counter()
            matrix = PanUKBBLDMatrix(pan_ukbb_bm_path=reference["bm_path"])._filter_hail_block_matrix(indices, ancestry)
            timed(metrics, f"block_matrix_read_{study_id}", matrix_start, ancestry=ancestry, n_indices=len(indices), shape=matrix.shape)
            print(json.dumps(metrics[-1], sort_keys=True))
            if args.stop_after == "bm_read":
                continue

            entries_start = time.perf_counter()
            entries = matrix.entries(keyed=False)
            filtered_entries = entries.filter((entries.i < entries.j) & (entries.entry != 0))
            n_entries = filtered_entries.count()
            timed(metrics, f"hail_entries_count_{study_id}", entries_start, ancestry=ancestry, nonzero_upper_triangle_entries=n_entries)
            print(json.dumps(metrics[-1], sort_keys=True))
            if args.stop_after == "entries_count":
                continue

            spark_start = time.perf_counter()
            spark_entries = filtered_entries.to_spark().select("i", "j", "entry")
            spark_count = spark_entries.count()
            timed(metrics, f"hail_to_spark_{study_id}", spark_start, spark_rows=spark_count)
            print(json.dumps(metrics[-1], sort_keys=True))
            spark_outputs.append(spark_entries)
            if args.stop_after == "spark_count":
                continue

        if args.stop_after == "write":
            if not spark_outputs:
                raise ValueError("No Spark LD output was produced for the selected locus set")
            write_start = time.perf_counter()
            combined_output = spark_outputs[0]
            for spark_output in spark_outputs[1:]:
                combined_output = combined_output.unionByName(spark_output)
            combined_output.write.mode("overwrite").parquet(str(args.output))
            timed(
                metrics,
                "write_combined_locus_set",
                write_start,
                output=str(args.output),
                input_study_loci=len(spark_outputs),
            )
            print(json.dumps(metrics[-1], sort_keys=True))
    finally:
        session.spark.stop()

    print(json.dumps({"metrics": metrics}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
