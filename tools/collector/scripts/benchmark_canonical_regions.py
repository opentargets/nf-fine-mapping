"""Benchmark canonical-region collection and verify pre/post logical equivalence.

The legacy reader below is intentionally kept in this benchmark as the
pre-optimization reference.  The benchmark runs the complete public command
twice against summary statistics from ``testdata``: once with the legacy
per-locus reader and once with the optimized set-based reader.
"""

from __future__ import annotations

import argparse
import csv
import json
import tempfile
import time
from pathlib import Path

import duckdb

import collector.canonical_regions as canonical_regions
from collector.canonical_regions import (
    CanonicalRegionInput,
    CollectCanonicalRegionsConfig,
    SourceLocus,
    _chromosome_sort_key,
    _deduplicated_sumstats_sql,
    _managed_duckdb,
    _quote_sql_string,
    run_collect_canonical_regions,
)


def _legacy_read_source_loci(prepared_inputs: tuple[CanonicalRegionInput, ...], min_maf: float) -> list[SourceLocus]:
    """Reference implementation from before the set-based lead query."""
    loci: list[SourceLocus] = []
    with _managed_duckdb() as con:
        for prepared_input in prepared_inputs:
            locus_rows = con.execute(
                f"""
                SELECT CAST(studyLocusId AS VARCHAR), CAST(chromosome AS VARCHAR),
                       CAST(locusStart AS INTEGER), CAST(locusEnd AS INTEGER)
                FROM {canonical_regions._read_parquet_sql(prepared_input.locus_breaker_path)}
                ORDER BY
                    CASE WHEN try_cast(chromosome AS INTEGER) IS NULL THEN 1 ELSE 0 END,
                    try_cast(chromosome AS INTEGER), chromosome, locusStart, locusEnd, studyLocusId
                """
            ).fetchall()
            for study_locus_id, chromosome, locus_start, locus_end in locus_rows:
                lead_row = con.execute(
                    f"""
                    SELECT position
                    FROM {_deduplicated_sumstats_sql(prepared_input.summary_statistics_path)}
                    WHERE chromosome = {_quote_sql_string(chromosome)}
                      AND position BETWEEN {locus_start} AND {locus_end}
                      AND least(CAST(effectAlleleFrequencyFromSource AS DOUBLE), 1.0 - CAST(effectAlleleFrequencyFromSource AS DOUBLE)) > {min_maf}
                    ORDER BY pValueExponent, pValueMantissa, position, variantId
                    LIMIT 1
                    """
                ).fetchone()
                if lead_row is not None:
                    loci.append(
                        SourceLocus(
                            study_id=prepared_input.study_id,
                            study_locus_id=study_locus_id,
                            ancestry=prepared_input.ancestry,
                            chromosome=chromosome,
                            locus_start=locus_start,
                            locus_end=locus_end,
                            lead_position=int(lead_row[0]),
                        )
                    )
    return sorted(
        loci,
        key=lambda locus: (_chromosome_sort_key(locus.chromosome), locus.locus_start, locus.locus_end, locus.source_key),
    )


def _make_locus_breaker_inputs(summary_paths: list[Path], output_dir: Path, loci_per_study: int) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = []
    for index, summary_path in enumerate(summary_paths):
        output_path = output_dir / f"study_{index}.locus.parquet"
        with duckdb.connect() as con:
            con.execute(
                f"""
                COPY (
                    WITH ranked AS (
                        SELECT *, row_number() OVER (ORDER BY position, variantId) AS row_number
                        FROM read_parquet({_quote_sql_string(summary_path.as_posix())})
                    )
                    SELECT
                        'locus-' || row_number::VARCHAR AS studyLocusId,
                        studyId,
                        chromosome,
                        position AS locusStart,
                        position + 1000 AS locusEnd
                    FROM ranked
                    WHERE row_number % greatest(1, CAST(floor((SELECT max(row_number) FROM ranked) / {loci_per_study}) AS BIGINT)) = 1
                    LIMIT {loci_per_study}
                ) TO {_quote_sql_string(output_path.as_posix())} (FORMAT PARQUET)
                """
            )
        output_paths.append(output_path)
    return output_paths


def _logical_rows(path: Path) -> tuple[list[tuple[object, ...]], list[str]]:
    pattern = f"{path.as_posix()}/**/*.parquet" if path.is_dir() else path.as_posix()
    with duckdb.connect() as con:
        columns = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet({_quote_sql_string(pattern)})").fetchall()]
        rows = con.execute(f"SELECT * FROM read_parquet({_quote_sql_string(pattern)})").fetchall()
    return sorted(rows, key=repr), columns


def _run(config: CollectCanonicalRegionsConfig, reader) -> tuple[float, dict[str, list[tuple[object, ...]]]]:
    original_reader = canonical_regions._read_source_loci
    canonical_regions._read_source_loci = reader
    try:
        started = time.perf_counter()
        run_collect_canonical_regions(config)
        elapsed = time.perf_counter() - started
    finally:
        canonical_regions._read_source_loci = original_reader
    outputs = {"stats": _logical_rows(config.stats_parquet_output)[0]}
    outputs.update({path.name: _logical_rows(path)[0] for path in config.fine_mapping_locus_set_output_dir.glob("*.parquet")})
    return elapsed, outputs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--loci-per-study", type=int, default=60)
    args = parser.parse_args()

    sumstats_root = args.repo_root / "testdata" / "sumstats"
    with (args.repo_root / "testdata" / "manifest.full.tsv").open(newline="") as manifest_file:
        manifest_rows = list(csv.DictReader(manifest_file, delimiter="\t"))
    runs: dict[str, list[dict[str, str]]] = {}
    for row in manifest_rows:
        runs.setdefault(row["runId"], []).append(row)
    if len(runs) != 4 or any(len(rows) != 3 for rows in runs.values()):
        raise SystemExit("expected four three-study runs in testdata/manifest.full.tsv")

    with tempfile.TemporaryDirectory(prefix="canonical-benchmark-") as temporary_dir:
        root = Path(temporary_dir)
        case_results = []
        for case_index, (run_id, rows) in enumerate(runs.items(), start=1):
            summary_paths = [next((sumstats_root / row["studyId"]).glob("*.parquet")) for row in rows]
            ancestries = tuple(row["majorAncestry"] for row in rows)
            input_dir = root / f"case_{case_index}" / "inputs"
            locus_paths = _make_locus_breaker_inputs(summary_paths, input_dir, args.loci_per_study)

            def config(
                label: str,
                *,
                case_run_id: str = run_id,
                case_locus_paths: list[Path] = locus_paths,
                case_ancestries: tuple[str, ...] = ancestries,
                case_summary_paths: list[Path] = summary_paths,
                case_number: int = case_index,
            ) -> CollectCanonicalRegionsConfig:
                return CollectCanonicalRegionsConfig(
                    run_id=case_run_id,
                    locus_breaker_paths=tuple(case_locus_paths),
                    ancestries=case_ancestries,
                    summary_statistics_paths=tuple(case_summary_paths),
                    fine_mapping_locus_set_output_dir=root / f"case_{case_number}" / label / "locus_sets",
                    stats_parquet_output=root / f"case_{case_number}" / label / "stats.parquet",
                    stats_json_output=root / f"case_{case_number}" / label / "stats.json",
                )

            baseline_seconds, baseline_outputs = _run(config("baseline"), _legacy_read_source_loci)
            optimized_seconds, optimized_outputs = _run(config("optimized"), canonical_regions._read_source_loci)
            if baseline_outputs != optimized_outputs:
                raise SystemExit(f"FAIL: logical outputs differ for {run_id}")

            baseline_stats = json.loads(config("baseline").stats_json_output.read_text())
            optimized_stats = json.loads(config("optimized").stats_json_output.read_text())
            baseline_stats.pop("timingsSeconds", None)
            optimized_stats.pop("timingsSeconds", None)
            if baseline_stats != optimized_stats:
                raise SystemExit(f"FAIL: stats JSON differs apart from timingsSeconds for {run_id}")
            case_results.append(
                {
                    "runId": run_id,
                    "studies": [row["studyId"] for row in rows],
                    "baselineSeconds": round(baseline_seconds, 3),
                    "optimizedSeconds": round(optimized_seconds, 3),
                    "speedup": round(baseline_seconds / optimized_seconds, 2),
                    "logicalOutputsEqual": True,
                    "statsEqualApartFromTimings": True,
                    "nOutputFiles": len(optimized_outputs) - 1,
                }
            )

        baseline_total = sum(result["baselineSeconds"] for result in case_results)
        optimized_total = sum(result["optimizedSeconds"] for result in case_results)
        print(
            json.dumps(
                {
                    "cases": case_results,
                    "lociPerStudy": args.loci_per_study,
                    "baselineSecondsTotal": round(baseline_total, 3),
                    "optimizedSecondsTotal": round(optimized_total, 3),
                    "speedupTotal": round(baseline_total / optimized_total, 2),
                    "allCasesPassed": True,
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
