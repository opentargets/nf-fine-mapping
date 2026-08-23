"""Tool for collecting and merging parquet files into a single output file."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer
from pydantic import ValidationError

from collector.canonical_regions import CollectCanonicalRegionsConfig, run_collect_canonical_regions
from collector.collect_loci import CollectFineMappingLociConfig, run_collect_finemapping_loci
from collector.empty_status import ValidationStage, emit_empty_status
from collector.hailing_ld import HailingLdConfig, HailingLdReference, run_hailing_ld
from collector.ld_pair_stats import emit_empty_ld_pair_status
from collector.ld_parity import LdParityConfig, compare_ld_outputs
from collector.locus_breaker import LocusBreakerConfig, run_locus_breaker
from collector.split_loci import SplitFineMappingLociConfig, run_split_finemapping_loci

app = typer.Typer()


@app.command(name="ld_parity")
def ld_parity(
    hailing: Annotated[Path, typer.Option("--hailing", help="Hailing Ducks MultiAncestryPairwiseLD Parquet dataset.")],
    gentropy: Annotated[Path, typer.Option("--gentropy", help="Gentropy MultiAncestryPairwiseLD Parquet dataset.")],
    report: Annotated[Path, typer.Option("--report", help="Output JSON parity report.")],
    hailing_stats: Annotated[Path | None, typer.Option("--hailing_stats", help="Optional Hailing Ducks stats JSONL.")] = None,
    gentropy_stats: Annotated[Path | None, typer.Option("--gentropy_stats", help="Optional Gentropy stats JSONL.")] = None,
    tolerance: Annotated[float, typer.Option("--tolerance", min=0, help="Maximum accepted absolute LD difference.")] = 1e-8,
):
    """Compare LD outputs independent of row order and chr contig prefixes."""
    try:
        parity = compare_ld_outputs(
            LdParityConfig(
                hailing_path=hailing,
                gentropy_path=gentropy,
                report_path=report,
                hailing_stats_path=hailing_stats,
                gentropy_stats_path=gentropy_stats,
                tolerance=tolerance,
            )
        )
    except (FileNotFoundError, OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    typer.echo(f"Wrote LD parity report to {report}")
    if parity["totals"]["shared_value_mismatches"]:
        raise typer.Exit(code=1)


@app.command(name="hailing_ld")
def hailing_ld(
    input: Annotated[Path, typer.Option("--input", help="FineMappingLocusSet Parquet file or directory dataset.")],
    study_metadata: Annotated[Path, typer.Option("--study_metadata", help="Study metadata JSONL with studyId and ancestry.")],
    output: Annotated[Path, typer.Option("--output", help="Flat MultiAncestryPairwiseLD output Parquet file.")],
    stats_output: Annotated[Path, typer.Option("--stats_output", help="Per-ancestry LD pair statistics JSONL file.")],
    ancestry: Annotated[list[str], typer.Option("--ancestry", help="Ancestry labels, in registry order.")],
    ht_path: Annotated[list[str], typer.Option("--ht_path", help="Native hg38 HailTable paths, in ancestry order.")],
    bm_path: Annotated[list[str], typer.Option("--bm_path", help="Native BlockMatrix paths, in ancestry order.")],
    native_contig_prefix: Annotated[str, typer.Option("--native_contig_prefix", help="Prefix used by the native HT contigs.")] = "chr",
    max_cached_blocks: Annotated[int, typer.Option("--max_cached_blocks", min=1)] = 8,
):
    """Query native Hail references and adapt them to MultiAncestryPairwiseLD."""
    if not (len(ancestry) == len(ht_path) == len(bm_path)):
        raise typer.BadParameter("ancestry, ht_path, and bm_path must have identical lengths")
    try:
        run_hailing_ld(
            HailingLdConfig(
                input_path=input,
                study_metadata_path=study_metadata,
                output_path=output,
                stats_output=stats_output,
                references=tuple(
                    HailingLdReference(ancestry=label, ht_path=ht, bm_path=bm) for label, ht, bm in zip(ancestry, ht_path, bm_path, strict=True)
                ),
                native_contig_prefix=native_contig_prefix,
                max_cached_blocks=max_cached_blocks,
            )
        )
    except (FileNotFoundError, IsADirectoryError, OSError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error


@app.command()
def collect(
    input: Annotated[Path, typer.Option(help="Input directory containing parquet files")],
    output: Annotated[Path, typer.Option(help="Path to the output parquet file")],
):
    if not input.exists():
        raise AssertionError("Input directory is required")
    if not output.parent.exists():
        raise AssertionError("Output directory is required")
    if not input.is_dir():
        raise AssertionError("Input should be a directory")
    if output.suffix != ".parquet":
        raise AssertionError("Output file should have a .parquet extension")

    import duckdb

    with duckdb.connect() as con:
        con.execute(f"COPY (SELECT * FROM parquet_scan('{input}/*.parquet')) TO '{output}' (FORMAT PARQUET)")


@app.command()
def intersection(
    input: Annotated[list[Path], typer.Option(help="Input summary statistics. Can be multiple parquet files.")],
    output: Annotated[Path, typer.Option(help="Path to the output parquet file")],
):
    for path in input:
        if not path.exists():
            raise AssertionError(f"Input file {path} does not exist")
        if not path.is_file():
            raise AssertionError(f"Input {path} should be a file")
        if path.suffix != ".parquet":
            raise AssertionError(f"Input file {path} should have a .parquet extension")
    if not output.parent.exists():
        raise AssertionError("Output directory is required")
    if output.suffix != ".parquet":
        raise AssertionError("Output file should have a .parquet extension")

    import duckdb

    with duckdb.connect() as con:
        files = ",".join(f"'{path.as_posix()}'" for path in input)
        con.execute(
            f"""
            COPY (
                SELECT chrom, pos, ref, alt
                FROM read_parquet([{files}])
                GROUP BY chrom, pos, ref, alt
                HAVING COUNT(*) = {len(input)}
            ) TO '{output}' (FORMAT PARQUET)
            """
        )


@app.command()
def transform(
    input: Annotated[Path, typer.Option(help="Input parquet file")],
    output: Annotated[Path, typer.Option(help="Path to the output parquet file")],
):
    if not input.exists():
        raise AssertionError("Input file is required")
    if not output.parent.exists():
        raise AssertionError("Output directory is required")
    if not input.is_file():
        raise AssertionError("Input should be a file")
    if input.suffix != ".parquet":
        raise AssertionError("Input file should have a .parquet extension")

    import duckdb

    with duckdb.connect() as con:
        con.execute(
            rf"""
            COPY (
                SELECT chromosome, variantId, position,
                regexp_extract(variantId, '^.*_\d+_(\w+)_\w+$', 1) AS referenceAllele,
                regexp_extract(variantId, '^.*_\d+_\w+_(\w+)$', 1) AS alternateAllele,
                (beta / standardError) AS zScore
                FROM read_parquet('{input}')
            ) TO '{output}' (FORMAT csv, DELIMITER '\t', HEADER true, COMPRESSION gzip)
            """
        )


@app.command(name="empty_status")
def empty_status(
    run_id: Annotated[str, typer.Option("--run_id", help="Pipeline run identifier for the status record.")],
    path: Annotated[Path, typer.Option("--path", help="Input parquet file or partitioned parquet directory.")],
    validation_stage: Annotated[
        ValidationStage,
        typer.Option("--validation_stage", help="Validation stage producing the empty-dataset status."),
    ],
    logical_path: Annotated[
        str | None,
        typer.Option("--logical_path", help="Logical dataset path to emit in the status record instead of the staged path."),
    ] = None,
):
    """Emit a JSONL status record when the input parquet dataset is logically empty."""
    try:
        status = emit_empty_status(run_id=run_id, path=path, logical_path=logical_path, validation_stage=validation_stage)
    except (FileNotFoundError, RuntimeError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    if status is not None:
        typer.echo(status)


@app.command(name="check_ld_pair_stats")
def check_ld_pair_stats(
    run_id: Annotated[str, typer.Option("--run_id", help="Pipeline run identifier for the status record.")],
    fine_mapping_locus_set_id: Annotated[
        str, typer.Option("--fine_mapping_locus_set_id", help="Fine-mapping locus-set identifier for the status record.")
    ],
    path: Annotated[Path, typer.Option("--path", help="Gentropy LD pair statistics JSONL path.")],
):
    """Emit one status record when any ancestry has zero LD pairs."""
    try:
        status = emit_empty_ld_pair_status(
            run_id=run_id,
            fine_mapping_locus_set_id=fine_mapping_locus_set_id,
            path=path,
        )
    except (FileNotFoundError, OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error
    if status is not None:
        typer.echo(status)


@app.command(name="locus_breaker")
def locus_breaker(
    input: Annotated[Path, typer.Option("--input", help="Input flat summary-statistics Parquet file or directory dataset.")],
    output: Annotated[Path, typer.Option("--output", help="Path to the output flat study-locus Parquet file.")],
    lbc_baseline_pvalue: Annotated[float, typer.Option("--lbc_baseline_pvalue", help="Baseline p-value for locus-breaker clumping.")] = 1.0e-5,
    lbc_distance_cutoff: Annotated[int, typer.Option("--lbc_distance_cutoff", help="Distance cutoff for locus-breaker clumping.")] = 250_000,
    lbc_pvalue_threshold: Annotated[
        float, typer.Option("--lbc_pvalue_threshold", help="Lead p-value threshold for locus-breaker clumping.")
    ] = 1.0e-8,
    lbc_flanking_distance: Annotated[int, typer.Option("--lbc_flanking_distance", help="Flanking distance for locus-breaker loci.")] = 100_000,
    large_loci_size: Annotated[int, typer.Option("--large_loci_size", help="Locus size threshold for WBC replacement.")] = 1_500_000,
    wbc_clump_distance: Annotated[int, typer.Option("--wbc_clump_distance", help="Clump distance for window-based clumping.")] = 500_000,
    wbc_pvalue_threshold: Annotated[float, typer.Option("--wbc_pvalue_threshold", help="P-value threshold for window-based clumping.")] = 1.0e-5,
    collect_locus: Annotated[
        bool,
        typer.Option("--collect_locus/--no_collect_locus", help="Collect summary-statistics records inside final locus boundaries."),
    ] = True,
    remove_mhc: Annotated[bool, typer.Option("--remove_mhc/--no_remove_mhc", help="Remove loci overlapping the MHC region.")] = True,
):
    """Generate a flat study-locus Parquet file with LocusBreaker-compatible schema."""
    config = LocusBreakerConfig(
        lbc_baseline_pvalue=lbc_baseline_pvalue,
        lbc_distance_cutoff=lbc_distance_cutoff,
        lbc_pvalue_threshold=lbc_pvalue_threshold,
        lbc_flanking_distance=lbc_flanking_distance,
        large_loci_size=large_loci_size,
        wbc_clump_distance=wbc_clump_distance,
        wbc_pvalue_threshold=wbc_pvalue_threshold,
        collect_locus=collect_locus,
        remove_mhc=remove_mhc,
    )
    run_locus_breaker(input, output, config)


@app.command(name="collect_finemapping_loci")
def collect_finemapping_loci(
    input: Annotated[
        list[Path],
        typer.Option("--input", help="Input flat StudyLocus Parquet file or directory dataset. Can be provided multiple times."),
    ],
    full_output: Annotated[Path, typer.Option("--full_output", help="Optional full-overlap output parquet path.")],
    partial_output: Annotated[Path, typer.Option("--partial_output", help="Required partial-overlap output parquet path.")],
    non_overlap_output: Annotated[Path, typer.Option("--non_overlap_output", help="Required non-overlap output parquet path.")],
    stats_output: Annotated[Path, typer.Option("--stats_output", help="Required JSON statistics output path.")],
):
    """Collect StudyLocus datasets into fine-mapping overlap classes."""
    config = CollectFineMappingLociConfig(
        input_paths=tuple(input),
        full_output=full_output,
        partial_output=partial_output,
        non_overlap_output=non_overlap_output,
        stats_output=stats_output,
    )
    try:
        run_collect_finemapping_loci(config)
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error


@app.command(name="collect_canonical_regions")
def collect_canonical_regions(
    run_id: Annotated[str, typer.Option("--run_id", help="Pipeline run identifier for canonical-region collection.")],
    locus_breaker: Annotated[
        list[Path],
        typer.Option(
            "--locus_breaker",
            help="Input LocusBreaker Parquet file or directory dataset. Can be provided multiple times.",
        ),
    ],
    ancestry: Annotated[list[str], typer.Option("--ancestry", help="Aligned ancestry label for each locus-breaker result.")],
    summary_statistics: Annotated[
        list[Path],
        typer.Option("--summary_statistics", help="Aligned original summary-statistics parquet file or directory dataset."),
    ],
    fine_mapping_locus_set_output_dir: Annotated[
        Path,
        typer.Option("--fine_mapping_locus_set_output_dir", help="Output directory for per-locus-set Parquet files."),
    ],
    stats_parquet_output: Annotated[
        Path,
        typer.Option("--stats_parquet_output", help="Candidate-level canonical-region statistics Parquet output path."),
    ],
    stats_json_output: Annotated[
        Path,
        typer.Option("--stats_json_output", help="Run-level canonical-region statistics JSON output path."),
    ],
    max_region_span_bp: Annotated[
        int, typer.Option("--max_region_span_bp", min=1, help="Inclusive maximum span for one merged canonical region in base pairs.")
    ] = 3_000_000,
):
    """Validate and normalize canonical-region collector inputs."""
    try:
        run_collect_canonical_regions(
            CollectCanonicalRegionsConfig(
                run_id=run_id,
                locus_breaker_paths=tuple(locus_breaker),
                ancestries=tuple(ancestry),
                summary_statistics_paths=tuple(summary_statistics),
                fine_mapping_locus_set_output_dir=fine_mapping_locus_set_output_dir,
                stats_parquet_output=stats_parquet_output,
                stats_json_output=stats_json_output,
                max_region_span_bp=max_region_span_bp,
            )
        )
    except (FileNotFoundError, ValidationError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error


@app.command(name="split_finemapping_loci")
def split_finemapping_loci(
    input: Annotated[Path, typer.Option("--input", help="Input full-overlap Parquet file or directory dataset.")],
    output: Annotated[Path, typer.Option("--output", help="Output directory for one Parquet file per locus set.")],
):
    """Split full-overlap loci into one Parquet file per fine-mapping locus set."""
    try:
        run_split_finemapping_loci(SplitFineMappingLociConfig(input_path=input, output_dir=output))
    except (FileNotFoundError, OSError, ValueError) as error:
        raise typer.BadParameter(str(error)) from error


@app.command()
def clumping_report(
    _input: Annotated[list[Path], typer.Argument(help="Input locus parquet files. Can be multiple parquet files.")],
    _output: Annotated[Path, typer.Option(help="Path to the output parquet file")],
):

    class LocusReportFields(StrEnum):
        STUDY_ID = "studyId"
        N_LOCUS = "nLocus"
        MEAN_LOCUS_SIZE = "meanLocusSize"
        MAX_LOCUS_SIZE = "maxLocusSize"
        MIN_LOCUS_SIZE = "minLocusSize"

    class StudyLocusFields(StrEnum):
        LOCUS = "locus"

    paths = [path.as_posix() for path in _input]

    import duckdb
    import polars as pl

    with duckdb.connect() as con:
        results = pl.DataFrame(
            {
                LocusReportFields.STUDY_ID.value: [],
                LocusReportFields.N_LOCUS.value: [],
                LocusReportFields.MEAN_LOCUS_SIZE.value: [],
                LocusReportFields.MAX_LOCUS_SIZE.value: [],
                LocusReportFields.MIN_LOCUS_SIZE.value: [],
            },
            schema={
                LocusReportFields.STUDY_ID.value: pl.Utf8,
                LocusReportFields.N_LOCUS.value: pl.Int64,
                LocusReportFields.MEAN_LOCUS_SIZE.value: pl.Float64,
                LocusReportFields.MAX_LOCUS_SIZE.value: pl.Int64,
                LocusReportFields.MIN_LOCUS_SIZE.value: pl.Int64,
            },
        )
        for path in paths:
            result = con.execute(
                f"""
                SELECT
                    {LocusReportFields.STUDY_ID.value},
                    count(*) as {LocusReportFields.N_LOCUS.value},
                    mean(len({StudyLocusFields.LOCUS.value})) as {LocusReportFields.MEAN_LOCUS_SIZE.value},
                    max(len({StudyLocusFields.LOCUS.value})) as {LocusReportFields.MAX_LOCUS_SIZE.value},
                    min(len({StudyLocusFields.LOCUS.value})) as {LocusReportFields.MIN_LOCUS_SIZE.value}
                FROM read_parquet('{path}')
                GROUP BY {LocusReportFields.STUDY_ID.value}
                """
            ).pl()
            results = pl.concat([results, result])
        results.write_parquet(_output.as_posix())
