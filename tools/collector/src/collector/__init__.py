"""Tool for collecting and merging parquet files into a single output file."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from collector.locus_breaker import LocusBreakerConfig, run_locus_breaker

app = typer.Typer()


@app.command()
def collect(
    input: Annotated[Path, typer.Option(help="Input directory containing parquet files")],
    output: Annotated[Path, typer.Option(help="Path to the output parquet file")],
):
    assert input.exists(), "Input directory is required"
    assert output.parent.exists(), "Output directory is required"
    assert input.is_dir(), "Input should be a directory"
    assert output.suffix == ".parquet", "Output file should have a .parquet extension"

    import duckdb

    with duckdb.connect() as con:
        con.execute(f"COPY (SELECT * FROM parquet_scan('{input}/*.parquet')) TO '{output}' (FORMAT PARQUET)")


@app.command()
def intersection(
    input: Annotated[list[Path], typer.Option(help="Input summary statistics. Can be multiple parquet files.")],
    output: Annotated[Path, typer.Option(help="Path to the output parquet file")],
):
    for path in input:
        assert path.exists(), f"Input file {path} does not exist"
        assert path.is_file(), f"Input {path} should be a file"
        assert path.suffix == ".parquet", f"Input file {path} should have a .parquet extension"
        assert output.parent.exists(), "Output directory is required"
        assert output.suffix == ".parquet", "Output file should have a .parquet extension"

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
    assert input.exists(), "Input file is required"
    assert output.parent.exists(), "Output directory is required"
    assert input.is_file(), "Input should be a file"
    assert input.suffix == ".parquet", "Input file should have a .parquet extension"

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
