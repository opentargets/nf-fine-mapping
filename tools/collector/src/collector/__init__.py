"""Tool for collecting and merging parquet files into a single output file."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

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
        _files = ",".join(f"'{path.as_posix()}'" for path in input)
        con.execute(
            f"""
            COPY (
                SELECT chrom, pos, ref, alt
                FROM read_parquet([{_files}])
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
