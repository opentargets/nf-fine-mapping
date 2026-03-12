"""Tool for collecting and merging parquet files into a single output file."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer()


@app.command()
def cli(
    input: Annotated[Path, typer.Option(help="Input directory containing parquet files")],
    output: Annotated[Path, typer.Option(help="Path to the output parquet file")],
):
    assert input.exists(), "Input directory is required"
    assert output.parent.exists(), "Output directory is required"
    assert input.is_dir(), "Input should be a directory"
    assert output.suffix == ".parquet", "Output file should have a .parquet extension"

    import duckdb

    con = duckdb.connect()
    con.execute(f"COPY (SELECT * FROM parquet_scan('{input}/*.parquet')) TO '{output}' (FORMAT PARQUET)")
