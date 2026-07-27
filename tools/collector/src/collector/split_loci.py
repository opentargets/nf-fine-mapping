"""Split collected fine-mapping loci into one Parquet file per locus set."""

from __future__ import annotations

from pathlib import Path

import duckdb
from pydantic import BaseModel, ConfigDict, Field


class SplitFineMappingLociConfig(BaseModel):
    """Input and output contract for the fine-mapping locus-set splitter."""

    model_config = ConfigDict(frozen=True)

    input_path: Path
    output_dir: Path
    parquet_glob: str = Field(default="**/*.parquet", min_length=1)


def _quote_sql_string(value: str) -> str:
    """Return a single-quoted DuckDB SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _parquet_source(path: Path, parquet_glob: str) -> str:
    """Return a DuckDB read_parquet source for a file or directory dataset."""
    source = path.as_posix() if path.is_file() else (path / parquet_glob).as_posix()
    return f"read_parquet({_quote_sql_string(source)}, union_by_name = true, hive_partitioning = true)"


def run_split_finemapping_loci(config: SplitFineMappingLociConfig) -> tuple[Path, ...]:
    """Write one flat Parquet file for every distinct non-null locus-set ID."""
    if not config.input_path.exists():
        raise FileNotFoundError(f"Input path does not exist: {config.input_path}")
    if config.input_path.is_file() and config.input_path.suffix != ".parquet":
        raise ValueError("Input file should have a .parquet extension")
    if not config.input_path.is_file() and not config.input_path.is_dir():
        raise ValueError("Input path should be a file or directory")

    source = _parquet_source(config.input_path, config.parquet_glob)
    config.output_dir.mkdir(parents=True, exist_ok=True)
    outputs: list[Path] = []

    with duckdb.connect() as con:
        ids = con.execute(
            f"""
            SELECT DISTINCT fineMappingLocusSetId
            FROM {source}
            WHERE fineMappingLocusSetId IS NOT NULL
            ORDER BY fineMappingLocusSetId
            """
        ).fetchall()
        if not ids:
            raise ValueError("Input dataset contains no non-null fineMappingLocusSetId values")

        for (locus_set_id,) in ids:
            output_path = config.output_dir / f"{locus_set_id}.parquet"
            con.execute(
                f"""
                COPY (
                    SELECT *
                    FROM {source}
                    WHERE fineMappingLocusSetId = {_quote_sql_string(locus_set_id)}
                    ORDER BY studyId, studyLocusId
                ) TO {_quote_sql_string(output_path.as_posix())} (FORMAT PARQUET)
                """
            )
            outputs.append(output_path)

    return tuple(outputs)
