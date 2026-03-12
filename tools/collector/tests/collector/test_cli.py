"""Test collector command line interface (CLI)"""

from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from collector import app

runner = CliRunner()


@pytest.fixture
def input_data(tmp_path: Path) -> str:
    """Setup test input data and return the path to the Parquet files."""
    test_input_dir = tmp_path / "input"
    test_input_dir.mkdir()

    con = duckdb.connect()
    try:
        # Build 2 Parquet files with the same data to test the CLI
        con.execute("CREATE TABLE test_df AS SELECT * FROM (VALUES ('A', 'B'), ('C', 'D')) AS t(a, b)")
        test_paths = [(test_input_dir / f"part_{i:04d}.parquet").as_posix() for i in range(2)]
        [con.execute(f"COPY test_df TO '{path}' (FORMAT 'parquet')") for path in test_paths]

    except Exception as e:
        print(f"Error setting up test data: {e}")
    finally:
        con.close()

    return test_input_dir.as_posix()


def test_app(input_data: str):
    input_dir = f"{input_data}"
    test_output_file = (Path(input_data).parent / "output.parquet").as_posix()
    result = runner.invoke(app, ["--input", input_dir, "--output", test_output_file])
    assert result.exit_code == 0
    assert Path(test_output_file).exists()


def test_app_invalid_input():
    result = runner.invoke(app, ["--input", "non_existent_dir/*.parquet", "--output", "output.parquet"])
    assert result.exit_code != 0


def test_app_invalid_output(input_data: str):
    input_dir = f"{input_data}"
    result = runner.invoke(app, ["--input", input_dir, "--output", "output.txt"])
    assert result.exit_code != 0
