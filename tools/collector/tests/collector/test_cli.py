"""Test collector command line interface (CLI)"""

import gzip
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from collector import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def collect_input(tmp_path: Path) -> Path:
    """Directory with 2 parquet files containing columns (a, b)."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE t AS SELECT * FROM (VALUES ('A', 'B'), ('C', 'D')) AS t(a, b)")
        for i in range(2):
            con.execute(f"COPY t TO '{input_dir / f'part_{i:04d}.parquet'}' (FORMAT 'parquet')")
    finally:
        con.close()

    return input_dir


@pytest.fixture
def intersection_inputs(tmp_path: Path) -> list[Path]:
    """Two parquet files with columns (chrom, pos, ref, alt). One variant is shared."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    shared = "VALUES ('chr1', 1000, 'A', 'G'), ('chr2', 2000, 'C', 'T')"
    only_in_first = "VALUES ('chr3', 3000, 'G', 'A')"
    only_in_second = "VALUES ('chr4', 4000, 'T', 'C')"

    con = duckdb.connect()
    try:
        file1 = input_dir / "file1.parquet"
        file2 = input_dir / "file2.parquet"
        con.execute(
            f"COPY (SELECT * FROM ({shared}) AS t(chrom, pos, ref, alt) "
            f"UNION ALL SELECT * FROM ({only_in_first}) AS t(chrom, pos, ref, alt)) "
            f"TO '{file1}' (FORMAT 'parquet')"
        )
        con.execute(
            f"COPY (SELECT * FROM ({shared}) AS t(chrom, pos, ref, alt) "
            f"UNION ALL SELECT * FROM ({only_in_second}) AS t(chrom, pos, ref, alt)) "
            f"TO '{file2}' (FORMAT 'parquet')"
        )
    finally:
        con.close()

    return [file1, file2]


@pytest.fixture
def transform_input(tmp_path: Path) -> Path:
    """Parquet file with columns (chromosome, variantId, position, beta, standardError)."""
    path = tmp_path / "sumstats.parquet"

    con = duckdb.connect()
    try:
        con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    ('chr1', 'chr1_1000_A_G', 1000, 0.5, 0.1),
                    ('chr2', 'chr2_2000_C_T', 2000, -0.3, 0.05)
                ) AS t(chromosome, variantId, position, beta, standardError)
            ) TO '{path}' (FORMAT 'parquet')
            """
        )
    finally:
        con.close()

    return path


# ---------------------------------------------------------------------------
# cli (collect) command tests
# ---------------------------------------------------------------------------


def test_cli_merges_parquets(collect_input: Path, tmp_path: Path):
    output = tmp_path / "output.parquet"
    result = runner.invoke(app, ["collect", "--input", str(collect_input), "--output", str(output)])
    assert result.exit_code == 0, result.output
    assert output.exists()

    con = duckdb.connect()
    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output}')").fetchone()
    if count is None:
        assert False, "Output parquet file is empty or malformed"
    con.close()
    # 2 files × 2 rows each = 4 rows
    assert count[0] == 4


def test_cli_invalid_input_dir():
    result = runner.invoke(app, ["collect", "--input", "/nonexistent/dir", "--output", "output.parquet"])
    assert result.exit_code != 0


def test_cli_invalid_output_extension(collect_input: Path, tmp_path: Path):
    result = runner.invoke(app, ["collect", "--input", str(collect_input), "--output", str(tmp_path / "output.txt")])
    assert result.exit_code != 0


def test_cli_output_dir_must_exist(collect_input: Path):
    result = runner.invoke(app, ["collect", "--input", str(collect_input), "--output", "/nonexistent/output.parquet"])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# intersection command tests
# ---------------------------------------------------------------------------


def test_intersection_returns_shared_variants(intersection_inputs: list[Path], tmp_path: Path):
    output = tmp_path / "intersection.parquet"
    args = ["intersection"] + [arg for p in intersection_inputs for arg in ("--input", str(p))] + ["--output", str(output)]
    result = runner.invoke(app, args)
    assert result.exit_code == 0, result.output
    assert output.exists()

    con = duckdb.connect()
    rows = con.execute(f"SELECT chrom, pos FROM read_parquet('{output}') ORDER BY pos").fetchall()
    con.close()
    # Only the 2 shared variants should appear
    assert rows == [("chr1", 1000), ("chr2", 2000)]


def test_intersection_excludes_non_shared_variants(intersection_inputs: list[Path], tmp_path: Path):
    output = tmp_path / "intersection.parquet"
    args = ["intersection"] + [arg for p in intersection_inputs for arg in ("--input", str(p))] + ["--output", str(output)]
    runner.invoke(app, args)

    con = duckdb.connect()
    chroms = {r[0] for r in con.execute(f"SELECT chrom FROM read_parquet('{output}')").fetchall()}
    con.close()
    assert "chr3" not in chroms
    assert "chr4" not in chroms


def test_intersection_single_file(intersection_inputs: list[Path], tmp_path: Path):
    """With a single input file, all rows should appear in the output."""
    output = tmp_path / "intersection.parquet"
    result = runner.invoke(app, ["intersection", "--input", str(intersection_inputs[0]), "--output", str(output)])
    assert result.exit_code == 0, result.output

    con = duckdb.connect()
    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output}')").fetchone()
    if count is None:
        assert False, "Output parquet file is empty or malformed"
    con.close()
    assert count[0] == 3  # 2 shared + 1 only_in_first


def test_intersection_invalid_input_file(tmp_path: Path):
    result = runner.invoke(app, ["intersection", "--input", "/nonexistent/file.parquet", "--output", str(tmp_path / "out.parquet")])
    assert result.exit_code != 0


def test_intersection_invalid_input_extension(intersection_inputs: list[Path], tmp_path: Path):
    txt_file = tmp_path / "file.txt"
    txt_file.write_text("not parquet")
    result = runner.invoke(app, ["intersection", "--input", str(txt_file), "--output", str(tmp_path / "out.parquet")])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# transform command tests
# ---------------------------------------------------------------------------


def test_transform_produces_gzip_tsv(transform_input: Path, tmp_path: Path):
    output = tmp_path / "output.tsv.gz"
    result = runner.invoke(app, ["transform", "--input", str(transform_input), "--output", str(output)])
    assert result.exit_code == 0, result.output
    assert output.exists()

    with gzip.open(output, "rt") as f:
        header = f.readline().strip().split("\t")

    assert header == ["chromosome", "variantId", "position", "referenceAllele", "alternateAllele", "zScore"]


def test_transform_computes_zscore(transform_input: Path, tmp_path: Path):
    output = tmp_path / "output.tsv.gz"
    runner.invoke(app, ["transform", "--input", str(transform_input), "--output", str(output)])

    con = duckdb.connect()
    rows = con.execute(f"SELECT variantId, zScore FROM read_csv('{output}', delim='\\t') ORDER BY variantId").fetchall()
    con.close()

    # beta / standardError: 0.5/0.1 = 5.0, -0.3/0.05 = -6.0
    assert rows[0][0] == "chr1_1000_A_G"
    assert rows[0][1] == pytest.approx(5.0)
    assert rows[1][0] == "chr2_2000_C_T"
    assert rows[1][1] == pytest.approx(-6.0)


def test_transform_extracts_alleles(transform_input: Path, tmp_path: Path):
    output = tmp_path / "output.tsv.gz"
    runner.invoke(app, ["transform", "--input", str(transform_input), "--output", str(output)])

    con = duckdb.connect()
    rows = con.execute(f"SELECT referenceAllele, alternateAllele FROM read_csv('{output}', delim='\\t') ORDER BY variantId").fetchall()
    con.close()

    assert rows[0] == ("A", "G")
    assert rows[1] == ("C", "T")


def test_transform_invalid_input_file(tmp_path: Path):
    result = runner.invoke(app, ["transform", "--input", "/nonexistent/file.parquet", "--output", str(tmp_path / "out.tsv.gz")])
    assert result.exit_code != 0


def test_transform_invalid_input_extension(tmp_path: Path):
    txt_file = tmp_path / "file.txt"
    txt_file.write_text("not parquet")
    result = runner.invoke(app, ["transform", "--input", str(txt_file), "--output", str(tmp_path / "out.tsv.gz")])
    assert result.exit_code != 0


def test_transform_output_dir_must_exist(transform_input: Path):
    result = runner.invoke(app, ["transform", "--input", str(transform_input), "--output", "/nonexistent/out.tsv.gz"])
    assert result.exit_code != 0
