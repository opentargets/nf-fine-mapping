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


@pytest.fixture
def locus_breaker_input(tmp_path: Path) -> Path:
    """Flat summary-statistics parquet with columns required by locus_breaker."""
    path = tmp_path / "locus_breaker_sumstats.parquet"

    con = duckdb.connect()
    try:
        con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    ('GCST_TEST', '1_100_A_C', '1', 100, 0.1, 1000, 1.0::FLOAT, -9, 0.1::FLOAT, 0.05),
                    ('GCST_TEST', '1_150_A_C', '1', 150, 0.2, 1000, 5.0::FLOAT, -6, 0.2::FLOAT, 0.06)
                ) AS t(
                    studyId,
                    variantId,
                    chromosome,
                    position,
                    beta,
                    sampleSize,
                    pValueMantissa,
                    pValueExponent,
                    effectAlleleFrequencyFromSource,
                    standardError
                )
            ) TO '{path}' (FORMAT 'parquet')
            """
        )
    finally:
        con.close()

    return path


@pytest.fixture
def locus_breaker_input_dir(locus_breaker_input: Path, tmp_path: Path) -> Path:
    """Directory dataset with one valid locus_breaker parquet part."""
    input_dir = tmp_path / "locus_breaker_input_dir"
    input_dir.mkdir()
    output_part = input_dir / "part-00000.parquet"

    con = duckdb.connect()
    try:
        con.execute(f"COPY (SELECT * FROM read_parquet('{locus_breaker_input}')) TO '{output_part}' (FORMAT 'parquet')")
    finally:
        con.close()

    return input_dir


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


# ---------------------------------------------------------------------------
# locus_breaker command tests
# ---------------------------------------------------------------------------


def test_locus_breaker_accepts_issue_one_options(locus_breaker_input: Path, tmp_path: Path):
    output = tmp_path / "study_locus.parquet"
    result = runner.invoke(
        app,
        [
            "locus_breaker",
            "--input",
            str(locus_breaker_input),
            "--output",
            str(output),
            "--lbc_baseline_pvalue",
            "1e-5",
            "--lbc_distance_cutoff",
            "250000",
            "--lbc_pvalue_threshold",
            "1e-8",
            "--lbc_flanking_distance",
            "100000",
            "--large_loci_size",
            "1500000",
            "--wbc_clump_distance",
            "500000",
            "--wbc_pvalue_threshold",
            "1e-5",
            "--no_collect_locus",
            "--no_remove_mhc",
        ],
    )

    assert result.exit_code == 0, result.output
    assert output.exists()

    delta_result = runner.invoke(app, ["locus_breaker", "--input", str(locus_breaker_input), "--output", str(output), "--output_delta", "x"])
    assert delta_result.exit_code != 0
    write_mode_result = runner.invoke(
        app,
        ["locus_breaker", "--input", str(locus_breaker_input), "--output", str(output), "--write_mode", "overwrite"],
    )
    assert write_mode_result.exit_code != 0


def test_locus_breaker_accepts_single_file_and_writes_flat_schema(locus_breaker_input: Path, tmp_path: Path):
    from collector.schema import LOCUS_STRUCT_SCHEMA, STUDY_LOCUS_SCHEMA

    output = tmp_path / "study_locus.parquet"
    result = runner.invoke(app, ["locus_breaker", "--input", str(locus_breaker_input), "--output", str(output)])

    assert result.exit_code == 0, result.output
    assert output.exists()

    con = duckdb.connect()
    try:
        rows = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{output}')").fetchall()
    finally:
        con.close()

    columns = [row[0] for row in rows]
    types = {row[0]: row[1] for row in rows}
    assert columns == list(STUDY_LOCUS_SCHEMA.column_names)
    assert types["studyLocusId"] == "VARCHAR"
    assert types["position"] == "INTEGER"
    assert types["pValueMantissa"] == "FLOAT"
    assert types["qualityControls"] == "VARCHAR[]"
    assert types["locus"].startswith("STRUCT(")
    assert types["locus"].endswith("[]")
    for field in LOCUS_STRUCT_SCHEMA.field_names:
        assert field.lower() in types["locus"].lower()


def test_locus_breaker_accepts_directory_input_and_creates_output_parents(locus_breaker_input_dir: Path, tmp_path: Path):
    output = tmp_path / "nested" / "study_locus.parquet"
    result = runner.invoke(app, ["locus_breaker", "--input", str(locus_breaker_input_dir), "--output", str(output)])

    assert result.exit_code == 0, result.output
    assert output.exists()


def test_locus_breaker_rejects_non_parquet_output(locus_breaker_input: Path, tmp_path: Path):
    result = runner.invoke(app, ["locus_breaker", "--input", str(locus_breaker_input), "--output", str(tmp_path / "study_locus.txt")])

    assert result.exit_code != 0


def test_locus_breaker_overwrites_existing_output(locus_breaker_input: Path, tmp_path: Path):
    output = tmp_path / "study_locus.parquet"
    output.write_text("not parquet")

    result = runner.invoke(app, ["locus_breaker", "--input", str(locus_breaker_input), "--output", str(output), "--no_collect_locus"])

    assert result.exit_code == 0, result.output
    con = duckdb.connect()
    try:
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output}')").fetchone()
    finally:
        con.close()
    assert count == (1,)


def test_locus_breaker_schema_contract_uses_pydantic_models():
    from pydantic import BaseModel

    from collector.locus_breaker import LocusBreakerConfig
    from collector.schema import STUDY_LOCUS_SCHEMA, DatasetSchema, StructSchema

    assert issubclass(LocusBreakerConfig, BaseModel)
    assert isinstance(STUDY_LOCUS_SCHEMA, DatasetSchema)
    assert isinstance(STUDY_LOCUS_SCHEMA.fields[-1].duckdb_type.item_schema, StructSchema)
    assert STUDY_LOCUS_SCHEMA.column_names[0] == "studyLocusId"
    assert STUDY_LOCUS_SCHEMA.fields[-1].duckdb_type.item_schema.field_names == (
        "is95CredibleSet",
        "is99CredibleSet",
        "logBF",
        "posteriorProbability",
        "variantId",
        "pValueMantissa",
        "pValueExponent",
        "beta",
        "standardError",
        "r2Overall",
    )
