"""Test collector command line interface (CLI)"""

import gzip
import hashlib
import json
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


@pytest.fixture
def study_locus_inputs(tmp_path: Path) -> list[Path]:
    """Two flat StudyLocus parquet files with one study each."""
    from collector.schema import STUDY_LOCUS_SCHEMA

    input_dir = tmp_path / "study_locus_inputs"
    input_dir.mkdir()
    locus_type = STUDY_LOCUS_SCHEMA.fields[-1].sql_type()
    files = [input_dir / "study_a.parquet", input_dir / "study_b.parquet"]

    con = duckdb.connect()
    try:
        for path, study_id, variant_id, position in [
            (files[0], "STUDY_A", "1_100_A_C", 100),
            (files[1], "STUDY_B", "1_1000_A_C", 1000),
        ]:
            con.execute(
                f"""
                COPY (
                    SELECT
                        md5('{study_id}' || '{variant_id}') AS studyLocusId,
                        '{study_id}' AS studyId,
                        '{variant_id}' AS variantId,
                        '1' AS chromosome,
                        {position}::INTEGER AS position,
                        0.1::DOUBLE AS beta,
                        1000::INTEGER AS sampleSize,
                        1.0::FLOAT AS pValueMantissa,
                        -9::INTEGER AS pValueExponent,
                        0.1::FLOAT AS effectAlleleFrequencyFromSource,
                        0.05::DOUBLE AS standardError,
                        []::VARCHAR[] AS qualityControls,
                        ({position} - 10)::INTEGER AS locusStart,
                        ({position} + 10)::INTEGER AS locusEnd,
                        []::{locus_type} AS locus
                ) TO '{path}' (FORMAT 'parquet')
                """
            )
    finally:
        con.close()

    return files


def _write_study_locus_file(
    tmp_path: Path,
    study_id: str,
    loci: list[tuple[str, int, int] | tuple[str, int, int, list[str]]],
) -> Path:
    """Write a test StudyLocus parquet file for one study."""
    from collector.schema import STUDY_LOCUS_SCHEMA

    path = tmp_path / f"{study_id}.parquet"
    locus_type = STUDY_LOCUS_SCHEMA.fields[-1].sql_type()
    values = []
    for locus in loci:
        locus_name, locus_start, locus_end = locus[:3]
        quality_controls = locus[3] if len(locus) == 4 else []
        variant_id = f"1_{locus_start}_A_C"
        quality_controls_sql = "[" + ", ".join(f"'{qc}'" for qc in quality_controls) + "]::VARCHAR[]"
        values.append(
            f"""
            SELECT
                md5('{study_id}' || '{locus_name}') AS studyLocusId,
                '{study_id}' AS studyId,
                '{variant_id}' AS variantId,
                '1' AS chromosome,
                {locus_start}::INTEGER AS position,
                0.1::DOUBLE AS beta,
                1000::INTEGER AS sampleSize,
                1.0::FLOAT AS pValueMantissa,
                -9::INTEGER AS pValueExponent,
                0.1::FLOAT AS effectAlleleFrequencyFromSource,
                0.05::DOUBLE AS standardError,
                {quality_controls_sql} AS qualityControls,
                {locus_start}::INTEGER AS locusStart,
                {locus_end}::INTEGER AS locusEnd,
                [
                    struct_pack(
                        is95CredibleSet := false,
                        is99CredibleSet := false,
                        logBF := NULL::DOUBLE,
                        posteriorProbability := NULL::DOUBLE,
                        variantId := '{variant_id}',
                        pValueMantissa := 1.0::FLOAT,
                        pValueExponent := -9::INTEGER,
                        beta := 0.1::DOUBLE,
                        standardError := 0.05::DOUBLE,
                        r2Overall := NULL::DOUBLE
                    )
                ]::{locus_type} AS locus
            """
        )

    con = duckdb.connect()
    try:
        con.execute(f"COPY ({' UNION ALL '.join(values)}) TO '{path}' (FORMAT 'parquet')")
    finally:
        con.close()

    return path


def _collect_finemapping_loci_args(input_paths: list[Path], output_dir: Path) -> tuple[list[str], dict[str, Path]]:
    """Return CLI args and named output paths for a collect_finemapping_loci run."""
    outputs = {
        "full": output_dir / "full" / "run.parquet",
        "partial": output_dir / "partial" / "run.parquet",
        "non_overlap": output_dir / "non_overlap" / "run.parquet",
        "stats": output_dir / "stats" / "run.json",
    }
    args = ["collect_finemapping_loci"]
    for input_path in input_paths:
        args.extend(["--input", str(input_path)])
    args.extend(
        [
            "--full_output",
            str(outputs["full"]),
            "--partial_output",
            str(outputs["partial"]),
            "--non_overlap_output",
            str(outputs["non_overlap"]),
            "--stats_output",
            str(outputs["stats"]),
        ]
    )
    return args, outputs


def _write_collected_locus_file(
    tmp_path: Path,
    study_id: str,
    rows: list[tuple[str, str, list[tuple[str, float | None, float | None]]]],
) -> Path:
    """Write a collected-locus parquet file with nested locus variants."""
    from collector.schema import COLLECTED_LOCUS_SCHEMA

    path = tmp_path / f"{study_id}_collected.parquet"
    locus_type = COLLECTED_LOCUS_SCHEMA.fields[-1].sql_type()
    row_sql = []
    for fine_mapping_locus_set_id, study_locus_id, variants in rows:
        variants_sql = ", ".join(
            f"""
            struct_pack(
                variantId := '{variant_id}',
                pValueMantissa := 1.0::FLOAT,
                pValueExponent := -9::INTEGER,
                beta := {beta if beta is not None else "NULL"}::DOUBLE,
                standardError := {standard_error if standard_error is not None else "NULL"}::DOUBLE
            )
            """
            for variant_id, beta, standard_error in variants
        )
        row_sql.append(
            f"""
            SELECT
                '{fine_mapping_locus_set_id}' AS fineMappingLocusSetId,
                '{study_locus_id}' AS studyLocusId,
                '{study_id}' AS studyId,
                '1' AS chromosome,
                100::INTEGER AS locusStart,
                200::INTEGER AS locusEnd,
                ['overlapping set']::VARCHAR[] AS qualityControls,
                [{variants_sql}]::{locus_type} AS locus
            """
        )

    con = duckdb.connect()
    try:
        con.execute(f"COPY ({' UNION ALL '.join(row_sql)}) TO '{path}' (FORMAT 'parquet')")
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
        raise AssertionError("Output parquet file is empty or malformed")
    con.close()
    # 2 files x 2 rows each = 4 rows
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
        raise AssertionError("Output parquet file is empty or malformed")
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


# ---------------------------------------------------------------------------
# collect_finemapping_loci command tests
# ---------------------------------------------------------------------------


def test_collect_finemapping_loci_accepts_inputs_creates_dirs_and_removes_stale_full_output(
    study_locus_inputs: list[Path],
    tmp_path: Path,
):
    from collector.schema import COLLECTED_LOCUS_SCHEMA

    output_dir = tmp_path / "collected"
    full_output = output_dir / "full" / "run.parquet"
    partial_output = output_dir / "partial" / "run.parquet"
    non_overlap_output = output_dir / "non_overlap" / "run.parquet"
    stats_output = output_dir / "stats" / "run.json"
    full_output.parent.mkdir(parents=True)
    full_output.write_text("stale parquet placeholder")

    args = ["collect_finemapping_loci"]
    for input_path in study_locus_inputs:
        args.extend(["--input", str(input_path)])
    args.extend(
        [
            "--full_output",
            str(full_output),
            "--partial_output",
            str(partial_output),
            "--non_overlap_output",
            str(non_overlap_output),
            "--stats_output",
            str(stats_output),
        ]
    )

    result = runner.invoke(app, args)

    assert result.exit_code == 0, result.output
    assert not full_output.exists()
    assert partial_output.exists()
    assert non_overlap_output.exists()
    assert stats_output.exists()

    con = duckdb.connect()
    try:
        partial_columns = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{partial_output}')").fetchall()]
        non_overlap_columns = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{non_overlap_output}')").fetchall()]
    finally:
        con.close()

    assert partial_columns == list(COLLECTED_LOCUS_SCHEMA.column_names)
    assert non_overlap_columns == list(COLLECTED_LOCUS_SCHEMA.column_names)


def test_collect_finemapping_loci_writes_stats_contract(study_locus_inputs: list[Path], tmp_path: Path):
    output_dir = tmp_path / "collected"
    stats_output = output_dir / "stats" / "run.json"

    args = ["collect_finemapping_loci"]
    for input_path in study_locus_inputs:
        args.extend(["--input", str(input_path)])
    args.extend(
        [
            "--full_output",
            str(output_dir / "full" / "run.parquet"),
            "--partial_output",
            str(output_dir / "partial" / "run.parquet"),
            "--non_overlap_output",
            str(output_dir / "non_overlap" / "run.parquet"),
            "--stats_output",
            str(stats_output),
        ]
    )

    result = runner.invoke(app, args)

    assert result.exit_code == 0, result.output
    stats = json.loads(stats_output.read_text())
    assert stats["nInputStudies"] == 2
    assert stats["studyIds"] == ["STUDY_A", "STUDY_B"]
    assert stats["inputFiles"] == {
        "STUDY_A": str(study_locus_inputs[0]),
        "STUDY_B": str(study_locus_inputs[1]),
    }
    assert stats["inputLocusRowCount"] == 2


def test_collect_finemapping_loci_accepts_directory_input(study_locus_inputs: list[Path], tmp_path: Path):
    input_dir = tmp_path / "directory_dataset"
    input_dir.mkdir()

    con = duckdb.connect()
    try:
        con.execute(f"COPY (SELECT * FROM read_parquet('{study_locus_inputs[0]}')) TO '{input_dir / 'part-0.parquet'}' (FORMAT 'parquet')")
    finally:
        con.close()

    output_dir = tmp_path / "collected"
    result = runner.invoke(
        app,
        [
            "collect_finemapping_loci",
            "--input",
            str(input_dir),
            "--full_output",
            str(output_dir / "full" / "run.parquet"),
            "--partial_output",
            str(output_dir / "partial" / "run.parquet"),
            "--non_overlap_output",
            str(output_dir / "non_overlap" / "run.parquet"),
            "--stats_output",
            str(output_dir / "stats" / "run.json"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "partial" / "run.parquet").exists()
    assert (output_dir / "non_overlap" / "run.parquet").exists()


def test_collect_finemapping_loci_accepts_hive_partitioned_gentropy_directory(
    study_locus_inputs: list[Path],
    tmp_path: Path,
):
    hive_input_dir = tmp_path / "gentropy_partitioned_study_locus"

    con = duckdb.connect()
    try:
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet('{study_locus_inputs[0]}')
            ) TO '{hive_input_dir}' (FORMAT 'parquet', PARTITION_BY (studyLocusId))
            """
        )
    finally:
        con.close()

    output_dir = tmp_path / "collected"
    result = runner.invoke(
        app,
        [
            "collect_finemapping_loci",
            "--input",
            str(hive_input_dir),
            "--input",
            str(study_locus_inputs[1]),
            "--full_output",
            str(output_dir / "full" / "run.parquet"),
            "--partial_output",
            str(output_dir / "partial" / "run.parquet"),
            "--non_overlap_output",
            str(output_dir / "non_overlap" / "run.parquet"),
            "--stats_output",
            str(output_dir / "stats" / "run.json"),
        ],
    )

    assert result.exit_code == 0, result.output
    stats = json.loads((output_dir / "stats" / "run.json").read_text())
    assert stats["inputLocusRowCount"] == 2
    assert stats["inputFiles"] == {
        "STUDY_A": str(hive_input_dir),
        "STUDY_B": str(study_locus_inputs[1]),
    }


def test_collect_finemapping_loci_rejects_input_count_study_id_mismatch(
    study_locus_inputs: list[Path],
    tmp_path: Path,
):
    output_dir = tmp_path / "collected"
    partial_output = output_dir / "partial" / "run.parquet"
    non_overlap_output = output_dir / "non_overlap" / "run.parquet"
    stats_output = output_dir / "stats" / "run.json"

    result = runner.invoke(
        app,
        [
            "collect_finemapping_loci",
            "--input",
            str(study_locus_inputs[0]),
            "--full_output",
            str(output_dir / "full" / "run.parquet"),
            "--partial_output",
            str(partial_output),
            "--non_overlap_output",
            str(non_overlap_output),
            "--stats_output",
            str(stats_output),
        ],
    )

    assert result.exit_code == 0, result.output

    mismatch_result = runner.invoke(
        app,
        [
            "collect_finemapping_loci",
            "--input",
            str(study_locus_inputs[0]),
            "--input",
            str(study_locus_inputs[0]),
            "--full_output",
            str(output_dir / "full" / "run.parquet"),
            "--partial_output",
            str(partial_output),
            "--non_overlap_output",
            str(non_overlap_output),
            "--stats_output",
            str(stats_output),
        ],
    )

    assert mismatch_result.exit_code != 0
    assert "distinct" in mismatch_result.output
    assert "studyId" in mismatch_result.output
    assert not partial_output.exists()
    assert not non_overlap_output.exists()
    assert not stats_output.exists()


def test_collect_finemapping_loci_schema_contract_uses_pydantic_models():
    from pydantic import BaseModel

    from collector.collect_loci import CollectFineMappingLociConfig
    from collector.schema import COLLECTED_LOCUS_SCHEMA, COLLECTED_LOCUS_STRUCT_SCHEMA, DatasetSchema, StructSchema

    assert issubclass(CollectFineMappingLociConfig, BaseModel)
    assert isinstance(COLLECTED_LOCUS_SCHEMA, DatasetSchema)
    assert isinstance(COLLECTED_LOCUS_STRUCT_SCHEMA, StructSchema)
    assert COLLECTED_LOCUS_SCHEMA.column_names == (
        "fineMappingLocusSetId",
        "studyLocusId",
        "studyId",
        "chromosome",
        "locusStart",
        "locusEnd",
        "qualityControls",
        "locus",
    )
    assert COLLECTED_LOCUS_STRUCT_SCHEMA.field_names == (
        "variantId",
        "pValueMantissa",
        "pValueExponent",
        "beta",
        "standardError",
    )


def test_collect_finemapping_loci_classifies_partial_and_non_overlapping_loci(tmp_path: Path):
    inputs = [
        _write_study_locus_file(tmp_path, "STUDY_A", [("a_partial", 100, 150, ["preexisting"]), ("a_non", 1000, 1050, [])]),
        _write_study_locus_file(tmp_path, "STUDY_B", [("b_partial", 140, 160, [])]),
        _write_study_locus_file(tmp_path, "STUDY_C", [("c_non", 2000, 2050, [])]),
    ]
    args, outputs = _collect_finemapping_loci_args(inputs, tmp_path / "collected")

    result = runner.invoke(app, args)

    assert result.exit_code == 0, result.output
    con = duckdb.connect()
    try:
        partial_rows = con.execute(
            f"""
            SELECT studyId, locusStart, locusEnd, qualityControls, fineMappingLocusSetId
            FROM read_parquet('{outputs["partial"]}')
            ORDER BY studyId, locusStart
            """
        ).fetchall()
        non_overlap_rows = con.execute(
            f"""
            SELECT studyId, locusStart, locusEnd, qualityControls, fineMappingLocusSetId
            FROM read_parquet('{outputs["non_overlap"]}')
            ORDER BY studyId, locusStart
            """
        ).fetchall()
    finally:
        con.close()

    assert partial_rows == [
        ("STUDY_A", 100, 150, ["preexisting", "partial-overlapping studyLocus"], None),
        ("STUDY_B", 140, 160, ["partial-overlapping studyLocus"], None),
    ]
    assert non_overlap_rows == [
        ("STUDY_A", 1000, 1050, ["non-overlapping studyLocus"], None),
        ("STUDY_C", 2000, 2050, ["non-overlapping studyLocus"], None),
    ]

    stats = json.loads(outputs["stats"].read_text())
    assert stats["overlapEdgeCount"] == 2
    assert stats["eligibleFullOverlapLocusCount"] == 0
    assert stats["partialOverlap"]["rowCount"] == 2
    assert stats["partialOverlap"]["studyIdsWithRows"] == ["STUDY_A", "STUDY_B"]
    assert stats["partialOverlap"]["byStudyId"] == {"STUDY_A": 1, "STUDY_B": 1}
    assert stats["nonOverlap"]["rowCount"] == 2
    assert stats["nonOverlap"]["studyIdsWithRows"] == ["STUDY_A", "STUDY_C"]
    assert stats["nonOverlap"]["byStudyId"] == {"STUDY_A": 1, "STUDY_C": 1}


def test_collect_finemapping_loci_writes_full_overlap_output_with_deterministic_set_id(tmp_path: Path):
    inputs = [
        _write_study_locus_file(tmp_path, "STUDY_A", [("a_full", 100, 200, [])]),
        _write_study_locus_file(tmp_path, "STUDY_B", [("b_full", 150, 250, [])]),
        _write_study_locus_file(tmp_path, "STUDY_C", [("c_full", 175, 225, [])]),
    ]
    args, outputs = _collect_finemapping_loci_args(inputs, tmp_path / "collected")

    result = runner.invoke(app, args)

    assert result.exit_code == 0, result.output
    assert outputs["full"].exists()
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT fineMappingLocusSetId, studyLocusId, studyId, qualityControls
            FROM read_parquet('{outputs["full"]}')
            ORDER BY studyId
            """
        ).fetchall()
    finally:
        con.close()

    member_ids = [row[1] for row in rows]
    expected_set_id = hashlib.md5("|".join(sorted(member_ids)).encode()).hexdigest()
    assert rows == [
        (expected_set_id, member_ids[0], "STUDY_A", ["overlapping set"]),
        (expected_set_id, member_ids[1], "STUDY_B", ["overlapping set"]),
        (expected_set_id, member_ids[2], "STUDY_C", ["overlapping set"]),
    ]

    stats = json.loads(outputs["stats"].read_text())
    assert stats["overlapEdgeCount"] == 6
    assert stats["eligibleFullOverlapLocusCount"] == 3
    assert stats["fullOverlap"]["fineMappingSetCount"] == 1
    assert stats["fullOverlap"]["rowCount"] == 3
    assert stats["fullOverlap"]["isEmpty"] is False
    assert stats["fullOverlap"]["studyIdsWithRows"] == ["STUDY_A", "STUDY_B", "STUDY_C"]
    assert stats["fullOverlap"]["byStudyId"] == {"STUDY_A": 1, "STUDY_B": 1, "STUDY_C": 1}
    assert stats["partialOverlap"]["rowCount"] == 0
    assert stats["partialOverlap"]["studyIdsWithRows"] == []
    assert stats["partialOverlap"]["byStudyId"] == {}
    assert stats["nonOverlap"]["rowCount"] == 0
    assert stats["nonOverlap"]["studyIdsWithRows"] == []
    assert stats["nonOverlap"]["byStudyId"] == {}


def test_collect_finemapping_loci_treats_single_input_loci_as_full_overlap_eligible(tmp_path: Path):
    inputs = [
        _write_study_locus_file(tmp_path, "STUDY_A", [("a_one", 100, 200, []), ("a_two", 1000, 1100, [])]),
    ]
    args, outputs = _collect_finemapping_loci_args(inputs, tmp_path / "collected")

    result = runner.invoke(app, args)

    assert result.exit_code == 0, result.output
    assert outputs["full"].exists()
    con = duckdb.connect()
    try:
        full_rows = con.execute(
            f"""
            SELECT fineMappingLocusSetId, studyId, locusStart
            FROM read_parquet('{outputs["full"]}')
            ORDER BY locusStart
            """
        ).fetchall()
    finally:
        con.close()
    assert len({row[0] for row in full_rows}) == 2
    assert [row[1:] for row in full_rows] == [("STUDY_A", 100), ("STUDY_A", 1000)]

    stats = json.loads(outputs["stats"].read_text())
    assert stats["nInputStudies"] == 1
    assert stats["overlapEdgeCount"] == 0
    assert stats["eligibleFullOverlapLocusCount"] == 2
    assert stats["fullOverlap"]["fineMappingSetCount"] == 2
    assert stats["fullOverlap"]["rowCount"] == 2
    assert stats["partialOverlap"]["rowCount"] == 0
    assert stats["nonOverlap"]["rowCount"] == 0


def test_collect_finemapping_loci_partial_output_can_include_all_observed_studies(tmp_path: Path):
    inputs = [
        _write_study_locus_file(tmp_path, "STUDY_A", [("a_to_b", 100, 150), ("a_to_c", 500, 550)]),
        _write_study_locus_file(tmp_path, "STUDY_B", [("b_to_a", 140, 160), ("b_to_c", 900, 950)]),
        _write_study_locus_file(tmp_path, "STUDY_C", [("c_to_a", 540, 560), ("c_to_b", 940, 960)]),
    ]
    args, outputs = _collect_finemapping_loci_args(inputs, tmp_path / "collected")

    result = runner.invoke(app, args)

    assert result.exit_code == 0, result.output
    stats = json.loads(outputs["stats"].read_text())
    assert stats["eligibleFullOverlapLocusCount"] == 0
    assert stats["partialOverlap"]["rowCount"] == 6
    assert stats["partialOverlap"]["studyIdsWithRows"] == ["STUDY_A", "STUDY_B", "STUDY_C"]
    assert stats["partialOverlap"]["byStudyId"] == {"STUDY_A": 2, "STUDY_B": 2, "STUDY_C": 2}


def test_collect_finemapping_loci_splits_many_to_many_full_overlap_sets(tmp_path: Path):
    inputs = [
        _write_study_locus_file(tmp_path, "STUDY_A", [("a_left", 100, 200), ("a_right", 300, 400)]),
        _write_study_locus_file(tmp_path, "STUDY_B", [("b_left", 150, 180), ("b_right", 350, 380)]),
        _write_study_locus_file(tmp_path, "STUDY_C", [("c_bridge", 175, 375)]),
    ]
    args, outputs = _collect_finemapping_loci_args(inputs, tmp_path / "collected")

    result = runner.invoke(app, args)

    assert result.exit_code == 0, result.output
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT fineMappingLocusSetId, studyId, locusStart
            FROM read_parquet('{outputs["full"]}')
            ORDER BY fineMappingLocusSetId, studyId, locusStart
            """
        ).fetchall()
    finally:
        con.close()

    set_ids = sorted({row[0] for row in rows})
    assert len(set_ids) == 2
    assert [row[1:] for row in rows if row[0] == set_ids[0]] in [
        [("STUDY_A", 100), ("STUDY_B", 150), ("STUDY_C", 175)],
        [("STUDY_A", 300), ("STUDY_B", 350), ("STUDY_C", 175)],
    ]
    assert [row[1:] for row in rows if row[0] == set_ids[1]] in [
        [("STUDY_A", 100), ("STUDY_B", 150), ("STUDY_C", 175)],
        [("STUDY_A", 300), ("STUDY_B", 350), ("STUDY_C", 175)],
    ]
    assert len(rows) == 6
    assert [row[1] for row in rows].count("STUDY_C") == 2

    stats = json.loads(outputs["stats"].read_text())
    assert stats["fullOverlap"]["fineMappingSetCount"] == 2
    assert stats["fullOverlap"]["rowCount"] == 6
    assert stats["fullOverlap"]["byStudyId"] == {"STUDY_A": 2, "STUDY_B": 2, "STUDY_C": 2}


def test_collect_finemapping_loci_set_ids_are_independent_of_input_order(tmp_path: Path):
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    first_inputs = [
        _write_study_locus_file(first_dir, "STUDY_A", [("a_full", 100, 200)]),
        _write_study_locus_file(first_dir, "STUDY_B", [("b_full", 150, 250)]),
        _write_study_locus_file(first_dir, "STUDY_C", [("c_full", 175, 225)]),
    ]
    second_inputs = [
        _write_study_locus_file(second_dir, "STUDY_C", [("c_full", 175, 225)]),
        _write_study_locus_file(second_dir, "STUDY_A", [("a_full", 100, 200)]),
        _write_study_locus_file(second_dir, "STUDY_B", [("b_full", 150, 250)]),
    ]
    first_args, first_outputs = _collect_finemapping_loci_args(first_inputs, tmp_path / "collected_first")
    second_args, second_outputs = _collect_finemapping_loci_args(second_inputs, tmp_path / "collected_second")

    first_result = runner.invoke(app, first_args)
    second_result = runner.invoke(app, second_args)

    assert first_result.exit_code == 0, first_result.output
    assert second_result.exit_code == 0, second_result.output
    con = duckdb.connect()
    try:
        first_rows = con.execute(
            f"""
            SELECT fineMappingLocusSetId, studyId, locusStart, locusEnd
            FROM read_parquet('{first_outputs["full"]}')
            ORDER BY fineMappingLocusSetId, studyId
            """
        ).fetchall()
        second_rows = con.execute(
            f"""
            SELECT fineMappingLocusSetId, studyId, locusStart, locusEnd
            FROM read_parquet('{second_outputs["full"]}')
            ORDER BY fineMappingLocusSetId, studyId
            """
        ).fetchall()
    finally:
        con.close()

    assert first_rows == second_rows


def test_collect_finemapping_loci_does_not_generate_cross_component_full_sets(tmp_path: Path):
    inputs = [
        _write_study_locus_file(tmp_path, "STUDY_A", [("a_left", 100, 200), ("a_right", 1000, 1100)]),
        _write_study_locus_file(tmp_path, "STUDY_B", [("b_left", 125, 225), ("b_right", 1025, 1125)]),
        _write_study_locus_file(tmp_path, "STUDY_C", [("c_left", 150, 250), ("c_right", 1050, 1150)]),
    ]
    args, outputs = _collect_finemapping_loci_args(inputs, tmp_path / "collected")

    result = runner.invoke(app, args)

    assert result.exit_code == 0, result.output
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT fineMappingLocusSetId, studyId, locusStart
            FROM read_parquet('{outputs["full"]}')
            ORDER BY fineMappingLocusSetId, studyId
            """
        ).fetchall()
    finally:
        con.close()

    sets = {}
    for set_id, study_id, locus_start in rows:
        sets.setdefault(set_id, []).append((study_id, locus_start))

    assert sorted(sets.values()) == [
        [("STUDY_A", 100), ("STUDY_B", 125), ("STUDY_C", 150)],
        [("STUDY_A", 1000), ("STUDY_B", 1025), ("STUDY_C", 1050)],
    ]

    stats = json.loads(outputs["stats"].read_text())
    assert stats["fullOverlap"]["fineMappingSetCount"] == 2
    assert stats["fullOverlap"]["componentCount"] == 2
    assert stats["fullOverlap"]["maxComponentLocusCount"] == 3
    assert stats["fullOverlap"]["maxComponentCandidateProductSize"] == 1


# ---------------------------------------------------------------------------
# study_locus_ld_annotation command tests
# ---------------------------------------------------------------------------


def test_study_locus_ld_annotation_writes_flattened_loci_and_ld_pair_contracts(tmp_path: Path):
    from collector.schema import FINE_MAPPING_LOCI_SCHEMA, LD_PAIRS_SCHEMA

    input_path = _write_collected_locus_file(
        tmp_path,
        "STUDY_A",
        rows=[
            ("set_a", "sl_1", [("1_100_A_C", 0.5, 0.1), ("1_120_G_T", 0.2, 0.0)]),
            ("set_b", "sl_2", [("1_200_C_A", -0.4, None)]),
        ],
    )
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(json.dumps({"studyId": "STUDY_A", "ancestry": "nfe", "sampleSize": 1234}))
    output_dir = tmp_path / "ld_annotation"

    result = runner.invoke(
        app,
        [
            "study_locus_ld_annotation",
            "--input",
            str(input_path),
            "--metadata_json",
            str(metadata_path),
            "--output_dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    fine_mapping_output = output_dir / "fine_mapping_loci.parquet"
    ld_pairs_output = output_dir / "ld_pairs.parquet"
    assert fine_mapping_output.exists()
    assert ld_pairs_output.exists()

    con = duckdb.connect()
    try:
        fine_mapping_columns = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{fine_mapping_output}')").fetchall()]
        ld_pairs_columns = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{ld_pairs_output}')").fetchall()]
        fine_mapping_rows = con.execute(
            f"""
            SELECT fineMappingLocusSetId, studyLocusId, studyId, ancestry, sampleSize, variantId, beta, standardError, z
            FROM read_parquet('{fine_mapping_output}')
            ORDER BY fineMappingLocusSetId, studyLocusId, variantId
            """
        ).fetchall()
        ld_pair_rows = con.execute(
            f"""
            SELECT ancestry, variantIdI, variantIdJ, r
            FROM read_parquet('{ld_pairs_output}')
            ORDER BY ancestry, variantIdI, variantIdJ
            """
        ).fetchall()
    finally:
        con.close()

    assert fine_mapping_columns == list(FINE_MAPPING_LOCI_SCHEMA.column_names)
    assert ld_pairs_columns == list(LD_PAIRS_SCHEMA.column_names)
    assert fine_mapping_rows == [
        ("set_a", "sl_1", "STUDY_A", "nfe", 1234, "1_100_A_C", 0.5, 0.1, 5.0),
        ("set_a", "sl_1", "STUDY_A", "nfe", 1234, "1_120_G_T", 0.2, 0.0, None),
        ("set_b", "sl_2", "STUDY_A", "nfe", 1234, "1_200_C_A", -0.4, None, None),
    ]
    assert ld_pair_rows == [
        ("nfe", "1_100_A_C", "1_100_A_C", 1.0),
        ("nfe", "1_100_A_C", "1_120_G_T", 0.0),
        ("nfe", "1_100_A_C", "1_200_C_A", 0.0),
        ("nfe", "1_120_G_T", "1_100_A_C", 0.0),
        ("nfe", "1_120_G_T", "1_120_G_T", 1.0),
        ("nfe", "1_120_G_T", "1_200_C_A", 0.0),
        ("nfe", "1_200_C_A", "1_100_A_C", 0.0),
        ("nfe", "1_200_C_A", "1_120_G_T", 0.0),
        ("nfe", "1_200_C_A", "1_200_C_A", 1.0),
    ]


def test_study_locus_ld_annotation_joins_metadata_per_study_id(tmp_path: Path):
    study_a_input = _write_collected_locus_file(
        tmp_path,
        "STUDY_A",
        rows=[("set_a", "sl_a", [("1_100_A_C", 0.5, 0.1)])],
    )
    study_b_input = _write_collected_locus_file(
        tmp_path,
        "STUDY_B",
        rows=[("set_a", "sl_b", [("1_120_G_T", -0.2, 0.05)])],
    )
    input_path = tmp_path / "combined_collected.parquet"
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(
        json.dumps(
            [
                {"studyId": "STUDY_A", "ancestry": "eur", "sampleSize": 1000},
                {"studyId": "STUDY_B", "ancestry": "afr", "sampleSize": 2000},
            ]
        )
    )
    output_dir = tmp_path / "ld_annotation"

    con = duckdb.connect()
    try:
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet(['{study_a_input}', '{study_b_input}'], union_by_name = true)
            ) TO '{input_path}' (FORMAT 'parquet')
            """
        )
    finally:
        con.close()

    result = runner.invoke(
        app,
        [
            "study_locus_ld_annotation",
            "--input",
            str(input_path),
            "--metadata_json",
            str(metadata_path),
            "--output_dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    fine_mapping_output = output_dir / "fine_mapping_loci.parquet"
    ld_pairs_output = output_dir / "ld_pairs.parquet"

    con = duckdb.connect()
    try:
        fine_mapping_rows = con.execute(
            f"""
            SELECT studyLocusId, studyId, ancestry, sampleSize, variantId, z
            FROM read_parquet('{fine_mapping_output}')
            ORDER BY studyId
            """
        ).fetchall()
        ld_pair_rows = con.execute(
            f"""
            SELECT ancestry, variantIdI, variantIdJ, r
            FROM read_parquet('{ld_pairs_output}')
            ORDER BY ancestry, variantIdI, variantIdJ
            """
        ).fetchall()
    finally:
        con.close()

    assert fine_mapping_rows == [
        ("sl_a", "STUDY_A", "eur", 1000, "1_100_A_C", 5.0),
        ("sl_b", "STUDY_B", "afr", 2000, "1_120_G_T", -4.0),
    ]
    assert ld_pair_rows == [
        ("afr", "1_100_A_C", "1_100_A_C", 1.0),
        ("afr", "1_100_A_C", "1_120_G_T", 0.0),
        ("afr", "1_120_G_T", "1_100_A_C", 0.0),
        ("afr", "1_120_G_T", "1_120_G_T", 1.0),
        ("eur", "1_100_A_C", "1_100_A_C", 1.0),
        ("eur", "1_100_A_C", "1_120_G_T", 0.0),
        ("eur", "1_120_G_T", "1_100_A_C", 0.0),
        ("eur", "1_120_G_T", "1_120_G_T", 1.0),
    ]


def test_study_locus_ld_annotation_rejects_metadata_study_id_mismatch(tmp_path: Path):
    input_path = _write_collected_locus_file(
        tmp_path,
        "STUDY_A",
        rows=[("set_a", "sl_a", [("1_100_A_C", 0.5, 0.1)])],
    )
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(json.dumps([{"studyId": "STUDY_B", "ancestry": "afr", "sampleSize": 2000}]))

    result = runner.invoke(
        app,
        [
            "study_locus_ld_annotation",
            "--input",
            str(input_path),
            "--metadata_json",
            str(metadata_path),
            "--output_dir",
            str(tmp_path / "ld_annotation"),
        ],
    )

    assert result.exit_code != 0
    assert "studyId values must exactly match" in result.output
