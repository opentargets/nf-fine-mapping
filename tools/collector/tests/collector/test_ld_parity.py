"""Tests for order-independent LD annotation parity reports."""

import json
from pathlib import Path

import duckdb
from typer.testing import CliRunner

from collector import app
from collector.ld_parity import LdParityConfig, compare_ld_outputs

runner = CliRunner()


def _write_ld(path: Path, rows: list[tuple[str, str, str, float]]) -> None:
    with duckdb.connect() as con:
        con.execute(
            """
            CREATE TABLE ld(
                ancestry VARCHAR,
                variantIdI VARCHAR,
                variantIdJ VARCHAR,
                r DOUBLE
            )
            """
        )
        con.executemany("INSERT INTO ld VALUES (?, ?, ?, ?)", rows)
        con.execute(f"COPY ld TO '{path}' (FORMAT PARQUET)")


def test_compare_ld_outputs_ignores_order_contig_prefix_and_pair_orientation(tmp_path: Path) -> None:
    hailing_path = tmp_path / "hailing.parquet"
    gentropy_path = tmp_path / "gentropy.parquet"
    report_path = tmp_path / "parity.json"
    hailing_stats = tmp_path / "hailing.stats.jsonl"
    gentropy_stats = tmp_path / "gentropy.stats.jsonl"

    _write_ld(
        hailing_path,
        [
            ("eas", "1_100_A_G", "1_100_A_G", 1.0),
            ("eas", "1_100_A_G", "1_150_A_AT", -0.25),
            ("eas", "1_200_C_T", "1_150_A_AT", 0.5),
            ("eas", "1_300_G_A", "1_300_G_A", 1.0),
            ("nfe", "1_400_T_C", "1_400_T_C", 1.0),
        ],
    )
    _write_ld(
        gentropy_path,
        [
            ("eas", "chr1_150_A_AT", "chr1_100_A_G", -0.25000000001),
            ("eas", "chr1_100_A_G", "chr1_100_A_G", 1.0),
            ("eas", "chr1_150_A_AT", "chr1_200_C_T", 0.5),
            ("eas", "chr1_350_C_G", "chr1_350_C_G", 1.0),
        ],
    )
    hailing_stats.write_text('{"ancestry":"eas","n_ld_pairs":4}\n{"ancestry":"nfe","n_ld_pairs":1}\n{"ancestry":"afr","n_ld_pairs":0}\n')
    gentropy_stats.write_text('{"ancestry":"eas","n_ld_pairs":4}\n{"ancestry":"nfe","n_ld_pairs":0}\n{"ancestry":"afr","n_ld_pairs":0}\n')

    report = compare_ld_outputs(
        LdParityConfig(
            hailing_path=hailing_path,
            gentropy_path=gentropy_path,
            report_path=report_path,
            hailing_stats_path=hailing_stats,
            gentropy_stats_path=gentropy_stats,
            tolerance=1e-8,
        )
    )

    assert report == json.loads(report_path.read_text())
    assert report["totals"] == {
        "hailing_pairs": 5,
        "gentropy_pairs": 4,
        "shared_pairs": 3,
        "hailing_only_pairs": 2,
        "gentropy_only_pairs": 1,
        "shared_value_mismatches": 0,
        "max_abs_ld_difference": 1.000000082740371e-11,
    }
    assert report["diagonal_differences"] == {
        "hailing_only": 2,
        "gentropy_only": 1,
        "shared_value_mismatches": 0,
    }
    assert report["ancestries"] == [
        {
            "ancestry": "afr",
            "hailing_pairs": 0,
            "gentropy_pairs": 0,
            "shared_pairs": 0,
            "hailing_only_pairs": 0,
            "gentropy_only_pairs": 0,
            "shared_value_mismatches": 0,
            "max_abs_ld_difference": None,
            "hailing_zero_pairs": True,
            "gentropy_zero_pairs": True,
        },
        {
            "ancestry": "eas",
            "hailing_pairs": 4,
            "gentropy_pairs": 4,
            "shared_pairs": 3,
            "hailing_only_pairs": 1,
            "gentropy_only_pairs": 1,
            "shared_value_mismatches": 0,
            "max_abs_ld_difference": 1.000000082740371e-11,
            "hailing_zero_pairs": False,
            "gentropy_zero_pairs": False,
        },
        {
            "ancestry": "nfe",
            "hailing_pairs": 1,
            "gentropy_pairs": 0,
            "shared_pairs": 0,
            "hailing_only_pairs": 1,
            "gentropy_only_pairs": 0,
            "shared_value_mismatches": 0,
            "max_abs_ld_difference": None,
            "hailing_zero_pairs": False,
            "gentropy_zero_pairs": True,
        },
    ]


def test_compare_ld_outputs_reports_shared_value_mismatches(tmp_path: Path) -> None:
    hailing_path = tmp_path / "hailing.parquet"
    gentropy_path = tmp_path / "gentropy.parquet"
    _write_ld(hailing_path, [("afr", "1_100_A_G", "1_200_C_T", 0.4)])
    _write_ld(gentropy_path, [("afr", "1_100_A_G", "1_200_C_T", -0.4)])

    report = compare_ld_outputs(
        LdParityConfig(
            hailing_path=hailing_path,
            gentropy_path=gentropy_path,
            report_path=tmp_path / "report.json",
            tolerance=1e-6,
        )
    )

    assert report["totals"]["shared_value_mismatches"] == 1
    assert report["totals"]["max_abs_ld_difference"] == 0.8
    assert report["diagonal_differences"]["shared_value_mismatches"] == 0


def test_ld_parity_cli_writes_report_and_fails_on_value_mismatch(tmp_path: Path) -> None:
    hailing_path = tmp_path / "hailing.parquet"
    gentropy_path = tmp_path / "gentropy.parquet"
    report_path = tmp_path / "parity.json"
    _write_ld(hailing_path, [("afr", "1_100_A_G", "1_200_C_T", 0.4)])
    _write_ld(gentropy_path, [("afr", "1_100_A_G", "1_200_C_T", -0.4)])

    result = runner.invoke(
        app,
        [
            "ld_parity",
            "--hailing",
            str(hailing_path),
            "--gentropy",
            str(gentropy_path),
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    assert report_path.exists()
    assert "Wrote LD parity report" in result.stdout
