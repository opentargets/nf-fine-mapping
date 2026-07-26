"""Tests for LD pair coverage validation."""

import json
from pathlib import Path

from typer.testing import CliRunner

from collector import app

runner = CliRunner()


def test_ld_pair_stats_reports_one_run_status_for_multiple_zero_ancestries(
    tmp_path: Path,
) -> None:
    """Any zero ancestry count emits one run-level status record."""
    stats_path = tmp_path / "stats.jsonl"
    stats_path.write_text(
        '{"ancestry":"afr","n_ld_pairs":0}\n'
        '{"ancestry":"nfe","n_ld_pairs":0}\n'
        '{"ancestry":"eas","n_ld_pairs":4}\n'
    )

    result = runner.invoke(
        app,
        [
            "check_ld_pair_stats",
            "--run_id",
            "run-123",
            "--path",
            str(stats_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "runId": "run-123",
        "path": str(stats_path),
        "validationStage": "LD_ANNOTATION",
        "reason": "EMPTY_LD_PAIRS",
    }


def test_ld_pair_stats_emits_no_status_when_all_ancestries_have_pairs(
    tmp_path: Path,
) -> None:
    """All positive ancestry counts produce no status output."""
    stats_path = tmp_path / "stats.jsonl"
    stats_path.write_text(
        '{"ancestry":"afr","n_ld_pairs":1}\n'
        '{"ancestry":"nfe","n_ld_pairs":4}\n'
    )

    result = runner.invoke(
        app,
        [
            "check_ld_pair_stats",
            "--run_id",
            "run-123",
            "--path",
            str(stats_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.stdout == ""
