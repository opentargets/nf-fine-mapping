"""Tests for collapsing a multi-ancestry locus set into a comparator arm.

The three properties asserted in ``meta_collapse``'s docstring are testable in
closed form, so most of these tests carry no numeric tolerance beyond floating
point:

* the collapsed LD diagonal is exactly 1, because ``sum_a u[a,i]^2 = 1``;
* two arms with identical standard errors give ``u = 1/sqrt(2)`` each, hence
  ``z_meta = sqrt(2) z`` and ``R_meta = R``;
* a single-arm input is a fixed point of the meta collapse.

``test_monte_carlo_matches_analytic_ld`` is the only statistical test, and it is
marked slow. It is the same check as ``docs/benchmarks/verify_meta_ld.py``,
reduced in size to stay inside a unit-test budget.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from collector import app
from collector.meta_collapse import MetaCollapseConfig, MetaCollapseError, run_meta_collapse

runner = CliRunner()

VARIANTS = ["1_100_A_G", "1_200_C_T", "1_300_G_A"]


def _write_metadata(path: Path, arms: list[tuple[str, str, float]]) -> None:
    path.write_text(
        "\n".join(json.dumps({"studyId": s, "ancestry": a, "sampleSize": n}) for s, a, n in arms) + "\n"
    )


def _write_locus_set(
    path: Path,
    locus_set_id: str,
    arms: dict[str, list[tuple[str, float, float]]],
) -> None:
    """One row per study; ``arms`` maps studyId to (variantId, beta, se) triples."""
    with duckdb.connect() as con:
        con.execute(
            """
            CREATE TABLE locus_set(
                fineMappingLocusSetId VARCHAR,
                studyLocusId VARCHAR,
                studyId VARCHAR,
                chromosome VARCHAR,
                locusStart INTEGER,
                locusEnd INTEGER,
                qualityControls VARCHAR[],
                locus STRUCT(
                    variantId VARCHAR,
                    pValueMantissa FLOAT,
                    pValueExponent INTEGER,
                    beta DOUBLE,
                    standardError DOUBLE
                )[]
            )
            """
        )
        for study_id, entries in arms.items():
            locus = [
                {
                    "variantId": variant,
                    "pValueMantissa": 1.0,
                    "pValueExponent": -8,
                    "beta": beta,
                    "standardError": se,
                }
                for variant, beta, se in entries
            ]
            con.execute(
                "INSERT INTO locus_set VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [locus_set_id, f"{study_id}_sl", study_id, "1", 100, 300, ["PASS"], locus],
            )
        con.execute(f"COPY locus_set TO '{path}' (FORMAT PARQUET)")


def _write_ld(path: Path, rows: list[tuple[str, str, str, float]]) -> None:
    with duckdb.connect() as con:
        con.execute(
            "CREATE TABLE ld(ancestry VARCHAR, variantIdI VARCHAR, variantIdJ VARCHAR, r DOUBLE)"
        )
        con.executemany("INSERT INTO ld VALUES (?, ?, ?, ?)", rows)
        con.execute(f"COPY ld TO '{path}' (FORMAT PARQUET)")


def _full_triangle(ancestry: str, variants: list[str], r: dict[tuple[str, str], float]) -> list[tuple[str, str, str, float]]:
    """Complete upper triangle including the unit diagonal, as the adapter emits."""
    rows = []
    for index_i, variant_i in enumerate(variants):
        for variant_j in variants[index_i:]:
            value = 1.0 if variant_i == variant_j else r[(variant_i, variant_j)]
            rows.append((ancestry, variant_i, variant_j, value))
    return rows


def _read_ld(path: Path) -> dict[tuple[str, str], float]:
    with duckdb.connect() as con:
        rows = con.execute(
            f"SELECT variantIdI, variantIdJ, r FROM read_parquet('{path}')"
        ).fetchall()
    return {(i, j): r for i, j, r in rows}


def _read_locus(path: Path) -> dict[str, tuple[float, float]]:
    with duckdb.connect() as con:
        rows = con.execute(
            f"SELECT entry.variantId, entry.beta, entry.standardError "
            f"FROM read_parquet('{path}') AS rows, UNNEST(rows.locus) AS exploded(entry)"
        ).fetchall()
    return {variant: (beta, se) for variant, beta, se in rows}


def _config(tmp_path: Path, **overrides) -> MetaCollapseConfig:
    defaults = dict(
        input_path=tmp_path / "locus_set.parquet",
        ld_path=tmp_path / "ld.parquet",
        study_metadata_path=tmp_path / "metadata.jsonl",
        output_path=tmp_path / "out" / "locus_set.parquet",
        ld_output_path=tmp_path / "out" / "ld.parquet",
        stats_output_path=tmp_path / "out" / "stats.json",
        run_id="RUN",
        fine_mapping_locus_set_id="LOCUS",
    )
    defaults.update(overrides)
    return MetaCollapseConfig(**defaults)


def _two_arm_fixture(tmp_path: Path, se_a: float = 0.10, se_b: float = 0.10) -> dict[tuple[str, str], float]:
    """Two arms over the same three variants, with a shared LD structure."""
    correlations = {
        (VARIANTS[0], VARIANTS[1]): 0.6,
        (VARIANTS[0], VARIANTS[2]): -0.2,
        (VARIANTS[1], VARIANTS[2]): 0.35,
    }
    _write_metadata(
        tmp_path / "metadata.jsonl",
        [("STUDY_A", "nfe", 100_000.0), ("STUDY_B", "afr", 20_000.0)],
    )
    _write_locus_set(
        tmp_path / "locus_set.parquet",
        "LOCUS",
        {
            "STUDY_A": [(variant, 0.20, se_a) for variant in VARIANTS],
            "STUDY_B": [(variant, 0.20, se_b) for variant in VARIANTS],
        },
    )
    _write_ld(
        tmp_path / "ld.parquet",
        _full_triangle("nfe", VARIANTS, correlations) + _full_triangle("afr", VARIANTS, correlations),
    )
    return correlations


def test_collapsed_diagonal_is_exactly_one_with_unequal_standard_errors(tmp_path: Path) -> None:
    """sum_a u[a,i]^2 = 1 by construction, whatever the per-arm precision."""
    correlations = {
        (VARIANTS[0], VARIANTS[1]): 0.6,
        (VARIANTS[0], VARIANTS[2]): -0.2,
        (VARIANTS[1], VARIANTS[2]): 0.35,
    }
    _write_metadata(
        tmp_path / "metadata.jsonl",
        [("STUDY_A", "nfe", 100_000.0), ("STUDY_B", "afr", 20_000.0), ("STUDY_C", "eas", 4_000.0)],
    )
    _write_locus_set(
        tmp_path / "locus_set.parquet",
        "LOCUS",
        {
            "STUDY_A": [(VARIANTS[0], 0.1, 0.011), (VARIANTS[1], 0.2, 0.017), (VARIANTS[2], -0.05, 0.023)],
            "STUDY_B": [(VARIANTS[0], 0.3, 0.041), (VARIANTS[1], 0.1, 0.052), (VARIANTS[2], 0.02, 0.037)],
            "STUDY_C": [(VARIANTS[0], -0.2, 0.093), (VARIANTS[1], 0.4, 0.081), (VARIANTS[2], 0.15, 0.077)],
        },
    )
    _write_ld(
        tmp_path / "ld.parquet",
        _full_triangle("nfe", VARIANTS, correlations)
        + _full_triangle("afr", VARIANTS, correlations)
        + _full_triangle("eas", VARIANTS, correlations),
    )

    stats = run_meta_collapse(_config(tmp_path))

    assert stats["nAncestryArms"] == 3
    assert stats["maxAbsDiagonalDeviation"] < 1e-12
    ld = _read_ld(tmp_path / "out" / "ld.parquet")
    for variant in VARIANTS:
        assert ld[(variant, variant)] == pytest.approx(1.0, abs=1e-12)


def test_two_identical_arms_give_closed_form_results(tmp_path: Path) -> None:
    """Equal standard errors give u = 1/sqrt(2), so R_meta = R and se halves in variance."""
    correlations = _two_arm_fixture(tmp_path, se_a=0.10, se_b=0.10)

    run_meta_collapse(_config(tmp_path))

    ld = _read_ld(tmp_path / "out" / "ld.parquet")
    for (variant_i, variant_j), expected in correlations.items():
        assert ld[(variant_i, variant_j)] == pytest.approx(expected, abs=1e-12)

    locus = _read_locus(tmp_path / "out" / "locus_set.parquet")
    for variant in VARIANTS:
        beta, se = locus[variant]
        # identical betas -> IVW mean is the same beta; two equal-precision arms
        # -> se shrinks by sqrt(2)
        assert beta == pytest.approx(0.20, abs=1e-12)
        assert se == pytest.approx(0.10 / math.sqrt(2.0), abs=1e-12)
        # and therefore z_meta = sqrt(2) * z
        assert (beta / se) == pytest.approx(math.sqrt(2.0) * (0.20 / 0.10), abs=1e-12)


def test_single_arm_input_is_a_fixed_point(tmp_path: Path) -> None:
    """With one arm, u = 1 and the collapse must return the input unchanged."""
    correlations = {
        (VARIANTS[0], VARIANTS[1]): 0.6,
        (VARIANTS[0], VARIANTS[2]): -0.2,
        (VARIANTS[1], VARIANTS[2]): 0.35,
    }
    _write_metadata(tmp_path / "metadata.jsonl", [("STUDY_A", "nfe", 100_000.0)])
    _write_locus_set(
        tmp_path / "locus_set.parquet",
        "LOCUS",
        {"STUDY_A": [(variant, 0.20, 0.05) for variant in VARIANTS]},
    )
    _write_ld(tmp_path / "ld.parquet", _full_triangle("nfe", VARIANTS, correlations))

    run_meta_collapse(_config(tmp_path))

    ld = _read_ld(tmp_path / "out" / "ld.parquet")
    for (variant_i, variant_j), expected in correlations.items():
        assert ld[(variant_i, variant_j)] == pytest.approx(expected, abs=1e-12)
    locus = _read_locus(tmp_path / "out" / "locus_set.parquet")
    for variant in VARIANTS:
        beta, se = locus[variant]
        assert beta == pytest.approx(0.20, abs=1e-12)
        assert se == pytest.approx(0.05, abs=1e-12)


def test_reversed_pair_orientation_does_not_change_the_result(tmp_path: Path) -> None:
    """Canonicalisation guards a silent halving when arms disagree on pair order."""
    correlations = _two_arm_fixture(tmp_path)
    baseline_config = _config(tmp_path)
    run_meta_collapse(baseline_config)
    baseline = _read_ld(tmp_path / "out" / "ld.parquet")

    reversed_rows: list[tuple[str, str, str, float]] = []
    for ancestry in ("nfe", "afr"):
        for index_i, variant_i in enumerate(VARIANTS):
            for variant_j in VARIANTS[index_i:]:
                value = 1.0 if variant_i == variant_j else correlations[(variant_i, variant_j)]
                # afr stores the pair the other way round
                if ancestry == "afr" and variant_i != variant_j:
                    reversed_rows.append((ancestry, variant_j, variant_i, value))
                else:
                    reversed_rows.append((ancestry, variant_i, variant_j, value))
    _write_ld(tmp_path / "ld_reversed.parquet", reversed_rows)

    run_meta_collapse(
        _config(
            tmp_path,
            ld_path=tmp_path / "ld_reversed.parquet",
            output_path=tmp_path / "rev" / "locus_set.parquet",
            ld_output_path=tmp_path / "rev" / "ld.parquet",
            stats_output_path=tmp_path / "rev" / "stats.json",
        )
    )
    assert _read_ld(tmp_path / "rev" / "ld.parquet") == baseline


def test_variant_present_in_one_arm_keeps_unit_diagonal(tmp_path: Path) -> None:
    """u = 1 for a single-arm variant; the term simply drops out elsewhere."""
    shared = VARIANTS[:2]
    correlations = {(shared[0], shared[1]): 0.6}
    _write_metadata(
        tmp_path / "metadata.jsonl",
        [("STUDY_A", "nfe", 100_000.0), ("STUDY_B", "afr", 20_000.0)],
    )
    _write_locus_set(
        tmp_path / "locus_set.parquet",
        "LOCUS",
        {
            # STUDY_A also carries VARIANTS[2]; STUDY_B does not
            "STUDY_A": [(variant, 0.2, 0.05) for variant in VARIANTS],
            "STUDY_B": [(variant, 0.2, 0.05) for variant in shared],
        },
    )
    nfe_rows = _full_triangle(
        "nfe",
        VARIANTS,
        {
            (VARIANTS[0], VARIANTS[1]): 0.6,
            (VARIANTS[0], VARIANTS[2]): -0.2,
            (VARIANTS[1], VARIANTS[2]): 0.35,
        },
    )
    _write_ld(tmp_path / "ld.parquet", nfe_rows + _full_triangle("afr", shared, correlations))

    stats = run_meta_collapse(_config(tmp_path))

    assert stats["nVariantsUnion"] == 3
    assert stats["nVariantsInAllArms"] == 2
    assert stats["nVariantsSingleArmOnly"] == 1
    assert stats["maxAbsDiagonalDeviation"] < 1e-12

    ld = _read_ld(tmp_path / "out" / "ld.parquet")
    # the single-arm variant keeps u = 1, so its own diagonal stays exactly 1
    assert ld[(VARIANTS[2], VARIANTS[2])] == pytest.approx(1.0, abs=1e-12)
    # its correlation to a shared variant carries only the nfe term, weighted by
    # that variant's nfe weight 1/sqrt(2) -- attenuation that is correct, being
    # the covariance of statistics computed on partially different samples
    assert ld[(VARIANTS[0], VARIANTS[2])] == pytest.approx(-0.2 / math.sqrt(2.0), abs=1e-12)


def test_missing_within_arm_pairs_are_refused(tmp_path: Path) -> None:
    """An absent pair would be summed as zero correlation, so refuse instead."""
    _two_arm_fixture(tmp_path)
    rows = [
        row
        for row in _full_triangle(
            "nfe",
            VARIANTS,
            {
                (VARIANTS[0], VARIANTS[1]): 0.6,
                (VARIANTS[0], VARIANTS[2]): -0.2,
                (VARIANTS[1], VARIANTS[2]): 0.35,
            },
        )
        if not (row[1] == VARIANTS[0] and row[2] == VARIANTS[1])
    ]
    _write_ld(tmp_path / "ld_missing.parquet", rows)
    _write_metadata(tmp_path / "metadata.jsonl", [("STUDY_A", "nfe", 100_000.0)])
    _write_locus_set(
        tmp_path / "locus_set.parquet",
        "LOCUS",
        {"STUDY_A": [(variant, 0.20, 0.05) for variant in VARIANTS]},
    )

    with pytest.raises(MetaCollapseError, match="absent although both variants are present"):
        run_meta_collapse(_config(tmp_path, ld_path=tmp_path / "ld_missing.parquet"))

    stats = json.loads((tmp_path / "out" / "stats.json").read_text())
    assert stats["nPairsMissingWithBothVariantsPresent"] == 1


def test_single_mode_filters_to_one_ancestry(tmp_path: Path) -> None:
    _two_arm_fixture(tmp_path)

    stats = run_meta_collapse(_config(tmp_path, mode="single", target_ancestry="nfe"))

    assert stats["mode"] == "single"
    assert stats["sampleSizeTotal"] == pytest.approx(100_000.0)
    with duckdb.connect() as con:
        ancestries = con.execute(
            f"SELECT DISTINCT ancestry FROM read_parquet('{tmp_path / 'out' / 'ld.parquet'}')"
        ).fetchall()
        studies = con.execute(
            f"SELECT DISTINCT studyId FROM read_parquet('{tmp_path / 'out' / 'locus_set.parquet'}')"
        ).fetchall()
    assert ancestries == [("nfe",)]
    assert studies == [("STUDY_A",)]


def test_single_mode_rejects_an_unknown_ancestry(tmp_path: Path) -> None:
    _two_arm_fixture(tmp_path)
    with pytest.raises(ValueError, match="not present in the study metadata"):
        run_meta_collapse(_config(tmp_path, mode="single", target_ancestry="amr"))


def test_cli_writes_all_three_outputs(tmp_path: Path) -> None:
    _two_arm_fixture(tmp_path)
    result = runner.invoke(
        app,
        [
            "meta_collapse",
            "--input", str(tmp_path / "locus_set.parquet"),
            "--multi_ancestry_pairwise_ld", str(tmp_path / "ld.parquet"),
            "--study_metadata", str(tmp_path / "metadata.jsonl"),
            "--output", str(tmp_path / "cli" / "locus_set.parquet"),
            "--ld_output", str(tmp_path / "cli" / "ld.parquet"),
            "--stats_output", str(tmp_path / "cli" / "stats.json"),
            "--run_id", "RUN",
            "--fine_mapping_locus_set_id", "LOCUS",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (tmp_path / "cli" / "locus_set.parquet").exists()
    assert (tmp_path / "cli" / "ld.parquet").exists()
    stats = json.loads((tmp_path / "cli" / "stats.json").read_text())
    assert stats["mode"] == "meta"
    assert stats["pairOrderCanonicalisationApplied"] is True
    assert stats["sampleSizeTotal"] == pytest.approx(120_000.0)


def test_cli_requires_target_ancestry_in_single_mode(tmp_path: Path) -> None:
    _two_arm_fixture(tmp_path)
    result = runner.invoke(
        app,
        [
            "meta_collapse",
            "--input", str(tmp_path / "locus_set.parquet"),
            "--multi_ancestry_pairwise_ld", str(tmp_path / "ld.parquet"),
            "--study_metadata", str(tmp_path / "metadata.jsonl"),
            "--output", str(tmp_path / "cli" / "locus_set.parquet"),
            "--ld_output", str(tmp_path / "cli" / "ld.parquet"),
            "--stats_output", str(tmp_path / "cli" / "stats.json"),
            "--run_id", "RUN",
            "--fine_mapping_locus_set_id", "LOCUS",
            "--mode", "single",
        ],
    )
    assert result.exit_code != 0


def test_output_is_deterministic(tmp_path: Path) -> None:
    _two_arm_fixture(tmp_path)
    run_meta_collapse(_config(tmp_path))
    first = (tmp_path / "out" / "ld.parquet").read_bytes()
    run_meta_collapse(
        _config(
            tmp_path,
            output_path=tmp_path / "again" / "locus_set.parquet",
            ld_output_path=tmp_path / "again" / "ld.parquet",
            stats_output_path=tmp_path / "again" / "stats.json",
        )
    )
    assert (tmp_path / "again" / "ld.parquet").read_bytes() == first


@pytest.mark.slow
def test_monte_carlo_matches_analytic_ld(tmp_path: Path) -> None:
    """Empirical covariance of the collapsed z-scores matches R_meta.

    The reduced form of docs/benchmarks/verify_meta_ld.py. Two arms with very
    different precision and a shared LD structure; the check is that the
    collapsed correlations sit within Monte Carlo noise of the analytic result.
    """
    numpy = pytest.importorskip("numpy")

    rng = numpy.random.default_rng(20260903)
    correlations = {
        (VARIANTS[0], VARIANTS[1]): 0.6,
        (VARIANTS[0], VARIANTS[2]): -0.2,
        (VARIANTS[1], VARIANTS[2]): 0.35,
    }
    matrix = numpy.array([[1.0, 0.6, -0.2], [0.6, 1.0, 0.35], [-0.2, 0.35, 1.0]])
    se_a, se_b = 0.011, 0.052

    _write_metadata(
        tmp_path / "metadata.jsonl",
        [("STUDY_A", "nfe", 100_000.0), ("STUDY_B", "afr", 20_000.0)],
    )
    _write_locus_set(
        tmp_path / "locus_set.parquet",
        "LOCUS",
        {
            "STUDY_A": [(variant, 0.0, se_a) for variant in VARIANTS],
            "STUDY_B": [(variant, 0.0, se_b) for variant in VARIANTS],
        },
    )
    _write_ld(
        tmp_path / "ld.parquet",
        _full_triangle("nfe", VARIANTS, correlations) + _full_triangle("afr", VARIANTS, correlations),
    )
    run_meta_collapse(_config(tmp_path))
    collapsed = _read_ld(tmp_path / "out" / "ld.parquet")

    precision = numpy.array([1.0 / se_a, 1.0 / se_b])
    weights = precision / numpy.sqrt((precision**2).sum())
    draws = 200_000
    chol = numpy.linalg.cholesky(matrix)
    z_meta = numpy.zeros((draws, 3))
    for weight in weights:
        z_meta += weight * (rng.standard_normal((draws, 3)) @ chol.T)
    empirical = numpy.corrcoef(z_meta, rowvar=False)

    monte_carlo_se = 1.0 / math.sqrt(draws)
    for index_i, variant_i in enumerate(VARIANTS):
        for index_j, variant_j in enumerate(VARIANTS):
            if index_i <= index_j:
                assert collapsed[(variant_i, variant_j)] == pytest.approx(
                    empirical[index_i, index_j], abs=6 * monte_carlo_se
                )
