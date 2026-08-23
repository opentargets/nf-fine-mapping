"""Tests for the Hailing Ducks LD adapter."""

import json
from importlib import import_module
from pathlib import Path

import duckdb
import pytest

from collector.hailing_ld import (
    HailingLdConfig,
    HailingLdReference,
    _materialize_reference,
    _prepare_request_files,
    run_hailing_ld,
)


def _write_locus_set(path: Path) -> None:
    with duckdb.connect() as con:
        con.execute(
            f"""
            COPY (
                SELECT
                    'set-1'::VARCHAR AS fineMappingLocusSetId,
                    'study-locus-1'::VARCHAR AS studyLocusId,
                    'study-1'::VARCHAR AS studyId,
                    '1'::VARCHAR AS chromosome,
                    100 AS locusStart,
                    200 AS locusEnd,
                    [
                        struct_pack(variantId := '1_100_A_G'),
                        struct_pack(variantId := '1_150_A_AT'),
                        struct_pack(variantId := '1_175_N_A'),
                        struct_pack(variantId := '1_180_G_C'),
                        struct_pack(variantId := '1_200_C_T')
                    ] AS locus
            ) TO '{path}' (FORMAT PARQUET)
            """
        )


def _write_metadata(path: Path) -> None:
    path.write_text('{"studyId":"study-1","ancestry":"eas","sampleSize":1000}\n')


def _write_two_study_locus_set(path: Path) -> None:
    with duckdb.connect() as con:
        con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    ('study-locus-1', 'study-1', '1', 100, 200, [struct_pack(variantId := '1_100_A_G')]),
                    ('study-locus-2', 'study-2', '1', 300, 400, [struct_pack(variantId := '1_300_C_T')])
                ) AS loci(studyLocusId, studyId, chromosome, locusStart, locusEnd, locus)
            ) TO '{path}' (FORMAT PARQUET)
            """
        )


def _write_multi_ancestry_locus_set(path: Path) -> None:
    with duckdb.connect() as con:
        con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    (
                        'set-1',
                        'study-locus-1',
                        'study-1',
                        '1',
                        100,
                        200,
                        [
                            struct_pack(variantId := '1_100_A_G'),
                            struct_pack(variantId := '1_150_A_AT')
                        ]
                    ),
                    (
                        'set-1',
                        'study-locus-2',
                        'study-2',
                        '1',
                        100,
                        200,
                        [
                            struct_pack(variantId := '1_150_A_AT'),
                            struct_pack(variantId := '1_180_G_C'),
                            struct_pack(variantId := '1_200_C_T')
                        ]
                    )
                ) AS loci(
                    fineMappingLocusSetId,
                    studyLocusId,
                    studyId,
                    chromosome,
                    locusStart,
                    locusEnd,
                    locus
                )
            ) TO '{path}' (FORMAT PARQUET)
            """
        )


def _write_multi_ancestry_disjoint_locus_set(path: Path) -> None:
    with duckdb.connect() as con:
        con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    (
                        'set-1',
                        'study-locus-1',
                        'study-1',
                        '1',
                        100,
                        200,
                        [
                            struct_pack(variantId := '1_100_A_G'),
                            struct_pack(variantId := '1_150_A_AT')
                        ]
                    ),
                    (
                        'set-1',
                        'study-locus-2',
                        'study-2',
                        '1',
                        100,
                        200,
                        [
                            struct_pack(variantId := '1_180_G_C'),
                            struct_pack(variantId := '1_200_C_T')
                        ]
                    )
                ) AS loci(
                    fineMappingLocusSetId,
                    studyLocusId,
                    studyId,
                    chromosome,
                    locusStart,
                    locusEnd,
                    locus
                )
            ) TO '{path}' (FORMAT PARQUET)
            """
        )


def test_prepare_requests_uses_native_chr_ids_and_preserves_original_ids(tmp_path: Path) -> None:
    input_path = tmp_path / "locus_set.parquet"
    requests_path = tmp_path / "requests.parquet"
    mapping_path = tmp_path / "mapping.parquet"
    _write_locus_set(input_path)

    with duckdb.connect() as con:
        _prepare_request_files(con, input_path, requests_path, mapping_path, "chr")
        request = con.execute(f"SELECT * FROM read_parquet('{requests_path}')").fetchone()
        mapping = con.execute(f"SELECT * FROM read_parquet('{mapping_path}') ORDER BY original_variant_id").fetchall()

    assert request == (
        "set-1",
        "chr1:100-200",
        ["chr1_100_A_G", "chr1_150_A_AT", "chr1_175_N_A", "chr1_180_G_C", "chr1_200_C_T"],
    )
    assert mapping == [
        ("set-1", "chr1_100_A_G", "1_100_A_G"),
        ("set-1", "chr1_150_A_AT", "1_150_A_AT"),
        ("set-1", "chr1_175_N_A", "1_175_N_A"),
        ("set-1", "chr1_180_G_C", "1_180_G_C"),
        ("set-1", "chr1_200_C_T", "1_200_C_T"),
    ]


def test_prepare_requests_keeps_study_locus_ids_when_no_locus_set_id_is_present(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "locus_set.parquet"
    requests_path = tmp_path / "requests.parquet"
    mapping_path = tmp_path / "mapping.parquet"
    _write_two_study_locus_set(input_path)

    with duckdb.connect() as con:
        _prepare_request_files(con, input_path, requests_path, mapping_path, "chr")
        request_ids = con.execute(f"SELECT locus_id FROM read_parquet('{requests_path}') ORDER BY locus_id").fetchall()
        mapped_ids = con.execute(f"SELECT original_variant_id FROM read_parquet('{mapping_path}') ORDER BY original_variant_id").fetchall()

    assert request_ids == [("study-locus-1",), ("study-locus-2",)]
    assert mapped_ids == [("1_100_A_G",), ("1_300_C_T",)]


def test_prepare_requests_uses_full_locus_set_union_for_each_requested_ancestry(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "locus_set.parquet"
    requests_path = tmp_path / "requests.parquet"
    mapping_path = tmp_path / "mapping.parquet"
    _write_multi_ancestry_locus_set(input_path)

    with duckdb.connect() as con:
        _prepare_request_files(con, input_path, requests_path, mapping_path, "chr")
        request = con.execute(f"SELECT locus_id, locus, variant_ids FROM read_parquet('{requests_path}')").fetchone()
        mapping = con.execute(
            f"""
            SELECT locus_id, native_variant_id, original_variant_id
            FROM read_parquet('{mapping_path}')
            ORDER BY native_variant_id
            """
        ).fetchall()

    assert request == (
        "set-1",
        "chr1:100-200",
        ["chr1_100_A_G", "chr1_150_A_AT", "chr1_180_G_C", "chr1_200_C_T"],
    )
    assert mapping == [
        ("set-1", "chr1_100_A_G", "1_100_A_G"),
        ("set-1", "chr1_150_A_AT", "1_150_A_AT"),
        ("set-1", "chr1_180_G_C", "1_180_G_C"),
        ("set-1", "chr1_200_C_T", "1_200_C_T"),
    ]


def test_materialize_passes_cache_size_as_named_hailing_ducks_argument(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    command: list[str] = []

    def fake_run(arguments, *, check):
        assert check is True
        command.extend(arguments)

    hailing_ld_module = import_module("collector.hailing_ld")
    monkeypatch.setattr(hailing_ld_module.subprocess, "run", fake_run)

    with duckdb.connect() as con:
        _materialize_reference(
            con,
            HailingLdReference(ancestry="nfe", ht_path="index.ht", bm_path="matrix.bm"),
            tmp_path / "requests.parquet",
            tmp_path / "ld.parquet",
            tmp_path / "status.parquet",
            17,
        )

    assert command[:2] == ["/usr/local/bin/duckdb", "-c"]
    assert "max_cached_blocks := 17" in command[2]


def test_run_hailing_ld_adapts_pairs_adds_diagonals_and_writes_stats(tmp_path: Path) -> None:
    input_path = tmp_path / "locus_set.parquet"
    metadata_path = tmp_path / "metadata.jsonl"
    output_path = tmp_path / "output" / "multi_ancestry_pairwise_ld.parquet"
    stats_path = tmp_path / "output" / "stats.jsonl"
    _write_locus_set(input_path)
    _write_metadata(metadata_path)

    references = (
        HailingLdReference(ancestry="eas", ht_path="s3://bucket/eas.ht", bm_path="s3://bucket/eas.bm"),
        HailingLdReference(ancestry="nfe", ht_path="local/nfe.ht", bm_path="local/nfe.bm"),
    )

    def fake_materialize(con, reference, _requests_path, ld_path, status_path, _max_cached_blocks):
        if reference.ancestry == "eas":
            con.execute(
                f"""
                COPY (
                    SELECT * FROM (VALUES
                        ('set-1', 10::BIGINT, 15::BIGINT, 0.10::DOUBLE),
                        ('set-1', 10::BIGINT, 20::BIGINT, -0.25::DOUBLE),
                        ('set-1', 10::BIGINT, 20::BIGINT, -0.25::DOUBLE),
                        ('set-1', 15::BIGINT, 20::BIGINT, -0.20::DOUBLE)
                    ) AS t(locus_id, idx_i, idx_j, r)
                ) TO '{ld_path}' (FORMAT PARQUET)
                """
            )
            con.execute(
                f"""
                COPY (
                    SELECT * FROM (VALUES
                        ('set-1', 'chr1_100_A_G', 10::BIGINT, 1, 0),
                        ('set-1', 'chr1_150_A_AT', 15::BIGINT, 1, 0),
                        ('set-1', 'chr1_175_N_A', NULL::BIGINT, NULL::INTEGER, 5),
                        ('set-1', 'chr1_180_G_C', NULL::BIGINT, NULL::INTEGER, 2),
                        ('set-1', 'chr1_200_C_T', 20::BIGINT, -1, 1)
                    ) AS t(locus_id, requested_variant_id, idx, allele_order, status_code)
                ) TO '{status_path}' (FORMAT PARQUET)
                """
            )
            return
        con.execute(
            f"""
            COPY (
                SELECT CAST(NULL AS VARCHAR) AS locus_id,
                       CAST(NULL AS BIGINT) AS idx_i,
                       CAST(NULL AS BIGINT) AS idx_j,
                       CAST(NULL AS DOUBLE) AS r
                WHERE false
            ) TO '{ld_path}' (FORMAT PARQUET)
            """
        )
        con.execute(
            f"""
            COPY (
                SELECT CAST(NULL AS VARCHAR) AS locus_id,
                       CAST(NULL AS VARCHAR) AS requested_variant_id,
                       CAST(NULL AS BIGINT) AS idx,
                       CAST(NULL AS INTEGER) AS allele_order,
                       CAST(NULL AS INTEGER) AS status_code
                WHERE false
            ) TO '{status_path}' (FORMAT PARQUET)
            """
        )

    run_hailing_ld(
        HailingLdConfig(
            input_path=input_path,
            study_metadata_path=metadata_path,
            output_path=output_path,
            stats_output=stats_path,
            references=references,
        ),
        materialize=fake_materialize,
    )

    with duckdb.connect() as con:
        rows = con.execute(f"SELECT * FROM read_parquet('{output_path}') ORDER BY ancestry, variantIdI, variantIdJ").fetchall()

    assert rows == [
        ("eas", "1_100_A_G", "1_100_A_G", 1.0),
        ("eas", "1_100_A_G", "1_150_A_AT", 0.1),
        ("eas", "1_100_A_G", "1_200_C_T", -0.25),
        ("eas", "1_150_A_AT", "1_150_A_AT", 1.0),
        ("eas", "1_150_A_AT", "1_200_C_T", -0.2),
        ("eas", "1_200_C_T", "1_200_C_T", 1.0),
    ]
    statistics = [json.loads(line) for line in stats_path.read_text().splitlines()]
    count_fields = {
        "ancestry",
        "n_requested_variants",
        "n_resolved_variants",
        "n_unresolved_variants",
        "n_unsupported_variants",
        "n_ld_pairs",
    }
    assert [{key: value for key, value in record.items() if key in count_fields} for record in statistics] == [
        {
            "ancestry": "eas",
            "n_requested_variants": 5,
            "n_resolved_variants": 3,
            "n_unresolved_variants": 1,
            "n_unsupported_variants": 1,
            "n_ld_pairs": 6,
        },
        {
            "ancestry": "nfe",
            "n_requested_variants": 0,
            "n_resolved_variants": 0,
            "n_unresolved_variants": 0,
            "n_unsupported_variants": 0,
            "n_ld_pairs": 0,
        },
    ]
    assert all(record["native_materialize_seconds"] >= 0 for record in statistics)
    assert all(record["adapter_seconds"] >= 0 for record in statistics)
    assert all(record["combined_output_seconds"] >= 0 for record in statistics)
    assert all(record["peak_child_rss_kib"] >= 0 for record in statistics)


def test_run_hailing_ld_requests_full_disjoint_locus_union_for_each_ancestry(tmp_path: Path) -> None:
    input_path = tmp_path / "locus_set.parquet"
    metadata_path = tmp_path / "metadata.jsonl"
    output_path = tmp_path / "output" / "multi_ancestry_pairwise_ld.parquet"
    stats_path = tmp_path / "output" / "stats.jsonl"
    _write_multi_ancestry_disjoint_locus_set(input_path)
    metadata_path.write_text('{"studyId":"study-1","ancestry":"eas","sampleSize":1000}\n{"studyId":"study-2","ancestry":"nfe","sampleSize":900}\n')

    references = (
        HailingLdReference(ancestry="eas", ht_path="eas.ht", bm_path="eas.bm"),
        HailingLdReference(ancestry="nfe", ht_path="nfe.ht", bm_path="nfe.bm"),
    )
    seen_requests: dict[str, list[tuple[str, str, list[str]]]] = {}

    def fake_materialize(con, reference, requests_path, ld_path, status_path, _max_cached_blocks):
        seen_requests[reference.ancestry] = con.execute(
            f"""
            SELECT locus_id, locus, variant_ids
            FROM read_parquet('{requests_path}')
            ORDER BY locus_id
            """
        ).fetchall()
        if reference.ancestry == "eas":
            con.execute(
                f"""
                COPY (
                    SELECT * FROM (VALUES
                        ('set-1', 10::BIGINT, 15::BIGINT, 0.25::DOUBLE)
                    ) AS t(locus_id, idx_i, idx_j, r)
                ) TO '{ld_path}' (FORMAT PARQUET)
                """
            )
            con.execute(
                f"""
                COPY (
                    SELECT * FROM (VALUES
                        ('set-1', 'chr1_100_A_G', 10::BIGINT, 1, 0),
                        ('set-1', 'chr1_150_A_AT', 15::BIGINT, 1, 0),
                        ('set-1', 'chr1_180_G_C', NULL::BIGINT, NULL::INTEGER, 2),
                        ('set-1', 'chr1_200_C_T', NULL::BIGINT, NULL::INTEGER, 2)
                    ) AS t(locus_id, requested_variant_id, idx, allele_order, status_code)
                ) TO '{status_path}' (FORMAT PARQUET)
                """
            )
            return
        con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    ('set-1', 18::BIGINT, 20::BIGINT, -0.40::DOUBLE)
                ) AS t(locus_id, idx_i, idx_j, r)
            ) TO '{ld_path}' (FORMAT PARQUET)
            """
        )
        con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    ('set-1', 'chr1_100_A_G', NULL::BIGINT, NULL::INTEGER, 2),
                    ('set-1', 'chr1_150_A_AT', NULL::BIGINT, NULL::INTEGER, 2),
                    ('set-1', 'chr1_180_G_C', 18::BIGINT, 1, 0),
                    ('set-1', 'chr1_200_C_T', 20::BIGINT, -1, 1)
                ) AS t(locus_id, requested_variant_id, idx, allele_order, status_code)
            ) TO '{status_path}' (FORMAT PARQUET)
            """
        )

    run_hailing_ld(
        HailingLdConfig(
            input_path=input_path,
            study_metadata_path=metadata_path,
            output_path=output_path,
            stats_output=stats_path,
            references=references,
        ),
        materialize=fake_materialize,
    )

    expected_request = [
        (
            "set-1",
            "chr1:100-200",
            ["chr1_100_A_G", "chr1_150_A_AT", "chr1_180_G_C", "chr1_200_C_T"],
        )
    ]
    assert seen_requests == {"eas": expected_request, "nfe": expected_request}

    with duckdb.connect() as con:
        rows = con.execute(f"SELECT * FROM read_parquet('{output_path}') ORDER BY ancestry, variantIdI, variantIdJ").fetchall()

    assert rows == [
        ("eas", "1_100_A_G", "1_100_A_G", 1.0),
        ("eas", "1_100_A_G", "1_150_A_AT", 0.25),
        ("eas", "1_150_A_AT", "1_150_A_AT", 1.0),
        ("nfe", "1_180_G_C", "1_180_G_C", 1.0),
        ("nfe", "1_180_G_C", "1_200_C_T", -0.4),
        ("nfe", "1_200_C_T", "1_200_C_T", 1.0),
    ]


def test_run_hailing_ld_rejects_conflicting_duplicate_pairs(tmp_path: Path) -> None:
    input_path = tmp_path / "locus_set.parquet"
    metadata_path = tmp_path / "metadata.jsonl"
    _write_locus_set(input_path)
    _write_metadata(metadata_path)
    config = HailingLdConfig(
        input_path=input_path,
        study_metadata_path=metadata_path,
        output_path=tmp_path / "ld.parquet",
        stats_output=tmp_path / "stats.jsonl",
        references=(HailingLdReference(ancestry="eas", ht_path="eas.ht", bm_path="eas.bm"),),
    )

    def conflicting_materialize(con, _reference, _requests_path, ld_path, status_path, _max_cached_blocks):
        con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    ('set-1', 10::BIGINT, 20::BIGINT, 0.25::DOUBLE),
                    ('set-1', 10::BIGINT, 20::BIGINT, 0.50::DOUBLE)
                ) AS t(locus_id, idx_i, idx_j, r)
            ) TO '{ld_path}' (FORMAT PARQUET)
            """
        )
        con.execute(
            f"""
            COPY (
                SELECT * FROM (VALUES
                    ('set-1', 'chr1_100_A_G', 10::BIGINT, 1, 0),
                    ('set-1', 'chr1_200_C_T', 20::BIGINT, -1, 1)
                ) AS t(locus_id, requested_variant_id, idx, allele_order, status_code)
            ) TO '{status_path}' (FORMAT PARQUET)
            """
        )

    with pytest.raises(RuntimeError, match="Conflicting LD values"):
        run_hailing_ld(config, materialize=conflicting_materialize)
