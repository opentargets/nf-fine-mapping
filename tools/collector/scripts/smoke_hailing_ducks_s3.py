"""Opt-in real Pan-UKBB S3 smoke test for Hailing Ducks LD extraction."""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from pathlib import Path

import duckdb


def _quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _build_requests(duckdb_cli: Path, ht_path: str, temporary_path: Path) -> tuple[Path, Path]:
    direct_requests = temporary_path / "direct.requests.parquet"
    flipped_requests = temporary_path / "flipped.requests.parquet"
    sql = f"""
        SET s3_region = 'us-east-1';
        CREATE TEMP TABLE sample_pool AS
        SELECT
            locus.contig AS contig,
            locus.position AS position,
            alleles,
            idx
        FROM hail_scan_table({_quote_sql_string(ht_path)})
        WHERE alleles IS NOT NULL
          AND len(alleles) = 2
          AND regexp_full_match(alleles[1], '[ACGT]+')
          AND regexp_full_match(alleles[2], '[ACGT]+')
        LIMIT 512;

        CREATE TEMP TABLE anchor AS
        SELECT *
        FROM sample_pool
        WHERE len(alleles[1]) > 1 OR len(alleles[2]) > 1
        LIMIT 1;

        CREATE TEMP TABLE sample_rows AS
        SELECT
            candidate.contig AS contig,
            candidate.position AS position,
            candidate.alleles AS alleles,
            candidate.idx AS idx
        FROM sample_pool AS candidate
        CROSS JOIN anchor
        WHERE candidate.contig = anchor.contig
          AND candidate.position BETWEEN anchor.position - 100000 AND anchor.position + 100000
        ORDER BY abs(candidate.position - anchor.position), candidate.idx
        LIMIT 12;

        COPY (
            SELECT
                'smoke_locus' AS locus_id,
                any_value(contig) || ':' || min(position) || '-' || max(position) AS locus,
                list(contig || '_' || position || '_' || alleles[1] || '_' || alleles[2] ORDER BY position, idx) AS variant_ids
            FROM sample_rows
        ) TO {_quote_sql_string(direct_requests.as_posix())} (FORMAT PARQUET);

        COPY (
            SELECT
                'smoke_locus' AS locus_id,
                any_value(sample_rows.contig) || ':' || min(sample_rows.position) || '-' || max(sample_rows.position) AS locus,
                list(
                    sample_rows.contig || '_' || sample_rows.position || '_' ||
                    CASE WHEN sample_rows.idx = anchor.idx THEN sample_rows.alleles[2] ELSE sample_rows.alleles[1] END || '_' ||
                    CASE WHEN sample_rows.idx = anchor.idx THEN sample_rows.alleles[1] ELSE sample_rows.alleles[2] END
                    ORDER BY sample_rows.position, sample_rows.idx
                ) AS variant_ids
            FROM sample_rows
            CROSS JOIN anchor
        ) TO {_quote_sql_string(flipped_requests.as_posix())} (FORMAT PARQUET);
    """
    subprocess.run([duckdb_cli.as_posix(), "-c", sql], check=True)  # noqa: S603
    return direct_requests, flipped_requests


def _materialize(
    duckdb_cli: Path,
    *,
    ht_path: str,
    bm_path: str,
    requests_path: Path,
    ld_path: Path,
    status_path: Path,
) -> None:
    sql = f"""
        SET s3_region = 'us-east-1';
        SELECT * FROM hail_ld_materialize(
            {_quote_sql_string(ht_path)},
            {_quote_sql_string(bm_path)},
            {_quote_sql_string(requests_path.as_posix())},
            {_quote_sql_string(ld_path.as_posix())},
            {_quote_sql_string(status_path.as_posix())},
            max_cached_blocks := 8
        );
    """
    subprocess.run([duckdb_cli.as_posix(), "-c", sql], check=True)  # noqa: S603


def _validate(direct_ld: Path, direct_status: Path, flipped_ld: Path, flipped_status: Path) -> None:
    with duckdb.connect() as con:
        direct_anchor = con.execute(
            """
            SELECT idx
            FROM read_parquet(?)
            WHERE status_code = 0
              AND (
                    length(split_part(requested_variant_id, '_', 3)) > 1
                 OR length(split_part(requested_variant_id, '_', 4)) > 1
              )
            LIMIT 1
            """,
            [direct_status.as_posix()],
        ).fetchone()
        if direct_anchor is None:
            raise RuntimeError("Real S3 smoke test did not resolve an indel exactly")
        anchor_idx = direct_anchor[0]
        flipped_anchor = con.execute(
            "SELECT count(*) FROM read_parquet(?) WHERE idx = ? AND status_code = 1 AND allele_order = -1",
            [flipped_status.as_posix(), anchor_idx],
        ).fetchone()
        if flipped_anchor is None or flipped_anchor[0] != 1:
            raise RuntimeError("Real S3 smoke test did not resolve the flipped indel")

        comparison = con.execute(
            """
            SELECT
                count(*) FILTER (WHERE d.idx_i = ? OR d.idx_j = ?) AS anchor_pairs,
                count(*) FILTER (
                    WHERE (d.idx_i = ? OR d.idx_j = ?)
                      AND abs(d.r + f.r) > 1e-8
                ) AS flipped_sign_mismatches,
                count(*) FILTER (
                    WHERE d.idx_i != ? AND d.idx_j != ?
                      AND abs(d.r - f.r) > 1e-8
                ) AS unchanged_sign_mismatches
            FROM read_parquet(?) AS d
            JOIN read_parquet(?) AS f USING (locus_id, idx_i, idx_j)
            """,
            [
                anchor_idx,
                anchor_idx,
                anchor_idx,
                anchor_idx,
                anchor_idx,
                anchor_idx,
                direct_ld.as_posix(),
                flipped_ld.as_posix(),
            ],
        ).fetchone()
        if comparison is None or comparison[0] == 0:
            raise RuntimeError("Real S3 smoke test found no resolved BM pair involving the indel")
        if comparison[1] or comparison[2]:
            raise RuntimeError(
                f"Signed LD validation failed: flipped={comparison[1] if comparison else 'unknown'}, "
                f"unchanged={comparison[2] if comparison else 'unknown'}"
            )


def main() -> None:
    """Run the opt-in remote HT, BM, indel, and signed-LD smoke test."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--ht-path", required=True)
    parser.add_argument("--bm-path", required=True)
    parser.add_argument("--duckdb-cli", type=Path, default=Path("/usr/local/bin/duckdb"))
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="hailing-ducks-s3-smoke-") as temporary_directory:
        temporary_path = Path(temporary_directory)
        direct_requests, flipped_requests = _build_requests(args.duckdb_cli, args.ht_path, temporary_path)
        direct_ld = temporary_path / "direct.ld.parquet"
        direct_status = temporary_path / "direct.status.parquet"
        flipped_ld = temporary_path / "flipped.ld.parquet"
        flipped_status = temporary_path / "flipped.status.parquet"
        _materialize(
            args.duckdb_cli,
            ht_path=args.ht_path,
            bm_path=args.bm_path,
            requests_path=direct_requests,
            ld_path=direct_ld,
            status_path=direct_status,
        )
        _materialize(
            args.duckdb_cli,
            ht_path=args.ht_path,
            bm_path=args.bm_path,
            requests_path=flipped_requests,
            ld_path=flipped_ld,
            status_path=flipped_status,
        )
        _validate(direct_ld, direct_status, flipped_ld, flipped_status)
    print("Hailing Ducks real-S3 smoke test passed: HT, BM, indel, and signed LD are valid.")


if __name__ == "__main__":
    main()
