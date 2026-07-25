"""Collect post-clumping StudyLocus datasets into fine-mapping locus classes."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from collector.schema import COLLECTED_LOCUS_SCHEMA


class CollectFineMappingLociConfig(BaseModel):
    """Path contract for the collect_finemapping_loci command."""

    model_config = ConfigDict(frozen=True)

    input_paths: tuple[Path, ...] = Field(min_length=1)
    full_output: Path
    partial_output: Path
    non_overlap_output: Path
    stats_output: Path


def _quote_sql_string(value: str) -> str:
    """Return a single-quoted SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _parquet_glob(path: Path) -> str:
    """Return the parquet reader path for a file or directory dataset."""
    if path.is_dir():
        return (path / "**" / "*.parquet").as_posix()
    return path.as_posix()


def _read_parquet_sql(input_paths: tuple[Path, ...]) -> str:
    """Return a DuckDB read_parquet expression for all StudyLocus inputs."""
    sources = [
        f"SELECT * FROM read_parquet({_quote_sql_string(_parquet_glob(path))}, union_by_name = true, hive_partitioning = true)"
        for path in input_paths
    ]
    return f"({' UNION ALL BY NAME '.join(sources)})"


def _prepare_output_paths(config: CollectFineMappingLociConfig) -> None:
    """Create output directories and remove stale output files."""
    for path in (config.full_output, config.partial_output, config.non_overlap_output, config.stats_output):
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.is_dir():
                raise IsADirectoryError(f"Output path is a directory: {path}")
            path.unlink()


def _collected_locus_projection_sql(
    quality_control_label: str,
    fine_mapping_locus_set_id_sql: str = "CAST(NULL AS VARCHAR)",
) -> str:
    """Return the reduced collected-locus select projection."""
    locus_type = COLLECTED_LOCUS_SCHEMA.fields[-1].sql_type()
    return f"""
    {fine_mapping_locus_set_id_sql} AS fineMappingLocusSetId,
    CAST(studyLocusId AS VARCHAR) AS studyLocusId,
    CAST(studyId AS VARCHAR) AS studyId,
    CAST(chromosome AS VARCHAR) AS chromosome,
    CAST(locusStart AS INTEGER) AS locusStart,
    CAST(locusEnd AS INTEGER) AS locusEnd,
    list_append(coalesce(qualityControls, []::VARCHAR[]), {_quote_sql_string(quality_control_label)}) AS qualityControls,
    CASE
        WHEN locus IS NULL THEN CAST(NULL AS {locus_type})
        ELSE CAST(
            list_transform(
                locus,
                locus_variant -> struct_pack(
                    variantId := locus_variant.variantId,
                    pValueMantissa := locus_variant.pValueMantissa,
                    pValueExponent := locus_variant.pValueExponent,
                    beta := locus_variant.beta,
                    standardError := locus_variant.standardError
                )
            ) AS {locus_type}
        )
    END AS locus
    """


def _copy_classified_loci(
    con: duckdb.DuckDBPyConnection,
    output: Path,
    overlap_class: str,
    quality_control_label: str,
) -> None:
    """Write one classified collected-locus parquet output."""
    con.execute(
        f"""
        COPY (
            SELECT
                {_collected_locus_projection_sql(quality_control_label)}
            FROM classified_loci
            WHERE overlapClass = {_quote_sql_string(overlap_class)}
            ORDER BY studyId, locusStart, locusEnd, studyLocusId
        ) TO {_quote_sql_string(output.as_posix())} (FORMAT PARQUET)
        """
    )


def _class_stats(con: duckdb.DuckDBPyConnection, overlap_class: str) -> dict[str, object]:
    """Return row-count statistics for one overlap class."""
    rows = con.execute(
        f"""
        SELECT studyId, count(*) AS rowCount
        FROM classified_loci
        WHERE overlapClass = {_quote_sql_string(overlap_class)}
        GROUP BY studyId
        ORDER BY studyId
        """
    ).fetchall()
    by_study_id = dict(rows)
    return {
        "rowCount": sum(by_study_id.values()),
        "studyIdsWithRows": list(by_study_id),
        "byStudyId": by_study_id,
    }


def _connected_components(
    nodes: set[str],
    undirected_edges: dict[str, set[str]],
) -> list[set[str]]:
    """Return deterministic connected components from an undirected graph."""
    components: list[set[str]] = []
    remaining = set(nodes)
    while remaining:
        start = min(remaining)
        component = {start}
        stack = [start]
        remaining.remove(start)
        while stack:
            node = stack.pop()
            for neighbor in sorted(undirected_edges.get(node, set())):
                if neighbor in remaining:
                    remaining.remove(neighbor)
                    component.add(neighbor)
                    stack.append(neighbor)
        components.append(component)
    return components


def _component_metrics(
    study_ids: list[str],
    eligible_loci: list[tuple[str, str]],
    overlap_edges: set[tuple[str, str]],
) -> dict[str, int]:
    """Return component-level metrics without generating candidate products in Python."""
    study_id_by_locus_id = dict(eligible_loci)
    eligible_locus_ids = set(study_id_by_locus_id)
    undirected_edges: dict[str, set[str]] = {study_locus_id: set() for study_locus_id in eligible_locus_ids}
    for left, right in overlap_edges:
        if left in eligible_locus_ids and right in eligible_locus_ids:
            undirected_edges[left].add(right)
            undirected_edges[right].add(left)

    components = _connected_components(eligible_locus_ids, undirected_edges)
    metrics = {
        "componentCount": len(components),
        "maxComponentLocusCount": max((len(component) for component in components), default=0),
        "maxComponentCandidateProductSize": 0,
    }

    for _study_locus_id, study_id in eligible_loci:
        if study_id not in study_ids:
            raise ValueError(f"Unexpected studyId in eligible loci: {study_id}")

    for component in components:
        component_loci_by_study_id: dict[str, list[str]] = {study_id: [] for study_id in study_ids}
        for study_locus_id in component:
            component_loci_by_study_id[study_id_by_locus_id[study_locus_id]].append(study_locus_id)

        if any(not component_loci_by_study_id[study_id] for study_id in study_ids):
            continue

        candidate_product_size = 1
        for study_id in study_ids:
            candidate_product_size *= len(component_loci_by_study_id[study_id])
        metrics["maxComponentCandidateProductSize"] = max(metrics["maxComponentCandidateProductSize"], candidate_product_size)

    return metrics


def _create_full_candidate_tables_sql(con: duckdb.DuckDBPyConnection, study_ids: list[str]) -> None:
    """Create full candidate sets and members with an exact N-study SQL join."""
    if not study_ids:
        con.execute(
            """
            CREATE TEMP TABLE full_candidate_sets (
                fineMappingLocusSetId VARCHAR,
                memberIds VARCHAR[]
            );
            CREATE TEMP TABLE full_candidate_members (
                fineMappingLocusSetId VARCHAR,
                studyLocusId VARCHAR
            );
            """
        )
        return

    aliases = [f"s{index}" for index in range(len(study_ids))]
    member_expression = "list_value(" + ", ".join(f"{alias}.studyLocusId" for alias in aliases) + ")"
    joins = [f"eligible_full_loci AS {aliases[0]}"]
    for index, (alias, study_id) in enumerate(zip(aliases[1:], study_ids[1:], strict=True), start=1):
        overlap_conditions = " AND ".join(
            f"EXISTS ("
            f"SELECT 1 FROM overlap_edges AS e{index}_{previous} "
            f"WHERE e{index}_{previous}.studyLocusId1 = {aliases[previous]}.studyLocusId "
            f"AND e{index}_{previous}.studyLocusId2 = {alias}.studyLocusId"
            f")"
            for previous in range(index)
        )
        joins.append(f"INNER JOIN eligible_full_loci AS {alias} ON {alias}.studyId = {_quote_sql_string(study_id)} AND {overlap_conditions}")

    first_study_filter = f"{aliases[0]}.studyId = {_quote_sql_string(study_ids[0])}"
    con.execute(
        f"""
        CREATE TEMP TABLE full_candidate_sets AS
        SELECT
            md5(array_to_string(list_sort({member_expression}), '|')) AS fineMappingLocusSetId,
            {member_expression} AS memberIds
        FROM {" ".join(joins)}
        WHERE {first_study_filter};

        CREATE TEMP TABLE full_candidate_members AS
        SELECT
            fineMappingLocusSetId,
            unnest(memberIds) AS studyLocusId
        FROM full_candidate_sets
        """
    )


def _copy_full_loci(con: duckdb.DuckDBPyConnection, output: Path) -> None:
    """Write full-overlap collected loci."""
    con.execute(
        f"""
        COPY (
            SELECT
                {_collected_locus_projection_sql("overlapping set", "full_candidate_members.fineMappingLocusSetId")}
            FROM full_candidate_members
            INNER JOIN classified_loci USING (studyLocusId)
            ORDER BY fineMappingLocusSetId, studyId, locusStart, locusEnd, studyLocusId
        ) TO {_quote_sql_string(output.as_posix())} (FORMAT PARQUET)
        """
    )


def _full_stats(
    con: duckdb.DuckDBPyConnection,
    component_metrics: dict[str, int],
) -> dict[str, object]:
    """Return row-count statistics for full candidate sets."""
    candidate_set_count_row = con.execute("SELECT count(*) FROM full_candidate_sets").fetchone()
    if candidate_set_count_row is None:
        raise RuntimeError("DuckDB returned no candidate-set count")
    candidate_set_count = candidate_set_count_row[0]
    if not candidate_set_count:
        return {
            "fineMappingSetCount": 0,
            "rowCount": 0,
            "isEmpty": True,
            "studyIdsWithRows": [],
            "byStudyId": {},
            **component_metrics,
        }

    rows = con.execute(
        """
        SELECT studyId, count(*) AS rowCount
        FROM full_candidate_members
        INNER JOIN classified_loci USING (studyLocusId)
        GROUP BY studyId
        ORDER BY studyId
        """
    ).fetchall()
    by_study_id = dict(rows)
    return {
        "fineMappingSetCount": candidate_set_count,
        "rowCount": sum(by_study_id.values()),
        "isEmpty": False,
        "studyIdsWithRows": list(by_study_id),
        "byStudyId": by_study_id,
        **component_metrics,
    }


def run_collect_finemapping_loci(config: CollectFineMappingLociConfig) -> None:
    """Validate inputs and write collect-loci output artifacts."""
    _prepare_output_paths(config)
    source = _read_parquet_sql(config.input_paths)

    with duckdb.connect() as con:
        con.execute(
            f"""
            CREATE TEMP TABLE input_loci AS
            SELECT *
            FROM {source}
            """
        )
        input_counts = con.execute(
            """
            SELECT
                count(*) AS inputRowCount,
                count(DISTINCT studyId) AS observedStudyCount
            FROM input_loci
            """
        ).fetchone()
        if input_counts is None:
            raise RuntimeError("DuckDB returned no input-locus counts")
        input_row_count, observed_study_count = input_counts
        study_ids = [
            row[0]
            for row in con.execute(
                """
                SELECT DISTINCT studyId
                FROM input_loci
                ORDER BY studyId
                """
            ).fetchall()
        ]

        if observed_study_count != len(config.input_paths):
            raise ValueError(
                "The number of input StudyLocus paths must match the number of distinct observed studyId values "
                f"({len(config.input_paths)} inputs, {observed_study_count} observed studyIds)."
            )

        input_files: dict[str, str] = {}
        for path in config.input_paths:
            path_source = _read_parquet_sql((path,))
            path_study_ids = [row[0] for row in con.execute(f"SELECT DISTINCT studyId FROM {path_source}").fetchall()]
            if len(path_study_ids) == 1:
                input_files[path_study_ids[0]] = path.as_posix()

        con.execute(
            """
            CREATE TEMP TABLE overlap_edges AS
            SELECT
                l1.studyLocusId AS studyLocusId1,
                l2.studyLocusId AS studyLocusId2,
                l2.studyId AS studyId2
            FROM input_loci AS l1
            INNER JOIN input_loci AS l2
                ON l1.studyId != l2.studyId
               AND l1.chromosome = l2.chromosome
               AND l1.locusStart <= l2.locusEnd
               AND l2.locusStart <= l1.locusEnd
            """
        )
        con.execute(
            f"""
            CREATE TEMP TABLE classified_loci AS
            WITH overlap_counts AS (
                SELECT
                    studyLocusId1 AS studyLocusId,
                    count(DISTINCT studyId2) AS nOverlappingStudies
                FROM overlap_edges
                GROUP BY studyLocusId1
            ),
            with_counts AS (
                SELECT
                    input_loci.*,
                    coalesce(overlap_counts.nOverlappingStudies, 0) AS nOverlappingStudies
                FROM input_loci
                LEFT JOIN overlap_counts USING (studyLocusId)
            )
            SELECT
                *,
                CASE
                    WHEN nOverlappingStudies = {len(config.input_paths) - 1} THEN 'full'
                    WHEN nOverlappingStudies > 0 THEN 'partial'
                    ELSE 'non_overlap'
                END AS overlapClass
            FROM with_counts
            """
        )

        overlap_edge_count_row = con.execute("SELECT count(*) FROM overlap_edges").fetchone()
        eligible_full_overlap_locus_count_row = con.execute("SELECT count(*) FROM classified_loci WHERE overlapClass = 'full'").fetchone()
        if overlap_edge_count_row is None or eligible_full_overlap_locus_count_row is None:
            raise RuntimeError("DuckDB returned no overlap counts")
        overlap_edge_count = overlap_edge_count_row[0]
        eligible_full_overlap_locus_count = eligible_full_overlap_locus_count_row[0]
        con.execute(
            """
            CREATE TEMP TABLE eligible_full_loci AS
            SELECT studyLocusId, studyId
            FROM classified_loci
            WHERE overlapClass = 'full'
            """
        )
        eligible_loci = con.execute(
            """
            SELECT studyLocusId, studyId
            FROM eligible_full_loci
            ORDER BY studyId, studyLocusId
            """
        ).fetchall()
        overlap_edges = {
            (study_locus_id_1, study_locus_id_2)
            for study_locus_id_1, study_locus_id_2 in con.execute(
                """
                SELECT studyLocusId1, studyLocusId2
                FROM overlap_edges
                """
            ).fetchall()
        }
        component_metrics = _component_metrics(study_ids, eligible_loci, overlap_edges)
        _create_full_candidate_tables_sql(con, study_ids)
        full_stats = _full_stats(con, component_metrics)
        partial_stats = _class_stats(con, "partial")
        non_overlap_stats = _class_stats(con, "non_overlap")

        if full_stats["fineMappingSetCount"]:
            _copy_full_loci(con, config.full_output)
        _copy_classified_loci(con, config.partial_output, "partial", "partial-overlapping studyLocus")
        _copy_classified_loci(con, config.non_overlap_output, "non_overlap", "non-overlapping studyLocus")

    stats = {
        "nInputStudies": len(config.input_paths),
        "studyIds": study_ids,
        "inputFiles": input_files,
        "inputLocusRowCount": input_row_count,
        "overlapEdgeCount": overlap_edge_count,
        "eligibleFullOverlapLocusCount": eligible_full_overlap_locus_count,
        "fullOverlap": full_stats,
        "partialOverlap": partial_stats,
        "nonOverlap": non_overlap_stats,
    }
    config.stats_output.write_text(json.dumps(stats, indent=2, sort_keys=True) + "\n", encoding="utf-8")
